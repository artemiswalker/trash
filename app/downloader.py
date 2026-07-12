from __future__ import annotations

import asyncio
import logging
import shutil
from dataclasses import dataclass, field
from pathlib import Path
from typing import AsyncIterator, Callable, Optional

from .config import settings
from .rate_limiter import Backoff, looks_rate_limited

log = logging.getLogger(__name__)


class GalleryDLNotFound(RuntimeError):
    pass


@dataclass
class DownloadResult:
    ok: bool
    files: list[Path] = field(default_factory=list)
    error_tail: str = ""
    attempts: int = 0


def _build_cmd(
    urls: list[str],
    dest_dir: Path,
    archive_file: Path,
    extra_args: Optional[list[str]] = None,
    links_file: Optional[Path] = None,
) -> list[str]:
    cmd = [
        "gallery-dl",
        "--no-mtime",
        "-D", str(dest_dir),
        "--download-archive", str(archive_file),
        "--sleep", f"{settings.gdl_sleep_min}-{settings.gdl_sleep_max}",
        "--sleep-request", settings.gdl_sleep_request,
        "--limit-rate", settings.gdl_limit_rate,
        "--retries", str(settings.gdl_retries),
        "-v",
    ]
    if extra_args:
        cmd.extend(extra_args)
    if links_file:
        cmd.extend(["-i", str(links_file)])
    else:
        cmd.extend(urls)
    return cmd


async def _stream_run(cmd: list[str]) -> tuple[int, str, Callable[[], int]]:
    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )

    count = 0
    stderr_chunks: list[str] = []

    async def read_stdout():
        nonlocal count
        assert proc.stdout is not None
        async for line in proc.stdout:
            text = line.decode(errors="replace")
            # gallery-dl -v prints one line per file operation
            if text.strip():
                count += 1

    async def read_stderr():
        assert proc.stderr is not None
        async for line in proc.stderr:
            stderr_chunks.append(line.decode(errors="replace"))
            if len(stderr_chunks) > 200:
                stderr_chunks.pop(0)

    await asyncio.gather(read_stdout(), read_stderr())
    returncode = await proc.wait()
    return returncode, "".join(stderr_chunks), (lambda: count)


async def run_with_progress(
    url: str,
    dest_dir: Path,
    archive_file: Path,
    on_progress: Optional[Callable[[int, Optional[str]], None]] = None,
    extra_args: Optional[list[str]] = None,
) -> DownloadResult:

    if shutil.which("gallery-dl") is None:
        raise GalleryDLNotFound(
            "gallery-dl not found on PATH. Install with: "
            "pip install gallery-dl --break-system-packages"
        )

    import json
    try:
        urls = json.loads(url)
        if not isinstance(urls, list):
            urls = [url]
    except Exception:
        urls = [url]

    links_file = dest_dir.parent / f"{dest_dir.name}_links.txt"
    try:
        links_file.write_text("\n".join(urls), encoding="utf-8")
    except Exception as e:
        log.exception("Failed to write links file: %s", links_file)
        return DownloadResult(ok=False, error_tail=f"Failed to create links file: {e}")

    dest_dir.mkdir(parents=True, exist_ok=True)
    backoff = Backoff(
        base_s=settings.gdl_backoff_base_s,
        multiplier=settings.gdl_backoff_multiplier,
        max_attempts=settings.gdl_max_run_retries,
    )

    attempts = 0
    last_stderr = ""
    try:
        while True:
            attempts += 1
            cmd = _build_cmd(urls, dest_dir, archive_file, extra_args, links_file)
            log.info("gallery-dl run attempt=%s url_count=%s args=%s", attempts, len(urls), extra_args)

        proc = await asyncio.create_subprocess_exec(
            *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
        )
        count = 0
        stderr_buf: list[str] = []

        async def pump_stdout():
            nonlocal count
            assert proc.stdout is not None
            async for line in proc.stdout:
                count += 1
                text = line.decode(errors="replace").strip()
                filename = None
                if text:
                    parts = text.split()
                    if parts:
                        last_part = parts[-1].strip("'\"")
                        if "/" in last_part or "\\" in last_part or "." in last_part:
                            try:
                                filename = Path(last_part).name
                            except Exception:
                                pass
                if on_progress:
                    on_progress(count, filename)

        async def pump_stderr():
            assert proc.stderr is not None
            async for line in proc.stderr:
                stderr_buf.append(line.decode(errors="replace"))

        try:
            await asyncio.wait_for(
                asyncio.gather(pump_stdout(), pump_stderr()),
                timeout=settings.gdl_run_timeout_s,
            )
            returncode = await proc.wait()
        except asyncio.TimeoutError:
            try:
                proc.kill()
                await proc.wait()
            except Exception:
                pass
            last_stderr = f"gallery-dl timed out after {settings.gdl_run_timeout_s}s"
            returncode = 1
        except asyncio.CancelledError:
            try:
                proc.terminate()
                await proc.wait()
            except Exception:
                pass
            raise

        last_stderr = last_stderr or "".join(stderr_buf)[-3000:]
        files = sorted(p for p in dest_dir.rglob("*") if p.is_file())

        if returncode == 0:
            return DownloadResult(ok=True, files=files, error_tail=last_stderr, attempts=attempts)

        rate_limited = looks_rate_limited(last_stderr)
        if not rate_limited or backoff.exhausted:
            return DownloadResult(
                ok=False, files=files, error_tail=last_stderr, attempts=attempts
            )

        delay = backoff.next_delay()
        log.warning(
            "gallery-dl looks rate-limited (attempt %s), backing off %.0fs before retry",
            attempts, delay,
        )
        await asyncio.sleep(delay)
        last_stderr = ""
    finally:
        if links_file and links_file.exists():
            links_file.unlink(missing_ok=True)
