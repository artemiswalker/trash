from __future__ import annotations

import asyncio
import logging
import shutil
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Optional

from ...config import settings
from ...rate_limiter import Backoff, looks_rate_limited

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
    extra_args: Optional[list[str]] = None,
    links_file: Optional[Path] = None,
) -> list[str]:
    cmd = [
        "gallery-dl",
        "--no-mtime",
        "-D", str(dest_dir),
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
            if text.strip():
                count += 1

    async def read_stderr():
        assert proc.stderr is not None
        async for line in proc.stderr:
            stderr_chunks.append(line.decode(errors="replace"))
            if len(stderr_chunks) > 200:
                stderr_chunks.pop(0)

    try:
        await asyncio.gather(read_stdout(), read_stderr())
        returncode = await proc.wait()
        return returncode, "".join(stderr_chunks), (lambda: count)
    except asyncio.CancelledError:
        try:
            proc.terminate()
            await proc.wait()
        except Exception:
            pass
        raise


async def run_with_progress(
    url: str,
    dest_dir: Path,
    on_progress: Optional[Callable[[int, Optional[str]], None]] = None,
    extra_args: Optional[list[str]] = None,
    register_proc: Optional[Callable[[asyncio.subprocess.Process], None]] = None,
) -> DownloadResult:

    if shutil.which("gallery-dl") is None:
        raise GalleryDLNotFound(
            "gallery-dl not found on PATH. Install with: "
            "pip install gallery-dl --break-system-packages"
        )

    try:
        urls = json.loads(url)
        if not isinstance(urls, list):
            urls = [url]
    except Exception:
        urls = [url]

    dest_dir.mkdir(parents=True, exist_ok=True)
    attempts = 0
    last_stderr = ""
    success_count = 0
    total_urls = len(urls)
    total_download_count = 0

    for idx, single_url in enumerate(urls, 1):
        backoff = Backoff(
            base_s=settings.gdl_backoff_base_s,
            multiplier=settings.gdl_backoff_multiplier,
            max_attempts=settings.gdl_max_run_retries,
        )

        url_success = False
        while True:
            attempts += 1
            cmd = _build_cmd([single_url], dest_dir, extra_args, links_file=None)
            log.info("gallery-dl run url %s/%s attempt=%s url=%s args=%s", idx, total_urls, attempts, single_url, extra_args)

            proc = await asyncio.create_subprocess_exec(
                *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
            )
            if register_proc:
                register_proc(proc)

            count = 0
            stderr_buf: list[str] = []

            async def pump_stdout():
                nonlocal count
                assert proc.stdout is not None
                async for line in proc.stdout:
                    text = line.decode(errors="replace").strip()
                    if not text:
                        continue

                    count += 1
                    filename = None
                    parts = text.split()
                    if parts:
                        last_part = parts[-1].strip("'\"")
                        if "/" in last_part or "\\" in last_part or "." in last_part:
                            try:
                                filename = Path(last_part).name
                            except Exception:
                                pass

                    if on_progress:
                        try:
                            on_progress(total_download_count + count, filename)
                        except TypeError:
                            on_progress(total_download_count + count)

            async def pump_stderr():
                assert proc.stderr is not None
                async for line in proc.stderr:
                    stderr_buf.append(line.decode(errors="replace"))

            try:
                await asyncio.gather(pump_stdout(), pump_stderr())
                returncode = await proc.wait()
            except asyncio.CancelledError:
                try:
                    proc.terminate()
                    await proc.wait()
                except Exception:
                    pass
                raise
            finally:
                if register_proc:
                    register_proc(None)

            last_stderr = last_stderr or "".join(stderr_buf)[-3000:]

            if returncode == 0:
                url_success = True
                success_count += 1
                total_download_count += count
                if on_progress:
                    on_progress(total_download_count)
                break

            rate_limited = looks_rate_limited(last_stderr)
            if not rate_limited or backoff.exhausted:
                log.error("gallery-dl failed for URL %s: %s", single_url, last_stderr)
                break

            delay = backoff.next_delay()
            log.warning(
                "gallery-dl looks rate-limited (attempt %s), backing off %.0fs before retry",
                attempts, delay,
            )
            await asyncio.sleep(delay)
            last_stderr = ""

    files = sorted(p for p in dest_dir.rglob("*") if p.is_file())
    ok = (success_count == total_urls)
    return DownloadResult(ok=ok, files=files, error_tail=last_stderr, attempts=attempts)
