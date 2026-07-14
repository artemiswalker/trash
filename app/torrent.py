from __future__ import annotations

import asyncio
import logging
import re
import shutil
from pathlib import Path
from typing import Callable, Optional

from .downloader import DownloadResult

log = logging.getLogger(__name__)

PROGRESS_RE = re.compile(
    r"\[#\w+\s+([^\s/]+)/([^\s(]+)\((\d+)%\)\s+CN:\d+\s+(?:SPD|DL):([0-9.]+[a-zA-Z/]*)"
)

TRACKERS = [
    "udp://tracker.opentrackr.org:1337/announce",
    "udp://open.stealth.si:80/announce",
    "udp://tracker.coppersurfer.tk:6969/announce",
    "udp://tracker.openbittorrent.com:6969/announce",
    "udp://tracker.internetwarriors.net:1337/announce",
    "udp://exodus.desync.com:6969/announce",
    "udp://open.demonii.com:1337/announce",
    "udp://tracker.cyberia.is:6969/announce",
    "udp://tracker.torrent.eu.org:451/announce",
    "udp://retracker.lanta-net.ru:2710/announce",
    "udp://tracker.tiny-vps.com:6969/announce",
    "udp://valkyrie.info:6969/announce",
    "udp://ipv4.tracker.harry.lu:80/announce",
    "http://tracker.gbitt.info:80/announce",
    "http://tracker.ipv6tracker.ru:80/announce"
]


def parse_speed_to_bytes(speed_str: str) -> float:
    """Parse speed/size string from aria2c to float bytes."""
    try:
        speed_str = speed_str.strip().lower()
        if speed_str.endswith("/s"):
            speed_str = speed_str[:-2]
        if speed_str.endswith("gib"):
            return float(speed_str[:-3]) * 1024 * 1024 * 1024
        elif speed_str.endswith("mib"):
            return float(speed_str[:-3]) * 1024 * 1024
        elif speed_str.endswith("kib"):
            return float(speed_str[:-3]) * 1024
        elif speed_str.endswith("gb"):
            return float(speed_str[:-2]) * 1024 * 1024 * 1024
        elif speed_str.endswith("mb"):
            return float(speed_str[:-2]) * 1024 * 1024
        elif speed_str.endswith("kb"):
            return float(speed_str[:-2]) * 1024
        elif speed_str.endswith("b"):
            return float(speed_str[:-1])
        return float(speed_str)
    except Exception:
        return 0.0


async def download_torrent_async(
    torrent_or_magnet: str,
    dest_dir: Path,
    on_progress: Optional[Callable[[float, float, float], None]] = None,
    register_proc: Optional[Callable[[asyncio.subprocess.Process], None]] = None,
) -> DownloadResult:
    """Download a torrent or magnet link asynchronously using aria2c."""
    if shutil.which("aria2c") is None:
        return DownloadResult(ok=False, error_tail="aria2c CLI client is not installed.")

    target = torrent_or_magnet
    if target.startswith("torrent:"):
        target = target[len("torrent:"):]

    tracker_arg = f"--bt-tracker={','.join(TRACKERS)}"

    cmd = [
        "aria2c",
        f"--dir={dest_dir}",
        "--seed-time=0",
        "--seed-ratio=0.0",
        "--bt-tracker-connect-timeout=10",
        "--bt-tracker-timeout=10",
        "--summary-interval=1",
        "--follow-torrent=mem",
        "--enable-dht=true",
        "--bt-enable-lpd=true",
        "--enable-peer-exchange=true",
        "--bt-max-peers=80",
        "--max-overall-upload-limit=50K",
        "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        tracker_arg,
        target
    ]

    log.info("Running aria2c command with optimized torrent parameters")
    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
    except Exception as e:
        log.exception("Failed to start aria2c for torrent %s", target)
        return DownloadResult(ok=False, error_tail=f"Failed to start aria2c: {e}")

    if register_proc:
        register_proc(proc)

    stderr_chunks: list[str] = []
    grace_terminate_started = False

    async def read_stdout():
        nonlocal grace_terminate_started
        assert proc.stdout is not None
        async for line in proc.stdout:
            line_text = line.decode(errors="replace").strip()
            if not line_text:
                continue

            parts = line_text.split("\r")
            for part in parts:
                text = part.strip()
                if not text:
                    continue

                is_complete = False
                match = PROGRESS_RE.search(text)
                if match:
                    downloaded_str = match.group(1)
                    pct_str = match.group(3)
                    speed_str = match.group(4)

                    try:
                        pct = float(pct_str)
                        if pct >= 100.0:
                            is_complete = True
                        downloaded_bytes = parse_speed_to_bytes(downloaded_str)
                        speed_bytes = parse_speed_to_bytes(speed_str)

                        if on_progress:
                            on_progress(pct, downloaded_bytes, speed_bytes)
                    except Exception:
                        pass

                if ("download complete" in text.lower() or is_complete) and not grace_terminate_started:
                    grace_terminate_started = True
                    log.info("Detected download complete in aria2c output, scheduling termination...")
                    async def grace_terminate():
                        await asyncio.sleep(4.0)
                        if proc.returncode is None:
                            log.info("aria2c did not exit after download complete; terminating process")
                            try:
                                proc.terminate()
                            except Exception:
                                pass
                    asyncio.create_task(grace_terminate())


    async def read_stderr():
        assert proc.stderr is not None
        async for line in proc.stderr:
            text = line.decode(errors="replace")
            stderr_chunks.append(text)
            if len(stderr_chunks) > 100:
                stderr_chunks.pop(0)

    try:
        await asyncio.gather(read_stdout(), read_stderr())
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

    ok = returncode == 0
    error_tail = "".join(stderr_chunks)

    files = []
    if ok and dest_dir.exists():
        files = [
            p for p in dest_dir.rglob("*")
            if p.is_file() and not p.name.endswith(".part") and not p.name.endswith(".aria2")
        ]

    return DownloadResult(ok=ok, files=files, error_tail=error_tail, attempts=1)
