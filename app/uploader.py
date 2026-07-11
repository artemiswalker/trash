from __future__ import annotations

import asyncio
import logging
import shutil
from collections.abc import Callable, Coroutine
from pathlib import Path

from pyrogram import Client
from pyrogram.errors import FloodWait, RPCError

from .config import settings

log = logging.getLogger(__name__)

IMAGE_EXT = {".jpg", ".jpeg", ".png", ".webp", ".gif"}
VIDEO_EXT = {".mp4", ".mov", ".webm", ".mkv"}

_upload_semaphore = asyncio.Semaphore(settings.tg_max_concurrent_uploads)


class UploadTooLarge(Exception):
    pass


async def extract_video_thumbnail(video_path: Path) -> Path | None:
    if shutil.which("ffmpeg") is None:
        log.warning("ffmpeg not found on PATH; skipping video thumbnail extraction")
        return None

    thumb_path = video_path.with_name(f"{video_path.stem}_thumb.jpg")
    try:
        proc = await asyncio.create_subprocess_exec(
            "ffmpeg",
            "-ss", "00:00:04",
            "-i", str(video_path),
            "-vframes", "1",
            "-q:v", "4",
            "-vf", "scale=320:-1",
            str(thumb_path),
            "-y",
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.DEVNULL,
        )
        await asyncio.wait_for(proc.wait(), timeout=10.0)
        if proc.returncode == 0 and thumb_path.exists():
            return thumb_path
    except Exception:
        log.exception("Failed to extract thumbnail for %s", video_path)
        if thumb_path.exists():
            try:
                thumb_path.unlink()
            except Exception:
                pass
    return None


async def upload_file(
    client: Client,
    chat_id: int,
    path: Path,
    progress: Callable[[int, int], Coroutine[None, None, None]] | None = None
) -> None:
    size = path.stat().st_size
    if size > settings.max_upload_bytes:
        raise UploadTooLarge(f"{path.name} is {size / 1e9:.2f}GB, exceeds 2GB MTProto limit")

    ext = path.suffix.lower()
    if ext in IMAGE_EXT:
        send = client.send_photo
    elif ext in VIDEO_EXT:
        send = client.send_video
    else:
        send = client.send_document

    thumb_path = None
    if ext in VIDEO_EXT:
        thumb_path = await extract_video_thumbnail(path)

    try:
        async with _upload_semaphore:
            attempt = 0
            while True:
                attempt += 1
                try:
                    kwargs = {"caption": path.name}
                    if thumb_path:
                        kwargs["thumb"] = str(thumb_path)
                    if progress:
                        kwargs["progress"] = progress
                    await send(chat_id, str(path), **kwargs)
                    return
                except FloodWait as e:
                    log.warning("FloodWait %ss on %s", e.value, path.name)
                    await asyncio.sleep(e.value + 1)
                    # FloodWait doesn't count against our retry budget — Telegram
                    # told us exactly what to do, so just try again after.
                except RPCError:
                    if attempt >= settings.tg_upload_max_retries:
                        raise
                    delay = 2**attempt
                    log.warning(
                        "Upload error on %s (attempt %s), retrying in %ss",
                        path.name, attempt, delay,
                    )
                    await asyncio.sleep(delay)
    finally:
        if thumb_path and thumb_path.exists():
            try:
                thumb_path.unlink()
            except Exception:
                pass
