from __future__ import annotations

import asyncio
import logging
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


async def upload_file(client: Client, chat_id: int, path: Path) -> None:
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

    async with _upload_semaphore:
        attempt = 0
        while True:
            attempt += 1
            try:
                await send(chat_id, str(path))
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
