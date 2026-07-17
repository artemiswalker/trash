from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable, Coroutine
from pathlib import Path

from pyrogram import Client
from pyrogram.errors import FloodWait, RPCError
from pyrogram.types import InputMediaPhoto, InputMediaVideo

from ..config import settings
from ..conversion import convert_image_to_png_async, CONVERSION_EXT
from .video import probe_video, extract_video_thumbnail, take_screenshots

log = logging.getLogger(__name__)

IMAGE_EXT = {
    ".jpg", ".jpeg", ".png", ".webp", ".gif",
    ".bmp", ".tiff", ".heic", ".heif", ".ico"
}
VIDEO_EXT = {
    ".mp4", ".mov", ".webm", ".mkv", ".avi", ".flv", ".wmv",
    ".3gp", ".mpeg", ".mpg", ".m4v", ".ts", ".f4v"
}

_upload_semaphore = asyncio.Semaphore(1)


class UploadTooLarge(Exception):
    pass


async def upload_file(
    client: Client,
    chat_id: int,
    path: Path,
    progress: Callable[[int, int], Coroutine[None, None, None]] | None = None
) -> None:
    ext = path.suffix.lower()
    converted_png_paths: list[Path] = []
    CONVERTIBLE_IMAGE_EXT = {".webp", ".bmp", ".tiff", ".heic", ".heif", ".ico"}
    if ext in CONVERTIBLE_IMAGE_EXT:
        log.info("Converting unsupported image %s to PNG for standard inline photo display", path.name)
        png_path = path.with_suffix(".png")
        success = await convert_image_to_png_async(path, png_path)
        if success:
            converted_png_paths.append(png_path)
            path = png_path
            ext = ".png"
            log.info("Successfully converted unsupported image to PNG: %s", png_path.name)
        else:
            log.warning("Failed to convert image %s to png", path.name)

    size = path.stat().st_size
    if size == 0:
        raise UploadTooLarge(f"{path.name} is empty (0 bytes) and cannot be uploaded")
    if size > settings.max_upload_bytes:
        raise UploadTooLarge(f"{path.name} is {size / 1e9:.2f}GB, exceeds 2GB MTProto limit")
    thumb_path = None
    screenshots: list[Path] = []
    video_meta = {}

    if ext in VIDEO_EXT:
        video_meta = await probe_video(path)
        duration = video_meta.get("duration", 0)
        log.info("Video '%s' duration: %s seconds", path.name, duration if duration > 0 else "unknown")

        thumb_path = await extract_video_thumbnail(path)
        if duration > 0:
            screenshots = await take_screenshots(path, duration)

    try:
        async with _upload_semaphore:
            if ext in CONVERSION_EXT:
                mode = "document"
            elif ext in VIDEO_EXT and screenshots:
                mode = "group"
            elif ext in VIDEO_EXT:
                mode = "video"
            elif ext in IMAGE_EXT:
                if ext in (".webp", ".gif", ".bmp", ".tiff", ".heic", ".heif", ".ico"):
                    mode = "document"
                else:
                    mode = "photo"
            else:
                mode = "document"

            attempt = 0
            while True:
                attempt += 1
                try:
                    if mode == "group":
                        video_kwargs = {"caption": path.name}
                        if thumb_path:
                            video_kwargs["thumb"] = str(thumb_path)
                        if progress:
                            video_kwargs["progress"] = progress
                        if "width" in video_meta:
                            video_kwargs["width"] = video_meta["width"]
                        if "height" in video_meta:
                            video_kwargs["height"] = video_meta["height"]
                        if "duration" in video_meta:
                            video_kwargs["duration"] = video_meta["duration"]

                        log.info("Group mode: Sending video separately first for %s", path.name)
                        await client.send_video(chat_id, str(path), **video_kwargs)

                        if screenshots:
                            try:
                                log.info("Group mode: Sending %s screenshots grouped for %s", len(screenshots), path.name)
                                media = [InputMediaPhoto(str(shot)) for shot in screenshots]
                                media[0].caption = f"Screenshots for {path.name}"
                                await client.send_media_group(chat_id, media=media)
                            except Exception as se:
                                log.warning("Failed to send screenshots for %s: %s", path.name, se)
                        return

                    elif mode == "video":
                        kwargs = {"caption": path.name}
                        if thumb_path:
                            kwargs["thumb"] = str(thumb_path)
                        if progress:
                            kwargs["progress"] = progress
                        if "width" in video_meta:
                            kwargs["width"] = video_meta["width"]
                        if "height" in video_meta:
                            kwargs["height"] = video_meta["height"]
                        if "duration" in video_meta:
                            kwargs["duration"] = video_meta["duration"]

                        await client.send_video(chat_id, str(path), **kwargs)
                        return

                    elif mode == "photo":
                        kwargs = {"caption": path.name}
                        if progress:
                            kwargs["progress"] = progress

                        await client.send_photo(chat_id, str(path), **kwargs)
                        return

                    else:  
                        kwargs = {"caption": path.name}
                        if thumb_path:
                            kwargs["thumb"] = str(thumb_path)
                        if progress:
                            kwargs["progress"] = progress

                        await client.send_document(chat_id, str(path), **kwargs)
                        return

                except FloodWait as e:
                    log.warning("FloodWait %ss on %s", e.value, path.name)
                    try:
                        from ..queue_manager import queue_manager
                        queue_manager.notify_floodwait(e.value)
                    except Exception:
                        pass
                    await asyncio.sleep(e.value + 1)
                except RPCError as e:
                    err_msg = str(e)
                    is_media_invalid = any(
                        term in err_msg
                        for term in (
                            "MEDIA_INVALID", "WEBP_REQUIRED", "PHOTO_INVALID",
                            "VIDEO_CONTENT_TYPE_INVALID", "PHOTO_EXT_INVALID",
                            "PHOTO_SAVE_FILE_INVALID"
                        )
                    )

                    if is_media_invalid:
                        if mode == "group":
                            log.warning("Media group upload failed with MediaInvalid for %s. Falling back to standalone video upload.", path.name)
                            mode = "video"
                            attempt = 0
                            continue
                        elif mode in ("video", "photo"):
                            log.warning("%s upload failed with MediaInvalid for %s. Falling back to document upload as last resort.", mode.capitalize(), path.name)
                            mode = "document"
                            attempt = 0
                            continue

                    if attempt >= settings.tg_upload_max_retries:
                        raise
                    delay = 2**attempt
                    log.warning(
                        "Upload error on %s (attempt %s, mode %s): %s. Retrying in %ss",
                        path.name, attempt, mode, e, delay,
                    )
                    await asyncio.sleep(delay)
    finally:
        for shot in screenshots:
            if shot.exists():
                try:
                    shot.unlink()
                except Exception:
                    pass
        if thumb_path and thumb_path.exists():
            try:
                thumb_path.unlink()
            except Exception:
                pass
        for p in converted_png_paths:
            if p.exists():
                try:
                    p.unlink()
                except Exception:
                    pass
