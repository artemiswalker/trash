from __future__ import annotations

import asyncio
import logging
import random
import shutil
import tempfile
from collections.abc import Callable, Coroutine
from pathlib import Path

from pyrogram import Client
from pyrogram.errors import FloodWait, RPCError
from pyrogram.types import InputMediaPhoto, InputMediaVideo

from .config import settings
from .conversion import convert_image_to_png_async, CONVERSION_EXT

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


async def extract_video_thumbnail(video_path: Path) -> Path | None:
    if shutil.which("ffmpeg") is None:
        log.warning("ffmpeg not found on PATH; skipping video thumbnail extraction")
        return None

    thumb_path = Path(tempfile.gettempdir()) / f"{video_path.stem}_thumb.jpg"
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


async def probe_video(video_path: Path) -> dict[str, int]:
    if shutil.which("ffprobe") is None:
        log.warning("ffprobe not found on PATH; skipping video probe")
        return {}

    try:
        proc = await asyncio.create_subprocess_exec(
            "ffprobe",
            "-v", "error",
            "-select_streams", "v:0",
            "-show_entries", "stream=width,height,duration",
            "-of", "default=noprint_wrappers=1",
            str(video_path),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.DEVNULL,
        )
        stdout_data, _ = await asyncio.wait_for(proc.communicate(), timeout=5.0)

        info = {}
        for line in stdout_data.decode(errors="replace").splitlines():
            if "=" in line:
                k, v = line.split("=", 1)
                k = k.strip()
                v = v.strip()
                if k == "width":
                    try:
                        info["width"] = int(v)
                    except ValueError:
                        pass
                elif k == "height":
                    try:
                        info["height"] = int(v)
                    except ValueError:
                        pass
                elif k == "duration":
                    try:
                        info["duration"] = int(round(float(v)))
                    except ValueError:
                        pass
        return info

    except Exception:
        log.exception("Failed to probe video %s", video_path)
        return {}


async def take_screenshots(video_path: Path, duration: int) -> list[Path]:
    if shutil.which("ffmpeg") is None:
        log.warning("ffmpeg not found; skipping screenshots")
        return []

    if duration <= 0:
        log.warning("Duration is 0 or unknown; skipping screenshots")
        return []

    timestamps = sorted([random.uniform(0.05 * duration, 0.95 * duration) for _ in range(9)])

    screenshots: list[Path] = []
    for i, ts in enumerate(timestamps):
        shot_path = Path(tempfile.gettempdir()) / f"{video_path.stem}_screenshot_{i}.jpg"
        try:
            proc = await asyncio.create_subprocess_exec(
                "ffmpeg",
                "-ss", f"{ts:.3f}",
                "-i", str(video_path),
                "-vframes", "1",
                "-q:v", "2",
                str(shot_path),
                "-y",
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.DEVNULL,
            )
            await asyncio.wait_for(proc.wait(), timeout=10.0)
            if proc.returncode == 0 and shot_path.exists():
                screenshots.append(shot_path)
        except Exception:
            log.exception("Failed to extract screenshot at %s for %s", ts, video_path)
            if shot_path.exists():
                try:
                    shot_path.unlink()
                except Exception:
                    pass
    return screenshots


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
                        from .queue_manager import queue_manager
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


async def split_video(video_path: Path, max_size_bytes: int) -> list[Path]:
    try:
        from .conversion import split_video_async
        parts = await split_video_async(video_path, max_size_bytes)
        if parts:
            return parts
    except Exception as e:
        log.exception("Failed to split video using PyAV segmenter: %s", video_path)

    return await split_binary(video_path, max_size_bytes)



async def split_binary(file_path: Path, max_size_bytes: int) -> list[Path]:
    parts = []
    chunk_size = int(max_size_bytes * 0.98)
    buffer_size = 1024 * 1024

    part_num = 1
    try:
        with open(file_path, "rb") as infile:
            while True:
                part_path = file_path.parent / f"{file_path.name}.{part_num:03d}"
                bytes_written = 0

                with open(part_path, "wb") as outfile:
                    while bytes_written < chunk_size:
                        chunk = infile.read(min(buffer_size, chunk_size - bytes_written))
                        if not chunk:
                            break
                        outfile.write(chunk)
                        bytes_written += len(chunk)

                if bytes_written == 0:
                    try:
                        part_path.unlink()
                    except Exception:
                        pass
                    break

                parts.append(part_path)
                part_num += 1
    except Exception:
        log.exception("Failed to binary split file: %s", file_path)
        for p in parts:
            try:
                p.unlink()
            except Exception:
                pass
        return []

    return parts


async def handle_large_file(path: Path, split_large_files: bool) -> list[Path]:
    max_size = int(1.95 * 1024 * 1024 * 1024)
    size = path.stat().st_size
    if size <= max_size:
        return [path]

    log.info("File '%s' size is %s bytes, exceeds 1.95GB threshold", path.name, size)
    if not split_large_files:
        log.info("split_large_files is False; deleting and skipping '%s'", path.name)
        try:
            path.unlink()
        except Exception:
            pass
        return []

    log.info("split_large_files is True; splitting '%s'", path.name)
    ext = path.suffix.lower()
    if ext in VIDEO_EXT:
        parts = await split_video(path, max_size)
    else:
        parts = await split_binary(path, max_size)

    if parts:
        log.info("Successfully split '%s' into %s parts", path.name, len(parts))
        try:
            path.unlink()
        except Exception:
            pass
        return parts
    else:
        log.error("Failed to split '%s'; deleting to avoid loop", path.name)
        try:
            path.unlink()
        except Exception:
            pass
        return []
