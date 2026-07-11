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

log = logging.getLogger(__name__)

IMAGE_EXT = {".jpg", ".jpeg", ".png", ".webp", ".gif"}
VIDEO_EXT = {
    ".mp4", ".mov", ".webm", ".mkv", ".avi", ".flv", ".wmv",
    ".3gp", ".mpeg", ".mpg", ".m4v", ".ts", ".f4v"
}

_upload_semaphore = asyncio.Semaphore(settings.tg_max_concurrent_uploads)


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
                    info["width"] = int(v)
                elif k == "height":
                    info["height"] = int(v)
                elif k == "duration":
                    info["duration"] = int(round(float(v)))
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
    size = path.stat().st_size
    if size > settings.max_upload_bytes:
        raise UploadTooLarge(f"{path.name} is {size / 1e9:.2f}GB, exceeds 2GB MTProto limit")

    ext = path.suffix.lower()
    if ext in IMAGE_EXT:
        send = lambda c, p, **kw: client.send_photo(c, p, **kw)
    elif ext in VIDEO_EXT:
        send = lambda c, p, **kw: client.send_video(c, p, **kw)
    else:
        send = lambda c, p, **kw: client.send_document(c, p, **kw)

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
            attempt = 0
            while True:
                attempt += 1
                try:
                    if ext in VIDEO_EXT and screenshots:
                        media = []
                        # Add video first so it is the cover and caption holder
                        video_kwargs = {
                            "media": str(path),
                            "caption": path.name,
                        }
                        if thumb_path:
                            video_kwargs["thumb"] = str(thumb_path)
                        if "width" in video_meta:
                            video_kwargs["width"] = video_meta["width"]
                        if "height" in video_meta:
                            video_kwargs["height"] = video_meta["height"]
                        if "duration" in video_meta:
                            video_kwargs["duration"] = video_meta["duration"]

                        media.append(InputMediaVideo(**video_kwargs))

                        # Add screenshots after the video
                        for shot in screenshots:
                            media.append(InputMediaPhoto(str(shot)))

                        # Dynamically patch save_file to track upload progress of the video file
                        import types
                        original_save_file = client.save_file

                        async def custom_save_file(client_inst, path_arg, *args, **kwargs):
                            if str(path_arg) == str(path):
                                kwargs["progress"] = progress
                            return await original_save_file(path_arg, *args, **kwargs)

                        client.save_file = types.MethodType(custom_save_file, client)
                        try:
                            await client.send_media_group(chat_id, media=media)
                        finally:
                            client.save_file = original_save_file
                    else:
                        kwargs = {"caption": path.name}
                        if thumb_path:
                            kwargs["thumb"] = str(thumb_path)
                        if progress:
                            kwargs["progress"] = progress

                        if ext in VIDEO_EXT:
                            if "width" in video_meta:
                                kwargs["width"] = video_meta["width"]
                            if "height" in video_meta:
                                kwargs["height"] = video_meta["height"]
                            if "duration" in video_meta:
                                kwargs["duration"] = video_meta["duration"]

                        await send(chat_id, str(path), **kwargs)
                    return
                except FloodWait as e:
                    log.warning("FloodWait %ss on %s", e.value, path.name)
                    await asyncio.sleep(e.value + 1)
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
