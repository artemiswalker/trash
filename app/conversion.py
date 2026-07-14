from __future__ import annotations

import asyncio
import logging
from pathlib import Path
from typing import Optional

from pyrogram import Client
from pyrogram.types import CallbackQuery, LinkPreviewOptions
from .status import compile_conversion_choice_status_text

log = logging.getLogger(__name__)

CONVERSION_EXT = {".ts", ".flv", ".avi", ".wmv", ".asf"}

# State registries
_conversion_ids: dict[int, dict[str, str]] = {}  
_conversion_events: dict[int, dict[str, asyncio.Event]] = {}  
_conversion_choices: dict[int, dict[str, str]] = {}  
_converted_files: dict[int, set[str]] = {}  


async def convert_media_async(input_path: Path, output_path: Path) -> bool:
    """Asynchronously convert video to MP4 container.
    First tries to do a direct stream copy (instant, zero quality loss).
    If that fails, falls back to transcoding with visually lossless settings (-crf 18)."""
    copy_cmd = [
        "ffmpeg", "-y", "-nostdin", "-i", str(input_path),
        "-c", "copy",
        str(output_path)
    ]
    log.info("Attempting fast stream copy conversion for %s", input_path.name)
    try:
        proc = await asyncio.create_subprocess_exec(
            *copy_cmd,
            stdin=asyncio.subprocess.DEVNULL,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.DEVNULL
        )
        returncode = await proc.wait()
        if returncode == 0:
            log.info("Fast stream copy conversion successful for %s", input_path.name)
            return True
    except Exception:
        log.exception("Fast stream copy failed for %s", input_path.name)

    output_path.unlink(missing_ok=True)

    log.warning("Fast stream copy failed or unsupported. Falling back to full H.264 transcoding for %s", input_path.name)
    transcode_cmd = [
        "ffmpeg", "-y", "-nostdin", "-i", str(input_path),
        "-c:v", "libx264", "-preset", "superfast", "-crf", "18",
        "-c:a", "aac", "-b:a", "192k", "-pix_fmt", "yuv420p",
        str(output_path)
    ]
    try:
        proc = await asyncio.create_subprocess_exec(
            *transcode_cmd,
            stdin=asyncio.subprocess.DEVNULL,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.DEVNULL
        )
        returncode = await proc.wait()
        return returncode == 0
    except Exception:
        log.exception("ffmpeg transcoding fallback failed for %s", input_path)
        return False


async def handle_conversion_choice(client: Client, callback_query: CallbackQuery, store, is_job_owner) -> None:
    data = callback_query.data
    parts = data.split(":", 2)
    choice = parts[0]  
    job_id = int(parts[1])
    conv_id = parts[2]

    job = await store.get_job(job_id)
    if not job:
        await callback_query.answer("Job not found.", show_alert=True)
        return

    if not is_job_owner(callback_query.message.chat.id, job):
        await callback_query.answer("You are not the owner of this job.", show_alert=True)
        return

    choice_type = "mp4" if choice == "convert_mp4" else "orig"

    if job_id not in _conversion_choices:
        _conversion_choices[job_id] = {}
    _conversion_choices[job_id][conv_id] = choice_type

    if job_id in _conversion_events and conv_id in _conversion_events[job_id]:
        _conversion_events[job_id][conv_id].set()

    choice_str = "Convert to MP4" if choice_type == "mp4" else "Upload Original"
    filename = _conversion_ids.get(job_id, {}).get(conv_id, "file")

    await callback_query.answer(f"Selected: {choice_str}")
    try:
        await callback_query.message.edit_text(
            compile_conversion_choice_status_text(job_id, filename, choice_str),
            link_preview_options=LinkPreviewOptions(is_disabled=True)
        )
    except Exception:
        pass


async def convert_image_to_png_async(input_path: Path, output_path: Path) -> bool:
    """Asynchronously convert an unsupported image format to PNG using ffmpeg."""
    cmd = [
        "ffmpeg", "-y", "-nostdin", "-i", str(input_path),
        str(output_path)
    ]
    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdin=asyncio.subprocess.DEVNULL,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.DEVNULL
        )
        returncode = await proc.wait()
        return returncode == 0
    except Exception:
        log.exception("ffmpeg image to PNG conversion failed for %s", input_path.name)
        return False
