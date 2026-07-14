from __future__ import annotations

import asyncio
import logging
import json
from pathlib import Path
from typing import Optional

from pyrogram import Client
from pyrogram.types import LinkPreviewOptions

log = logging.getLogger(__name__)



def format_size(size_bytes: float) -> str:
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if size_bytes < 1024.0:
            return f"{size_bytes:.1f} {unit}"
        size_bytes /= 1024.0
    return f"{size_bytes:.1f} PB"


def make_progress_bar(pct: float) -> str:
    filled = int(round(pct / 10))
    bar = "■" * filled + "□" * (10 - filled)
    return bar


async def safe_edit(client: Client, chat_id: int, message_id: int, text: str) -> bool:
    from pyrogram.errors import FloodWait, MessageNotModified
    try:
        await client.edit_message_text(
            chat_id,
            message_id,
            text,
            link_preview_options=LinkPreviewOptions(is_disabled=True)
        )
        return True
    except MessageNotModified:
        return True
    except FloodWait as e:
        log.warning("Telegram FloodWait: waiting %s seconds on status edit", e.value)
        await asyncio.sleep(e.value + 1)
        return False
    except Exception as e:
        log.warning("Failed to edit status message: %s", e)
        return False


def format_url_display(url_json: str) -> str:
    try:
        urls = json.loads(url_json)
        if isinstance(urls, list):
            if len(urls) == 1:
                return f"`{urls[0]}`"
            return f"`{urls[0]}` (+ {len(urls) - 1} more)"
    except Exception:
        pass
    return f"`{url_json}`"


def compile_split_prompt_text(job_id: int, url_or_target: str, is_torrent: bool = False, is_unzip: bool = False) -> str:
    if is_torrent:
        title = f"**Torrent Job #{job_id} registered**"
        target_label = "Target"
    elif is_unzip:
        title = f"**Job #{job_id} registered**"
        target_label = "Archive"
    else:
        title = f"**Job #{job_id} registered**"
        target_label = "URL"
    
    display = format_url_display(url_or_target) if not (is_torrent or is_unzip) else url_or_target
    return (
        f"{title}\n"
        f"- **{target_label}**: {display}\n\n"
        "Do you want to split files larger than 2GB for this job?"
    )


def compile_queued_status_text(job_id: int, url: str, args_display: str) -> str:
    return (
        f"**Queued (job #{job_id})**\n"
        f"- **URL**: {format_url_display(url)}{args_display}"
    )


def compile_unzip_download_status_text(job_id: int, filename: str, current: int, total: int) -> str:
    pct = current * 100.0 / total if total > 0 else 0.0
    bar = make_progress_bar(pct)
    return (
        f"**Job #{job_id} registered**\n"
        f"- **Archive**: `{filename}`\n\n"
        f"Downloading archive to VPS: {pct:.1f}%\n"
        f"  `[{bar}]`\n"
        f"Downloaded: {format_size(current)} of {format_size(total)}"
    )


def compile_archive_prompt_text(job_id: int, filename: str) -> str:
    return (
        f"**Job #{job_id} - Archive Detected**\n"
        f"- **File**: `{filename}`\n\n"
        "Do you want to upload the archive file only, or extract its contents and upload both?"
    )


def compile_archive_choice_status_text(job_id: int, filename: str, choice_str: str) -> str:
    return (
        f"**Job #{job_id} - Archive Choice**\n"
        f"- **File**: `{filename}`\n"
        f"- **Selected**: `{choice_str}`\n\n"
        "Processing choice..."
    )


def compile_conversion_prompt_text(job_id: int, filename: str) -> str:
    return (
        f"**Job #{job_id} - Incompatible Video Format**\n"
        f"- **File**: `{filename}`\n\n"
        "The file format is not natively supported for Telegram inline streaming. "
        "Do you want to convert it to MP4 first, or upload the original as a document?"
    )


def compile_conversion_choice_status_text(job_id: int, filename: str, choice_str: str) -> str:
    return (
        f"**Job #{job_id} - Media Conversion**\n"
        f"- **File**: `{filename}`\n\n"
        f"Choice selected: **{choice_str}**"
    )


def compile_extraction_status_text(job_id: int, filename: str) -> str:
    return f"**Job #{job_id} - Archive Extraction**\nExtracting `{filename}`..."


def compile_conversion_running_status_text(job_id: int, filename: str) -> str:
    return f"**Job #{job_id} - Media Conversion**\nConverting `{filename}` to standard MP4 container..."


def compile_conversion_failed_status_text(job_id: int, filename: str) -> str:
    return f"**Job #{job_id} - Conversion Failed**\nFailed to convert `{filename}`. Uploading original as document."


def compile_extraction_failed_status_text(job_id: int, filename: str) -> str:
    return f"**Job #{job_id} - Extraction Failed**\nFailed to extract `{filename}`."


def compile_extraction_success_status_text(job_id: int, filename: str) -> str:
    return f"**Job #{job_id} - Archive Extracted**\nSuccessfully extracted `{filename}` into download directory."
