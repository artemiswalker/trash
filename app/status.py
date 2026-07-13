from __future__ import annotations

import asyncio
import logging
import json
from pathlib import Path
from typing import Optional

from pyrogram import Client
from pyrogram.types import LinkPreviewOptions

log = logging.getLogger(__name__)

# Global metrics and trackers
_current_job_id: Optional[int] = None
_active_job_metrics = {
    "download_speed": 0.0,
    "upload_speed": 0.0,
    "current_download_file": None,
    "current_upload_file": None,
    "current_upload_pct": 0.0,
    "total_downloaded_bytes": 0,
    "download_count": 0,
}


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


def compile_status_text(
    job,
    downloader_done: bool,
    uploader_done: bool,
    download_count: int,
    sent: int,
    skipped_len: int,
    current_upload_file: Optional[str],
    current_upload_pct: float,
    upload_speed: float
) -> str:
    parsed_args = []
    if job.args:
        try:
            parsed_args = json.loads(job.args)
        except Exception:
            pass
    args_str = " ".join(parsed_args) if parsed_args else "None"
    split_str = "Yes" if job.split_large_files else "No"

    download_speed = _active_job_metrics["download_speed"]
    total_downloaded_bytes = _active_job_metrics["total_downloaded_bytes"]

    dl_speed_str = format_size(download_speed)
    dl_bytes_str = format_size(total_downloaded_bytes)
    dl_file = _active_job_metrics["current_download_file"] if not downloader_done else None
    total_files_str = str(download_count) if downloader_done else "Calculating"

    status_text = (
        f"**Active Job Status**\n"
        f"- **Job ID**: #{job.id}\n"
        f"- **Status**: `{'Uploading' if downloader_done else 'Downloading & Uploading'}`\n"
        f"- **URL**: {format_url_display(job.url)}\n"
        f"- **Args**: `{args_str}`\n"
        f"- **Split > 2GB**: {split_str}\n\n"
        f"**Downloader Metrics**\n"
    )

    if not downloader_done:
        if dl_file:
            status_text += f"- **Current File**: `{dl_file}`\n"
        status_text += (
            f"- **Files Downloaded**: {download_count}\n"
            f"- **Downloaded Data**: {dl_bytes_str}\n"
            f"- **Download Speed**: {dl_speed_str}/s\n\n"
        )
    else:
        status_text += (
            f"- **Downloading**: `Complete` (Total: {dl_bytes_str})\n"
            f"- **Files Downloaded**: {download_count}\n\n"
        )

    status_text += (
        f"**Uploader Metrics**\n"
        f"- **Files Sent**: {sent} / {total_files_str}\n"
        f"- **Files Skipped**: {skipped_len}\n"
    )

    if current_upload_file:
        up_speed_str = format_size(upload_speed)
        bar = make_progress_bar(current_upload_pct)
        status_text += (
            f"- **Current File**: `{current_upload_file}`\n"
            f"- **Upload Progress**: {current_upload_pct:.1f}%\n"
            f"  `[{bar}]`\n"
            f"- **Upload Speed**: {up_speed_str}/s\n"
        )

    return status_text
