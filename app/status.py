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


def compile_job_status_text(job, job_state) -> str:
    parsed_args = []
    if job.args:
        try:
            parsed_args = json.loads(job.args)
        except Exception:
            pass
    args_str = " ".join(parsed_args) if parsed_args else "None"
    split_str = "Yes" if job.split_large_files else "No"

    job_text = (
        f"**Active Job #{job.id}**\n"
        f"- **Status**: `{job.status}`\n"
        f"- **URL**: {format_url_display(job.url)}\n"
        f"- **Args**: `{args_str}`\n"
        f"- **Split > 2GB**: {split_str}\n"
    )

    if job.status == "downloading" or not job_state.downloader_done.is_set():
        dl_speed_str = format_size(job_state.download_speed)
        dl_bytes_str = format_size(job_state.total_downloaded_bytes)

        is_torrent = (
            job.url.startswith("magnet:") or
            job.url.startswith("torrent:") or
            job.url.endswith(".torrent") or
            "magnet:?xt=" in job.url
        )

        job_text += "**Downloader Metrics**\n"
        if is_torrent:
            bar = make_progress_bar(job_state.download_pct)
            job_text += (
                f"  - **Progress**: {job_state.download_pct:.1f}%\n"
                f"    `[{bar}]`\n"
                f"  - **Downloaded**: {dl_bytes_str}\n"
                f"  - **Speed**: {dl_speed_str}/s\n"
            )
        else:
            if job_state.current_download_file:
                job_text += f"  - **Current File**: `{job_state.current_download_file}`\n"
            job_text += (
                f"  - **Files Downloaded**: {job_state.download_count}\n"
                f"  - **Downloaded**: {dl_bytes_str}\n"
                f"  - **Speed**: {dl_speed_str}/s\n"
            )

    if job.status == "uploading" or job_state.sent > 0 or job_state.current_upload_file:
        ul_speed_str = format_size(job_state.upload_speed)
        bar = make_progress_bar(job_state.current_upload_pct)

        job_text += (
            f"**Uploader Metrics**\n"
            f"  - **Files Sent**: {job_state.sent} / {job.total_files if job.total_files > 0 else 'Calculating'}\n"
            f"  - **Files Skipped**: {len(job_state.skipped)}\n"
        )
        if job_state.current_upload_file:
            job_text += (
                f"  - **Current File**: `{job_state.current_upload_file}`\n"
                f"  - **Progress**: {job_state.current_upload_pct:.1f}%\n"
                f"    `[{bar}]`\n"
                f"  - **Speed**: {ul_speed_str}/s\n"
            )

    return job_text
