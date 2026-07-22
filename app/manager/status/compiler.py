from __future__ import annotations

import json
from pathlib import Path
from typing import Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from ...db import Job
    from ..state import JobState

from .messaging import format_size, make_progress_bar

def make_marquee_bar(width: int = 10) -> str:
    import time
    pos = int(time.time() * 2) % (width * 2 - 2)
    if pos >= width:
        pos = (width * 2 - 2) - pos
    bar = ["░"] * width
    bar[pos] = "█"
    return "".join(bar)

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


def compile_split_prompt_text(job_id: str, url_or_target: str, is_torrent: bool = False, is_unzip: bool = False) -> str:
    if is_torrent:
        title = f"**Torrent Job #{job_id} Registered**"
        target_label = "Target"
    elif is_unzip:
        title = f"**Job #{job_id} Registered**"
        target_label = "Archive"
    else:
        title = f"**Job #{job_id} Registered**"
        target_label = "URL"
    
    display = format_url_display(url_or_target) if not (is_torrent or is_unzip) else url_or_target
    return (
        f"{title}\n"
        f"------------------------------------\n"
        f"- **{target_label}**: {display}\n\n"
        "Do you want to split files larger than 2GB for this job?"
    )


def compile_queued_status_text(job_id: str, url: str, args_display: str) -> str:
    cleaned_url = url
    if url.startswith("[") and url.endswith("]"):
        try:
            import json
            parsed = json.loads(url)
            if parsed and isinstance(parsed, list):
                cleaned_url = parsed[0]
        except Exception:
            pass

    is_torrent = (
        cleaned_url.startswith("magnet:") or
        cleaned_url.startswith("torrent:") or
        cleaned_url.endswith(".torrent") or
        "magnet:?xt=" in cleaned_url
    )
    if is_torrent:
        if cleaned_url.startswith("torrent:"):
            torrent_path = cleaned_url[len("torrent:"):]
            name = Path(torrent_path).name
            return (
                f"**Queued (job #{job_id})**\n"
                f"------------------------------------\n"
                f"- **File**: `{name}`\n"
                f"- **Tool**: `aria2c`"
            )
        else:
            magnet_disp = cleaned_url[:60] + "..." if len(cleaned_url) > 60 else cleaned_url
            return (
                f"**Queued (job #{job_id})**\n"
                f"------------------------------------\n"
                f"- **Link**: `{magnet_disp}`\n"
                f"- **Tool**: `aria2c`"
            )

    is_gdrive = (
        cleaned_url.startswith("gdrive:") or
        cleaned_url.startswith("gd2tg:") or
        "drive.google.com" in cleaned_url or
        "docs.google.com" in cleaned_url
    )
    if is_gdrive:
        gdrive_disp = cleaned_url
        for prefix in ("gdrive:", "gd2tg:"):
            if gdrive_disp.startswith(prefix):
                gdrive_disp = gdrive_disp[len(prefix):]
        gdrive_disp = gdrive_disp[:55] + "..." if len(gdrive_disp) > 55 else gdrive_disp
        return (
            f"**Queued (job #{job_id})**\n"
            f"------------------------------------\n"
            f"- **Type**: `Google Drive Download`\n"
            f"- **Tool**: `Google Drive API`\n"
            f"- **Link**: `{gdrive_disp}`{args_display}"
        )

    return (
        f"**Queued (job #{job_id})**\n"
        f"------------------------------------\n"
        f"- **URL**: {format_url_display(url)}{args_display}\n"
        f"- **Tool**: `gallery-dl`"
    )


def compile_unzip_download_status_text(job_id: str, filename: str, current: int, total: int) -> str:
    pct = current * 100.0 / total if total > 0 else 0.0
    bar = make_progress_bar(pct)
    return (
        f"**Job #{job_id} Registered**\n"
        f"------------------------------------\n"
        f"- **Archive**: `{filename}`\n\n"
        f"Downloading: {pct:.1f}%\n"
        f"  `[{bar}]`\n"
        f"Downloaded: {format_size(current)} of {format_size(total)}"
    )


def compile_archive_prompt_text(job_id: str, filename: str) -> str:
    return (
        f"**Job #{job_id} - Archive Detected**\n"
        f"------------------------------------\n"
        f"- **File**: `{filename}`\n\n"
        "Do you want to upload the archive file only, or extract its contents and upload both?"
    )


def compile_archive_choice_status_text(job_id: str, filename: str, choice_str: str) -> str:
    return (
        f"**Job #{job_id} - Archive Choice**\n"
        f"------------------------------------\n"
        f"- **File**: `{filename}`\n"
        f"- **Selected**: `{choice_str}`\n\n"
        "Processing choice..."
    )


def compile_conversion_prompt_text(job_id: str, filename: str) -> str:
    return (
        f"**Job #{job_id} - Incompatible Video Format**\n"
        f"------------------------------------\n"
        f"- **File**: `{filename}`\n\n"
        "The file format is not natively supported for Telegram inline streaming. "
        "Do you want to convert it to MP4 first, or upload the original as a document?"
    )


def compile_audio_conversion_prompt_text(job_id: str, filename: str) -> str:
    return (
        f"**Job #{job_id} - High Quality Audio Format**\n"
        f"------------------------------------\n"
        f"- **File**: `{filename}`\n\n"
        "The file is in a lossless or uncompressed audio format. "
        "Do you want to convert it to MP3 (optimized & mastered using Pedalboard) first, or upload the original?"
    )


def compile_conversion_choice_status_text(job_id: str, filename: str, choice_str: str) -> str:
    return (
        f"**Job #{job_id} - Media Conversion**\n"
        f"------------------------------------\n"
        f"- **File**: `{filename}`\n\n"
        f"Choice selected: **{choice_str}**"
    )


def compile_extraction_status_text(job_id: str, filename: str) -> str:
    return (
        f"**Job #{job_id} - Archive Extraction**\n"
        f"------------------------------------\n"
        f"Extracting `{filename}`..."
    )


def compile_conversion_running_status_text(job_id: str, filename: str) -> str:
    return (
        f"**Job #{job_id} - Media Conversion**\n"
        f"------------------------------------\n"
        f"Converting `{filename}` to standard MP4 container..."
    )


def compile_audio_conversion_running_status_text(job_id: str, filename: str) -> str:
    return (
        f"**Job #{job_id} - Audio Processing & Conversion**\n"
        f"------------------------------------\n"
        f"Applying Pedalboard mastering chain and converting `{filename}` to MP3..."
    )


def compile_conversion_failed_status_text(job_id: str, filename: str) -> str:
    return (
        f"**Job #{job_id} - Conversion Failed**\n"
        f"------------------------------------\n"
        f"Failed to convert `{filename}`. Uploading original as document."
    )


def compile_audio_conversion_failed_status_text(job_id: str, filename: str) -> str:
    return (
        f"**Job #{job_id} - Audio Processing Failed**\n"
        f"------------------------------------\n"
        f"Failed to process `{filename}`. Uploading original file."
    )


def compile_extraction_failed_status_text(job_id: str, filename: str) -> str:
    return (
        f"**Job #{job_id} - Extraction Failed**\n"
        f"------------------------------------\n"
        f"Failed to extract `{filename}`."
    )


def compile_extraction_success_status_text(job_id: str, filename: str) -> str:
    return (
        f"**Job #{job_id} - Archive Extracted**\n"
        f"------------------------------------\n"
        f"Successfully extracted `{filename}` into download directory."
    )


def compile_job_status_text(job, job_state) -> str:
    cleaned_url = job.url
    if job.url.startswith("[") and job.url.endswith("]"):
        try:
            import json
            parsed = json.loads(job.url)
            if parsed and isinstance(parsed, list):
                cleaned_url = parsed[0]
        except Exception:
            pass

def format_user_args(args_raw: Optional[str]) -> str:
    if not args_raw:
        return ""
    try:
        data = json.loads(args_raw)
        if isinstance(data, list):
            user_flags = [str(item) for item in data if item]
            return " ".join(user_flags)
        elif isinstance(data, dict):
            user_flags = []
            fmt = data.get("archive_format")
            if fmt:
                user_flags.append(f"-{fmt}")
            if data.get("mirror_pixeldrain"):
                user_flags.append("-pd")
            extra = data.get("custom_args") or data.get("extra_args")
            if isinstance(extra, list):
                user_flags.extend([str(x) for x in extra])
            elif isinstance(extra, str) and extra:
                user_flags.append(extra)
            return " ".join(user_flags)
    except Exception:
        return str(args_raw).strip()
    return ""


def compile_job_status_text(job: Job, job_state: JobState) -> str:
    cleaned_url = job.url
    if job.url.startswith("[") and job.url.endswith("]"):
        try:
            parsed = json.loads(job.url)
            if parsed and isinstance(parsed, list):
                cleaned_url = parsed[0]
        except Exception:
            pass

    is_torrent = (
        cleaned_url.startswith("magnet:") or
        cleaned_url.startswith("torrent:") or
        cleaned_url.endswith(".torrent") or
        "magnet:?xt=" in cleaned_url
    )

    is_gdrive = (
        cleaned_url.startswith("gdrive:") or
        cleaned_url.startswith("gd2tg:") or
        "drive.google.com" in cleaned_url or
        "docs.google.com" in cleaned_url
    )

    # Determine active tool dynamically based on current phase
    if getattr(job_state, "is_archiving", False):
        import shutil
        active_tool = "7z" if shutil.which("7z") else ("zip" if shutil.which("zip") else "zipfile")
    elif getattr(job_state, "is_converting", False):
        active_tool = "FFmpeg"
    elif job.status == "uploading" or job_state.sent > 0 or job_state.current_upload_file:
        active_tool = "Pyrogram (Telegram Uploader)"
    elif is_gdrive:
        active_tool = "Google Drive API"
    elif is_torrent:
        active_tool = "aria2c"
    elif cleaned_url.startswith("unzip:"):
        active_tool = "Pyrogram Downloader"
    else:
        active_tool = "gallery-dl"

    split_str = "Enabled" if job.split_large_files else "Disabled"
    
    text = (
        f"**Active Task Details**\n"
        f"------------------------------------\n"
        f"**Job ID:** `{job.id}`\n"
        f"**Status:** `{job.status.upper()}`\n"
    )

    if is_torrent:
        torrent_name = getattr(job_state, "torrent_name", None)
        if torrent_name:
            text += f"**Name:** `{torrent_name}`\n"
        elif cleaned_url.startswith("torrent:"):
            torrent_path = cleaned_url[len("torrent:"):]
            name = Path(torrent_path).name
            text += f"**Torrent File:** `{name}`\n"
        else:
            magnet_disp = cleaned_url[:50] + "..." if len(cleaned_url) > 50 else cleaned_url
            text += f"**Magnet Link:** `{magnet_disp}`\n"
    else:
        text += f"**URL:** {format_url_display(job.url)}\n"
        user_args_str = format_user_args(job.args)
        if user_args_str:
            text += f"**Args:** `{user_args_str}`\n"
            
    text += f"**Split > 2GB:** `{split_str}`\n"
    text += f"------------------------------------\n"

    if not job_state.downloader_done.is_set():
        dl_speed_str = format_size(job_state.download_speed)
        dl_bytes_str = format_size(job_state.total_downloaded_bytes)
        dl_tool = "Google Drive API" if is_gdrive else ("aria2c" if is_torrent else ("Pyrogram Downloader" if cleaned_url.startswith("unzip:") else "gallery-dl"))
        
        if is_gdrive:
            marquee = make_marquee_bar()
            text += (
                f"**GDrive Downloader Metrics**\n"
                f"| Tool: `{dl_tool}`\n"
                f"| `[{marquee}]`\n"
                f"| Downloaded: `{dl_bytes_str}`\n"
                f"| Speed: `{dl_speed_str}/s`\n"
            )
            if job_state.current_download_file:
                text += f"| Current File: `{job_state.current_download_file}`\n"
        elif is_torrent:
            bar = make_progress_bar(job_state.download_pct)
            seeders = getattr(job_state, "torrent_seeders", 0)
            peers = getattr(job_state, "torrent_peers", 0)
            text += (
                f"**Downloader Metrics**\n"
                f"| Tool: `{dl_tool}`\n"
                f"| Progress: `{job_state.download_pct:.1f}%`\n"
                f"| `[{bar}]`\n"
                f"| Downloaded: `{dl_bytes_str}`\n"
                f"| Speed: `{dl_speed_str}/s`\n"
                f"| Peers: `Seeders: {seeders} | Leechers/Peers: {peers}`\n"
            )
        else:
            marquee = make_marquee_bar()
            text += (
                f"**Downloader Metrics**\n"
                f"| Tool: `{dl_tool}`\n"
                f"| Files Downloaded: `{job_state.download_count}`\n"
                f"| `[{marquee}]`\n"
                f"| Downloaded: `{dl_bytes_str}`\n"
                f"| Speed: `{dl_speed_str}/s`\n"
            )
            if job_state.current_download_file:
                text += f"| Current File: `{job_state.current_download_file}`\n"
    elif getattr(job_state, "is_archiving", False):
        import shutil
        archiver_tool = "7z" if shutil.which("7z") else ("zip" if shutil.which("zip") else "zipfile")
        fmt = getattr(job_state, "archive_format", "ZIP") or "ZIP"
        text += (
            f"**Folder Compression & Archiving**\n"
            f"| Tool: `{archiver_tool}`\n"
            f"| Format: `{fmt.upper()}`\n"
            f"| Status: `Compressing downloaded folders...`\n"
        )
    elif getattr(job_state, "is_converting", False):
        conv_file = getattr(job_state, "conversion_file", "media file")
        text += (
            f"**Media Conversion**\n"
            f"| Tool: `FFmpeg`\n"
            f"| Converting File: `{conv_file}`\n"
            f"| *(Converting format to standard MP4 container)*\n"
        )
    elif job.status == "uploading" or job_state.sent > 0 or job_state.current_upload_file:
        ul_speed_str = format_size(job_state.upload_speed)
        text += (
            f"**Uploader Metrics**\n"
            f"| Tool: `Pyrogram (Telegram Uploader)`\n"
            f"| Files Sent: `{job_state.sent} / {job.total_files if job.total_files > 0 else 'Calculating'}`\n"
            f"| Files Skipped: `{len(job_state.skipped)}`\n"
        )
        if job_state.current_upload_file:
            bar = make_progress_bar(job_state.current_upload_pct)
            text += (
                f"| Current File: `{job_state.current_upload_file}`\n"
                f"| Progress: `{job_state.current_upload_pct:.1f}%`\n"
                f"| `[{bar}]`\n"
                f"| Speed: `{ul_speed_str}/s`\n"
            )
    else:
        text += f"**Status:** `{job.status.upper()}`\n"

    return text
