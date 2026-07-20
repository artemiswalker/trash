import json
from pathlib import Path
from typing import Optional

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

    return (
        f"**Queued (job #{job_id})**\n"
        f"------------------------------------\n"
        f"- **URL**: {format_url_display(url)}{args_display}"
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

    is_torrent = (
        cleaned_url.startswith("magnet:") or
        cleaned_url.startswith("torrent:") or
        cleaned_url.endswith(".torrent") or
        "magnet:?xt=" in cleaned_url
    )

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
        text += f"**Tool:** `aria2c`\n"
    else:
        text += f"**URL:** {format_url_display(job.url)}\n"
        parsed_args = []
        if job.args:
            try:
                parsed_args = json.loads(job.args)
            except Exception:
                pass
        if parsed_args:
            text += f"**Args:** `{' '.join(parsed_args)}`\n"
            
    text += f"**Split > 2GB:** `{split_str}`\n"
    text += f"------------------------------------\n"

    if not job_state.downloader_done.is_set():
        dl_speed_str = format_size(job_state.download_speed)
        dl_bytes_str = format_size(job_state.total_downloaded_bytes)
        
        text += "**Downloader Metrics**\n"
        if is_torrent:
            bar = make_progress_bar(job_state.download_pct)
            seeders = getattr(job_state, "torrent_seeders", 0)
            peers = getattr(job_state, "torrent_peers", 0)
            text += (
                f"| Progress: `{job_state.download_pct:.1f}%`\n"
                f"| `[{bar}]`\n"
                f"| Downloaded: `{dl_bytes_str}`\n"
                f"| Speed: `{dl_speed_str}/s`\n"
                f"| Peers: `Seeders: {seeders} | Leechers/Peers: {peers}`\n"
            )
        else:
            marquee = make_marquee_bar()
            text += (
                f"| Files Downloaded: `{job_state.download_count}`\n"
                f"| `[{marquee}]`\n"
                f"| Downloaded: `{dl_bytes_str}`\n"
                f"| Speed: `{dl_speed_str}/s`\n"
            )
            if job_state.current_download_file:
                text += f"| Current File: `{job_state.current_download_file}`\n"
    elif getattr(job_state, "is_converting", False):
        conv_file = getattr(job_state, "conversion_file", "media file")
        text += (
            f"**Media Conversion**\n"
            f"| Converting File: `{conv_file}`\n"
            f"| *(Converting format to standard MP4 container)*\n"
        )
    elif job.status == "uploading" or job_state.sent > 0 or job_state.current_upload_file:
        ul_speed_str = format_size(job_state.upload_speed)
        text += (
            f"**Uploader Metrics**\n"
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
