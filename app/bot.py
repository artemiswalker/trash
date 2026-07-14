from __future__ import annotations

import asyncio
import json
import logging
import logging.handlers
import random
import shutil
import time
from pathlib import Path

from pyrogram import Client, filters, idle
from pyrogram.enums import ChatType
from pyrogram.types import Message, CallbackQuery, LinkPreviewOptions, ForceReply

from .config import settings
from .db import Job, JobStatus, JobStore
from .downloader import GalleryDLNotFound, run_with_progress
from .uploader import UploadTooLarge, upload_file
from .middleware import is_job_owner
from .status import (
    format_size,
    make_progress_bar,
    safe_edit,
    format_url_display,
    compile_split_prompt_text,
    compile_queued_status_text,
    compile_unzip_download_status_text,
    compile_archive_prompt_text,
    compile_conversion_prompt_text,
    compile_extraction_status_text,
    compile_conversion_running_status_text,
    compile_conversion_failed_status_text,
    compile_extraction_failed_status_text,
    compile_extraction_success_status_text,
)
from . import status
from .archive import (
    _archive_ids,
    _archive_events,
    _archive_choices,
    _extracted_archives,
    _extracted_file_names,
    ARCHIVE_EXT,
    extract_archive_async,
    handle_archive_choice,
    ArchivePasswordRequired,
)
from .conversion import (
    _conversion_ids,
    _conversion_events,
    _conversion_choices,
    _converted_files,
    CONVERSION_EXT,
    convert_media_async,
    handle_conversion_choice,
)

log = logging.getLogger("tgdl_bot")


def setup_logging() -> None:
    level = getattr(logging, settings.log_level.upper(), logging.INFO)
    fmt = logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s")

    root = logging.getLogger()
    root.setLevel(level)

    console = logging.StreamHandler()
    console.setFormatter(fmt)
    root.addHandler(console)

    file_handler = logging.handlers.RotatingFileHandler(
        settings.log_dir / "bot.log", maxBytes=10_000_000, backupCount=5
    )
    file_handler.setFormatter(fmt)
    root.addHandler(file_handler)

    logging.getLogger("pyrogram").setLevel(logging.WARNING)


store = JobStore(settings.db_path)
app = Client(
    "tgdl_bot",
    api_id=settings.tg_api_id,
    api_hash=settings.tg_api_hash,
    bot_token=settings.tg_bot_token,
    workdir=str(settings.data_dir),
)
from .queue_manager import queue_manager, _password_prompt_events, _password_prompt_messages
_shutdown_event = asyncio.Event()


async def log_upload(job_id: int, filename: str) -> None:
    log_path = settings.log_dir / "uploads.log"
    log_path.parent.mkdir(parents=True, exist_ok=True)

    def append_to_file():
        with open(log_path, "a", encoding="utf-8") as f:
            f.write(f"{time.strftime('%Y-%m-%d %H:%M:%S')} - Job #{job_id} - Uploaded: {filename}\n")

    await asyncio.to_thread(append_to_file)


async def cleanup_orphaned_directories() -> None:
    """Scan downloads directory and delete any directories job_{id} that are
    not active, queued, or waiting in the database."""
    if not settings.downloads_dir.exists():
        return

    try:
        cur = await store.db.execute(
            "SELECT id FROM jobs WHERE status IN ('queued', 'downloading', 'uploading', 'waiting')"
        )
        rows = await cur.fetchall()
        keep_ids = {f"job_{r['id']}" for r in rows}

        def run_cleanup():
            for p in settings.downloads_dir.iterdir():
                if p.is_dir() and p.name.startswith("job_"):
                    if p.name not in keep_ids:
                        log.info("Cleaning up orphaned directory: %s", p)
                        shutil.rmtree(p, ignore_errors=True)

        await asyncio.to_thread(run_cleanup)
    except Exception:
        log.exception("Error during orphaned directories cleanup")


async def safe_edit(chat_id: int, message_id: int, text: str) -> bool:
    from pyrogram.errors import FloodWait, MessageNotModified

    try:
        await app.edit_message_text(chat_id, message_id, text, link_preview_options=LinkPreviewOptions(is_disabled=True))
        return True
    except MessageNotModified:
        return True
    except FloodWait as e:
        log.warning("Telegram FloodWait: waiting %s seconds", e.value)
        await asyncio.sleep(e.value + 1)
        return False
    except Exception as e:
        log.warning("Failed to edit status message: %s", e)
        return False


async def safe_send(chat_id: int, text: str, **kwargs) -> Message | None:
    from pyrogram.errors import FloodWait
    for _ in range(3):
        try:
            return await app.send_message(chat_id, text, **kwargs)
        except FloodWait as e:
            log.warning("Telegram FloodWait: waiting %s seconds on send", e.value)
            await asyncio.sleep(e.value + 1)
        except Exception as e:
            log.warning("Failed to send message: %s", e)
            return None
    return None


def format_size(size_bytes: float) -> str:
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if size_bytes < 1024.0:
            return f"{size_bytes:.1f} {unit}"
        size_bytes /= 1024.0
    return f"{size_bytes:.1f} PB"



@app.on_message(filters.command("start"))
async def start_cmd(_, message: Message) -> None:
    text = (
        "Send me links to download media files (e.g., videos, photo albums) and upload them to Telegram.\n\n"
        "**Usage:**\n"
        "• **Single URL**: `https://example.com/album1`\n"
        "• **Shorthand options**: `https://example.com/album1 pages=1-16`\n"
        "• **Multiple URLs**: `https://example.com/album1 https://example.com/album2`\n"
        "• **Links File (.txt)**: Send a `.txt` file containing URLs (one per line) and **reply to it** with `/gdl` to process them.\n\n"
        "**Commands:**\n"
        "• /status — View active download/upload metrics or queued jobs.\n"
        "• /cancel — Cancel the active task and clean up temporary storage.\n\n"
        "**Large Files:**\n"
        "• If a file exceeds 1.95GB, you will be prompted to either split it into sub-2GB playable segment files or skip it."
    )
    await message.reply_text(text, link_preview_options=LinkPreviewOptions(is_disabled=True))


@app.on_message(filters.command("status"))
async def status_cmd(_, message: Message) -> None:
    import json
    chat_id = message.chat.id

    active_jobs = queue_manager.get_active_jobs_for_chat(chat_id)
    response = ""
    
    if active_jobs:
        for job_state in active_jobs:
            job = await store.get_job(job_state.job_id)
            if not job:
                continue
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

            if job.status == JobStatus.DOWNLOADING or not job_state.downloader_done.is_set():
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
            
            if job.status == JobStatus.UPLOADING or job_state.sent > 0 or job_state.current_upload_file:
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
            
            response += job_text + "\n"
    else:
        response = "**Bot Status: Idle**\nNo active download/upload task is currently running.\n"

    queued = [q for q in await store.queued_jobs() if is_job_owner(chat_id, q)]

    cur = await store.db.execute("SELECT * FROM jobs WHERE status = 'waiting' AND chat_id = ? ORDER BY id", (chat_id,))
    waiting_rows = await cur.fetchall()
    waiting = [store._row_to_job(r) for r in waiting_rows]

    if queued:
        response += f"\n**Queued Jobs ({len(queued)})**:"
        for i, q_job in enumerate(queued[:5], 1):
            q_parsed = []
            if q_job.args:
                try:
                    q_parsed = json.loads(q_job.args)
                except Exception:
                    pass
            q_args_str = f" (Args: `{' '.join(q_parsed)}`)" if q_parsed else ""
            response += f"\n{i}. Job #{q_job.id}: {format_url_display(q_job.url)}{q_args_str}"
        if len(queued) > 5:
            response += f"\n…and {len(queued) - 5} more queued job(s)"

    if waiting:
        response += f"\n\n**Awaiting Split Choice Confirmation ({len(waiting)})**:"
        for i, w_job in enumerate(waiting[:5], 1):
            w_parsed = []
            if w_job.args:
                try:
                    w_parsed = json.loads(w_job.args)
                except Exception:
                    pass
            w_args_str = f" (Args: `{' '.join(w_parsed)}`)" if w_parsed else ""
            response += f"\n{i}. Job #{w_job.id}: {format_url_display(w_job.url)}{w_args_str}"
        if len(waiting) > 5:
            response += f"\n…and {len(waiting) - 5} more awaiting confirmation"

    await message.reply_text(response, link_preview_options=LinkPreviewOptions(is_disabled=True))


@app.on_message(filters.command("cancel"))
async def cancel_cmd(_, message: Message) -> None:
    chat_id = message.chat.id
    
    cmd_parts = message.text.split()
    if len(cmd_parts) > 1:
        try:
            job_id = int(cmd_parts[1])
        except ValueError:
            await message.reply_text("Invalid job ID format. Please use `/cancel <job_id>` or just `/cancel`.")
            return

        job = await store.get_job(job_id)
        if not job or not is_job_owner(chat_id, job):
            await message.reply_text(f"Job #{job_id} not found or not owned by you.")
            return

        if job.status in (JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.CANCELLED):
            await message.reply_text(f"Job #{job_id} is already in `{job.status}` state.")
            return

        cancelled = await queue_manager.cancel_job(job.id)
        if cancelled:
            await message.reply_text(f"Instantly aborted and cancelled active job #{job.id}.")
        else:
            await store.update_progress(job.id, status=JobStatus.CANCELLED)
            await message.reply_text(f"Job #{job.id} has been cancelled successfully.")
        return

    cur = await store.db.execute(
        "SELECT id, url, status FROM jobs WHERE chat_id = ? AND status IN ('queued', 'waiting', 'downloading', 'uploading')",
        (chat_id,)
    )
    rows = await cur.fetchall()
    if not rows:
        await message.reply_text("No active or queued jobs found for this chat.")
        return

    if len(rows) == 1:
        job_id = rows[0]["id"]
        job_status = rows[0]["status"]
        cancelled = await queue_manager.cancel_job(job_id)
        if not cancelled:
            await store.update_progress(job_id, status=JobStatus.CANCELLED)
        await message.reply_text(f"Job #{job_id} ({job_status}) has been cancelled.")
        return

    from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton
    buttons = []
    for r in rows:
        jid = r["id"]
        url = r["url"]
        jstatus = r["status"]
        label = url.split(":", 1)[1] if ":" in url else url
        label = label.split("/")[-1] or label
        if len(label) > 25:
            label = label[:22] + "…"
            
        btn_text = f"#{jid} - {label} ({jstatus})"
        buttons.append([InlineKeyboardButton(btn_text, callback_data=f"cancel_job:{jid}")])

    await message.reply_text(
        "**Select a job to cancel:**",
        reply_markup=InlineKeyboardMarkup(buttons)
    )


@app.on_message(filters.command("gdl"))
async def gdl_cmd(_, message: Message) -> None:
    if not message.reply_to_message or not message.reply_to_message.document:
        await message.reply_text("Reply to a .txt file containing URLs with `/gdl [options]`.")
        return

    doc = message.reply_to_message.document
    if not (doc.file_name.endswith(".txt") or (doc.mime_type and doc.mime_type.startswith("text/"))):
        await message.reply_text("Please reply to a text (.txt) file.")
        return

    temp_path = await message.reply_to_message.download()
    if not temp_path or not Path(temp_path).exists():
        await message.reply_text("Failed to download the file.")
        return

    try:
        content = Path(temp_path).read_text(encoding="utf-8", errors="ignore")
    except Exception as e:
        await message.reply_text(f"Failed to read the file: {e}")
        return
    finally:
        Path(temp_path).unlink(missing_ok=True)

    urls = []
    for line in content.splitlines():
        line = line.strip()
        if line.startswith(("http://", "https://")):
            urls.append(line)

    if not urls:
        await message.reply_text("No valid URLs found in the text file.")
        return

    urls_json = json.dumps(urls)

    job = await store.create_job(message.chat.id, urls_json, split_large_files=1, args=None)
    await store.update_progress(job.id, status="waiting")

    from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton
    keyboard = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("Yes, split them", callback_data=f"split_yes:{job.id}"),
            InlineKeyboardButton("No, skip them", callback_data=f"split_no:{job.id}")
        ]
    ])

    prompt_text = compile_split_prompt_text(job.id, urls_json)
    status_msg = await message.reply_text(
        prompt_text,
        reply_markup=keyboard,
        link_preview_options=LinkPreviewOptions(is_disabled=True)
    )
    await store.set_status_message(job.id, status_msg.id)


@app.on_message(filters.command("tor"))
async def tor_cmd(_, message: Message) -> None:
    target_url = None

    if message.reply_to_message and message.reply_to_message.document:
        doc = message.reply_to_message.document
        if doc.file_name.endswith(".torrent") or (doc.mime_type and "torrent" in doc.mime_type):
            temp_path = await message.reply_to_message.download()
            if temp_path:
                torrents_dir = settings.data_dir / "torrents"
                torrents_dir.mkdir(parents=True, exist_ok=True)
                
                import uuid
                dest_path = torrents_dir / f"{uuid.uuid4()}.torrent"
                import shutil
                try:
                    shutil.move(temp_path, dest_path)
                    target_url = f"torrent:{dest_path.absolute()}"
                except Exception as e:
                    log.exception("Failed to save replied torrent file")
                    await message.reply_text(f"Failed to save torrent file: {e}")
                    return
            else:
                await message.reply_text("Failed to download replied torrent file.")
                return

    if not target_url:
        cmd_args = message.command
        if len(cmd_args) < 2:
            await message.reply_text("Send a magnet link or reply to a `.torrent` file with `/tor <magnet/url>`.")
            return
        
        input_url = cmd_args[1].strip()
        if input_url.startswith("magnet:") or input_url.startswith(("http://", "https://")):
            target_url = input_url
        else:
            await message.reply_text("Please provide a valid magnet link or torrent URL.")
            return

    job = await store.create_job(message.chat.id, target_url, split_large_files=1, args=None)
    await store.update_progress(job.id, status="waiting")

    from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton
    keyboard = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("Yes, split them", callback_data=f"split_yes:{job.id}"),
            InlineKeyboardButton("No, skip them", callback_data=f"split_no:{job.id}")
        ]
    ])

    url_display = target_url
    if target_url.startswith("magnet:"):
        url_display = target_url[:60] + "..." if len(target_url) > 60 else target_url
    elif target_url.startswith("torrent:"):
        url_display = "local torrent file"

    prompt_text = compile_split_prompt_text(job.id, url_display, is_torrent=True)
    status_msg = await message.reply_text(
        prompt_text,
        reply_markup=keyboard,
        link_preview_options=LinkPreviewOptions(is_disabled=True)
    )
    await store.set_status_message(job.id, status_msg.id)


@app.on_message(filters.command("unzip"))
async def unzip_cmd(_, message: Message) -> None:
    if not message.reply_to_message or not message.reply_to_message.document:
        await message.reply_text("Please reply to an archive file (.zip, .rar, .7z, etc.) with `/unzip`.")
        return

    doc = message.reply_to_message.document
    filename = doc.file_name or "archive.zip"
    ext = Path(filename).suffix.lower()
    
    from .archive import ARCHIVE_EXT
    if ext not in ARCHIVE_EXT:
        supported_list = ", ".join(sorted(ARCHIVE_EXT))
        await message.reply_text(f"Unsupported archive format. Supported formats: {supported_list}")
        return

    # Parse optional password
    cmd_parts = message.text.split(maxsplit=1)
    password = cmd_parts[1].strip() if len(cmd_parts) > 1 else None
    
    import json
    args_json = json.dumps({"password": password}) if password else None

    job = await store.create_job(message.chat.id, f"unzip:{filename}", split_large_files=1, args=args_json)
    await store.update_progress(job.id, status="waiting")

    dest_dir = (settings.downloads_dir / job.download_dir).resolve()
    dest_dir.mkdir(parents=True, exist_ok=True)
    
    status_msg = await message.reply_text(
        f"**Job #{job.id} registered**\n"
        f"- **Archive**: `{filename}`\n\n"
        "Downloading archive to VPS..."
    )
    await store.set_status_message(job.id, status_msg.id)

    last_edit_time = 0.0
    async def on_download_progress(current, total):
        nonlocal last_edit_time
        import time
        now = time.time()
        if now - last_edit_time < 3.0 and current != total:
            return
        last_edit_time = now
        try:
            await status_msg.edit_text(
                compile_unzip_download_status_text(job.id, filename, current, total)
            )
        except Exception:
            pass

    try:
        await message.reply_to_message.download(
            file_name=str(dest_dir / filename),
            progress=on_download_progress
        )
    except Exception as e:
        log.exception("Failed to download replied archive file")
        await status_msg.edit_text(f"Failed to download archive: {e}")
        await store.update_progress(job.id, status=JobStatus.FAILED, error=str(e), url="")
        return

    limit_2gb = int(1.95 * 1024 * 1024 * 1024)
    if doc.file_size and doc.file_size < limit_2gb:
        await store.db.execute(
            "UPDATE jobs SET status = ?, split_large_files = ? WHERE id = ?",
            (JobStatus.QUEUED, 1, job.id)
        )
        await store.db.commit()
        await status_msg.edit_text(
            compile_queued_status_text(job.id, f"unzip:{filename}", "")
        )
        await queue_manager.add_job(job.id)
        return

    from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton
    keyboard = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("Yes, split them", callback_data=f"split_yes:{job.id}"),
            InlineKeyboardButton("No, skip them", callback_data=f"split_no:{job.id}")
        ]
    ])

    prompt_text = compile_split_prompt_text(job.id, filename, is_unzip=True)
    await status_msg.edit_text(prompt_text, reply_markup=keyboard)


def extract_domain_name(url: str) -> str:
    from urllib.parse import urlparse
    try:
        netloc = urlparse(url).netloc.lower()
        if not netloc:
            return "generic"
        if ":" in netloc:
            netloc = netloc.split(":")[0]
        parts = netloc.split(".")
        if len(parts) >= 3 and parts[-2] in ("co", "com", "org", "net", "gov", "edu", "ac"):
            return parts[-3]
        if len(parts) >= 2:
            return parts[-2]
        return parts[0]
    except Exception:
        return "generic"


def format_url_display(url_field: str) -> str:
    import json
    try:
        urls = json.loads(url_field)
        if isinstance(urls, list):
            if len(urls) > 1:
                return f"{urls[0]} (and {len(urls) - 1} more)"
            return urls[0]
    except Exception:
        pass
    return url_field


def sanitize_gdl_args(args: list[str], url: Optional[str | list[str]] = None) -> list[str]:
    sanitized = []
    skip_next = False

    is_multi_url = False
    base_url = None
    if url:
        if isinstance(url, list):
            is_multi_url = len(url) > 1
            base_url = url[0] if url else None
        elif isinstance(url, str):
            if url.startswith("["):
                try:
                    import json
                    parsed = json.loads(url)
                    if isinstance(parsed, list):
                        is_multi_url = len(parsed) > 1
                        base_url = parsed[0] if parsed else None
                except Exception:
                    pass
            if not base_url:
                base_url = url

    rewritten_args = []
    i = 0
    while i < len(args):
        arg = args[i]
        if arg == "--extractor-argument" and i + 1 < len(args):
            val = args[i + 1]
            if ":" in val:
                parts_colon = val.split(":", 1)
                extractor = parts_colon[0]
                rest = parts_colon[1]
                rewritten_args.append("-o")
                rewritten_args.append(f"extractor.{extractor}.{rest}")
            else:
                rewritten_args.append("-o")
                if val.startswith("extractor."):
                    rewritten_args.append(val)
                else:
                    rewritten_args.append(f"extractor.{val}")
            i += 2
        elif not arg.startswith("-") and "=" in arg:
            if ":" in arg:
                parts_colon = arg.split(":", 1)
                extractor = parts_colon[0]
                rest = parts_colon[1]
                rewritten_args.append("-o")
                rewritten_args.append(f"extractor.{extractor}.{rest}")
                i += 1
            else:
                if is_multi_url:
                    i += 1
                else:
                    extractor = extract_domain_name(base_url) if base_url else "generic"
                    rewritten_args.append("-o")
                    rewritten_args.append(f"extractor.{extractor}.{arg}")
                    i += 1
        else:
            rewritten_args.append(arg)
            i += 1

    for idx, arg in enumerate(rewritten_args):
        if skip_next:
            skip_next = False
            continue

        if arg in ("-d", "--directory", "--config"):
            skip_next = True
            continue

        if arg in ("-h", "--help", "--version"):
            continue

        if arg in ("-o", "--option"):
            if idx + 1 < len(rewritten_args):
                val = rewritten_args[idx + 1]
                if "base-directory" in val or "directory" in val or "path" in val:
                    skip_next = True
                    continue
            else:
                continue

        sanitized.append(arg)
    return sanitized


@app.on_message(filters.reply & filters.text, group=-1)
async def handle_password_reply(_, message: Message) -> None:
    reply = message.reply_to_message
    if not reply or not reply.id:
        return
    
    prompt_info = _password_prompt_messages.get(reply.id)
    if not prompt_info:
        return
        
    message.stop_propagation()
    
    job_id, archive_id, chat_id = prompt_info
    
    job = await store.get_job(job_id)
    if not job or not is_job_owner(message.chat.id, job):
        return

    password = message.text.strip()
    
    if job_id in _password_prompt_events and archive_id in _password_prompt_events[job_id]:
        event, data = _password_prompt_events[job_id][archive_id]
        data["password"] = password
        event.set()
        
    try:
        await message.delete()
    except Exception:
        pass
    
    _password_prompt_messages.pop(reply.id, None)


@app.on_message(filters.text & ~filters.command(["start", "status", "cancel", "gdl", "tor", "unzip"]))
async def handle_link(_, message: Message) -> None:
    text = (message.text or "").strip()
    is_private = message.chat.type == ChatType.PRIVATE

    import shlex
    import json

    try:
        tokens = shlex.split(text)
    except Exception:
        tokens = text.split()

    if not tokens:
        return

    urls = []
    raw_args = []
    for token in tokens:
        if token.startswith(("http://", "https://")):
            urls.append(token)
        else:
            raw_args.append(token)

    if not urls:
        if is_private:
            await message.reply_text("Send an actual URL.")
        return

    sanitized_args = sanitize_gdl_args(raw_args, urls)
    args_json = json.dumps(sanitized_args) if sanitized_args else None
    urls_json = json.dumps(urls)

    job = await store.create_job(message.chat.id, urls_json, split_large_files=1, args=args_json)
    await store.update_progress(job.id, status="waiting")

    from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton
    keyboard = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("Yes, split them", callback_data=f"split_yes:{job.id}"),
            InlineKeyboardButton("No, skip them", callback_data=f"split_no:{job.id}")
        ]
    ])

    args_display = f"\n- **Args**: `{' '.join(sanitized_args)}`" if sanitized_args else ""
    url_display = format_url_display(urls_json)
    prompt_text = (
        f"**Job #{job.id} registered**\n"
        f"- **URL**: {url_display}{args_display}\n\n"
        "Do you want to split files larger than 2GB for this job?"
    )
    status_msg = await message.reply_text(
        prompt_text,
        reply_markup=keyboard,
        link_preview_options=LinkPreviewOptions(is_disabled=True)
    )
    await store.set_status_message(job.id, status_msg.id)


@app.on_callback_query(filters.regex(r"^split_(yes|no):(\d+)$"))
async def handle_split_choice(_, callback_query: CallbackQuery) -> None:
    data = callback_query.data
    choice, job_id_str = data.split(":")
    job_id = int(job_id_str)
    split_choice = 1 if choice == "split_yes" else 0

    job = await store.get_job(job_id)
    if not job:
        await callback_query.answer("Job not found.", show_alert=True)
        return

    if not is_job_owner(callback_query.message.chat.id, job):
        await callback_query.answer("Unauthorized: You cannot manage split choices for this job.", show_alert=True)
        return

    if job.status != "waiting":
        await callback_query.answer("This job choice has already been processed.")
        return

    await store.db.execute(
        "UPDATE jobs SET status = ?, split_large_files = ? WHERE id = ?",
        (JobStatus.QUEUED, split_choice, job_id)
    )
    await store.db.commit()

    import json
    parsed_args = []
    if job.args:
        try:
            parsed_args = json.loads(job.args)
        except Exception:
            pass
    args_display = f"\n- **Args**: `{' '.join(parsed_args)}`" if parsed_args else ""
    status_text = compile_queued_status_text(job_id, job.url, args_display)
    await callback_query.message.edit_text(status_text, link_preview_options=LinkPreviewOptions(is_disabled=True))
    await callback_query.answer("Choice registered.")
    await queue_manager.add_job(job_id)


@app.on_callback_query(filters.regex(r"^archive_(only|ext):(\d+):(.+)$"))
async def handle_archive_choice_cb(_, callback_query: CallbackQuery) -> None:
    await handle_archive_choice(callback_query, store, is_job_owner)


@app.on_callback_query(filters.regex(r"^convert_(mp4|orig):(\d+):(.+)$"))
async def handle_conversion_choice_cb(client: Client, callback_query: CallbackQuery) -> None:
    await handle_conversion_choice(client, callback_query, store, is_job_owner)


@app.on_callback_query(filters.regex(r"^cancel_job:(\d+)$"))
async def handle_cancel_job_cb(_, callback_query: CallbackQuery) -> None:
    data = callback_query.data
    job_id = int(data.split(":")[1])
    chat_id = callback_query.message.chat.id

    job = await store.get_job(job_id)
    if not job:
        await callback_query.answer("Job not found.", show_alert=True)
        return

    if not is_job_owner(chat_id, job):
        await callback_query.answer("Unauthorized: You do not own this job.", show_alert=True)
        return

    if job.status in (JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.CANCELLED):
        await callback_query.answer(f"Job is already {job.status}.")
        try:
            await callback_query.message.delete()
        except Exception:
            pass
        return

    cancelled = await queue_manager.cancel_job(job_id)
    if not cancelled:
        await store.update_progress(job_id, status=JobStatus.CANCELLED)

    await callback_query.answer(f"Job #{job_id} cancelled.")
    try:
        await callback_query.message.edit_text(
            f"**Job #{job_id} cancelled successfully** by owner.",
            reply_markup=None
        )
    except Exception:
        pass


async def requeue_incomplete_jobs() -> None:
    """On startup, put back-in-progress and queued jobs onto the queue so
    interrupted runs resume instead of silently vanishing."""
    for job in [*await store.resumable_jobs(), *await store.queued_jobs()]:
        log.info("Resuming job #%s (%s)", job.id, job.status)
        await queue_manager.add_job(job.id)


async def _startup() -> None:
    await store.open()
    await cleanup_orphaned_directories()
    await queue_manager.start(app, store)


async def main() -> None:
    setup_logging()

    if shutil.which("gallery-dl") is None:
        log.warning(
            "gallery-dl not found on PATH — install with "
            "`pip install gallery-dl --break-system-packages`"
        )

    if shutil.which("aria2c") is None:
        log.warning(
            "aria2c not found on PATH — torrent downloads will fail. "
            "Please install it using: `sudo apt-get install aria2`"
        )

    await _startup()

    try:
        async with app:
            log.info("Bot started.")
            try:
                from pyrogram.types import BotCommand
                await app.set_bot_commands([
                    BotCommand("start", "Start the bot and see instructions"),
                    BotCommand("gdl", "Process replied .txt links file with optional arguments"),
                    BotCommand("tor", "Download torrent/magnet link or replied .torrent file"),
                    BotCommand("unzip", "Extract archive (.zip, .rar, .7z) and upload contents"),
                    BotCommand("status", "Check current active job details or queue status"),
                    BotCommand("cancel", "Instantly abort the active download/upload task"),
                ])
                log.info("Bot commands set successfully.")
            except Exception as e:
                log.warning("Failed to set bot commands: %s", e)
            await requeue_incomplete_jobs()
            await idle()

        log.info("Shutting down queue manager and workers…")
        await queue_manager.stop()
    finally:
        await store.close()
        log.info("Shutdown complete.")


if __name__ == "__main__":
    asyncio.get_event_loop().run_until_complete(main())
