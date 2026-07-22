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
from .uploader import UploadTooLarge, upload_file, upload_to_pixeldrain
from .middleware import is_job_owner
from .manager import (
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
from . import manager as status
from .manager.archive import (
    _archive_ids,
    _archive_events,
    _archive_choices,
    _extracted_archives,
    _extracted_file_names,
    ARCHIVE_EXT,
    extract_archive_async,
    handle_archive_choice,
    ArchivePasswordRequired,
    get_split_archive_info,
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
from .manager import queue_manager, _password_prompt_events, _password_prompt_messages
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



@app.on_message(filters.command(["start", "help"]))
async def start_cmd(_, message: Message) -> None:
    text = (
        "Send me links to download media files (e.g., videos, photo albums) and upload them to Telegram.\n\n"
        "**Usage:**\n"
        "• **Google Drive**: `/gd2tg <gdrive_link> [-zip|-7z] [-pd]` (downloads GDrive link & archives folders, add `-pd` to mirror unsplit archives to Pixeldrain).\n"
        "• **Single URL**: `https://example.com/album1`\n"
        "• **Shorthand options**: `https://example.com/album1 pages=1-16`\n"
        "• **Multiple URLs**: `https://example.com/album1 https://example.com/album2`\n"
        "• **Direct Torrent / Magnet**: Send a `magnet:` link or `.torrent` link directly to download it.\n"
        "• **Links File (.txt)**: Send a `.txt` file containing URLs (one per line) and **reply to it** with `/gdl` to process them.\n\n"
        "**Commands:**\n"
        "• /gd2tg — Download Google Drive link & upload to Telegram. To setup credentials, reply to any Service Account `.json` file with `/gd2tg` (0-browser setup).\n"
        "• /tor — Download a magnet link or `.torrent` file (e.g., `/tor magnet:?xt=...` or reply to a `.torrent` file with `/tor`).\n"
        "• /unzip — Reply to a zip/rar/7z archive with `/unzip [password]` to extract and upload its contents.\n"
        "• /pdup — Reply to a media file to upload it directly to Pixeldrain.\n"
        "• /status — View active download/upload metrics or queued jobs.\n"
        "• /cancel — Cancel the active task and clean up temporary storage.\n\n"
        "**Large Files:**\n"
        "• If a file exceeds 1.95GB, it will automatically split into sub-2GB segment files to satisfy Telegram limits."
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
            from .manager import compile_job_status_text
            job_text = compile_job_status_text(job, job_state)
            response += job_text + "\n"
    else:
        response = "**Bot Status: Idle**\nNo active download/upload task is currently running.\n"

    queued = [q for q in await store.queued_jobs() if is_job_owner(chat_id, q)]

    cur = await store.db.execute("SELECT * FROM jobs WHERE status = 'waiting' AND chat_id = ? ORDER BY created_at", (chat_id,))
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
        job_id = cmd_parts[1].strip()

        job = await store.get_job(job_id)
        if not job or not is_job_owner(chat_id, job):
            await message.reply_text(f"Job #{job_id} not found or not owned by you.")
            return

        if job.status in (JobStatus.DONE, JobStatus.FAILED, JobStatus.CANCELLED):
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
        ],
        [
            InlineKeyboardButton("Cancel", callback_data=f"cancel_job:{job.id}")
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
        parts = message.text.split(maxsplit=1)
        if len(parts) < 2:
            await message.reply_text("Send a magnet link or reply to a `.torrent` file with `/tor <magnet/url>`.")
            return
        
        input_url = parts[1].strip()
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
        ],
        [
            InlineKeyboardButton("Cancel", callback_data=f"cancel_job:{job.id}")
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


_split_archive_sessions: dict[int, dict] = {}


def compile_split_session_text(prefix: str, ext: str, parts: dict[int, Message]) -> str:
    sorted_parts = sorted(parts.keys())
    parts_list = []
    max_part = max(sorted_parts) if sorted_parts else 0
    
    for i in range(1, max_part + 2):
        if i in parts:
            filename = parts[i].document.file_name
            parts_list.append(f"**Part {i}**: `{filename}`")
        else:
            if i == 1 or i <= max_part:
                parts_list.append(f"**Part {i}**: _Waiting for file..._")
            else:
                break
                
    parts_str = "\n".join(parts_list)
    
    text = (
        f"**Split Archive Session**\n"
        f"- **Base Pattern**: `{prefix}.*`\n\n"
        f"**Instructions:**\n"
        f"Please upload/forward the remaining parts of this archive to this chat.\n\n"
        f"**Parts Received:**\n"
        f"{parts_str}\n\n"
        f"When all parts are uploaded, click **Start Extraction** below."
    )
    return text


@app.on_message(filters.command("unzip"))
async def unzip_cmd(_, message: Message) -> None:
    cmd_parts = (message.text or "").split(maxsplit=2)
    if len(cmd_parts) >= 2 and cmd_parts[1].lower() == "split":
        chat_id = message.chat.id
        user_id = message.from_user.id if message.from_user else chat_id
        session_key = chat_id
        
        print(f"[DEBUG_SPLIT] Starting /unzip split session. chat_id={chat_id}, user_id={user_id}", flush=True)
        
        if session_key in _split_archive_sessions:
            old_session = _split_archive_sessions.pop(session_key)
            if old_session.get("timeout_task"):
                old_session["timeout_task"].cancel()
            try:
                await old_session["status_msg"].edit_text("**Session replaced by a new one.**")
            except Exception:
                pass
                
        password = cmd_parts[2].strip() if len(cmd_parts) > 2 else None
        
        from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton
        
        def get_split_session_keyboard(c_id: int, u_id: int) -> InlineKeyboardMarkup:
            return InlineKeyboardMarkup([
                [
                    InlineKeyboardButton("Start Extraction", callback_data=f"split_start:{c_id}:{u_id}"),
                    InlineKeyboardButton("Cancel", callback_data=f"split_cancel:{c_id}:{u_id}")
                ]
            ])
            
        status_msg = await message.reply_text(
            "**Split Archive Session Started**\n\n"
            "Please send or forward the split archive parts (e.g. `.001`, `.002`, or `.part1.rar` files) to this chat.\n\n"
            "**Waiting for files...**",
            reply_markup=get_split_session_keyboard(chat_id, user_id)
        )
        
        async def split_session_timeout(c_id: int, delay: int = 300):
            await asyncio.sleep(delay)
            s_key = c_id
            if s_key in _split_archive_sessions:
                session = _split_archive_sessions.pop(s_key)
                try:
                    await session["status_msg"].edit_text("**Split Archive Session Expired** (Timeout due to inactivity).")
                except Exception:
                    pass

        timeout_task = asyncio.create_task(split_session_timeout(chat_id))
        
        _split_archive_sessions[session_key] = {
            "prefix": None,
            "ext": None,
            "pattern": None,
            "parts": {},
            "status_msg": status_msg,
            "dest_dir": None,
            "job_id": None,
            "password": password,
            "timeout_task": timeout_task
        }
        return

    if not message.reply_to_message or not message.reply_to_message.document:
        await message.reply_text("Please reply to an archive file (.zip, .rar, .7z, etc.) with `/unzip`.")
        return

    doc = message.reply_to_message.document
    filename = doc.file_name or "archive.zip"
    ext = Path(filename).suffix.lower()
    
    from .manager.archive import ARCHIVE_EXT
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
    
    from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("Cancel", callback_data=f"cancel_job:{job.id}")]
    ])
    status_msg = await message.reply_text(
        f"**Job #{job.id} registered**\n"
        f"- **Archive**: `{filename}`\n\n"
        "Downloading archive...",
        reply_markup=keyboard
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

        db_job = await store.get_job(job.id)
        if db_job and db_job.status == JobStatus.CANCELLED:
            raise asyncio.CancelledError("Job cancelled by user")

        try:
            await status_msg.edit_text(
                compile_unzip_download_status_text(job.id, filename, current, total),
                reply_markup=keyboard
            )
        except Exception:
            pass

    try:
        await message.reply_to_message.download(
            file_name=str(dest_dir / filename),
            progress=on_download_progress
        )
    except (asyncio.CancelledError, Exception) as e:
        db_job = await store.get_job(job.id)
        if db_job and db_job.status == JobStatus.CANCELLED:
            log.info("Unzip job #%s download aborted due to cancellation", job.id)
            shutil.rmtree(dest_dir, ignore_errors=True)
            return
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
        from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("Cancel", callback_data=f"cancel_job:{job.id}")]
        ])
        await status_msg.edit_text(
            compile_queued_status_text(job.id, f"unzip:{filename}", ""),
            reply_markup=keyboard
        )
        await queue_manager.add_job(job.id)
        return

    from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton
    keyboard = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("Yes, split them", callback_data=f"split_yes:{job.id}"),
            InlineKeyboardButton("No, skip them", callback_data=f"split_no:{job.id}")
        ],
        [
            InlineKeyboardButton("Cancel", callback_data=f"cancel_job:{job.id}")
        ]
    ])

    prompt_text = compile_split_prompt_text(job.id, filename, is_unzip=True)
    await status_msg.edit_text(prompt_text, reply_markup=keyboard)


@app.on_message(filters.command("pdup"))
async def pdup_cmd(_, message: Message) -> None:
    replied = message.reply_to_message
    if not replied:
        await message.reply_text("Please reply to a media message (file, video, photo, audio, etc.) with `/pdup` to upload it to Pixeldrain.")
        return

    media = (
        replied.document
        or replied.video
        or replied.audio
        or replied.photo
        or replied.voice
        or replied.animation
        or replied.video_note
    )

    if not media:
        await message.reply_text("The replied message does not contain any valid media file.")
        return

    api_key = settings.pixeldrain_api_key
    if not api_key:
        await message.reply_text("Pixeldrain API key is not configured. Please add `PIXELDRAIN_API_KEY` to your environment or `.env` file.")
        return

    filename = "file"
    file_size = 0
    if replied.document:
        filename = replied.document.file_name or "file.bin"
        file_size = replied.document.file_size
    elif replied.video:
        filename = replied.video.file_name or "video.mp4"
        file_size = replied.video.file_size
    elif replied.audio:
        filename = replied.audio.file_name or "audio.mp3"
        file_size = replied.audio.file_size
    elif replied.photo:
        filename = "photo.jpg"
        file_size = replied.photo.file_size
    elif replied.voice:
        filename = "voice.ogg"
        file_size = replied.voice.file_size
    elif replied.animation:
        filename = replied.animation.file_name or "animation.mp4"
        file_size = replied.animation.file_size
    elif replied.video_note:
        filename = "video_note.mp4"
        file_size = replied.video_note.file_size

    status_msg = await message.reply_text(
        f"**Pixeldrain Upload:** `{filename}`\n"
        f"- **Size**: `{format_size(file_size)}`\n"
        f"- **Status**: `Downloading...`"
    )

    import tempfile
    import shutil
    from pathlib import Path
    
    temp_dir = Path(tempfile.mkdtemp(dir=str(settings.downloads_dir)))
    download_path = temp_dir / filename

    last_edit_time = 0.0
    
    async def on_download_progress(current, total):
        nonlocal last_edit_time
        import time
        now = time.time()
        if now - last_edit_time < 3.0 and current != total:
            return
        last_edit_time = now
        pct = (current / total) * 100.0 if total else 0.0
        bar = make_progress_bar(pct)
        try:
            await status_msg.edit_text(
                f"**Pixeldrain Upload:** `{filename}`\n"
                f"- **Size**: `{format_size(total)}`\n"
                f"- **Status**: `Downloading ({pct:.1f}%)...`\n"
                f"{bar}\n"
                f"Downloaded: `{format_size(current)}` of `{format_size(total)}`"
            )
        except Exception:
            pass

    try:
        await replied.download(
            file_name=str(download_path),
            progress=on_download_progress
        )
    except Exception as e:
        log.exception("Failed to download replied media")
        await status_msg.edit_text(f"Failed to download media: {e}")
        shutil.rmtree(temp_dir, ignore_errors=True)
        return

    if not download_path.exists():
        await status_msg.edit_text("Error: Downloaded file not found.")
        shutil.rmtree(temp_dir, ignore_errors=True)
        return

    await status_msg.edit_text(
        f"**Pixeldrain Upload:** `{filename}`\n"
        f"- **Size**: `{format_size(file_size)}`\n"
        f"- **Status**: `Uploading to Pixeldrain...`"
    )

    last_edit_time = 0.0

    async def on_upload_progress(current, total):
        nonlocal last_edit_time
        import time
        now = time.time()
        if now - last_edit_time < 3.0 and current != total:
            return
        last_edit_time = now
        pct = (current / total) * 100.0 if total else 0.0
        bar = make_progress_bar(pct)
        try:
            await status_msg.edit_text(
                f"**Pixeldrain Upload:** `{filename}`\n"
                f"- **Size**: `{format_size(total)}`\n"
                f"- **Status**: `Uploading to Pixeldrain ({pct:.1f}%)...`\n"
                f"{bar}\n"
                f"Uploaded: `{format_size(current)}` of `{format_size(total)}`"
            )
        except Exception:
            pass

    try:
        domain = settings.pixeldrain_domain or "pixeldrain.com"
        response_data, upload_logs = await upload_to_pixeldrain(
            download_path,
            api_key=api_key,
            progress_callback=on_upload_progress,
            domain=domain
        )

        if "error" in response_data:
            err_msg = response_data["error"]
            await status_msg.edit_text(f"Upload failed: {err_msg}\n\nLogs:\n" + "\n".join(upload_logs))
        else:
            file_id = response_data.get("id")
            if not file_id:
                await status_msg.edit_text("Uploaded successfully but no file ID returned from Pixeldrain.")
                return

            from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton
            
            file_size_formatted = format_size(download_path.stat().st_size)
            
            text = (
                f"**File Name:** `{filename}`\n"
                f"**File Size:** `{file_size_formatted}`\n"
                f"**Status:** `Uploaded Successfully!`"
            )
            
            reply_markup = InlineKeyboardMarkup(
                [
                    [
                        InlineKeyboardButton(text="Open Link", url=f"https://{domain}/u/{file_id}"),
                        InlineKeyboardButton(text="Direct Link", url=f"https://{domain}/api/file/{file_id}"),
                    ],
                    [
                        InlineKeyboardButton(
                            text="Share Link",
                            url=f"https://telegram.me/share/url?url=https://{domain}/u/{file_id}",
                        )
                    ]
                ]
            )
            
            await status_msg.edit_text(
                text=text,
                reply_markup=reply_markup,
                link_preview_options=LinkPreviewOptions(is_disabled=True)
            )

    except Exception as e:
        log.exception("Unexpected error in pdup command")
        await status_msg.edit_text(f"Unexpected error: {e}")
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)


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


@app.on_message(filters.text, group=-1)
async def handle_password_reply(_, message: Message) -> None:
    chat_id = message.chat.id
    text = (message.text or "").strip()
    if not text or text.startswith("/"):
        return

    reply_to_message_id = message.reply_to_message_id
    prompt_msg_id = None
    prompt_info = None

    if reply_to_message_id:
        prompt_info = _password_prompt_messages.get(reply_to_message_id)
        if prompt_info:
            prompt_msg_id = reply_to_message_id

    if not prompt_info:
        for mid, info in list(_password_prompt_messages.items()):
            if info[2] == chat_id:
                prompt_info = info
                prompt_msg_id = mid
                break

    if not prompt_info:
        return
        
    message.stop_propagation()
    
    job_id, archive_id, chat_id = prompt_info
    
    job = await store.get_job(job_id)
    if not job or not is_job_owner(message.chat.id, job):
        return

    password = text
    
    if job_id in _password_prompt_events and archive_id in _password_prompt_events[job_id]:
        event, data = _password_prompt_events[job_id][archive_id]
        data["password"] = password
        event.set()
        
    try:
        await message.delete()
    except Exception:
        pass
    
    if prompt_msg_id:
        _password_prompt_messages.pop(prompt_msg_id, None)


@app.on_message(group=-2)
async def handle_document_part(_, message: Message) -> None:
    import traceback
    try:
        chat_id = message.chat.id
        
        session_key = chat_id
        if session_key not in _split_archive_sessions:
            return
            
        if not message.document:
            return
            
        filename = message.document.file_name
        print(f"[DEBUG_SPLIT] handle_document_part triggered via catch-all. chat_id={chat_id}, filename={filename}", flush=True)
        
        if not filename:
            print("[DEBUG_SPLIT] filename is empty", flush=True)
            return
            
        session = _split_archive_sessions[session_key]
        
        if session["prefix"] is None:
            print(f"[DEBUG_SPLIT] Initializing session pattern for filename: {filename}", flush=True)
            split_info = get_split_archive_info(filename)
            if not split_info:
                print(f"[DEBUG_SPLIT] get_split_archive_info returned None for {filename}", flush=True)
                return
                
            session["prefix"] = split_info["prefix"]
            session["ext"] = split_info["ext"]
            session["pattern"] = split_info["pattern"]
            print(f"[DEBUG_SPLIT] Pattern initialized. Prefix: {session['prefix']}, Ext: {session['ext']}", flush=True)
            
        if not session["pattern"].match(filename):
            print(f"[DEBUG_SPLIT] Filename {filename} does not match active prefix: {session['prefix']}", flush=True)
            return
            
        split_info = get_split_archive_info(filename)
        if not split_info:
            print(f"[DEBUG_SPLIT] get_split_archive_info returned None on second check for {filename}", flush=True)
            return
            
        part_num = split_info["part"]
        session["parts"][part_num] = message
        print(f"[DEBUG_SPLIT] Part {part_num} added. Current parts: {list(session['parts'].keys())}", flush=True)
        
        if session.get("timeout_task"):
            session["timeout_task"].cancel()
            
        async def split_session_timeout(c_id: int, delay: int = 300):
            await asyncio.sleep(delay)
            s_key = c_id
            if s_key in _split_archive_sessions:
                session = _split_archive_sessions.pop(s_key)
                try:
                    await session["status_msg"].edit_text("**Split Archive Session Expired** (Timeout due to inactivity).")
                except Exception:
                    pass

        session["timeout_task"] = asyncio.create_task(split_session_timeout(chat_id))
        
        keyboard = session["status_msg"].reply_markup
        new_text = compile_split_session_text(session["prefix"], session["ext"], session["parts"])
        
        try:
            await session["status_msg"].edit_text(new_text, reply_markup=keyboard)
        except Exception:
            pass
            
        message.stop_propagation()
    except Exception as e:
        if e.__class__.__name__ == "StopPropagation":
            raise
        print("[DEBUG_SPLIT] Exception in handle_document_part:")
        traceback.print_exc()
        raise e



@app.on_message(filters.text & ~filters.command(["start", "help", "status", "cancel", "gdl", "tor", "unzip", "gd2tg"]))
async def handle_link(_, message: Message) -> None:
    chat_id = message.chat.id
    if chat_id in _pending_oauth_flows:
        code = (message.text or "").strip()
        flow, prompt_msg_id = _pending_oauth_flows.pop(chat_id)
        status_msg = await message.reply_text("Verifying authorization code...")
        try:
            from .gdrive import finish_oauth_flow_and_save
            finish_oauth_flow_and_save(flow, code, user_id=chat_id)
            await status_msg.edit_text(
                "**Google Drive Authentication Complete!**\n\n"
                f"Token saved to `auth/{chat_id}/token.pickle`. You can now download Google Drive links using:\n"
                "`/gd2tg <gdrive_link> [-zip|-7z]`"
            )

            return
        except Exception as e:
            log.exception("Failed to complete GDrive OAuth flow")
            await status_msg.edit_text(
                f"**Authentication Failed:** {e}\n\n"
                "Please reply to your `credentials.json` file with `/gd2tg` to try again."
            )
            return

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
        if token.startswith(("http://", "https://", "magnet:")):
            urls.append(token)
        else:
            raw_args.append(token)

    if not urls:
        if is_private:
            await message.reply_text("Send an actual URL.")
        return

    is_torrent_job = False
    first_url = urls[0]
    if first_url.startswith("magnet:") or first_url.endswith(".torrent") or "magnet:?xt=" in first_url:
        is_torrent_job = True

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
        ],
        [
            InlineKeyboardButton("Cancel", callback_data=f"cancel_job:{job.id}")
        ]
    ])

    if is_torrent_job:
        url_display = first_url
        if first_url.startswith("magnet:"):
            url_display = first_url[:60] + "..." if len(first_url) > 60 else first_url
        prompt_text = compile_split_prompt_text(job.id, url_display, is_torrent=True)
    else:
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


@app.on_callback_query(filters.regex(r"^split_(yes|no):(\w+)$"))
async def handle_split_choice(_, callback_query: CallbackQuery) -> None:
    data = callback_query.data
    choice, job_id = data.split(":")
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


async def run_split_archive_download_and_extract(session: dict, status_msg: Message) -> None:
    parts = session["parts"]
    sorted_parts = sorted(parts.keys())
    password = session["password"]
    chat_id = status_msg.chat.id
    
    first_part_msg = parts[1]
    filename = first_part_msg.document.file_name
    
    import json
    args_json = json.dumps({"password": password}) if password else None

    job = await store.create_job(chat_id, f"unzip:{filename}", split_large_files=1, args=args_json)
    await store.update_progress(job.id, status="waiting")
    await store.set_status_message(job.id, status_msg.id)

    dest_dir = (settings.downloads_dir / job.download_dir).resolve()
    dest_dir.mkdir(parents=True, exist_ok=True)
    
    await status_msg.edit_text(
        f"**Job #{job.id} registered**\n"
        f"- **Archive**: `{filename}`\n\n"
        "Starting download of parts..."
    )
    
    try:
        for idx, part_num in enumerate(sorted_parts, start=1):
            part_msg = parts[part_num]
            part_filename = part_msg.document.file_name
            
            last_edit_time = 0.0
            async def on_download_progress(current, total):
                nonlocal last_edit_time
                import time
                now = time.time()
                if now - last_edit_time < 3.0 and current != total:
                    return
                last_edit_time = now
                try:
                    display_name = f"{part_filename} (Part {idx}/{len(sorted_parts)})"
                    await status_msg.edit_text(
                        compile_unzip_download_status_text(job.id, display_name, current, total)
                    )
                except Exception:
                    pass
            
            await part_msg.download(
                file_name=str(dest_dir / part_filename),
                progress=on_download_progress
            )
            
    except Exception as e:
        log.exception("Failed to download replied split archive file")
        await status_msg.edit_text(f"Failed to download archive: {e}")
        await store.update_progress(job.id, status=JobStatus.FAILED, error=str(e), url="")
        return

    total_size = sum(parts[part_num].document.file_size or 0 for part_num in sorted_parts)
    limit_2gb = int(1.95 * 1024 * 1024 * 1024)
    
    if total_size < limit_2gb:
        await store.db.execute(
            "UPDATE jobs SET status = ?, split_large_files = ? WHERE id = ?",
            (JobStatus.QUEUED, 1, job.id)
        )
        await store.db.commit()
        from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("Cancel", callback_data=f"cancel_job:{job.id}")]
        ])
        await status_msg.edit_text(
            compile_queued_status_text(job.id, f"unzip:{filename}", ""),
            reply_markup=keyboard
        )
        await queue_manager.add_job(job.id)
        return

    from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton
    keyboard = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("Yes, split them", callback_data=f"split_yes:{job.id}"),
            InlineKeyboardButton("No, skip them", callback_data=f"split_no:{job.id}")
        ],
        [
            InlineKeyboardButton("Cancel", callback_data=f"cancel_job:{job.id}")
        ]
    ])

    prompt_text = compile_split_prompt_text(job.id, filename, is_unzip=True)
    await status_msg.edit_text(prompt_text, reply_markup=keyboard)


@app.on_callback_query(filters.regex(r"^split_cancel:(-?\d+):(-?\d+)$"))
async def handle_split_cancel_cb(_, callback_query: CallbackQuery) -> None:
    data = callback_query.data
    _, chat_id_str, user_id_str = data.split(":")
    chat_id = int(chat_id_str)
    user_id = int(user_id_str)
    
    print(f"[DEBUG_SPLIT] handle_split_cancel_cb triggered. chat_id={chat_id}, user_id={user_id}", flush=True)
    
    req_user_id = callback_query.from_user.id if callback_query.from_user else callback_query.message.chat.id
    if req_user_id != user_id:
        await callback_query.answer("Unauthorized: You did not start this session.", show_alert=True)
        return
        
    session_key = chat_id
    if session_key in _split_archive_sessions:
        session = _split_archive_sessions.pop(session_key)
        if session.get("timeout_task"):
            session["timeout_task"].cancel()
        await callback_query.message.edit_text("**Split Archive Session Cancelled.**")
        await callback_query.answer("Session cancelled.")
    else:
        await callback_query.answer("Session not found or already expired.", show_alert=True)


@app.on_callback_query(filters.regex(r"^split_start:(-?\d+):(-?\d+)$"))
async def handle_split_start_cb(_, callback_query: CallbackQuery) -> None:
    data = callback_query.data
    _, chat_id_str, user_id_str = data.split(":")
    chat_id = int(chat_id_str)
    user_id = int(user_id_str)
    
    print(f"[DEBUG_SPLIT] handle_split_start_cb triggered. chat_id={chat_id}, user_id={user_id}", flush=True)
    
    req_user_id = callback_query.from_user.id if callback_query.from_user else callback_query.message.chat.id
    if req_user_id != user_id:
        await callback_query.answer("Unauthorized: You did not start this session.", show_alert=True)
        return
        
    session_key = chat_id
    if session_key not in _split_archive_sessions:
        print(f"[DEBUG_SPLIT] session key {session_key} not in active sessions: {list(_split_archive_sessions.keys())}", flush=True)
        await callback_query.answer("Session not found or already expired.", show_alert=True)
        return
        
    session = _split_archive_sessions[session_key]
    parts = session["parts"]
    
    print(f"[DEBUG_SPLIT] Starting extraction check. Uploaded parts: {list(parts.keys())}", flush=True)
    
    if not parts:
        await callback_query.answer("No parts uploaded yet. Please send some split archive files first.", show_alert=True)
        return
        
    sorted_parts = sorted(parts.keys())
    missing_parts = []
    for i in range(1, max(sorted_parts) + 1):
        if i not in parts:
            missing_parts.append(i)
            
    if missing_parts:
        missing_str = ", ".join(map(str, missing_parts))
        print(f"[DEBUG_SPLIT] Missing parts detected: {missing_parts}", flush=True)
        await callback_query.answer(f"Missing parts: {missing_str}. Please upload them before starting.", show_alert=True)
        return
        
    _split_archive_sessions.pop(session_key)
    if session.get("timeout_task"):
        session["timeout_task"].cancel()
        
    await callback_query.answer("Starting extraction job...")
    asyncio.create_task(run_split_archive_download_and_extract(session, callback_query.message))


@app.on_callback_query(filters.regex(r"^archive_(only|ext):(\w+):(.+)$"))
async def handle_archive_choice_cb(_, callback_query: CallbackQuery) -> None:
    await handle_archive_choice(callback_query, store, is_job_owner)


@app.on_callback_query(filters.regex(r"^convert_(mp4|mp3|orig):(\w+):(.+)$"))
async def handle_conversion_choice_cb(client: Client, callback_query: CallbackQuery) -> None:
    await handle_conversion_choice(client, callback_query, store, is_job_owner)


@app.on_callback_query(filters.regex(r"^cancel_job:(\w+)$"))
async def handle_cancel_job_cb(_, callback_query: CallbackQuery) -> None:
    data = callback_query.data
    job_id = data.split(":")[1]
    chat_id = callback_query.message.chat.id

    job = await store.get_job(job_id)
    if not job:
        await callback_query.answer("Job not found.", show_alert=True)
        return

    if not is_job_owner(chat_id, job):
        await callback_query.answer("Unauthorized: You do not own this job.", show_alert=True)
        return

    if job.status in (JobStatus.DONE, JobStatus.FAILED, JobStatus.CANCELLED):
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
    """On startup, mark active jobs as failed and put queued jobs onto the queue."""
    try:
        cur = await store.db.execute(
            "SELECT id FROM jobs WHERE status IN ('downloading', 'uploading')"
        )
        rows = await cur.fetchall()
        for r in rows:
            job_id = r["id"]
            await store.update_progress(job_id, status=JobStatus.FAILED, error="Aborted due to bot restart")
    except Exception as e:
        log.warning("Failed to clean up incomplete active jobs on startup: %s", e)

    for job in await store.queued_jobs():
        log.info("Resuming job #%s (%s)", job.id, job.status)
        await queue_manager.add_job(job.id)


_pending_oauth_flows: dict[int, tuple[Any, int]] = {}


def get_message_user_id(message: Message) -> int:
    if message.from_user:
        return message.from_user.id
    if message.sender_chat:
        return message.sender_chat.id
    return message.chat.id


@app.on_message(filters.command(["gd2tg"]))
async def gd2tg_handler(_, message: Message) -> None:
    user_id = get_message_user_id(message)
    user_auth_dir = settings.auth_dir / str(user_id)

    # Check if user replied to any .json file
    reply_msg = message.reply_to_message
    if reply_msg and reply_msg.document and reply_msg.document.file_name and reply_msg.document.file_name.lower().endswith(".json"):
        user_auth_dir.mkdir(parents=True, exist_ok=True)
        orig_filename = reply_msg.document.file_name
        temp_download_path = (user_auth_dir / f"temp_{orig_filename}").resolve()

        status_msg = await message.reply_text("Downloading JSON credentials file...")
        try:
            downloaded_str = await app.download_media(reply_msg, file_name=str(temp_download_path))
            downloaded_path = Path(downloaded_str) if downloaded_str else temp_download_path

            with open(downloaded_path, "r", encoding="utf-8") as f:
                data = json.load(f)

            if isinstance(data, dict) and data.get("type") == "service_account":

                accounts_dir = user_auth_dir / "accounts"
                accounts_dir.mkdir(parents=True, exist_ok=True)
                sa_path = accounts_dir / orig_filename
                downloaded_path.replace(sa_path)
                await status_msg.edit_text(
                    f"**Service Account Added Successfully!**\n\n"
                    f"Saved `{orig_filename}` to `auth/{user_id}/accounts/`.\n"
                    f"You can now download Google Drive links directly using `/gd2tg <gdrive_link> [-zip|-7z]`."
                )
                return
            elif isinstance(data, dict) and ("installed" in data or "web" in data):
                creds_json_path = user_auth_dir / "credentials.json"
                downloaded_path.replace(creds_json_path)
                await status_msg.edit_text(
                    f"**Client Secrets JSON Saved!**\n\n"
                    f"Saved credentials to `auth/{user_id}/credentials.json`.\n\n"
                    f"To generate your token (since Google retired OOB authorization), run the following command:\n"
                    f"`python3 -m app.gdrive.setup auth/{user_id}/credentials.json {user_id}`\n\n"
                    f"*Tip:* You can also upload Service Account `.json` keys directly to this chat for zero-browser setup!"
                )
                return
            else:
                downloaded_path.unlink(missing_ok=True)
                await status_msg.edit_text(
                    "**Invalid JSON File:** The uploaded file is neither a Google OAuth client secret file nor a Service Account key."
                )
                return
        except Exception as e:
            if 'downloaded_path' in locals() and downloaded_path.exists():
                downloaded_path.unlink(missing_ok=True)
            elif temp_download_path.exists():
                temp_download_path.unlink(missing_ok=True)
            log.exception("Failed to process GDrive JSON file")
            await status_msg.edit_text(f"**Failed to process JSON file:** {e}")
            return

    # Check if user replied to a .pickle token file
    if reply_msg and reply_msg.document and reply_msg.document.file_name and reply_msg.document.file_name.lower().endswith(".pickle"):
        user_auth_dir.mkdir(parents=True, exist_ok=True)
        token_path = (user_auth_dir / "token.pickle").resolve()

        status_msg = await message.reply_text("Downloading token.pickle file...")
        try:
            downloaded_str = await app.download_media(reply_msg, file_name=str(token_path))
            await status_msg.edit_text(
                f"**OAuth Token Saved Successfully!**\n\n"
                f"Saved to `auth/{user_id}/token.pickle`.\n"
                f"You can now download Google Drive links directly using `/gd2tg <gdrive_link> [-zip|-7z]`."
            )
            return
        except Exception as e:
            log.exception("Failed to save token.pickle file")
            await status_msg.edit_text(f"**Failed to save token file:** {e}")
            return

    # Check if credentials exist before attempting a download
    from .gdrive import GoogleDriveAuthManager
    auth_mgr = GoogleDriveAuthManager(user_id=user_id)
    if not auth_mgr.has_credentials():
        await message.reply_text(
            "**Google Drive credentials not found!**\n\n"
            "**Option 1: Service Account (Recommended - Zero Browser Steps):**\n"
            "1. Upload your Service Account `.json` key file to this chat.\n"
            f"2. **Reply to the `.json` file** with `/gd2tg` to activate it.\n\n"
            "**Option 2: OAuth Token Pickle File:**\n"
            "1. Upload your `token.pickle` file to this chat.\n"
            f"2. **Reply to the `token.pickle` file** with `/gd2tg` to activate it.\n\n"
            "*(Note: Bot owner can also set up global credentials in `auth/` for all users).*",
            link_preview_options=LinkPreviewOptions(is_disabled=True)
        )
        return

    text = message.text or ""

    parts = text.split()
    if len(parts) < 2:
        await message.reply_text(
            "**Usage:** `/gd2tg <gdrive_link> [-zip|-7z] [-pd]`\n"
            "Downloads a Google Drive link, archives each folder individually, and uploads to Telegram.\n"
            "• Use `-pd` flag to mirror original unsplit archives to Pixeldrain as well.",
            link_preview_options=LinkPreviewOptions(is_disabled=True)
        )
        return

    link = parts[1]
    archive_fmt = None
    mirror_pixeldrain = False

    for part in parts[2:]:
        p_lower = part.lower().lstrip("-")
        if p_lower in ("zip", "7z"):
            archive_fmt = p_lower
        elif p_lower in ("pd", "pixeldrain"):
            mirror_pixeldrain = True

    if not archive_fmt and mirror_pixeldrain:
        archive_fmt = "zip"

    try:
        from .gdrive import get_id_from_url
        get_id_from_url(link)
    except Exception as e:
        await message.reply_text(
            f"**Invalid Google Drive Link:** {e}",
            link_preview_options=LinkPreviewOptions(is_disabled=True)
        )
        return

    args_json = json.dumps({
        "gdrive": True,
        "archive_format": archive_fmt,
        "mirror_pixeldrain": mirror_pixeldrain,
        "user_id": user_id
    })
    urls_json = json.dumps([f"gd2tg:{link}"])

    job = await store.create_job(message.chat.id, urls_json, split_large_files=1, args=args_json)
    await queue_manager.add_job(job.id)

    pd_info = " + Pixeldrain Mirror" if mirror_pixeldrain else ""
    fmt_info = archive_fmt.upper() if archive_fmt else "RAW"
    args_disp = f"\n- **Archive Format**: `{fmt_info}`{pd_info}"
    from .manager.status import compile_queued_status_text
    queued_text = compile_queued_status_text(job.id, f"gd2tg:{link}", args_disp)

    status_msg = await message.reply_text(queued_text)
    await store.set_status_message(job.id, status_msg.id)


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
                    BotCommand("gd2tg", "Download GDrive link, archive folders, upload to Telegram"),
                    BotCommand("gdl", "Process replied .txt links file with optional arguments"),
                    BotCommand("tor", "Download torrent/magnet link or replied .torrent file"),
                    BotCommand("unzip", "Extract archive (.zip, .rar, .7z) and upload contents"),
                    BotCommand("pdup", "Upload replied media directly to Pixeldrain"),
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
