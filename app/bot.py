from __future__ import annotations

import asyncio
import logging
import logging.handlers
import random
import shutil
import time
from pathlib import Path

from pyrogram import Client, filters, idle
from pyrogram.enums import ChatType
from pyrogram.types import Message, CallbackQuery

from .config import settings
from .db import Job, JobStatus, JobStore
from .downloader import GalleryDLNotFound, run_with_progress
from .uploader import UploadTooLarge, upload_file

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

    # pyrogram is chatty at INFO; keep it at WARNING unless debugging
    logging.getLogger("pyrogram").setLevel(logging.WARNING)


store = JobStore(settings.db_path)
app = Client(
    "tgdl_bot",
    api_id=settings.tg_api_id,
    api_hash=settings.tg_api_hash,
    bot_token=settings.tg_bot_token,
    workdir=str(settings.data_dir),
)
job_queue: asyncio.Queue[int] = asyncio.Queue()
_shutdown_event = asyncio.Event()
_current_job_id: int | None = None


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


async def safe_edit(chat_id: int, message_id: int, text: str) -> None:
    from pyrogram.errors import FloodWait, MessageNotModified

    try:
        await app.edit_message_text(chat_id, message_id, text, disable_web_page_preview=True)
    except MessageNotModified:
        pass
    except FloodWait as e:
        await asyncio.sleep(e.value + 1)


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


async def process_job(job: Job) -> None:
    global _current_job_id
    _current_job_id = job.id
    chat_id = job.chat_id
    msg_id = job.status_message_id
    dest_dir = settings.downloads_dir / job.download_dir

    async def report(text: str) -> None:
        if msg_id:
            await safe_edit(chat_id, msg_id, text)

    # In-memory tracking to avoid duplicate concurrent uploads and reduce DB hits
    uploading_files: set[str] = set()
    uploaded_filenames = await store.get_uploaded_filenames(job.id)
    sent = len(uploaded_filenames)
    skipped: list[tuple[str, str]] = []
    session_uploaded_count = 0

    current_upload_file: str | None = None
    current_upload_pct: float = 0.0
    upload_speed: float = 0.0
    last_uploaded_bytes = 0
    last_progress_edit = 0.0
    last_upload_speed_time = 0.0
    last_downloader_edit = 0.0

    # For download size/speed tracking
    total_downloaded_bytes = 0
    download_speed = 0.0
    last_download_size = 0
    last_download_time = time.time()
    deleted_bytes = 0

    status_lock = asyncio.Lock()
    downloader_done = asyncio.Event()
    uploader_done = asyncio.Event()
    download_count = 0

    async def update_status_msg() -> None:
        async with status_lock:
            dl_size_str = format_size(total_downloaded_bytes)
            dl_speed_str = format_size(download_speed)

            if downloader_done.is_set():
                status_text = (
                    f"**Downloading complete!** (Total: **{dl_size_str}**)\n"
                    f"**Uploading remaining files…**\n"
                    f"   • **Sent:** `{sent}`\n"
                    f"   • **Skipped:** `{len(skipped)}`"
                )
            else:
                status_text = (
                    f"**Downloading & Uploading…**\n\n"
                    f"**Downloader Status:**\n"
                    f"   • **Processed:** ~`{download_count}` items\n"
                    f"   • **Size:** `{dl_size_str}`\n"
                    f"   • **Speed:** `{dl_speed_str}/s`\n\n"
                    f"**Uploader Status:**\n"
                    f"   • **Sent:** `{sent}`\n"
                    f"   • **Skipped:** `{len(skipped)}`"
                )

            if current_upload_file:
                up_speed_str = format_size(upload_speed)
                bar = make_progress_bar(current_upload_pct)
                status_text += (
                    f"\n\n**Current Upload:**\n"
                    f"   • **File:** `{current_upload_file}`\n"
                    f"   • **Progress:** `{current_upload_pct:.1f}%` `[{bar}]`\n"
                    f"   • **Speed:** `{up_speed_str}/s`"
                )

            await report(status_text)

    def on_progress(count: int) -> None:
        nonlocal download_count, last_downloader_edit
        download_count = count
        now = time.time()
        # Rate limit status edits during download to at most once every 3.0 seconds
        if now - last_downloader_edit >= 3.0:
            last_downloader_edit = now
            asyncio.create_task(update_status_msg())

    async def on_upload_progress(current: int, total: int) -> None:
        nonlocal current_upload_pct, last_progress_edit, upload_speed, last_uploaded_bytes, last_upload_speed_time
        if total == 0:
            return
        pct = current * 100.0 / total
        now = time.time()

        # Calculate upload speed
        dt = now - last_upload_speed_time
        if dt >= 1.0 or last_upload_speed_time == 0.0:
            bytes_diff = current - last_uploaded_bytes
            speed = bytes_diff / dt if dt > 0 else 0.0
            upload_speed = 0.7 * speed + 0.3 * upload_speed if last_uploaded_bytes > 0 else speed
            last_uploaded_bytes = current
            last_upload_speed_time = now

        # Edit progress message at most once every 3.0 seconds, or if finished/significant jump
        if pct - current_upload_pct >= 10.0 or now - last_progress_edit >= 3.0 or current == total:
            current_upload_pct = pct
            last_progress_edit = now
            await update_status_msg()

    async def run_downloader():
        try:
            extra_args = None
            if job.args:
                import json
                try:
                    extra_args = json.loads(job.args)
                except Exception:
                    log.exception("Failed to parse extra args from job: %s", job.args)

            return await run_with_progress(
                job.url, dest_dir, settings.gdl_archive_path, on_progress=on_progress, extra_args=extra_args
            )
        finally:
            downloader_done.set()

    async def monitor_download_speed():
        nonlocal total_downloaded_bytes, download_speed, last_download_size, last_download_time
        try:
            while not downloader_done.is_set():
                if not dest_dir.exists():
                    await asyncio.sleep(2)
                    continue

                try:
                    on_disk = sum(p.stat().st_size for p in dest_dir.rglob("*") if p.is_file())
                    current_size = on_disk + deleted_bytes
                except Exception:
                    current_size = last_download_size

                now = time.time()
                dt = now - last_download_time
                if dt >= 1.0:
                    speed = (current_size - last_download_size) / dt
                    download_speed = 0.7 * speed + 0.3 * download_speed if last_download_size > 0 else speed
                    last_download_size = current_size
                    last_download_time = now
                    total_downloaded_bytes = current_size

                await asyncio.sleep(2)
        except asyncio.CancelledError:
            pass

    async def perform_uploads() -> None:
        nonlocal sent, session_uploaded_count, current_upload_file, current_upload_pct, last_progress_edit
        nonlocal upload_speed, last_uploaded_bytes, last_upload_speed_time, deleted_bytes
        if not dest_dir.exists():
            return

        try:
            files = sorted(
                [p for p in dest_dir.rglob("*") if p.is_file() and not p.name.endswith(".part")]
            )
        except Exception:
            log.exception("Failed to scan directory %s", dest_dir)
            return

        pending = [
            f for f in files
            if f.name not in uploaded_filenames
            and f.name not in uploading_files
        ]

        for f in pending:
            if _shutdown_event.is_set():
                return
            db_job = await store.get_job(job.id)
            if db_job and db_job.status == JobStatus.CANCELLED:
                return

            # Check for files larger than 1.95GB (Telegram MTProto limit safety threshold)
            max_limit = int(1.95 * 1024 * 1024 * 1024)
            if f.exists() and f.stat().st_size > max_limit:
                from .uploader import handle_large_file
                split_parts = await handle_large_file(f, bool(db_job.split_large_files))
                if not split_parts:
                    skipped.append((f.name, "File exceeds 1.95GB limit and was skipped"))
                    await store.update_progress(job.id, sent_files=sent, skipped_files=len(skipped))
                    await update_status_msg()
                    continue
                # Append split parts to pending list to process in the current run
                for part in split_parts:
                    if part not in pending:
                        pending.append(part)
                continue

            uploading_files.add(f.name)
            try:
                await store.update_progress(job.id, status=JobStatus.UPLOADING)

                current_upload_file = f.name
                current_upload_pct = 0.0
                upload_speed = 0.0
                last_uploaded_bytes = 0
                last_upload_speed_time = time.time()
                last_progress_edit = time.time()
                await update_status_msg()

                await upload_file(app, chat_id, f, progress=on_upload_progress)
                await store.mark_uploaded(job.id, f.name)

                uploaded_filenames.add(f.name)
                sent += 1
                await log_upload(job.id, f.name)
                log.info("Successfully uploaded %s for job %s", f.name, job.id)

                # Cleanup file immediately after successful upload to avoid storage issues
                try:
                    f_size = f.stat().st_size
                    f.unlink(missing_ok=True)
                    deleted_bytes += f_size
                except Exception:
                    log.exception("Failed to delete file after upload: %s", f)

            except UploadTooLarge as e:
                skipped.append((f.name, str(e)))
                log.warning("File too large to upload: %s", f.name)
            except Exception as e:  # noqa: BLE001
                log.exception("Upload failed for %s", f)
                skipped.append((f.name, f"error: {e}"))
            finally:
                current_upload_file = None
                current_upload_pct = 0.0
                upload_speed = 0.0
                if f.name in uploading_files:
                    uploading_files.remove(f.name)

            await store.update_progress(job.id, sent_files=sent, skipped_files=len(skipped))
            await update_status_msg()

            session_uploaded_count += 1
            if session_uploaded_count % settings.tg_batch_size == 0:
                await asyncio.sleep(settings.tg_batch_cooldown_s)
            else:
                await asyncio.sleep(
                    random.uniform(settings.tg_upload_delay_min, settings.tg_upload_delay_max)
                )

    async def run_uploader() -> None:
        try:
            while not downloader_done.is_set():
                if _shutdown_event.is_set():
                    return
                await perform_uploads()
                try:
                    await asyncio.wait_for(downloader_done.wait(), timeout=5.0)
                except asyncio.TimeoutError:
                    pass
            await perform_uploads()
        finally:
            uploader_done.set()

    async def check_cancellation() -> None:
        try:
            while not (downloader_done.is_set() and uploader_done.is_set()):
                if _shutdown_event.is_set():
                    downloader_task.cancel()
                    uploader_task.cancel()
                    monitor_task.cancel()
                    break
                db_job = await store.get_job(job.id)
                if db_job and db_job.status == JobStatus.CANCELLED:
                    downloader_task.cancel()
                    uploader_task.cancel()
                    monitor_task.cancel()
                    break
                await asyncio.sleep(2)
        except asyncio.CancelledError:
            pass

    downloader_task = asyncio.create_task(run_downloader())
    uploader_task = asyncio.create_task(run_uploader())
    monitor_task = asyncio.create_task(monitor_download_speed())
    cancellation_task = asyncio.create_task(check_cancellation())

    try:
        await store.update_progress(job.id, status=JobStatus.DOWNLOADING)
        await report(f"Downloading:\n{job.url}\n(rate-limited, large albums take a while)")

        result = await downloader_task
        await uploader_task
    except asyncio.CancelledError:
        log.info("Job %s was cancelled/aborted", job.id)
        downloader_task.cancel()
        uploader_task.cancel()
        monitor_task.cancel()
        await asyncio.gather(downloader_task, uploader_task, monitor_task, return_exceptions=True)

        db_job = await store.get_job(job.id)
        if _shutdown_event.is_set() or (db_job and db_job.status == JobStatus.QUEUED):
            await store.update_progress(job.id, status=JobStatus.QUEUED)
            await report("Paused for shutdown — will resume on next start.")
        else:
            await store.update_progress(job.id, status=JobStatus.CANCELLED)
            await report("Job cancelled.")
            shutil.rmtree(dest_dir, ignore_errors=True)
        return
    except GalleryDLNotFound as e:
        await store.update_progress(job.id, status=JobStatus.FAILED, error=str(e))
        await report(str(e))
        return
    except Exception as e:  # noqa: BLE001
        log.exception("job %s failed", job.id)
        await store.update_progress(job.id, status=JobStatus.FAILED, error=str(e))
        await report(f"Job failed with an unexpected error: {e}")
        return
    finally:
        monitor_task.cancel()
        cancellation_task.cancel()
        await asyncio.gather(monitor_task, cancellation_task, return_exceptions=True)
        _current_job_id = None

    # Final cleanup and report for successful run
    if not result.ok and sent == 0:
        await store.update_progress(
            job.id, status=JobStatus.FAILED, error=result.error_tail[-1500:]
        )
        await report(
            f"gallery-dl failed after {result.attempts} attempt(s) and produced no files.\n"
            f"Last output:\n```\n{result.error_tail[-800:]}\n```"
        )
        return

    # Scan to see if there are any remaining files that were skipped or not uploaded
    files_remaining = []
    if dest_dir.exists():
        files_remaining = [p for p in dest_dir.rglob("*") if p.is_file() and not p.name.endswith(".part")]

    await store.update_progress(job.id, status=JobStatus.DONE, sent_files=sent, skipped_files=len(skipped))
    summary = f"Done. Uploaded {sent} file(s) total."
    if skipped:
        preview = "\n".join(f"- {n} ({info})" for n, info in skipped[:20])
        more = f"\n…and {len(skipped) - 20} more" if len(skipped) > 20 else ""
        summary += f"\nSkipped:\n{preview}{more}"
    await app.send_message(chat_id, summary, disable_web_page_preview=True)

    # cleanup: remove local directory once everyone is accounted for
    shutil.rmtree(dest_dir, ignore_errors=True)


async def worker_loop() -> None:
    while not _shutdown_event.is_set():
        try:
            job_id = await asyncio.wait_for(job_queue.get(), timeout=1.0)
        except asyncio.TimeoutError:
            continue
        job = await store.get_job(job_id)
        if job is None:
            continue
        await process_job(job)


@app.on_message(filters.command("start"))
async def start_cmd(_, message: Message) -> None:
    await message.reply_text(
        "Send me a link and I'll fetch it with gallery-dl and upload the results here.\n"
        "Large albums are throttled to avoid rate limits — I'll post progress as I go.\n\n"
        "Commands: /status, /cancel"
    )


@app.on_message(filters.command("status"))
async def status_cmd(_, message: Message) -> None:
    if _current_job_id is not None:
        job = await store.get_job(_current_job_id)
        if job:
            await message.reply_text(
                f"Job #{job.id}: {job.status}\n"
                f"{job.sent_files}/{job.total_files} sent, {job.skipped_files} skipped"
            )
            return
    queued = await store.queued_jobs()
    await message.reply_text(f"Nothing running. {len(queued)} job(s) queued." if queued else "Idle. No jobs queued.")


@app.on_message(filters.command("cancel"))
async def cancel_cmd(_, message: Message) -> None:
    if _current_job_id is None:
        await message.reply_text("Nothing is currently running.")
        return
    await store.update_progress(_current_job_id, status=JobStatus.CANCELLED)
    await message.reply_text(
        f"Marked job #{_current_job_id} for cancellation — it'll stop after the current file finishes."
    )


def sanitize_gdl_args(args: list[str]) -> list[str]:
    sanitized = []
    skip_next = False
    for i, arg in enumerate(args):
        if skip_next:
            skip_next = False
            continue

        # Strip directory/config override options
        if arg in ("-d", "--directory", "--config"):
            skip_next = True
            continue

        # Ignore help / version arguments that would prevent download
        if arg in ("-h", "--help", "--version"):
            continue

        # If it is -o or --option, check the value
        if arg in ("-o", "--option"):
            if i + 1 < len(args):
                val = args[i + 1]
                # If it tries to set base-directory, skip both
                if "base-directory" in val or "directory" in val or "path" in val:
                    skip_next = True
                    continue
            else:
                continue

        sanitized.append(arg)
    return sanitized


@app.on_message(filters.text & ~filters.command(["start", "status", "cancel"]))
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

    url = tokens[0]
    raw_args = tokens[1:]

    if not url.startswith(("http://", "https://")):
        if is_private:
            await message.reply_text("Send an actual URL.")
        return

    sanitized_args = sanitize_gdl_args(raw_args)
    args_json = json.dumps(sanitized_args) if sanitized_args else None

    # Create job in "waiting" status
    job = await store.create_job(message.chat.id, url, split_large_files=1, args=args_json)
    await store.update_progress(job.id, status="waiting")

    # Send confirmation inline keyboard
    from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton
    keyboard = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("Yes, split them", callback_data=f"split_yes:{job.id}"),
            InlineKeyboardButton("No, skip them", callback_data=f"split_no:{job.id}")
        ]
    ])

    args_display = f"\n- **Args**: `{' '.join(sanitized_args)}`" if sanitized_args else ""
    prompt_text = (
        f"**Job #{job.id} registered**\n"
        f"- **URL**: {url}{args_display}\n\n"
        "Do you want to split files larger than 2GB for this job?"
    )
    status_msg = await message.reply_text(
        prompt_text,
        reply_markup=keyboard,
        disable_web_page_preview=True
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

    if job.status != "waiting":
        await callback_query.answer("This job choice has already been processed.")
        return

    # Update job in database to QUEUED and save the split choice
    await store.db.execute(
        "UPDATE jobs SET status = ?, split_large_files = ? WHERE id = ?",
        (JobStatus.QUEUED, split_choice, job_id)
    )
    await store.db.commit()

    # Edit the message to show queued status (removing the inline keyboard)
    import json
    parsed_args = []
    if job.args:
        try:
            parsed_args = json.loads(job.args)
        except Exception:
            pass
    args_display = f"\n- **Args**: `{' '.join(parsed_args)}`" if parsed_args else ""
    status_text = (
        f"**Queued (job #{job_id})**\n"
        f"- **URL**: {job.url}{args_display}"
    )
    await callback_query.message.edit_text(status_text, disable_web_page_preview=True)
    await callback_query.answer("Choice registered.")

    # Put the job on queue
    await job_queue.put(job_id)


async def requeue_incomplete_jobs() -> None:
    """On startup, put back-in-progress and queued jobs onto the queue so
    interrupted runs resume instead of silently vanishing."""
    for job in [*await store.resumable_jobs(), *await store.queued_jobs()]:
        log.info("Resuming job #%s (%s)", job.id, job.status)
        await job_queue.put(job.id)


async def _startup() -> None:
    await store.open()
    await cleanup_orphaned_directories()


async def main() -> None:
    setup_logging()

    if shutil.which("gallery-dl") is None:
        log.warning(
            "gallery-dl not found on PATH — install with "
            "`pip install gallery-dl --break-system-packages`"
        )

    await _startup()

    try:
        worker_task = None
        async with app:
            log.info("Bot started.")
            try:
                from pyrogram.types import BotCommand
                await app.set_bot_commands([
                    BotCommand("start", "Start the bot and see instructions"),
                    BotCommand("status", "Check current active job details or queue status"),
                    BotCommand("cancel", "Instantly abort the active download/upload task"),
                ])
                log.info("Bot commands set successfully.")
            except Exception as e:
                log.warning("Failed to set bot commands: %s", e)
            await requeue_incomplete_jobs()
            worker_task = asyncio.create_task(worker_loop())
            await idle()  # blocks until SIGINT/SIGTERM

        log.info("Shutting down, finishing current file then stopping…")
        _shutdown_event.set()
        if worker_task:
            try:
                await asyncio.wait_for(worker_task, timeout=35)
            except asyncio.TimeoutError:
                worker_task.cancel()
    finally:
        await store.close()
        log.info("Shutdown complete.")


if __name__ == "__main__":
    asyncio.get_event_loop().run_until_complete(main())
