from __future__ import annotations

import asyncio
import logging
import logging.handlers
import random
import shutil
import time
from pathlib import Path

from pyrogram import Client, filters, idle
from pyrogram.types import Message

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
    not active or queued in the database."""
    if not settings.downloads_dir.exists():
        return

    try:
        active_jobs = await store.resumable_jobs()
        queued_jobs = await store.queued_jobs()
        keep_ids = {f"job_{job.id}" for job in [*active_jobs, *queued_jobs]}

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
        await app.edit_message_text(chat_id, message_id, text)
    except MessageNotModified:
        pass
    except FloodWait as e:
        await asyncio.sleep(e.value + 1)


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

    status_lock = asyncio.Lock()
    downloader_done = asyncio.Event()
    uploader_done = asyncio.Event()
    download_count = 0

    async def update_status_msg() -> None:
        async with status_lock:
            if downloader_done.is_set():
                await report(
                    f"Uploading remaining files… {sent} sent, {len(skipped)} skipped."
                )
            else:
                await report(
                    f"Downloading & Uploading…\n"
                    f"Processed ~{download_count} items.\n"
                    f"Uploaded: {sent} sent, {len(skipped)} skipped."
                )

    def on_progress(count: int) -> None:
        nonlocal download_count
        download_count = count
        asyncio.create_task(update_status_msg())

    async def run_downloader():
        try:
            return await run_with_progress(
                job.url, dest_dir, settings.gdl_archive_path, on_progress=on_progress
            )
        finally:
            downloader_done.set()

    async def perform_uploads() -> None:
        nonlocal sent, session_uploaded_count
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

            uploading_files.add(f.name)
            try:
                await store.update_progress(job.id, status=JobStatus.UPLOADING)
                await upload_file(app, chat_id, f)
                await store.mark_uploaded(job.id, f.name)

                uploaded_filenames.add(f.name)
                sent += 1
                await log_upload(job.id, f.name)
                log.info("Successfully uploaded %s for job %s", f.name, job.id)

                # Cleanup file immediately after successful upload to avoid storage issues
                try:
                    f.unlink(missing_ok=True)
                except Exception:
                    log.exception("Failed to delete file after upload: %s", f)

            except UploadTooLarge as e:
                skipped.append((f.name, str(e)))
                log.warning("File too large to upload: %s", f.name)
            except Exception as e:  # noqa: BLE001
                log.exception("Upload failed for %s", f)
                skipped.append((f.name, f"error: {e}"))
            finally:
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
                    break
                db_job = await store.get_job(job.id)
                if db_job and db_job.status == JobStatus.CANCELLED:
                    downloader_task.cancel()
                    uploader_task.cancel()
                    break
                await asyncio.sleep(2)
        except asyncio.CancelledError:
            pass

    downloader_task = asyncio.create_task(run_downloader())
    uploader_task = asyncio.create_task(run_uploader())
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
        await asyncio.gather(downloader_task, uploader_task, return_exceptions=True)

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
        cancellation_task.cancel()
        await asyncio.gather(cancellation_task, return_exceptions=True)
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
    await app.send_message(chat_id, summary)

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


@app.on_message(filters.text & ~filters.command(["start", "status", "cancel"]))
async def handle_link(_, message: Message) -> None:
    text = (message.text or "").strip()
    if not text.startswith(("http://", "https://")):
        await message.reply_text("Send an actual URL.")
        return

    job = await store.create_job(message.chat.id, text)
    status_msg = await message.reply_text(f"Queued (job #{job.id}).")
    await store.set_status_message(job.id, status_msg.id)
    await job_queue.put(job.id)


async def requeue_incomplete_jobs() -> None:
    """On startup, put back-in-progress and queued jobs onto the queue so
    interrupted runs resume instead of silently vanishing."""
    for job in [*await store.resumable_jobs(), *await store.queued_jobs()]:
        log.info("Resuming job #%s (%s)", job.id, job.status)
        await job_queue.put(job.id)


async def _startup() -> None:
    await store.open()
    await cleanup_orphaned_directories()
    await requeue_incomplete_jobs()


async def main() -> None:
    setup_logging()

    if shutil.which("gallery-dl") is None:
        log.warning(
            "gallery-dl not found on PATH — install with "
            "`pip install gallery-dl --break-system-packages`"
        )

    await _startup()
    worker_task = asyncio.create_task(worker_loop())

    async with app:
        log.info("Bot started.")
        await idle()  # blocks until SIGINT/SIGTERM

    log.info("Shutting down, finishing current file then stopping…")
    _shutdown_event.set()
    try:
        await asyncio.wait_for(worker_task, timeout=35)
    except asyncio.TimeoutError:
        worker_task.cancel()
    await store.close()
    log.info("Shutdown complete.")


if __name__ == "__main__":
    asyncio.get_event_loop().run_until_complete(main())
