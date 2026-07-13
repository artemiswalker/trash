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
from pyrogram.types import Message, CallbackQuery, LinkPreviewOptions

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
    compile_status_text,
)
from . import status
from .archive import (
    _archive_ids,
    _archive_events,
    _archive_choices,
    _extracted_archives,
    ARCHIVE_EXT,
    extract_archive_async,
    handle_archive_choice,
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
job_queue: asyncio.Queue[int] = asyncio.Queue()
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


def format_size(size_bytes: float) -> str:
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if size_bytes < 1024.0:
            return f"{size_bytes:.1f} {unit}"
        size_bytes /= 1024.0
    return f"{size_bytes:.1f} PB"


async def process_job(job: Job) -> None:
    status._current_job_id = job.id
    status._active_job_metrics.update({
        "download_speed": 0.0,
        "upload_speed": 0.0,
        "current_download_file": None,
        "current_upload_file": None,
        "current_upload_pct": 0.0,
        "total_downloaded_bytes": 0,
        "download_count": 0,
    })
    chat_id = job.chat_id
    msg_id = job.status_message_id
    dest_dir = settings.downloads_dir / job.download_dir

    last_edited_text = ""
    _rotating_status = False

    async def rotate_status_message(new_text: str) -> None:
        nonlocal msg_id, last_edited_text, _rotating_status
        if _rotating_status:
            return
        _rotating_status = True
        try:
            if msg_id:
                try:
                    await app.delete_messages(chat_id, msg_id)
                except Exception:
                    pass
                msg_id = None
            try:
                new_msg = await app.send_message(
                    chat_id,
                    new_text,
                    link_preview_options=LinkPreviewOptions(is_disabled=True)
                )
                msg_id = new_msg.id
                last_edited_text = new_text
                await store.set_status_message(job.id, msg_id)
            except Exception:
                log.exception("Failed to send new status message")
        finally:
            _rotating_status = False

    async def report(text: str) -> bool:
        nonlocal last_edited_text
        if msg_id:
            success = await safe_edit(chat_id, msg_id, text)
            if success:
                last_edited_text = text
            return success
        else:
            await rotate_status_message(text)
            return True

    uploading_files: set[str] = set()
    uploaded_filenames = await store.get_uploaded_filenames(job.id)
    sent = len(uploaded_filenames)
    skipped: list[tuple[str, str]] = []
    session_uploaded_count = 0

    current_upload_file: str | None = None
    current_upload_pct: float = 0.0
    upload_speed: float = 0.0
    last_uploaded_bytes = 0
    last_upload_speed_time = 0.0
    last_download_file: str | None = None

    total_downloaded_bytes = 0
    download_speed = 0.0
    last_download_size = 0
    last_download_time = time.time()
    deleted_bytes = 0

    status_lock = asyncio.Lock()
    downloader_done = asyncio.Event()
    uploader_done = asyncio.Event()
    download_count = 0
    trigger_event = asyncio.Event()

    async def perform_status_edit() -> bool:
        async with status_lock:
            nonlocal msg_id, last_edited_text
            if downloader_done.is_set() and current_upload_file:
                try:
                    f_path = dest_dir / current_upload_file
                    if f_path.exists() and f_path.stat().st_size < 25 * 1024 * 1024:
                        if msg_id:
                            try:
                                await app.delete_messages(chat_id, msg_id)
                            except Exception:
                                pass
                            msg_id = None
                            last_edited_text = ""
                            await store.set_status_message(job.id, None)
                        return True
                except Exception:
                    pass

            status_text = compile_status_text(
                job=job,
                downloader_done=downloader_done.is_set(),
                uploader_done=uploader_done.is_set(),
                download_count=download_count,
                sent=sent,
                skipped_len=len(skipped),
                current_upload_file=current_upload_file,
                current_upload_pct=current_upload_pct,
                upload_speed=upload_speed,
            )

            if status_text == last_edited_text:
                return True

            return await report(status_text)

    async def status_updater_loop() -> None:
        base_cooldown = 20.0
        current_cooldown = base_cooldown
        edit_count = 0
        try:
            while not (downloader_done.is_set() and uploader_done.is_set()):
                if _shutdown_event.is_set():
                    break
                try:
                    await asyncio.wait_for(trigger_event.wait(), timeout=current_cooldown)
                except asyncio.TimeoutError:
                    pass
                else:
                    trigger_event.clear()

                edit_count += 1
                if edit_count > 60:
                    current_cooldown = max(current_cooldown, 45.0)
                elif edit_count > 30:
                    current_cooldown = max(current_cooldown, 30.0)

                success = await perform_status_edit()
                if not success:
                    current_cooldown = min(current_cooldown + 15.0, 90.0)
                    log.info("Status updater loop backed off to %ss cooldown", current_cooldown)
                else:
                    target = 45.0 if edit_count > 60 else (30.0 if edit_count > 30 else base_cooldown)
                    current_cooldown = max(current_cooldown - 2.0, target)

                await asyncio.sleep(current_cooldown)

            await perform_status_edit()
        except asyncio.CancelledError:
            pass
        except Exception:
            log.exception("Error in status updater loop")

    def on_progress(count: int, filename: Optional[str] = None) -> None:
        nonlocal download_count
        download_count = count
        status._active_job_metrics["download_count"] = count

    async def on_upload_progress(current: int, total: int) -> None:
        nonlocal current_upload_pct, upload_speed, last_uploaded_bytes, last_upload_speed_time
        if total == 0:
            return
        pct = current * 100.0 / total
        now = time.time()
        dt = now - last_upload_speed_time
        if dt >= 1.0 or last_upload_speed_time == 0.0:
            bytes_diff = current - last_uploaded_bytes
            speed = bytes_diff / dt if dt > 0 else 0.0
            upload_speed = 0.7 * speed + 0.3 * upload_speed if last_uploaded_bytes > 0 else speed
            last_uploaded_bytes = current
            last_upload_speed_time = now
            status._active_job_metrics["upload_speed"] = upload_speed

        current_upload_pct = pct
        status._active_job_metrics["current_upload_pct"] = pct

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
                job.url, dest_dir, on_progress=on_progress, extra_args=extra_args
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
                    status._active_job_metrics["download_speed"] = download_speed
                    status._active_job_metrics["total_downloaded_bytes"] = total_downloaded_bytes

                current_file = None
                try:
                    part_files = sorted(p.name for p in dest_dir.rglob("*.part") if p.is_file())
                    if part_files:
                        current_file = part_files[0]
                except Exception:
                    pass

                if status._active_job_metrics.get("current_download_file") != current_file:
                    status._active_job_metrics["current_download_file"] = current_file
                    trigger_event.set()

                await asyncio.sleep(2)
        except asyncio.CancelledError:
            pass

    async def perform_uploads() -> None:
        nonlocal sent, session_uploaded_count, current_upload_file, current_upload_pct
        nonlocal upload_speed, last_uploaded_bytes, last_upload_speed_time, deleted_bytes
        nonlocal msg_id, last_edited_text
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

            # Check if this file is an archive
            is_archive = f.suffix.lower() in ARCHIVE_EXT
            if is_archive:
                # Find or generate archive_id
                if job.id not in _archive_ids:
                    _archive_ids[job.id] = {}
                archive_id = None
                for aid, fname in _archive_ids[job.id].items():
                    if fname == f.name:
                        archive_id = aid
                        break
                if archive_id is None:
                    archive_id = str(len(_archive_ids[job.id]) + 1)
                    _archive_ids[job.id][archive_id] = f.name

                # Check if choice exists
                job_choices = _archive_choices.get(job.id, {})
                if archive_id not in job_choices:
                    from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton
                    keyboard = InlineKeyboardMarkup([
                        [
                            InlineKeyboardButton("Archive Only", callback_data=f"archive_only:{job.id}:{archive_id}"),
                            InlineKeyboardButton("Archive + Extract", callback_data=f"archive_ext:{job.id}:{archive_id}")
                        ]
                    ])
                    prompt_text = (
                        f"**Job #{job.id} - Archive Detected**\n"
                        f"- **File**: `{f.name}`\n\n"
                        f"Do you want to upload the archive file only, or extract its contents and upload both?"
                    )
                    await app.send_message(
                        chat_id,
                        prompt_text,
                        reply_markup=keyboard,
                        link_preview_options=LinkPreviewOptions(is_disabled=True)
                    )

                    if job.id not in _archive_events:
                        _archive_events[job.id] = {}
                    _archive_events[job.id][archive_id] = asyncio.Event()

                    # Wait for user input
                    await _archive_events[job.id][archive_id].wait()

                # Choice is now set!
                choice = _archive_choices[job.id][archive_id]
                if choice == "ext" and f.name not in _extracted_archives.get(job.id, set()):
                    if job.id not in _extracted_archives:
                        _extracted_archives[job.id] = set()
                    _extracted_archives[job.id].add(f.name)

                    log.info("Extracting archive %s for job %s", f.name, job.id)
                    extracted = await extract_archive_async(f, dest_dir)
                    if extracted:
                        log.info("Successfully extracted archive %s", f.name)
                        await app.send_message(
                            chat_id,
                            f"**Job #{job.id} - Archive Extracted**\nSuccessfully extracted `{f.name}` into download directory.",
                            link_preview_options=LinkPreviewOptions(is_disabled=True)
                        )
                        # Break loop to force re-scanning of extracted files
                        break
                    else:
                        log.error("Failed to extract archive %s", f.name)
                        await app.send_message(
                            chat_id,
                            f"**Job #{job.id} - Extraction Failed**\nFailed to extract `{f.name}`.",
                            link_preview_options=LinkPreviewOptions(is_disabled=True)
                        )

            # Check if this file needs conversion
            is_incompatible = f.suffix.lower() in CONVERSION_EXT
            if is_incompatible and f.name not in _converted_files.get(job.id, set()):
                if job.id not in _conversion_ids:
                    _conversion_ids[job.id] = {}
                conv_id = None
                for cid, fname in _conversion_ids[job.id].items():
                    if fname == f.name:
                        conv_id = cid
                        break
                if conv_id is None:
                    conv_id = str(len(_conversion_ids[job.id]) + 1)
                    _conversion_ids[job.id][conv_id] = f.name

                job_choices = _conversion_choices.get(job.id, {})
                if conv_id not in job_choices:
                    from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton
                    keyboard = InlineKeyboardMarkup([
                        [
                            InlineKeyboardButton("Convert to MP4", callback_data=f"convert_mp4:{job.id}:{conv_id}"),
                            InlineKeyboardButton("Original File", callback_data=f"convert_orig:{job.id}:{conv_id}")
                        ]
                    ])
                    prompt_text = (
                        f"**Job #{job.id} - Incompatible Video Format**\n"
                        f"- **File**: `{f.name}`\n\n"
                        f"The file format is not natively supported for Telegram inline streaming. "
                        f"Do you want to convert it to MP4 first, or upload the original as a document?"
                    )
                    await app.send_message(
                        chat_id,
                        prompt_text,
                        reply_markup=keyboard,
                        link_preview_options=LinkPreviewOptions(is_disabled=True)
                    )

                    if job.id not in _conversion_events:
                        _conversion_events[job.id] = {}
                    _conversion_events[job.id][conv_id] = asyncio.Event()

                    await _conversion_events[job.id][conv_id].wait()

                choice = _conversion_choices[job.id][conv_id]
                if choice == "mp4":
                    if job.id not in _converted_files:
                        _converted_files[job.id] = set()
                    _converted_files[job.id].add(f.name)

                    log.info("Converting video %s to MP4 for job %s", f.name, job.id)
                    output_name = f.stem + "_converted.mp4"
                    output_path = f.parent / output_name

                    await app.send_message(
                        chat_id,
                        f"**Job #{job.id} - Media Conversion**\nConverting `{f.name}` to standard MP4 container...",
                        link_preview_options=LinkPreviewOptions(is_disabled=True)
                    )

                    success = await convert_media_async(f, output_path)
                    if success:
                        log.info("Successfully converted video %s to %s", f.name, output_name)
                        f.unlink(missing_ok=True)
                        break
                    else:
                        log.error("Failed to convert video %s", f.name)
                        await app.send_message(
                            chat_id,
                            f"**Job #{job.id} - Conversion Failed**\nFailed to convert `{f.name}`. Uploading original as document.",
                            link_preview_options=LinkPreviewOptions(is_disabled=True)
                        )
                        _conversion_choices[job.id][conv_id] = "orig"

            max_limit = int(1.95 * 1024 * 1024 * 1024)
            if f.exists() and f.stat().st_size > max_limit:
                from .uploader import handle_large_file
                split_parts = await handle_large_file(f, bool(db_job.split_large_files))
                if not split_parts:
                    skipped.append((f.name, "File exceeds 1.95GB limit and was skipped"))
                    await store.update_progress(job.id, sent_files=sent, skipped_files=len(skipped))
                    trigger_event.set()
                    continue
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
                status._active_job_metrics["current_upload_file"] = f.name
                status._active_job_metrics["current_upload_pct"] = 0.0
                status._active_job_metrics["upload_speed"] = 0.0
                trigger_event.set()

                await upload_file(app, chat_id, f, progress=on_upload_progress)
                await store.mark_uploaded(job.id, f.name)

                uploaded_filenames.add(f.name)
                sent += 1
                await log_upload(job.id, f.name)
                log.info("Successfully uploaded %s for job %s", f.name, job.id)

                try:
                    f_size = f.stat().st_size
                    f.unlink(missing_ok=True)
                    deleted_bytes += f_size
                except Exception:
                    log.exception("Failed to delete file after upload: %s", f)

            except UploadTooLarge as e:
                skipped.append((f.name, str(e)))
                log.warning("File too large to upload: %s", f.name)
            except Exception as e:  
                log.exception("Upload failed for %s", f)
                skipped.append((f.name, f"error: {e}"))
            finally:
                current_upload_file = None
                current_upload_pct = 0.0
                upload_speed = 0.0
                status._active_job_metrics["current_upload_file"] = None
                status._active_job_metrics["current_upload_pct"] = 0.0
                status._active_job_metrics["upload_speed"] = 0.0
                if f.name in uploading_files:
                    uploading_files.remove(f.name)

            await store.update_progress(job.id, sent_files=sent, skipped_files=len(skipped))
            trigger_event.set()

            if msg_id:
                try:
                    await app.delete_messages(chat_id, msg_id)
                except Exception:
                    pass
                msg_id = None
                last_edited_text = ""
                await store.set_status_message(job.id, None)

            session_uploaded_count += 1
            if session_uploaded_count % settings.tg_batch_size == 0:
                await asyncio.sleep(settings.tg_batch_cooldown_s)
            else:
                await asyncio.sleep(
                    random.uniform(settings.tg_upload_delay_min, settings.tg_upload_delay_max)
                )

    async def run_uploader() -> None:
        try:
            while True:
                if _shutdown_event.is_set():
                    return
                await perform_uploads()

                # Check if we can stop
                if downloader_done.is_set():
                    has_pending = False
                    if dest_dir.exists():
                        try:
                            files = [p for p in dest_dir.rglob("*") if p.is_file() and not p.name.endswith(".part")]
                            pending = [
                                f for f in files
                                if f.name not in uploaded_filenames
                                and f.name not in uploading_files
                            ]
                            if pending:
                                has_pending = True
                        except Exception:
                            pass
                    if not has_pending:
                        break

                try:
                    await asyncio.wait_for(trigger_event.wait(), timeout=5.0)
                    trigger_event.clear()
                except asyncio.TimeoutError:
                    pass
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
    updater_task = asyncio.create_task(status_updater_loop())

    try:
        await store.update_progress(job.id, status=JobStatus.DOWNLOADING)
        await report(f"Downloading:\n{job.url}\n(rate-limited, large albums take a while)")

        result = await downloader_task
        trigger_event.set()
        await uploader_task
    except asyncio.CancelledError:
        log.info("Job %s was cancelled/aborted", job.id)
        downloader_task.cancel()
        uploader_task.cancel()
        monitor_task.cancel()
        updater_task.cancel()
        await asyncio.gather(downloader_task, uploader_task, monitor_task, updater_task, return_exceptions=True)

        db_job = await store.get_job(job.id)
        if _shutdown_event.is_set() or (db_job and db_job.status == JobStatus.QUEUED):
            await store.update_progress(job.id, status=JobStatus.QUEUED)
            await report("Paused for shutdown — will resume on next start.")
        else:
            await store.update_progress(job.id, status=JobStatus.CANCELLED, url="")
            await report("Job cancelled.")
            shutil.rmtree(dest_dir, ignore_errors=True)
        return
    except GalleryDLNotFound as e:
        await store.update_progress(job.id, status=JobStatus.FAILED, error=str(e), url="")
        await report(str(e))
        return
    except Exception as e:  
        log.exception("job %s failed", job.id)
        await store.update_progress(job.id, status=JobStatus.FAILED, error=str(e), url="")
        await report(f"Job failed with an unexpected error: {e}")
        return
    finally:
        monitor_task.cancel()
        cancellation_task.cancel()
        updater_task.cancel()
        await asyncio.gather(monitor_task, cancellation_task, updater_task, return_exceptions=True)
        if msg_id:
            try:
                await app.delete_messages(chat_id, msg_id)
            except Exception:
                pass
        _archive_ids.pop(job.id, None)
        _archive_events.pop(job.id, None)
        _archive_choices.pop(job.id, None)
        _extracted_archives.pop(job.id, None)
        _conversion_ids.pop(job.id, None)
        _conversion_events.pop(job.id, None)
        _conversion_choices.pop(job.id, None)
        _converted_files.pop(job.id, None)
        status._current_job_id = None

    if not result.ok and sent == 0:
        await store.update_progress(
            job.id, status=JobStatus.FAILED, error=result.error_tail[-1500:], url=""
        )
        await report(
            f"gallery-dl failed after {result.attempts} attempt(s) and produced no files.\n"
            f"Last output:\n```\n{result.error_tail[-800:]}\n```"
        )
        return

    files_remaining = []
    if dest_dir.exists():
        files_remaining = [p for p in dest_dir.rglob("*") if p.is_file() and not p.name.endswith(".part")]

    await store.update_progress(job.id, status=JobStatus.DONE, sent_files=sent, skipped_files=len(skipped), url="")
    
    if not result.ok:
        summary = (
            f"Completed with some errors. Uploaded {sent} file(s) total.\n\n"
            f"**Error tail:**\n"
            f"```\n{result.error_tail[-600:]}\n```"
        )
    else:
        summary = f"Done. Uploaded {sent} file(s) total."

    if skipped:
        preview = "\n".join(f"- {n} ({info})" for n, info in skipped[:20])
        more = f"\n…and {len(skipped) - 20} more" if len(skipped) > 20 else ""
        summary += f"\nSkipped:\n{preview}{more}"
    await app.send_message(chat_id, summary, link_preview_options=LinkPreviewOptions(is_disabled=True))

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
        try:
            await process_job(job)
        finally:
            status._current_job_id = None


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

    if status._current_job_id is not None:
        job = await store.get_job(status._current_job_id)
        if job and is_job_owner(chat_id, job):
            parsed_args = []
            if job.args:
                try:
                    parsed_args = json.loads(job.args)
                except Exception:
                    pass
            args_str = " ".join(parsed_args) if parsed_args else "None"
            split_str = "Yes" if job.split_large_files else "No"

            dl_speed = status._active_job_metrics["download_speed"]
            ul_speed = status._active_job_metrics["upload_speed"]
            dl_bytes = status._active_job_metrics["total_downloaded_bytes"]
            dl_count = status._active_job_metrics["download_count"]
            dl_file = status._active_job_metrics["current_download_file"]
            ul_pct = status._active_job_metrics["current_upload_pct"]
            ul_file = status._active_job_metrics["current_upload_file"]

            dl_speed_str = format_size(dl_speed)
            ul_speed_str = format_size(ul_speed)
            dl_bytes_str = format_size(dl_bytes)

            bar = make_progress_bar(ul_pct)

            status_text = (
                f"**Active Job Status**\n"
                f"- **Job ID**: #{job.id}\n"
                f"- **Status**: `{job.status}`\n"
                f"- **URL**: {format_url_display(job.url)}\n"
                f"- **Args**: `{args_str}`\n"
                f"- **Split > 2GB**: {split_str}\n\n"
                f"**Downloader Metrics**\n"
            )
            if job.status == JobStatus.DOWNLOADING and dl_file:
                status_text += f"- **Current File**: `{dl_file}`\n"
            status_text += (
                f"- **Files Downloaded**: {dl_count}\n"
                f"- **Downloaded Data**: {dl_bytes_str}\n"
                f"- **Download Speed**: {dl_speed_str}/s\n\n"
                f"**Uploader Metrics**\n"
                f"- **Files Sent**: {job.sent_files} / {job.total_files if job.total_files > 0 else 'Calculating'}\n"
                f"- **Files Skipped**: {job.skipped_files}\n"
            )
            if ul_file:
                status_text += (
                    f"- **Current File**: `{ul_file}`\n"
                    f"- **Upload Progress**: {ul_pct:.1f}%\n"
                    f"  `[{bar}]`\n"
                    f"- **Upload Speed**: {ul_speed_str}/s\n"
                )

            await message.reply_text(status_text, link_preview_options=LinkPreviewOptions(is_disabled=True))
            return

    queued = [q for q in await store.queued_jobs() if is_job_owner(chat_id, q)]

    cur = await store.db.execute("SELECT * FROM jobs WHERE status = 'waiting' AND chat_id = ? ORDER BY id", (chat_id,))
    waiting_rows = await cur.fetchall()
    waiting = [store._row_to_job(r) for r in waiting_rows]

    response = "**Bot Status: Idle**\nNo active download/upload task is currently running."

    if queued:
        response += f"\n\n**Queued Jobs ({len(queued)})**:"
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
    active_cancelled = False

    if status._current_job_id is not None:
        job = await store.get_job(status._current_job_id)
        if job and is_job_owner(chat_id, job):
            await store.update_progress(job.id, status=JobStatus.CANCELLED)
            await message.reply_text(
                f"Marked active job #{job.id} for cancellation — it'll stop after the current file finishes."
            )
            active_cancelled = True

    cur = await store.db.execute(
        "SELECT id FROM jobs WHERE chat_id = ? AND status IN ('queued', 'waiting')",
        (chat_id,)
    )
    rows = await cur.fetchall()
    cancelled_ids = [r[0] for r in rows]
    if cancelled_ids:
        for cid in cancelled_ids:
            await store.update_progress(cid, status=JobStatus.CANCELLED)
        await message.reply_text(
            f"Cancelled {len(cancelled_ids)} queued/waiting job(s) for this chat."
        )
    elif not active_cancelled:
        await message.reply_text("No active or queued jobs found for this chat.")


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

    url_display = format_url_display(urls_json)
    prompt_text = (
        f"**Job #{job.id} registered**\n"
        f"- **URL**: {url_display}\n\n"
        "Do you want to split files larger than 2GB for this job?"
    )
    status_msg = await message.reply_text(
        prompt_text,
        reply_markup=keyboard,
        link_preview_options=LinkPreviewOptions(is_disabled=True)
    )
    await store.set_status_message(job.id, status_msg.id)


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


@app.on_message(filters.text & ~filters.command(["start", "status", "cancel", "gdl"]))
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
    status_text = (
        f"**Queued (job #{job_id})**\n"
        f"- **URL**: {format_url_display(job.url)}{args_display}"
    )
    await callback_query.message.edit_text(status_text, link_preview_options=LinkPreviewOptions(is_disabled=True))
    await callback_query.answer("Choice registered.")
    await job_queue.put(job_id)


@app.on_callback_query(filters.regex(r"^archive_(only|ext):(\d+):(\d+)$"))
async def handle_archive_choice_cb(_, callback_query: CallbackQuery) -> None:
    await handle_archive_choice(callback_query, store, is_job_owner)


@app.on_callback_query(filters.regex(r"^convert_(mp4|orig):(\d+):(\d+)$"))
async def handle_conversion_choice_cb(client: Client, callback_query: CallbackQuery) -> None:
    await handle_conversion_choice(client, callback_query, store, is_job_owner)


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
                    BotCommand("gdl", "Process replied .txt links file with optional arguments"),
                    BotCommand("status", "Check current active job details or queue status"),
                    BotCommand("cancel", "Instantly abort the active download/upload task"),
                ])
                log.info("Bot commands set successfully.")
            except Exception as e:
                log.warning("Failed to set bot commands: %s", e)
            await requeue_incomplete_jobs()
            worker_task = asyncio.create_task(worker_loop())
            await idle()

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
