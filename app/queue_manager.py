from __future__ import annotations

import asyncio
import logging
import random
import time
import shutil
from pathlib import Path
from typing import Callable, Coroutine, Optional

from pyrogram import Client
from pyrogram.types import LinkPreviewOptions, ForceReply, Message

from .config import settings
from .db import Job, JobStatus, JobStore
from .uploader import upload_file, UploadTooLarge, handle_large_file
from .conversion import (
    convert_media_async,
    convert_audio_async,
    CONVERSION_EXT,
    AUDIO_CONVERSION_EXT,
    _conversion_ids,
    _conversion_events,
    _conversion_choices,
    _converted_files
)
from .archive import (
    extract_archive_async,
    ARCHIVE_EXT,
    ArchivePasswordRequired,
    _archive_ids,
    _archive_events,
    _archive_choices,
    _extracted_archives,
    _extracted_file_names
)
from .downloader import GalleryDLNotFound

log = logging.getLogger(__name__)

_password_prompt_events: dict[int, dict[str, tuple[asyncio.Event, dict]]] = {}
_password_prompt_messages: dict[int, tuple[int, str, int]] = {}


class JobState:
    def __init__(self, job: Job, dest_dir: Path):
        self.job = job
        self.job_id = job.id
        self.dest_dir = dest_dir
        self.downloader_done = asyncio.Event()
        self.uploader_done = asyncio.Event()
        self.trigger_event = asyncio.Event()
        self.download_speed = 0.0
        self.total_downloaded_bytes = 0
        self.download_count = 0
        self.download_pct = 0.0
        self.current_download_file = None
        self.upload_speed = 0.0
        self.current_upload_pct = 0.0
        self.current_upload_file = None
        self.sent = 0
        self.skipped = []
        self.uploaded_filenames = set()
        self.uploading_files = set()
        self.active_process = None
        self.msg_id = job.status_message_id
        self.last_edited_text = ""
        self.session_uploaded_count = 0
        self.deleted_bytes = 0
        self.initial_download_msg = None
        self.is_converting = False
        self.conversion_file = None


class QueueManager:
    def __init__(self):
        self.client: Optional[Client] = None
        self.store: Optional[JobStore] = None
        self.download_queue: asyncio.Queue[int] = asyncio.Queue()
        self.upload_queue: asyncio.Queue[int] = asyncio.Queue()
        self.jobs: dict[int, JobState] = {}
        self.download_workers: list[asyncio.Task] = []
        self.upload_workers: list[asyncio.Task] = []
        self.is_running = False
        self.upload_delay_multiplier = 1.0

    def notify_floodwait(self, seconds: int) -> None:
        self.upload_delay_multiplier = min(self.upload_delay_multiplier + 1.0, 5.0)
        log.warning("Uploader hit FloodWait. Increased upload delay multiplier to %.1f", self.upload_delay_multiplier)

    async def start(self, client: Client, store: JobStore) -> None:
        self.client = client
        self.store = store
        self.is_running = True
        num_dl = settings.tg_max_concurrent_downloads
        num_ul = settings.tg_max_concurrent_uploads
        
        # Start the global aria2c daemon
        try:
            from .torrent import start_aria2_daemon
            await start_aria2_daemon()
        except Exception as e:
            log.error("Failed to start global aria2c daemon at QueueManager startup: %s", e)

        for i in range(num_dl):
            self.download_workers.append(asyncio.create_task(self._download_worker_loop(i)))
        for i in range(num_ul):
            self.upload_workers.append(asyncio.create_task(self._upload_worker_loop(i)))
            
        log.info("Queue manager started with %s download and %s upload workers", num_dl, num_ul)

    async def stop(self) -> None:
        self.is_running = False
        for w in self.download_workers:
            w.cancel()
        for w in self.upload_workers:
            w.cancel()
        for job_id in list(self.jobs.keys()):
            await self.cancel_job(job_id)
        self.download_workers.clear()
        self.upload_workers.clear()
        
        # Stop the global aria2c daemon
        try:
            from .torrent import stop_aria2_daemon
            await stop_aria2_daemon()
        except Exception as e:
            log.error("Failed to stop global aria2c daemon at QueueManager shutdown: %s", e)

        log.info("Queue manager stopped")


    async def add_job(self, job_id: int) -> None:
        await self.download_queue.put(job_id)
        log.info("Job #%s added to download queue", job_id)

    async def cancel_job(self, job_id: int) -> bool:
        job_state = self.jobs.get(job_id)
        
        if self.store:
            await self.store.update_progress(job_id, status=JobStatus.CANCELLED)
            
        if not job_state:
            return False
            
        log.info("Cancelling job #%s", job_id)
        
        if job_state.active_process:
            try:
                job_state.active_process.kill()
            except Exception:
                pass
        job_state.downloader_done.set()
        job_state.uploader_done.set()
        job_state.trigger_event.set()
        shutil.rmtree(job_state.dest_dir, ignore_errors=True)
        self.jobs.pop(job_id, None)
        return True

    def get_active_jobs_for_chat(self, chat_id: int) -> list[JobState]:
        return [js for js in self.jobs.values() if js.job.chat_id == chat_id]

    async def register_process(self, job_id: int, proc: asyncio.subprocess.Process) -> None:
        job_state = self.jobs.get(job_id)
        if job_state:
            job_state.active_process = proc

    async def unregister_process(self, job_id: int) -> None:
        job_state = self.jobs.get(job_id)
        if job_state:
            job_state.active_process = None

    async def _download_worker_loop(self, worker_id: int) -> None:
        while self.is_running:
            try:
                job_id = await self.download_queue.get()
            except asyncio.CancelledError:
                break
                
            try:
                job = await self.store.get_job(job_id)
                if not job:
                    self.download_queue.task_done()
                    continue
                    
                dest_dir = (settings.downloads_dir / job.download_dir).resolve()
                job_state = JobState(job, dest_dir)
                self.jobs[job_id] = job_state
                await self.upload_queue.put(job_id)
                await self._process_download(job_state)
            except Exception:
                log.exception("Error in download worker %s", worker_id)
            finally:
                self.download_queue.task_done()

    async def _upload_worker_loop(self, worker_id: int) -> None:
        while self.is_running:
            try:
                job_id = await self.upload_queue.get()
            except asyncio.CancelledError:
                break
                
            try:
                job_state = self.jobs.get(job_id)
                if not job_state:
                    self.upload_queue.task_done()
                    continue
                    
                await self._process_upload(job_state)
            except Exception:
                log.exception("Error in upload worker %s", worker_id)
            finally:
                self.upload_queue.task_done()

    async def _process_download(self, job_state: JobState) -> None:
        job = job_state.job
        chat_id = job.chat_id
        dest_dir = job_state.dest_dir
        
        async def report(text: str) -> None:
            await safe_send(self.client, chat_id, text, link_preview_options=LinkPreviewOptions(is_disabled=True))

        try:
            is_torrent = (
                job.url.startswith("magnet:") or
                job.url.startswith("torrent:") or
                job.url.endswith(".torrent") or
                "magnet:?xt=" in job.url
            )
            is_unzip = job.url.startswith("unzip:")

            if is_torrent:
                initial_text = "Starting torrent download..."
            else:
                initial_text = f"Downloading:\n{job.url}\n(large files or magnet links take a while)"

            await self.store.update_progress(job.id, status=JobStatus.DOWNLOADING)
            job_state.initial_download_msg = await safe_send(
                self.client,
                chat_id,
                initial_text,
                link_preview_options=LinkPreviewOptions(is_disabled=True)
            )

            def reg(proc):
                job_state.active_process = proc

            monitor_task = None
            if not is_torrent and not is_unzip:
                async def monitor_download_speed():
                    last_download_size = 0
                    last_download_time = time.time()
                    while not job_state.downloader_done.is_set():
                        await asyncio.sleep(1.0)
                        if not dest_dir.exists():
                            continue
                        try:
                            on_disk = sum(p.stat().st_size for p in dest_dir.rglob("*") if p.is_file())
                            current_size = on_disk + job_state.deleted_bytes
                        except Exception:
                            continue

                        now = time.time()
                        dt = now - last_download_time
                        if dt >= 1.0:
                            speed = (current_size - last_download_size) / dt
                            job_state.download_speed = 0.7 * speed + 0.3 * job_state.download_speed if last_download_size > 0 else speed
                            last_download_size = current_size
                            last_download_time = now
                            job_state.total_downloaded_bytes = current_size
                            job_state.trigger_event.set()

                        try:
                            part_files = sorted(p.name for p in dest_dir.rglob("*.part") if p.is_file())
                            if part_files:
                                job_state.current_download_file = part_files[0]
                        except Exception:
                            pass

                monitor_task = asyncio.create_task(monitor_download_speed())

            if is_unzip:
                from .downloader import DownloadResult
                archive_files = []
                if dest_dir.exists():
                    archive_files = [p for p in dest_dir.iterdir() if p.is_file()]
                result = DownloadResult(ok=True, files=archive_files)
            elif is_torrent:
                def on_torrent_progress(
                    pct: float,
                    downloaded_bytes: float,
                    speed_bytes: float,
                    seeders: int = 0,
                    connections: int = 0,
                    name: Optional[str] = None
                ) -> None:
                    job_state.download_pct = pct
                    job_state.total_downloaded_bytes = downloaded_bytes
                    job_state.download_speed = speed_bytes
                    job_state.torrent_seeders = seeders
                    job_state.torrent_peers = connections
                    if name:
                        job_state.torrent_name = name

                from .torrent import download_torrent_async
                result = await download_torrent_async(
                    job.url, dest_dir, on_progress=on_torrent_progress, register_proc=reg
                )


            else:
                extra_args = None
                if job.args:
                    import json
                    try:
                        extra_args = json.loads(job.args)
                    except Exception:
                        pass

                def on_download_progress(count: int, filename: Optional[str] = None) -> None:
                    job_state.download_count = count
                    if filename:
                        job_state.current_download_file = filename
                    job_state.trigger_event.set()

                from .downloader import run_with_progress
                result = await run_with_progress(
                    job.url, dest_dir, on_progress=on_download_progress, extra_args=extra_args, register_proc=reg
                )

            job_state.downloader_result = result
            log.info("Download finished for job #%s (ok=%s)", job.id, result.ok)
        except Exception as e:
            from .downloader import DownloadResult
            log.exception("Download failed for job #%s", job.id)
            job_state.downloader_result = DownloadResult(ok=False, error_tail=str(e))
        finally:
            job_state.downloader_done.set()
            job_state.trigger_event.set()
            if monitor_task:
                monitor_task.cancel()
                await asyncio.gather(monitor_task, return_exceptions=True)

    async def _process_upload(self, job_state: JobState) -> None:
        job = job_state.job
        chat_id = job.chat_id
        dest_dir = job_state.dest_dir
        is_torrent = (
            job.url.startswith("magnet:") or
            job.url.startswith("torrent:") or
            job.url.endswith(".torrent") or
            "magnet:?xt=" in job.url
        )
        
        async def report(text: str) -> None:
            await safe_send(self.client, chat_id, text, link_preview_options=LinkPreviewOptions(is_disabled=True))

        async def status_updater_loop() -> None:
            while not job_state.uploader_done.is_set():
                try:
                    await asyncio.sleep(4)
                    if job_state.uploader_done.is_set():
                        break
                        
                    db_job = await self.store.get_job(job.id)
                    if not db_job or db_job.status == JobStatus.CANCELLED:
                        break
                        
                    if job_state.msg_id:
                        from .status import compile_job_status_text
                        status_text = compile_job_status_text(db_job, job_state)
                        if status_text != job_state.last_edited_text:
                            if await safe_edit(self.client, chat_id, job_state.msg_id, status_text):
                                job_state.last_edited_text = status_text
                                if getattr(job_state, "initial_download_msg", None):
                                    try:
                                        await self.client.delete_messages(chat_id, job_state.initial_download_msg.id)
                                        job_state.initial_download_msg = None
                                    except Exception:
                                        pass
                except Exception:
                    pass

        async def perform_uploads() -> None:
            nonlocal job
            if not dest_dir.exists():
                return

            try:
                files = sorted(p for p in dest_dir.rglob("*") if p.is_file())
            except Exception:
                return

            db_total = len(files)
            if job.total_files != db_total:
                await self.store.update_progress(job.id, total_files=db_total)
                job = await self.store.get_job(job.id)

            pending = [
                f for f in files
                if str(f.relative_to(dest_dir)) not in job_state.uploaded_filenames
                and str(f.relative_to(dest_dir)) not in job_state.uploading_files
            ]


            for f in pending:
                if not job_state.downloader_done.is_set():
                    try:
                        sz1 = f.stat().st_size
                        await asyncio.sleep(1.5)
                        sz2 = f.stat().st_size
                        if sz1 != sz2 or sz1 == 0:
                            continue
                    except Exception:
                        continue

                db_job = await self.store.get_job(job.id)
                if db_job and db_job.status == JobStatus.CANCELLED:
                    return

                f_rel = str(f.relative_to(dest_dir))

                is_archive = f.suffix.lower() in ARCHIVE_EXT
                if is_archive:
                    archive_prompt_msg_id = None
                    if job.id not in _archive_ids:
                        _archive_ids[job.id] = {}
                    archive_id = f_rel
                    if archive_id not in _archive_ids[job.id]:
                        _archive_ids[job.id][archive_id] = f_rel

                    if job.url.startswith("unzip:"):
                        if job.id not in _archive_choices:
                            _archive_choices[job.id] = {}
                        _archive_choices[job.id][archive_id] = "ext"
                    else:
                        if job.id not in _archive_choices or archive_id not in _archive_choices[job.id]:
                            from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton
                            from .status import compile_archive_prompt_text
                            prompt_text = compile_archive_prompt_text(job.id, f.name)
                            kb = InlineKeyboardMarkup([
                                [
                                    InlineKeyboardButton("Upload Archive Only", callback_data=f"archive_only:{job.id}:{archive_id}"),
                                    InlineKeyboardButton("Upload + Extract", callback_data=f"archive_ext:{job.id}:{archive_id}")
                                ]
                            ])
                            prompt_msg = await safe_send(self.client, chat_id, prompt_text, reply_markup=kb)
                            if prompt_msg:
                                archive_prompt_msg_id = prompt_msg.id

                            if job.id not in _archive_events:
                                _archive_events[job.id] = {}
                            _archive_events[job.id][archive_id] = asyncio.Event()

                            await _archive_events[job.id][archive_id].wait()

                    choice = _archive_choices[job.id][archive_id]
                    if choice == "ext" and f_rel not in _extracted_archives.get(job.id, set()):
                        if job.id not in _extracted_archives:
                            _extracted_archives[job.id] = set()
                        _extracted_archives[job.id].add(f_rel)

                        from .status import compile_extraction_status_text
                        status_msg = await safe_send(
                            self.client,
                            chat_id,
                            compile_extraction_status_text(job.id, f.name),
                            link_preview_options=LinkPreviewOptions(is_disabled=True)
                        )

                        before_files = set()
                        try:
                            before_files = {p.resolve() for p in dest_dir.rglob("*") if p.is_file()}
                        except Exception:
                            pass

                        try:
                            password = None
                            if job.args:
                                import json
                                try:
                                    parsed = json.loads(job.args)
                                    if isinstance(parsed, dict):
                                        password = parsed.get("password")
                                except Exception:
                                    pass

                            extracted = await extract_archive_async(f, dest_dir, password=password)
                            if extracted:
                                log.info("Successfully extracted archive %s", f.name)
                                try:
                                    after_files = {p.resolve() for p in dest_dir.rglob("*") if p.is_file()}
                                    new_files = after_files - before_files
                                    if job.id not in _extracted_file_names:
                                        _extracted_file_names[job.id] = set()
                                    for new_f in new_files:
                                        _extracted_file_names[job.id].add(new_f.name)
                                except Exception:
                                    pass

                                if job.url.startswith("unzip:"):
                                    try:
                                        f.unlink(missing_ok=True)
                                    except Exception:
                                        pass

                                if archive_prompt_msg_id:
                                    try:
                                        await self.client.delete_messages(chat_id, archive_prompt_msg_id)
                                    except Exception:
                                        pass
                                try:
                                    if status_msg:
                                        await self.client.delete_messages(chat_id, status_msg.id)
                                except Exception:
                                    pass

                                from .status import compile_extraction_success_status_text
                                success_msg = await safe_send(
                                    self.client,
                                    chat_id,
                                    compile_extraction_success_status_text(job.id, f.name),
                                    link_preview_options=LinkPreviewOptions(is_disabled=True)
                                )
                                async def delete_success_msg(m):
                                    await asyncio.sleep(5)
                                    try:
                                        await self.client.delete_messages(chat_id, m.id)
                                    except Exception:
                                        pass
                                if success_msg:
                                    asyncio.create_task(delete_success_msg(success_msg))

                                break
                            else:
                                log.error("Failed to extract archive %s", f.name)
                                try:
                                    if status_msg:
                                        await self.client.delete_messages(chat_id, status_msg.id)
                                except Exception:
                                    pass
                                from .status import compile_extraction_failed_status_text
                                fail_msg = await safe_send(
                                    self.client,
                                    chat_id,
                                    compile_extraction_failed_status_text(job.id, f.name),
                                    link_preview_options=LinkPreviewOptions(is_disabled=True)
                                )
                                if job.url.startswith("unzip:"):
                                    raise Exception(f"Failed to extract archive {f.name}")
                                if archive_prompt_msg_id:
                                    try:
                                        await self.client.delete_messages(chat_id, archive_prompt_msg_id)
                                    except Exception:
                                        pass
                                async def delete_fail_msg(m):
                                    await asyncio.sleep(5)
                                    try:
                                        await self.client.delete_messages(chat_id, m.id)
                                    except Exception:
                                        pass
                                if fail_msg:
                                    asyncio.create_task(delete_fail_msg(fail_msg))
                        except ArchivePasswordRequired:
                            log.warning("Archive %s requires a password to extract", f.name)
                            try:
                                if status_msg:
                                    await self.client.delete_messages(chat_id, status_msg.id)
                            except Exception:
                                pass
                            
                            prompt_msg = await safe_send(
                                self.client,
                                chat_id,
                                f"**Password Required**: `{f.name}` is password-protected or password was incorrect.\n\n"
                                f"Please reply directly to this message with the password to extract it.",
                                reply_markup=ForceReply(placeholder="Enter archive password")
                            )
                            if prompt_msg:
                                if job.id not in _password_prompt_events:
                                    _password_prompt_events[job.id] = {}
                                event = asyncio.Event()
                                data = {"password": None}
                                _password_prompt_events[job.id][archive_id] = (event, data)
                                _password_prompt_messages[prompt_msg.id] = (job.id, archive_id, chat_id)
                                
                                try:
                                    await asyncio.wait_for(event.wait(), timeout=300)
                                    new_password = data["password"]
                                    
                                    import json
                                    job_args_dict = {}
                                    if job.args:
                                        try:
                                            job_args_dict = json.loads(job.args)
                                        except Exception:
                                            pass
                                    job_args_dict["password"] = new_password
                                    await self.store.db.execute(
                                        "UPDATE jobs SET args = ? WHERE id = ?",
                                        (json.dumps(job_args_dict), job.id)
                                    )
                                    await self.store.db.commit()
                                    
                                    job_state.job = await self.store.get_job(job.id)
                                    job = job_state.job
                                    
                                    try:
                                        await self.client.delete_messages(chat_id, prompt_msg.id)
                                    except Exception:
                                        pass
                                    break
                                except asyncio.TimeoutError:
                                    try:
                                        await self.client.delete_messages(chat_id, prompt_msg.id)
                                    except Exception:
                                        pass
                                    await safe_send(
                                        self.client,
                                        chat_id,
                                        f"**Job #{job.id} aborted**: Timeout waiting for password for `{f.name}`."
                                    )
                                    raise Exception(f"Timeout waiting for password for {f.name}")
                                finally:
                                    _password_prompt_events.get(job.id, {}).pop(archive_id, None)
                                    _password_prompt_messages.pop(prompt_msg.id, None)
                            else:
                                raise
                        except Exception:
                            try:
                                if status_msg:
                                    await self.client.delete_messages(chat_id, status_msg.id)
                            except Exception:
                                pass
                            raise

                    if choice == "only" and archive_prompt_msg_id:
                        try:
                            await self.client.delete_messages(chat_id, archive_prompt_msg_id)
                        except Exception:
                            pass

                if job.url.startswith("unzip:") and f.suffix.lower() in ARCHIVE_EXT:
                    try:
                        f.unlink(missing_ok=True)
                    except Exception:
                        pass
                    continue

                is_incompatible = f.suffix.lower() in CONVERSION_EXT
                if is_incompatible and not is_torrent:
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

                    choice = _conversion_choices.get(job.id, {}).get(conv_id)
                    if choice != "orig" and f.name not in _converted_files.get(job.id, set()):
                         conversion_prompt_msg_id = None
                         if choice is None:
                             from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton
                             from .status import compile_conversion_prompt_text
                             keyboard = InlineKeyboardMarkup([
                                 [
                                     InlineKeyboardButton("Convert to MP4", callback_data=f"convert_mp4:{job.id}:{conv_id}"),
                                     InlineKeyboardButton("Original File", callback_data=f"convert_orig:{job.id}:{conv_id}")
                                 ]
                             ])
                             prompt_text = compile_conversion_prompt_text(job.id, f.name)
                             prompt_msg = await safe_send(self.client, chat_id, prompt_text, reply_markup=keyboard)
                             if prompt_msg:
                                 conversion_prompt_msg_id = prompt_msg.id

                             if job.id not in _conversion_events:
                                 _conversion_events[job.id] = {}
                             _conversion_events[job.id][conv_id] = asyncio.Event()

                             await _conversion_events[job.id][conv_id].wait()
                             choice = _conversion_choices.get(job.id, {}).get(conv_id)

                         if choice == "mp4":
                             if job.id not in _converted_files:
                                 _converted_files[job.id] = set()
                             _converted_files[job.id].add(f.name)

                             log.info("Converting video %s to MP4 for job %s", f.name, job.id)
                             output_name = f.stem + "_converted.mp4"
                             output_path = f.parent / output_name

                             from .status import compile_conversion_running_status_text
                             conv_msg = await safe_send(
                                 self.client,
                                 chat_id,
                                 compile_conversion_running_status_text(job.id, f.name)
                             )

                             job_state.is_converting = True
                             job_state.conversion_file = f.name
                             job_state.trigger_event.set()

                             try:
                                 success = await convert_media_async(f, output_path)
                             finally:
                                 job_state.is_converting = False
                                 job_state.conversion_file = None
                                 job_state.trigger_event.set()

                             if conv_msg:
                                 try:
                                     await self.client.delete_messages(chat_id, conv_msg.id)
                                 except Exception:
                                     pass

                             if success:
                                 log.info("Successfully converted video %s to %s", f.name, output_name)
                                 try:
                                     f.unlink(missing_ok=True)
                                 except Exception:
                                     pass

                                 if conversion_prompt_msg_id:
                                     try:
                                         await self.client.delete_messages(chat_id, conversion_prompt_msg_id)
                                     except Exception:
                                         pass
                                 break
                             else:
                                 log.error("Failed to convert video %s", f.name)
                                 from .status import compile_conversion_failed_status_text
                                 fail_msg = await safe_send(
                                     self.client,
                                     chat_id,
                                     compile_conversion_failed_status_text(job.id, f.name)
                                 )
                                 async def delete_fail_msg(m):
                                     await asyncio.sleep(5)
                                     try:
                                         await self.client.delete_messages(chat_id, m.id)
                                     except Exception:
                                         pass
                                 if fail_msg:
                                     asyncio.create_task(delete_fail_msg(fail_msg))

                                 if job.id not in _conversion_choices:
                                     _conversion_choices[job.id] = {}
                                 _conversion_choices[job.id][conv_id] = "orig"

                         if choice == "orig" and conversion_prompt_msg_id:
                             try:
                                 await self.client.delete_messages(chat_id, conversion_prompt_msg_id)
                             except Exception:
                                 pass

                # Audio format conversion & processing using Pedalboard
                is_audio_incompatible = f.suffix.lower() in AUDIO_CONVERSION_EXT
                if is_audio_incompatible and not is_torrent:
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

                    choice = _conversion_choices.get(job.id, {}).get(conv_id)
                    if choice != "orig" and f.name not in _converted_files.get(job.id, set()):
                        conversion_prompt_msg_id = None
                        if choice is None:
                            from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton
                            from .status import compile_audio_conversion_prompt_text
                            keyboard = InlineKeyboardMarkup([
                                [
                                    InlineKeyboardButton("Convert to MP3", callback_data=f"convert_mp3:{job.id}:{conv_id}"),
                                    InlineKeyboardButton("Original File", callback_data=f"convert_orig:{job.id}:{conv_id}")
                                ]
                            ])
                            prompt_text = compile_audio_conversion_prompt_text(job.id, f.name)
                            prompt_msg = await safe_send(self.client, chat_id, prompt_text, reply_markup=keyboard)
                            if prompt_msg:
                                conversion_prompt_msg_id = prompt_msg.id

                            if job.id not in _conversion_events:
                                _conversion_events[job.id] = {}
                            _conversion_events[job.id][conv_id] = asyncio.Event()

                            await _conversion_events[job.id][conv_id].wait()
                            choice = _conversion_choices.get(job.id, {}).get(conv_id)

                        if choice == "mp3":
                            if job.id not in _converted_files:
                                _converted_files[job.id] = set()
                            _converted_files[job.id].add(f.name)

                            log.info("Converting/processing audio %s to MP3 for job %s", f.name, job.id)
                            output_name = f.stem + "_converted.mp3"
                            output_path = f.parent / output_name

                            from .status import compile_audio_conversion_running_status_text
                            conv_msg = await safe_send(
                                self.client,
                                chat_id,
                                compile_audio_conversion_running_status_text(job.id, f.name)
                            )

                            job_state.is_converting = True
                            job_state.conversion_file = f.name
                            job_state.trigger_event.set()

                            try:
                                success = await convert_audio_async(f, output_path)
                            finally:
                                job_state.is_converting = False
                                job_state.conversion_file = None
                                job_state.trigger_event.set()

                            if conv_msg:
                                try:
                                    await self.client.delete_messages(chat_id, conv_msg.id)
                                except Exception:
                                    pass

                            if success:
                                log.info("Successfully converted audio %s to %s", f.name, output_name)
                                try:
                                    f.unlink(missing_ok=True)
                                except Exception:
                                    pass

                                if conversion_prompt_msg_id:
                                    try:
                                        await self.client.delete_messages(chat_id, conversion_prompt_msg_id)
                                    except Exception:
                                        pass
                                break
                            else:
                                log.error("Failed to convert audio %s", f.name)
                                from .status import compile_audio_conversion_failed_status_text
                                fail_msg = await safe_send(
                                    self.client,
                                    chat_id,
                                    compile_audio_conversion_failed_status_text(job.id, f.name)
                                )
                                async def delete_fail_msg(m):
                                    await asyncio.sleep(5)
                                    try:
                                        await self.client.delete_messages(chat_id, m.id)
                                    except Exception:
                                        pass
                                if fail_msg:
                                    asyncio.create_task(delete_fail_msg(fail_msg))

                                if job.id not in _conversion_choices:
                                    _conversion_choices[job.id] = {}
                                _conversion_choices[job.id][conv_id] = "orig"

                        if choice == "orig" and conversion_prompt_msg_id:
                            try:
                                await self.client.delete_messages(chat_id, conversion_prompt_msg_id)
                            except Exception:
                                pass

                # Handle large files (>1.95GB) splitting before upload
                try:
                    split_parts = await handle_large_file(f, bool(job.split_large_files))
                except Exception as sle:
                    log.exception("Error while handling large file split for %s: %s", f.name, sle)
                    split_parts = [f]

                if not split_parts:
                    # File was skipped (deleted because it was too large and split was false or failed)
                    await self.store.mark_uploaded(job.id, f_rel)
                    break

                if len(split_parts) == 1 and split_parts[0] == f:
                    # File was not split, proceed normally
                    pass
                else:
                    # File was split/deleted. Mark the original file as complete in the database and break to scan the parts
                    await self.store.mark_uploaded(job.id, f_rel)
                    break

                job_state.uploading_files.add(f_rel)
                try:
                    await self.store.update_progress(job.id, status=JobStatus.UPLOADING)
                    
                    last_uploaded_bytes = 0
                    last_upload_speed_time = time.time()
                    
                    async def progress_cb(current, total):
                        nonlocal last_uploaded_bytes, last_upload_speed_time
                        job_state.current_upload_pct = (current / total) * 100 if total > 0 else 0.0
                        now = time.time()
                        elapsed = now - last_upload_speed_time
                        if elapsed >= 1.0:
                            uploaded_since_last = current - last_uploaded_bytes
                            job_state.upload_speed = uploaded_since_last / elapsed
                            last_uploaded_bytes = current
                            last_upload_speed_time = now

                    job_state.current_upload_file = f.name
                    await upload_file(self.client, chat_id, f, progress=progress_cb)
                    await self.store.mark_uploaded(job.id, f_rel)

                    job_state.uploaded_filenames.add(f_rel)
                    job_state.sent += 1
                    await log_upload(job.id, f.name)
                    log.info("Successfully uploaded %s for job %s", f.name, job.id)

                    try:
                        f_size = f.stat().st_size
                        f.unlink(missing_ok=True)
                        job_state.deleted_bytes += f_size
                    except Exception:
                        log.exception("Failed to delete file after upload: %s", f)

                except UploadTooLarge as e:
                    job_state.skipped.append((f.name, str(e)))
                except Exception as e:  
                    log.exception("Upload failed for %s", f)
                    job_state.skipped.append((f.name, f"error: {e}"))
                finally:
                    job_state.current_upload_file = None
                    job_state.current_upload_pct = 0.0
                    job_state.upload_speed = 0.0
                    if f_rel in job_state.uploading_files:
                        job_state.uploading_files.remove(f_rel)

                await self.store.update_progress(job.id, sent_files=job_state.sent, skipped_files=len(job_state.skipped))
                job_state.trigger_event.set()

                # Decay delay multiplier slowly on successful upload
                self.upload_delay_multiplier = max(self.upload_delay_multiplier - 0.05, 1.0)

                job_state.session_uploaded_count += 1
                if job_state.session_uploaded_count % settings.tg_batch_size == 0:
                    await asyncio.sleep(settings.tg_batch_cooldown_s * self.upload_delay_multiplier)
                else:
                    delay = random.uniform(settings.tg_upload_delay_min, settings.tg_upload_delay_max) * self.upload_delay_multiplier
                    await asyncio.sleep(delay)

        async def run_uploader() -> None:
            if is_torrent:
                while not job_state.downloader_done.is_set():
                    await asyncio.sleep(2.0)

                try:
                    if dest_dir.exists():
                        files = sorted(p for p in dest_dir.rglob("*") if p.is_file())
                        for f in files:
                            db_job = await self.store.get_job(job.id)
                            if not db_job or db_job.status == JobStatus.CANCELLED or job_state.uploader_done.is_set():
                                break

                            if f.suffix.lower() in CONVERSION_EXT:
                                job_state.is_converting = True
                                job_state.conversion_file = f.name
                                job_state.trigger_event.set()

                                output_path = f.with_suffix(".mp4")
                                if output_path.exists():
                                    output_path = f.with_name(f"{f.stem}_converted.mp4")

                                log.info("Converting incompatible torrent file %s to %s", f.name, output_path.name)
                                success = await convert_media_async(f, output_path)
                                if success:
                                    log.info("Successfully converted incompatible torrent file %s", f.name)
                                    try:
                                        f.unlink(missing_ok=True)
                                    except Exception:
                                        pass
                                else:
                                    log.error("Failed to convert incompatible torrent file %s", f.name)
                            elif f.suffix.lower() in AUDIO_CONVERSION_EXT:
                                job_state.is_converting = True
                                job_state.conversion_file = f.name
                                job_state.trigger_event.set()

                                output_path = f.with_suffix(".mp3")
                                if output_path.exists():
                                    output_path = f.with_name(f"{f.stem}_converted.mp3")

                                log.info("Converting incompatible audio torrent file %s to %s", f.name, output_path.name)
                                success = await convert_audio_async(f, output_path)
                                if success:
                                    log.info("Successfully converted incompatible audio torrent file %s", f.name)
                                    try:
                                        f.unlink(missing_ok=True)
                                    except Exception:
                                        pass
                                else:
                                    log.error("Failed to convert incompatible audio torrent file %s", f.name)
                except Exception as ce:
                    log.exception("Error during torrent media conversion: %s", ce)
                finally:
                    job_state.is_converting = False
                    job_state.conversion_file = None
                    job_state.trigger_event.set()
            else:
                while not job_state.downloader_done.is_set():
                    has_completed_file = False
                    if dest_dir.exists():
                        try:
                            files = [p for p in dest_dir.rglob("*") if p.is_file() and not p.name.endswith(".part")]
                            for f in files:
                                sz1 = f.stat().st_size
                                await asyncio.sleep(0.5)
                                sz2 = f.stat().st_size
                                if sz1 == sz2 and sz1 > 0:
                                    has_completed_file = True
                                    break
                        except Exception:
                            pass
                    if has_completed_file:
                        break
                    await asyncio.sleep(2.0)


            while True:
                await perform_uploads()

                if job_state.downloader_done.is_set():
                    has_pending = False
                    if dest_dir.exists():
                        try:
                            files = [p for p in dest_dir.rglob("*") if p.is_file() and not p.name.endswith(".part")]
                            pending = [
                                f for f in files
                                if str(f.relative_to(dest_dir)) not in job_state.uploaded_filenames
                                and str(f.relative_to(dest_dir)) not in job_state.uploading_files
                            ]
                            if pending:
                                has_pending = True
                        except Exception:
                            pass
                    if not has_pending:
                        break

                try:
                    await asyncio.wait_for(job_state.trigger_event.wait(), timeout=5.0)
                    job_state.trigger_event.clear()
                except asyncio.TimeoutError:
                    pass

        updater_task = asyncio.create_task(status_updater_loop())
        try:
            await run_uploader()
            
            dl_res = getattr(job_state, "downloader_result", None)
            
            if dl_res and not dl_res.ok and job_state.sent == 0:
                await self.store.update_progress(
                    job.id, status=JobStatus.FAILED, error=dl_res.error_tail[-1500:], url=""
                )
                await report(
                    f"Download failed after {dl_res.attempts} attempt(s) and produced no files.\n"
                    f"Last output:\n```\n{dl_res.error_tail[-800:]}\n```"
                )
                return

            await self.store.update_progress(job.id, status=JobStatus.DONE, sent_files=job_state.sent, skipped_files=len(job_state.skipped), url="")

            summary = f"Done. Uploaded {job_state.sent} file(s) total."
            if dl_res and not dl_res.ok:
                summary = (
                    f"Completed with some errors. Uploaded {job_state.sent} file(s) total.\n\n"
                    f"**Error tail:**\n"
                    f"```\n{dl_res.error_tail[-600:]}\n```"
                )
            if job_state.skipped:
                preview = "\n".join(f"- {n} ({info})" for n, info in job_state.skipped[:20])
                more = f"\n…and {len(job_state.skipped) - 20} more" if len(job_state.skipped) > 20 else ""
                summary += f"\nSkipped:\n{preview}{more}"
                
            await report(summary)
            shutil.rmtree(dest_dir, ignore_errors=True)
            
        except Exception as e:
            log.exception("Upload process failed for job #%s", job.id)
            await self.store.update_progress(job.id, status=JobStatus.FAILED, error=str(e), url="")
            await report(f"Job failed with error: {e}")
            shutil.rmtree(dest_dir, ignore_errors=True)
        finally:
            job_state.uploader_done.set()
            updater_task.cancel()
            await asyncio.gather(updater_task, return_exceptions=True)
            
            _archive_ids.pop(job.id, None)
            _archive_events.pop(job.id, None)
            _archive_choices.pop(job.id, None)
            _extracted_archives.pop(job.id, None)
            _extracted_file_names.pop(job.id, None)
            _conversion_ids.pop(job.id, None)
            _conversion_events.pop(job.id, None)
            _conversion_choices.pop(job.id, None)
            _converted_files.pop(job.id, None)
            _password_prompt_events.pop(job.id, None)
            to_remove = [mid for mid, info in _password_prompt_messages.items() if info[0] == job.id]
            for mid in to_remove:
                _password_prompt_messages.pop(mid, None)
            self.jobs.pop(job.id, None)


async def safe_send(client: Client, chat_id: int, text: str, **kwargs) -> Message | None:
    from pyrogram.errors import FloodWait
    for _ in range(3):
        try:
            return await client.send_message(chat_id, text, **kwargs)
        except FloodWait as e:
            log.warning("Telegram FloodWait: waiting %s seconds on send", e.value)
            await asyncio.sleep(e.value + 1)
        except Exception as e:
            log.warning("Failed to send message: %s", e)
            return None
    return None


async def safe_edit(client: Client, chat_id: int, message_id: int, text: str) -> bool:
    from pyrogram.errors import FloodWait, MessageNotModified
    try:
        await client.edit_message_text(chat_id, message_id, text, link_preview_options=LinkPreviewOptions(is_disabled=True))
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


async def log_upload(job_id: int, filename: str) -> None:
    log_path = settings.log_dir / "uploads.log"
    log_path.parent.mkdir(parents=True, exist_ok=True)

    def append_to_file():
        with open(log_path, "a", encoding="utf-8") as f:
            f.write(f"{time.strftime('%Y-%m-%d %H:%M:%S')} - Job #{job_id} - Uploaded: {filename}\n")

    await asyncio.to_thread(append_to_file)


def is_job_owner(chat_id: int, job: Job) -> bool:
    return job.chat_id == chat_id

queue_manager = QueueManager()
