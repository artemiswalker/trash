from __future__ import annotations

import asyncio
import logging
import random
import time
import shutil
from pathlib import Path
from typing import Optional

import json
from pyrogram import Client
from pyrogram.types import LinkPreviewOptions, Message, ForceReply, InlineKeyboardMarkup, InlineKeyboardButton

from ..config import settings
from ..db import Job, JobStatus, JobStore
# conversion and archive imports moved inside methods to break circular dependencies

# Local imports from manager package
from .state import JobState
from .status import (
    safe_edit,
    compile_queued_status_text,
    compile_job_status_text,
    compile_archive_prompt_text,
    compile_extraction_status_text,
    compile_extraction_success_status_text,
    compile_extraction_failed_status_text,
    compile_conversion_prompt_text,
    compile_conversion_running_status_text,
    compile_conversion_failed_status_text,
    compile_audio_conversion_prompt_text,
    compile_audio_conversion_running_status_text,
    compile_audio_conversion_failed_status_text,
)

log = logging.getLogger(__name__)

_password_prompt_events: dict[str, dict[str, tuple[asyncio.Event, dict]]] = {}
_password_prompt_messages: dict[int, tuple[str, str, int]] = {}


class QueueManager:
    def __init__(self):
        self.client: Optional[Client] = None
        self.store: Optional[JobStore] = None
        self.download_queue: asyncio.Queue[str] = asyncio.Queue()
        self.upload_queue: asyncio.Queue[str] = asyncio.Queue()
        self.jobs: dict[str, JobState] = {}
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
            from ..downloader import start_aria2_daemon
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
            from ..downloader import stop_aria2_daemon
            await stop_aria2_daemon()
        except Exception as e:
            log.error("Failed to stop global aria2c daemon at QueueManager shutdown: %s", e)

        log.info("Queue manager stopped")

    async def add_job(self, job_id: str) -> None:
        await self.download_queue.put(job_id)
        log.info("Job #%s added to download queue", job_id)

    async def cancel_job(self, job_id: str) -> bool:
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

        if job_state.active_download_task:
            try:
                job_state.active_download_task.cancel()
            except Exception:
                pass

        if job_state.active_upload_task:
            try:
                job_state.active_upload_task.cancel()
            except Exception:
                pass

        job_state.downloader_done.set()
        job_state.uploader_done.set()
        job_state.trigger_event.set()
        shutil.rmtree(job_state.dest_dir, ignore_errors=True)
        shutil.rmtree(job_state.dest_dir.parent / f"{job_state.dest_dir.name}_extracted", ignore_errors=True)
        self.jobs.pop(job_id, None)
        return True

    def get_active_jobs_for_chat(self, chat_id: int) -> list[JobState]:
        return [js for js in self.jobs.values() if js.job.chat_id == chat_id]

    async def register_process(self, job_id: str, proc: asyncio.subprocess.Process) -> None:
        job_state = self.jobs.get(job_id)
        if job_state:
            job_state.active_process = proc

    async def unregister_process(self, job_id: str) -> None:
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
                if not job or job.status == JobStatus.CANCELLED:
                    self.download_queue.task_done()
                    continue
                    
                dest_dir = (settings.downloads_dir / job.download_dir).resolve()
                job_state = JobState(job, dest_dir)
                if job.status_message_id:
                    job_state.msg_id = job.status_message_id
                self.jobs[job_id] = job_state
                await self.upload_queue.put(job_id)
                await self._process_download(job_state)
            except asyncio.CancelledError:
                if not self.is_running:
                    break
                log.info("Job execution cancelled in download worker %s", worker_id)
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
                    
                db_job = await self.store.get_job(job_id)
                if not db_job or db_job.status == JobStatus.CANCELLED:
                    self.upload_queue.task_done()
                    self.jobs.pop(job_id, None)
                    continue
                    
                await self._process_upload(job_state)
            except asyncio.CancelledError:
                if not self.is_running:
                    break
                log.info("Job execution cancelled in upload worker %s", worker_id)
            except Exception:
                log.exception("Error in upload worker %s", worker_id)
            finally:
                self.upload_queue.task_done()

    async def _process_download(self, job_state: JobState) -> None:
        job_state.active_download_task = asyncio.current_task()
        job = job_state.job
        chat_id = job.chat_id
        dest_dir = job_state.dest_dir
        
        async def report(text: str) -> None:
            await safe_send(self.client, chat_id, text, link_preview_options=LinkPreviewOptions(is_disabled=True))

        monitor_task = None
        try:
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
            is_unzip = cleaned_url.startswith("unzip:")
            is_gdrive = (
                cleaned_url.startswith("gdrive:") or
                cleaned_url.startswith("gd2tg:") or
                "drive.google.com" in cleaned_url or
                "docs.google.com" in cleaned_url
            )

            await self.store.update_progress(job.id, status=JobStatus.DOWNLOADING)

            if not job_state.msg_id:
                initial_text = compile_queued_status_text(job.id, job.url, "")
                init_msg = await safe_send(
                    self.client,
                    chat_id,
                    initial_text,
                    link_preview_options=LinkPreviewOptions(is_disabled=True)
                )
                if init_msg:
                    job_state.msg_id = init_msg.id
                    await self.store.set_status_message(job.id, init_msg.id)

            job_state.trigger_event.set()

            async def download_status_updater_loop() -> None:
                while not job_state.downloader_done.is_set():
                    try:
                        await asyncio.wait_for(job_state.trigger_event.wait(), timeout=3.0)
                        job_state.trigger_event.clear()
                        if job_state.downloader_done.is_set():
                            break

                        db_job = await self.store.get_job(job.id)
                        if not db_job or db_job.status == JobStatus.CANCELLED:
                            break

                        if job_state.msg_id:
                            status_text = compile_job_status_text(db_job, job_state)
                            if status_text != job_state.last_edited_text:
                                from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton
                                keyboard = InlineKeyboardMarkup([
                                    [InlineKeyboardButton("Cancel", callback_data=f"cancel_job:{job.id}")]
                                ])
                                if await safe_edit(self.client, chat_id, job_state.msg_id, status_text, reply_markup=keyboard):
                                    job_state.last_edited_text = status_text
                    except asyncio.TimeoutError:
                        try:
                            db_job = await self.store.get_job(job.id)
                            if db_job and db_job.status != JobStatus.CANCELLED and job_state.msg_id:
                                status_text = compile_job_status_text(db_job, job_state)
                                if status_text != job_state.last_edited_text:
                                    from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton
                                    keyboard = InlineKeyboardMarkup([
                                        [InlineKeyboardButton("Cancel", callback_data=f"cancel_job:{job.id}")]
                                    ])
                                    if await safe_edit(self.client, chat_id, job_state.msg_id, status_text, reply_markup=keyboard):
                                        job_state.last_edited_text = status_text
                        except Exception:
                            pass
                    except Exception:
                        pass

            download_updater_task = asyncio.create_task(download_status_updater_loop())

            def reg(proc):
                job_state.active_process = proc

            if not is_torrent and not is_unzip and not is_gdrive:
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
                            speed = max(0.0, (current_size - last_download_size) / dt)
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
                from ..downloader import DownloadResult
                archive_files = []
                if dest_dir.exists():
                    archive_files = [p for p in dest_dir.iterdir() if p.is_file()]
                result = DownloadResult(ok=True, files=archive_files)
            elif is_gdrive:
                from ..gdrive import GoogleDriveDownloader, archive_all_folders_in_dir
                from ..downloader import DownloadResult

                gdrive_link = cleaned_url
                for prefix in ("gdrive:", "gd2tg:"):
                    if gdrive_link.startswith(prefix):
                        gdrive_link = gdrive_link[len(prefix):]

                def on_gdrive_progress(downloaded: int, speed: float, filename: str) -> None:
                    job_state.total_downloaded_bytes = downloaded
                    job_state.download_speed = speed
                    job_state.current_download_file = filename
                    job_state.trigger_event.set()

                gdrive_user_id = None
                archive_fmt = None
                mirror_pixeldrain = False
                if job.args:
                    try:
                        args_dict = json.loads(job.args)
                        if isinstance(args_dict, dict):
                            archive_fmt = args_dict.get("archive_format")
                            mirror_pixeldrain = bool(args_dict.get("mirror_pixeldrain"))
                            gdrive_user_id = args_dict.get("user_id")
                    except Exception:
                        pass

                if not gdrive_user_id:
                    gdrive_user_id = chat_id

                downloader = GoogleDriveDownloader(user_id=gdrive_user_id, progress_callback=on_gdrive_progress)

                await downloader.download_link(gdrive_link, dest_dir)


                if archive_fmt:
                    log.info("Archiving downloaded GDrive folders in %s format for job #%s", archive_fmt, job.id)
                    job_state.is_archiving = True
                    job_state.archive_format = archive_fmt
                    job_state.trigger_event.set()
                    try:
                        await archive_all_folders_in_dir(dest_dir, archive_format=archive_fmt)
                    finally:
                        job_state.is_archiving = False
                        job_state.trigger_event.set()


                final_files = [p for p in dest_dir.rglob("*") if p.is_file()]
                result = DownloadResult(ok=True, files=final_files)
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
                    job_state.trigger_event.set()

                from ..downloader import download_torrent_async
                result = await download_torrent_async(
                    cleaned_url, dest_dir, on_progress=on_torrent_progress, register_proc=reg
                )

            else:
                extra_args = None
                if job.args:
                    try:
                        extra_args = json.loads(job.args)
                    except Exception:
                        pass

                def on_download_progress(count: int, filename: Optional[str] = None) -> None:
                    job_state.download_count = count
                    if filename:
                        job_state.current_download_file = filename
                    job_state.trigger_event.set()

                from ..downloader import run_with_progress
                result = await run_with_progress(
                    job.url, dest_dir, on_progress=on_download_progress, extra_args=extra_args, register_proc=reg
                )


            job_state.downloader_result = result
            log.info("Download finished for job #%s (ok=%s)", job.id, result.ok)
        except Exception as e:
            from ..downloader import DownloadResult
            log.exception("Download failed for job #%s", job.id)
            job_state.downloader_result = DownloadResult(ok=False, error_tail=str(e))
        finally:
            job_state.downloader_done.set()
            job_state.trigger_event.set()
            if 'download_updater_task' in locals() and download_updater_task:
                download_updater_task.cancel()
                await asyncio.gather(download_updater_task, return_exceptions=True)
            if monitor_task:
                monitor_task.cancel()
                await asyncio.gather(monitor_task, return_exceptions=True)

    async def _process_upload(self, job_state: JobState) -> None:
        job_state.active_upload_task = asyncio.current_task()
        from ..conversion import (
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
            _extracted_file_names,
            get_split_archive_info
        )

        job = job_state.job
        chat_id = job.chat_id
        dest_dir = job_state.dest_dir
        extract_dir = dest_dir.parent / f"{dest_dir.name}_extracted"
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
                        status_text = compile_job_status_text(db_job, job_state)
                        if status_text != job_state.last_edited_text:
                            from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton
                            keyboard = InlineKeyboardMarkup([
                                [InlineKeyboardButton("Cancel", callback_data=f"cancel_job:{job.id}")]
                            ])
                            if await safe_edit(self.client, chat_id, job_state.msg_id, status_text, reply_markup=keyboard):
                                job_state.last_edited_text = status_text
                except Exception:

                    pass

        async def perform_uploads() -> None:
            nonlocal job
            if not dest_dir.exists():
                return

            from ..uploader import should_ignore_file

            if job_state.downloader_done.is_set() and job_state.uploader_done.is_set():
                try:
                    for d in (dest_dir, extract_dir):
                        if d.exists():
                            for p in list(d.rglob("*")):
                                if p.is_file() and should_ignore_file(p):
                                    try:
                                        p.unlink()
                                    except Exception:
                                        pass
                except Exception:
                    pass

            try:
                files = []
                if dest_dir.exists():
                    files.extend(sorted(p for p in dest_dir.rglob("*") if p.is_file() and not should_ignore_file(p)))
                if extract_dir.exists():
                    files.extend(sorted(p for p in extract_dir.rglob("*") if p.is_file() and not should_ignore_file(p)))
            except Exception:
                return

            db_total = len(files)
            if job.total_files != db_total:
                await self.store.update_progress(job.id, total_files=db_total)
                job = await self.store.get_job(job.id)

            pending = []
            for f in files:
                try:
                    f_rel = str(f.relative_to(extract_dir))
                except ValueError:
                    f_rel = str(f.relative_to(dest_dir))
                if (f_rel not in job_state.uploaded_filenames and 
                    f_rel not in job_state.uploading_files and 
                    f_rel not in job_state.failed_uploads):
                    pending.append(f)

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

                if job_state.uploader_done.is_set():
                    return

                try:
                    f_rel = str(f.relative_to(extract_dir))
                except ValueError:
                    f_rel = str(f.relative_to(dest_dir))

                is_archive = f.suffix.lower() in ARCHIVE_EXT
                f_split = get_split_archive_info(f.name)
                if not is_archive and f_split:
                    if f_split["part"] == 1:
                        base_ext = f".{f_split.get('ext')}" if f_split.get("ext") else None
                        if not base_ext or base_ext.lower() in ARCHIVE_EXT:
                            is_archive = True

                if f_split and f_split["part"] > 1:
                    if job.url.startswith("unzip:"):
                        continue

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
                            prompt_text = compile_archive_prompt_text(job.id, f.name)
                            kb = InlineKeyboardMarkup([
                                [
                                    InlineKeyboardButton("Upload Archive Only", callback_data=f"archive_only:{job.id}:{archive_id}"),
                                    InlineKeyboardButton("Upload + Extract", callback_data=f"archive_ext:{job.id}:{archive_id}")
                                ],
                                [
                                    InlineKeyboardButton("Cancel", callback_data=f"cancel_job:{job.id}")
                                ]
                            ])
                            prompt_msg = await safe_send(self.client, chat_id, prompt_text, reply_markup=kb)
                            if prompt_msg:
                                archive_prompt_msg_id = prompt_msg.id

                                if job.id not in _archive_events:
                                    _archive_events[job.id] = {}
                                _archive_events[job.id][archive_id] = asyncio.Event()

                                start_t = time.time()
                                while not job_state.uploader_done.is_set() and not _archive_events[job.id][archive_id].is_set():
                                    if time.time() - start_t >= 15.0:
                                        break
                                    try:
                                        await asyncio.wait_for(_archive_events[job.id][archive_id].wait(), timeout=2.0)
                                    except asyncio.TimeoutError:
                                        pass
                                if job_state.uploader_done.is_set():
                                    return

                                if not _archive_events[job.id][archive_id].is_set():
                                    if archive_prompt_msg_id:
                                        try:
                                            await self.client.delete_messages(chat_id, archive_prompt_msg_id)
                                        except Exception:
                                            pass
                                        archive_prompt_msg_id = None
                                    if job.id not in _archive_choices:
                                        _archive_choices[job.id] = {}
                                    _archive_choices[job.id][archive_id] = "only"
                            else:
                                if job.id not in _archive_choices:
                                    _archive_choices[job.id] = {}
                                _archive_choices[job.id][archive_id] = "only"

                    choice = _archive_choices[job.id][archive_id]
                    if choice == "ext" and f_rel not in _extracted_archives.get(job.id, set()):
                        if job.id not in _extracted_archives:
                            _extracted_archives[job.id] = set()
                        _extracted_archives[job.id].add(f_rel)

                        status_msg = await safe_send(
                            self.client,
                            chat_id,
                            compile_extraction_status_text(job.id, f.name),
                            reply_markup=InlineKeyboardMarkup([
                                [InlineKeyboardButton("Cancel", callback_data=f"cancel_job:{job.id}")]
                            ]),
                            link_preview_options=LinkPreviewOptions(is_disabled=True)
                        )

                        extract_dir.mkdir(parents=True, exist_ok=True)
                        before_files = set()
                        try:
                            before_files = {p.resolve() for p in extract_dir.rglob("*") if p.is_file()}
                        except Exception:
                            pass

                        try:
                            password = None
                            if job.args:
                                try:
                                    parsed = json.loads(job.args)
                                    if isinstance(parsed, dict):
                                        password = parsed.get("password")
                                except Exception:
                                    pass

                            extracted = await extract_archive_async(f, extract_dir, password=password)
                            if extracted:
                                log.info("Successfully extracted archive %s", f.name)
                                job_state.trigger_event.set()
                                try:
                                    after_files = {p.resolve() for p in extract_dir.rglob("*") if p.is_file()}
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
                                    job_state.uploaded_filenames.add(f_rel)

                                    if f_split and f_split["part"] == 1:
                                        for sibling in dest_dir.iterdir():
                                             if sibling.is_file() and f_split["pattern"].match(sibling.name):
                                                 try:
                                                     sibling.unlink(missing_ok=True)
                                                 except Exception:
                                                     pass
                                                 try:
                                                     sib_rel = str(sibling.relative_to(dest_dir))
                                                     job_state.uploaded_filenames.add(sib_rel)
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
                                end_extraction_msg = compile_extraction_success_status_text(job.id, f.name)
                                success_msg = await safe_send(
                                    self.client,
                                    chat_id,
                                    end_extraction_msg,
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
                                    start_time = time.time()
                                    while not job_state.uploader_done.is_set() and not event.is_set():
                                        if time.time() - start_time >= 300:
                                            raise asyncio.TimeoutError()
                                        try:
                                            await asyncio.wait_for(event.wait(), timeout=2.0)
                                        except asyncio.TimeoutError:
                                            pass
                                    if job_state.uploader_done.is_set():
                                        return
                                    new_password = data["password"]
                                    
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

                is_file_archive = f.suffix.lower() in ARCHIVE_EXT
                if not is_file_archive:
                    f_split = get_split_archive_info(f.name)
                    if f_split:
                        base_ext = f".{f_split.get('ext')}" if f_split.get("ext") else None
                        if not base_ext or base_ext.lower() in ARCHIVE_EXT:
                            is_file_archive = True

                if job.url.startswith("unzip:") and is_file_archive:
                    try:
                        f.unlink(missing_ok=True)
                    except Exception:
                        pass
                    job_state.uploaded_filenames.add(f_rel)

                    f_split = get_split_archive_info(f.name)
                    if f_split and f_split["part"] == 1:
                        for sibling in dest_dir.iterdir():
                            if sibling.is_file() and f_split["pattern"].match(sibling.name):
                                try:
                                    sibling.unlink(missing_ok=True)
                                except Exception:
                                    pass
                                try:
                                    sib_rel = str(sibling.relative_to(dest_dir))
                                    job_state.uploaded_filenames.add(sib_rel)
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
                              keyboard = InlineKeyboardMarkup([
                                  [
                                      InlineKeyboardButton("Convert to MP4", callback_data=f"convert_mp4:{job.id}:{conv_id}"),
                                      InlineKeyboardButton("Original File", callback_data=f"convert_orig:{job.id}:{conv_id}")
                                  ],
                                  [
                                      InlineKeyboardButton("Cancel", callback_data=f"cancel_job:{job.id}")
                                  ]
                              ])
                              prompt_text = compile_conversion_prompt_text(job.id, f.name)
                              prompt_msg = await safe_send(self.client, chat_id, prompt_text, reply_markup=keyboard)
                              if prompt_msg:
                                   conversion_prompt_msg_id = prompt_msg.id

                                   if job.id not in _conversion_events:
                                        _conversion_events[job.id] = {}
                                   _conversion_events[job.id][conv_id] = asyncio.Event()

                                   start_t = time.time()
                                   while not job_state.uploader_done.is_set() and not _conversion_events[job.id][conv_id].is_set():
                                        if time.time() - start_t >= 15.0:
                                            break
                                        try:
                                            await asyncio.wait_for(_conversion_events[job.id][conv_id].wait(), timeout=2.0)
                                        except asyncio.TimeoutError:
                                            pass
                                   if job_state.uploader_done.is_set():
                                        return

                                   if not _conversion_events[job.id][conv_id].is_set():
                                        if conversion_prompt_msg_id:
                                            try:
                                                await self.client.delete_messages(chat_id, conversion_prompt_msg_id)
                                            except Exception:
                                                pass
                                            conversion_prompt_msg_id = None
                                        if job.id not in _conversion_choices:
                                            _conversion_choices[job.id] = {}
                                        _conversion_choices[job.id][conv_id] = "orig"
                              else:
                                   if job.id not in _conversion_choices:
                                        _conversion_choices[job.id] = {}
                                   _conversion_choices[job.id][conv_id] = "orig"
                              choice = _conversion_choices.get(job.id, {}).get(conv_id)

                         if choice == "mp4":
                             if job.id not in _converted_files:
                                 _converted_files[job.id] = set()
                             _converted_files[job.id].add(f.name)

                             log.info("Converting video %s to MP4 for job %s", f.name, job.id)
                             output_name = f.stem + "_converted.mp4"
                             output_path = f.parent / output_name

                             conv_msg = await safe_send(
                                 self.client,
                                 chat_id,
                                 compile_conversion_running_status_text(job.id, f.name),
                                 reply_markup=InlineKeyboardMarkup([
                                     [InlineKeyboardButton("Cancel", callback_data=f"cancel_job:{job.id}")]
                                 ])
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
                            keyboard = InlineKeyboardMarkup([
                                [
                                    InlineKeyboardButton("Convert to MP3", callback_data=f"convert_mp3:{job.id}:{conv_id}"),
                                    InlineKeyboardButton("Original File", callback_data=f"convert_orig:{job.id}:{conv_id}")
                                ],
                                [
                                    InlineKeyboardButton("Cancel", callback_data=f"cancel_job:{job.id}")
                                ]
                            ])
                            prompt_text = compile_audio_conversion_prompt_text(job.id, f.name)
                            prompt_msg = await safe_send(self.client, chat_id, prompt_text, reply_markup=keyboard)
                            if prompt_msg:
                                conversion_prompt_msg_id = prompt_msg.id

                                if job.id not in _conversion_events:
                                    _conversion_events[job.id] = {}
                                _conversion_events[job.id][conv_id] = asyncio.Event()

                                start_t = time.time()
                                while not job_state.uploader_done.is_set() and not _conversion_events[job.id][conv_id].is_set():
                                    if time.time() - start_t >= 15.0:
                                        break
                                    try:
                                        await asyncio.wait_for(_conversion_events[job.id][conv_id].wait(), timeout=2.0)
                                    except asyncio.TimeoutError:
                                        pass
                                if job_state.uploader_done.is_set():
                                    return

                                if not _conversion_events[job.id][conv_id].is_set():
                                    if conversion_prompt_msg_id:
                                        try:
                                            await self.client.delete_messages(chat_id, conversion_prompt_msg_id)
                                        except Exception:
                                            pass
                                        conversion_prompt_msg_id = None
                                    if job.id not in _conversion_choices:
                                        _conversion_choices[job.id] = {}
                                    _conversion_choices[job.id][conv_id] = "orig"
                            else:
                                if job.id not in _conversion_choices:
                                    _conversion_choices[job.id] = {}
                                _conversion_choices[job.id][conv_id] = "orig"
                            choice = _conversion_choices.get(job.id, {}).get(conv_id)

                        if choice == "mp3":
                            if job.id not in _converted_files:
                                _converted_files[job.id] = set()
                            _converted_files[job.id].add(f.name)

                            log.info("Converting/processing audio %s to MP3 for job %s", f.name, job.id)
                            output_name = f.stem + "_converted.mp3"
                            output_path = f.parent / output_name

                            conv_msg = await safe_send(
                                self.client,
                                chat_id,
                                compile_audio_conversion_running_status_text(job.id, f.name),
                                reply_markup=InlineKeyboardMarkup([
                                    [InlineKeyboardButton("Cancel", callback_data=f"cancel_job:{job.id}")]
                                ])
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
                from ..uploader import handle_large_file, upload_file, UploadTooLarge
                try:
                    split_parts = await handle_large_file(f, bool(job.split_large_files))
                except Exception as sle:
                    log.exception("Error while handling large file split for %s: %s", f.name, sle)
                    split_parts = [f]

                if not split_parts:
                    await self.store.mark_uploaded(job.id, f_rel)
                    break

                if len(split_parts) == 1 and split_parts[0] == f:
                    pass
                else:
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
                            job_state.upload_speed = max(0.0, uploaded_since_last / elapsed)
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
                    job_state.failed_uploads.add(f_rel)
                except Exception as e:  
                    log.exception("Upload failed for %s", f)
                    job_state.skipped.append((f.name, f"error: {e}"))
                    job_state.failed_uploads.add(f_rel)
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
            from ..uploader import should_ignore_file
            from ..conversion import (
                convert_media_async,
                convert_audio_async,
                CONVERSION_EXT,
                AUDIO_CONVERSION_EXT,
                _conversion_ids,
                _conversion_events,
                _conversion_choices,
                _converted_files
            )
            if is_torrent:
                while not job_state.downloader_done.is_set():
                    await asyncio.sleep(2.0)

                try:
                    if dest_dir.exists():
                        files = sorted(p for p in dest_dir.rglob("*") if p.is_file() and not should_ignore_file(p))
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
                    if job_state.uploader_done.is_set():
                        break
                    has_completed_file = False
                    if dest_dir.exists():
                        try:
                            files = [p for p in dest_dir.rglob("*") if p.is_file() and not p.name.endswith(".part") and not should_ignore_file(p)]
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
                if job_state.uploader_done.is_set():
                    break
                await perform_uploads()

                if job_state.downloader_done.is_set():
                    has_pending = False
                    if dest_dir.exists():
                        try:
                            files = [p for p in dest_dir.rglob("*") if p.is_file() and not p.name.endswith(".part") and not should_ignore_file(p)]
                            pending = [
                                f for f in files
                                if str(f.relative_to(dest_dir)) not in job_state.uploaded_filenames
                                and str(f.relative_to(dest_dir)) not in job_state.uploading_files
                                and str(f.relative_to(dest_dir)) not in job_state.failed_uploads
                            ]
                            if pending:
                                has_pending = True
                        except Exception:
                            pass
                    if extract_dir.exists():
                        try:
                            files = [p for p in extract_dir.rglob("*") if p.is_file() and not p.name.endswith(".part") and not should_ignore_file(p)]
                            pending = [
                                f for f in files
                                if str(f.relative_to(extract_dir)) not in job_state.uploaded_filenames
                                and str(f.relative_to(extract_dir)) not in job_state.uploading_files
                                and str(f.relative_to(extract_dir)) not in job_state.failed_uploads
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

            if getattr(job_state, "pixeldrain_links", None):
                link_lines = [f"• `{fname}`: {url}" for fname, url in job_state.pixeldrain_links]
                summary += f"\n\n**Pixeldrain Mirror Links:**\n" + "\n".join(link_lines)

            await report(summary)

            shutil.rmtree(dest_dir, ignore_errors=True)
            shutil.rmtree(extract_dir, ignore_errors=True)
            
        except Exception as e:
            log.exception("Upload process failed for job #%s", job.id)
            await self.store.update_progress(job.id, status=JobStatus.FAILED, error=str(e), url="")
            await report(f"Job failed with error: {e}")
            shutil.rmtree(dest_dir, ignore_errors=True)
            shutil.rmtree(extract_dir, ignore_errors=True)
        finally:
            shutil.rmtree(extract_dir, ignore_errors=True)
            job_state.uploader_done.set()
            updater_task.cancel()
            await asyncio.gather(updater_task, return_exceptions=True)
            
            # Edit the status message one final time to remove buttons and show final state
            db_job = await self.store.get_job(job.id)
            if db_job and job_state.msg_id:
                if db_job.status == JobStatus.DONE:
                    final_text = f"**Job #{job.id} Completed Successfully**\n------------------------------------\nUploaded {job_state.sent} file(s) total."
                elif db_job.status == JobStatus.FAILED:
                    final_text = f"**Job #{job.id} Failed**\n------------------------------------\nError: {db_job.error}"
                elif db_job.status == JobStatus.CANCELLED:
                    final_text = f"**Job #{job.id} Cancelled**\n------------------------------------\nCancelled successfully by owner."
                else:
                    final_text = compile_job_status_text(db_job, job_state)
                await safe_edit(self.client, chat_id, job_state.msg_id, final_text, reply_markup=None)

            from .archive import _archive_ids, _archive_events, _archive_choices, _extracted_archives, _extracted_file_names
            from ..conversion import _conversion_ids, _conversion_events, _conversion_choices, _converted_files

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


async def log_upload(job_id: str, filename: str) -> None:
    log_path = settings.log_dir / "uploads.log"
    log_path.parent.mkdir(parents=True, exist_ok=True)

    def append_to_file():
        with open(log_path, "a", encoding="utf-8") as f:
            f.write(f"{time.strftime('%Y-%m-%d %H:%M:%S')} - Job #{job_id} - Uploaded: {filename}\n")

    await asyncio.to_thread(append_to_file)


queue_manager = QueueManager()
