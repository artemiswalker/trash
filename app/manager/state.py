from __future__ import annotations

import asyncio
from pathlib import Path
from ..db import Job

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
        self.failed_uploads = set()
        self.active_process = None
        self.active_download_task = None
        self.active_upload_task = None
        self.msg_id = job.status_message_id
        self.last_edited_text = ""
        self.session_uploaded_count = 0
        self.deleted_bytes = 0
        self.initial_download_msg = None
        self.is_converting = False
        self.conversion_file = None
        self.is_archiving = False
        self.archive_format = None
        self.pixeldrain_links: list[tuple[str, str]] = []