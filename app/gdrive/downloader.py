from __future__ import annotations

import asyncio
import io
import logging
import os
import time
from pathlib import Path
from typing import Callable, Optional

from googleapiclient.http import MediaIoBaseDownload

from .client import EXPORT_MAP, G_DRIVE_DIR_MIME_TYPE, GoogleDriveClient, get_id_from_url, sanitize_filename

log = logging.getLogger(__name__)


class GoogleDriveDownloader:
    def __init__(
        self,
        client: Optional[GoogleDriveClient] = None,
        user_id: Optional[int | str] = None,
        progress_callback: Optional[Callable[[int, float, str], None]] = None,
    ):
        self.client = client or GoogleDriveClient(user_id=user_id)
        self.progress_callback = progress_callback
        self.total_bytes = 0
        self.downloaded_bytes = 0
        self.start_time = time.time()

    async def download_link(self, link_or_id: str, dest_dir: Path) -> Path:
        file_id = get_id_from_url(link_or_id)
        meta = await asyncio.to_thread(self.client.get_file_metadata, file_id)

        dest_dir.mkdir(parents=True, exist_ok=True)
        raw_name = meta.get("name", file_id)
        name = sanitize_filename(raw_name)
        mime_type = meta.get("mimeType", "")

        self.start_time = time.time()

        if mime_type == G_DRIVE_DIR_MIME_TYPE:
            folder_path = dest_dir / name
            await self._download_folder(file_id, folder_path)
            return folder_path
        else:
            file_path = await self._download_file(meta, dest_dir)
            return file_path

    async def _download_folder(self, folder_id: str, folder_path: Path) -> None:
        folder_path.mkdir(parents=True, exist_ok=True)
        items = await asyncio.to_thread(self.client.list_folder_contents, folder_id)

        for item in items:
            mime = item.get("mimeType", "")
            safe_name = sanitize_filename(item.get("name", item["id"]))
            if mime == G_DRIVE_DIR_MIME_TYPE:
                subfolder_path = folder_path / safe_name
                await self._download_folder(item["id"], subfolder_path)
            else:
                item_copy = dict(item)
                item_copy["name"] = safe_name
                await self._download_file(item_copy, folder_path)

    async def _download_file(self, meta: dict, parent_dir: Path) -> Path:
        file_id = meta["id"]
        name = sanitize_filename(meta["name"])
        mime_type = meta.get("mimeType", "")

        if mime_type in EXPORT_MAP:
            export_info = EXPORT_MAP[mime_type]
            if not name.endswith(export_info["ext"]):
                name += export_info["ext"]
            request = self.client.service.files().export_media(
                fileId=file_id, mimeType=export_info["mime"]
            )
        else:
            request = self.client.service.files().get_media(
                fileId=file_id, supportsAllDrives=True
            )

        file_path = parent_dir / name
        log.info("Downloading GDrive file '%s' to %s", name, file_path)

        initial_downloaded = self.downloaded_bytes

        def _do_download():
            with open(file_path, "wb") as fh:
                downloader = MediaIoBaseDownload(fh, request, chunksize=8 * 1024 * 1024)
                done = False
                while not done:
                    status, done = downloader.next_chunk()
                    if status:
                        current_file_bytes = status.resumable_progress
                        self.downloaded_bytes = initial_downloaded + current_file_bytes
                        elapsed = max(time.time() - self.start_time, 0.1)
                        speed = self.downloaded_bytes / elapsed
                        if self.progress_callback:
                            self.progress_callback(self.downloaded_bytes, speed, name)

        await asyncio.to_thread(_do_download)
        return file_path
