from __future__ import annotations

import asyncio
import logging
import shutil
from pathlib import Path
from typing import Optional

from pyrogram.types import CallbackQuery, LinkPreviewOptions
from .status import compile_archive_choice_status_text

log = logging.getLogger(__name__)

_archive_ids: dict[int, dict[str, str]] = {}
_archive_events: dict[int, dict[str, asyncio.Event]] = {}
_archive_choices: dict[int, dict[str, str]] = {}
_extracted_archives: dict[int, set[str]] = {}
_extracted_file_names: dict[int, set[str]] = {}
ARCHIVE_EXT = {".zip", ".7z", ".rar", ".tar", ".gz", ".bz2", ".xz", ".tgz"}


class ArchivePasswordRequired(Exception):
    pass


async def extract_archive_async(archive_path: Path, extract_dir: Path, password: Optional[str] = None) -> bool:
    ext = archive_path.suffix.lower()

    if ext == ".zip" and shutil.which("unzip"):
        try:
            log.info("Extracting %s using unzip command line tool", archive_path.name)
            args = ["unzip", "-o"]
            if password:
                args.extend(["-P", password])
            args.extend([str(archive_path), "-d", str(extract_dir)])
            
            proc = await asyncio.create_subprocess_exec(
                *args,
                stdin=asyncio.subprocess.DEVNULL,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            stdout, stderr = await proc.communicate()
            output = (stdout.decode(errors="ignore") + stderr.decode(errors="ignore")).lower()
            if proc.returncode == 0:
                return True
            log.warning("unzip command returned non-zero code: %s", proc.returncode)
            if "password" in output or "incorrect password" in output or "encrypted" in output:
                raise ArchivePasswordRequired()
        except ArchivePasswordRequired:
            raise
        except Exception:
            log.exception("unzip command failed")

    if ext == ".rar" and shutil.which("unrar"):
        try:
            log.info("Extracting %s using unrar command line tool", archive_path.name)
            args = ["unrar", "x", "-y"]
            if password:
                args.append(f"-p{password}")
            else:
                args.append("-p-")
            args.extend([str(archive_path), str(extract_dir)])
            
            proc = await asyncio.create_subprocess_exec(
                *args,
                stdin=asyncio.subprocess.DEVNULL,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            stdout, stderr = await proc.communicate()
            output = (stdout.decode(errors="ignore") + stderr.decode(errors="ignore")).lower()
            if proc.returncode == 0:
                return True
            log.warning("unrar command returned non-zero code: %s", proc.returncode)
            if "password" in output or "encrypted" in output:
                raise ArchivePasswordRequired()
        except ArchivePasswordRequired:
            raise
        except Exception:
            log.exception("unrar command failed")

    if ext in (".tar", ".gz", ".bz2", ".xz", ".tgz", ".tbz2", ".txz") and shutil.which("tar"):
        try:
            log.info("Extracting %s using tar command line tool", archive_path.name)
            proc = await asyncio.create_subprocess_exec(
                "tar", "-xf", str(archive_path), "-C", str(extract_dir),
                stdin=asyncio.subprocess.DEVNULL,
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.DEVNULL
            )
            await proc.wait()
            if proc.returncode == 0:
                return True
            log.warning("tar command returned non-zero code: %s", proc.returncode)
        except Exception:
            log.exception("tar command failed")

    if shutil.which("7z"):
        try:
            log.info("Extracting %s using 7z command line tool", archive_path.name)
            args = ["7z", "x", "-y", f"-o{extract_dir}"]
            if password:
                args.append(f"-p{password}")
            args.append(str(archive_path))
            
            proc = await asyncio.create_subprocess_exec(
                *args,
                stdin=asyncio.subprocess.DEVNULL,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            stdout, stderr = await proc.communicate()
            output = (stdout.decode(errors="ignore") + stderr.decode(errors="ignore")).lower()
            if proc.returncode == 0:
                return True
            log.warning("7z command returned non-zero code: %s", proc.returncode)
            if "password" in output or "encrypted" in output:
                raise ArchivePasswordRequired()
        except ArchivePasswordRequired:
            raise
        except Exception:
            log.exception("7z command failed")

    return False


async def handle_archive_choice(
    callback_query: CallbackQuery,
    store,
    is_job_owner
) -> None:
    data = callback_query.data
    parts = data.split(":", 2)
    choice = parts[0].split("_")[1]  
    job_id = int(parts[1])
    archive_id = parts[2]

    job = await store.get_job(job_id)
    if not job:
        await callback_query.answer("Job not found.", show_alert=True)
        return

    if not is_job_owner(callback_query.message.chat.id, job):
        await callback_query.answer("Unauthorized: You cannot manage archive choices for this job.", show_alert=True)
        return

    filename = _archive_ids.get(job_id, {}).get(archive_id)
    if not filename:
        await callback_query.answer("Archive choice expired or not found.", show_alert=True)
        return

    if job_id not in _archive_choices:
        _archive_choices[job_id] = {}
    _archive_choices[job_id][archive_id] = choice

    if job_id in _archive_events and archive_id in _archive_events[job_id]:
        _archive_events[job_id][archive_id].set()

    choice_str = "Upload Archive Only" if choice == "only" else "Upload Archive + Extract Contents"
    status_text = compile_archive_choice_status_text(job.id, filename, choice_str)
    await callback_query.message.edit_text(status_text, link_preview_options=LinkPreviewOptions(is_disabled=True))
    await callback_query.answer(f"Selected: {choice_str}")
