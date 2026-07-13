from __future__ import annotations

import asyncio
import logging
import shutil
from pathlib import Path

from pyrogram.types import CallbackQuery, LinkPreviewOptions

log = logging.getLogger(__name__)

# Global archive state registries
_archive_ids: dict[int, dict[str, str]] = {}
_archive_events: dict[int, dict[str, asyncio.Event]] = {}
_archive_choices: dict[int, dict[str, str]] = {}
_extracted_archives: dict[int, set[str]] = {}
ARCHIVE_EXT = {".zip", ".7z", ".rar", ".tar", ".gz", ".bz2", ".xz", ".tgz"}


async def extract_archive_async(archive_path: Path, extract_dir: Path) -> bool:
    loop = asyncio.get_running_loop()
    try:
        await loop.run_in_executor(None, shutil.unpack_archive, str(archive_path), str(extract_dir))
        return True
    except Exception:
        pass

    ext = archive_path.suffix.lower()
    if ext == ".zip" and shutil.which("unzip"):
        try:
            proc = await asyncio.create_subprocess_exec(
                "unzip", "-o", str(archive_path), "-d", str(extract_dir),
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.DEVNULL
            )
            await proc.wait()
            if proc.returncode == 0:
                return True
        except Exception:
            pass

    if shutil.which("7z"):
        try:
            proc = await asyncio.create_subprocess_exec(
                "7z", "x", "-y", f"-o{extract_dir}", str(archive_path),
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.DEVNULL
            )
            await proc.wait()
            if proc.returncode == 0:
                return True
        except Exception:
            pass

    return False


async def handle_archive_choice(
    callback_query: CallbackQuery,
    store,
    is_job_owner
) -> None:
    data = callback_query.data
    parts = data.split(":")
    choice = parts[0].split("_")[1]  # "only" or "ext"
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

    # Save choice
    if job_id not in _archive_choices:
        _archive_choices[job_id] = {}
    _archive_choices[job_id][archive_id] = choice

    # Trigger event
    if job_id in _archive_events and archive_id in _archive_events[job_id]:
        _archive_events[job_id][archive_id].set()

    # Update message text
    choice_str = "Upload Archive Only" if choice == "only" else "Upload Archive + Extract Contents"
    status_text = (
        f"**Job #{job.id} - Archive Choice**\n"
        f"- **File**: `{filename}`\n"
        f"- **Selected**: `{choice_str}`\n\n"
        f"Processing choice..."
    )
    await callback_query.message.edit_text(status_text, link_preview_options=LinkPreviewOptions(is_disabled=True))
    await callback_query.answer(f"Selected: {choice_str}")
