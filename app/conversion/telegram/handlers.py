from __future__ import annotations

import asyncio
import logging
from pyrogram import Client
from pyrogram.types import CallbackQuery, LinkPreviewOptions
from ...status import compile_conversion_choice_status_text

log = logging.getLogger(__name__)

# State dictionaries for managing conversion flow
_conversion_ids: dict[int, dict[str, str]] = {}  
_conversion_events: dict[int, dict[str, asyncio.Event]] = {}  
_conversion_choices: dict[int, dict[str, str]] = {}  
_converted_files: dict[int, set[str]] = {}  

async def handle_conversion_choice(
    client: Client,
    callback_query: CallbackQuery,
    store,
    is_job_owner
) -> None:
    data = callback_query.data
    parts = data.split(":", 2)
    choice = parts[0]  
    job_id = int(parts[1])
    conv_id = parts[2]

    job = await store.get_job(job_id)
    if not job:
        await callback_query.answer("Job not found.", show_alert=True)
        return

    if not is_job_owner(callback_query.message.chat.id, job):
        await callback_query.answer("You are not the owner of this job.", show_alert=True)
        return

    if choice == "convert_mp4":
        choice_type = "mp4"
        choice_str = "Convert to MP4"
    elif choice == "convert_mp3":
        choice_type = "mp3"
        choice_str = "Convert to MP3"
    else:
        choice_type = "orig"
        choice_str = "Upload Original"

    if job_id not in _conversion_choices:
        _conversion_choices[job_id] = {}
    _conversion_choices[job_id][conv_id] = choice_type

    if job_id in _conversion_events and conv_id in _conversion_events[job_id]:
        _conversion_events[job_id][conv_id].set()

    filename = _conversion_ids.get(job_id, {}).get(conv_id, "file")

    await callback_query.answer(f"Selected: {choice_str}")
    try:
        await callback_query.message.edit_text(
            compile_conversion_choice_status_text(job_id, filename, choice_str),
            link_preview_options=LinkPreviewOptions(is_disabled=True)
        )
    except Exception:
        pass
