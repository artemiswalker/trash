from __future__ import annotations

import asyncio
import logging
from pyrogram import Client
from pyrogram.types import LinkPreviewOptions

log = logging.getLogger(__name__)


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


async def safe_edit(client: Client, chat_id: int, message_id: int, text: str) -> bool:
    from pyrogram.errors import FloodWait, MessageNotModified
    try:
        await client.edit_message_text(
            chat_id,
            message_id,
            text,
            link_preview_options=LinkPreviewOptions(is_disabled=True)
        )
        return True
    except MessageNotModified:
        return True
    except FloodWait as e:
        log.warning("Telegram FloodWait: waiting %s seconds on status edit", e.value)
        await asyncio.sleep(e.value + 1)
        return False
    except Exception as e:
        log.warning("Failed to edit status message: %s", e)
        return False
