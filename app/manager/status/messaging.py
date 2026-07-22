from __future__ import annotations

import asyncio
import logging
from typing import Optional
from pyrogram import Client
from pyrogram.types import LinkPreviewOptions, Message

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


async def safe_send(client: Client, chat_id: int, text: str, **kwargs) -> Message | None:
    from pyrogram.errors import FloodWait
    for _ in range(3):
        try:
            return await client.send_message(chat_id, text, **kwargs)
        except FloodWait as e:
            log.warning("Telegram FloodWait: waiting %s seconds on send", e.value)
            await asyncio.sleep(e.value + 1)
        except Exception as e:
            log.warning("Failed to send message to chat %s: %s", chat_id, e)
            return None
    return None


async def safe_edit(client: Client, chat_id: int, message_id: int, text: str, reply_markup=None) -> bool:
    from pyrogram.errors import FloodWait, MessageNotModified
    try:
        await client.edit_message_text(
            chat_id,
            message_id,
            text,
            reply_markup=reply_markup,
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
        log.warning("Failed to edit status message %s in chat %s: %s", message_id, chat_id, e)
        return False


async def safe_delete(client: Client, chat_id: int, message_id: int) -> bool:
    from pyrogram.errors import FloodWait
    try:
        await client.delete_messages(chat_id, message_id)
        return True
    except FloodWait as e:
        log.warning("Telegram FloodWait: waiting %s seconds on message delete", e.value)
        await asyncio.sleep(e.value + 1)
        return False
    except Exception as e:
        log.warning("Failed to delete message %s in chat %s: %s", message_id, chat_id, e)
        return False


async def safe_pin(client: Client, chat_id: int, message_id: int, disable_notification: bool = True) -> bool:
    from pyrogram.errors import FloodWait
    try:
        await client.pin_chat_message(chat_id, message_id, disable_notification=disable_notification, both_sides=True)
        return True
    except FloodWait as e:
        log.warning("Telegram FloodWait: waiting %s seconds on pin_chat_message", e.value)
        await asyncio.sleep(e.value + 1)
        return False
    except Exception:
        try:
            await client.pin_chat_message(chat_id, message_id, disable_notification=disable_notification)
            return True
        except Exception as ex:
            log.warning("Failed to pin status message %s in chat %s: %s", message_id, chat_id, ex)
            return False
