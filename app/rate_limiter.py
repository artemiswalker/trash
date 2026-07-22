from __future__ import annotations

import asyncio
import logging
import random
import re
import time
from typing import Optional

log = logging.getLogger(__name__)

RATE_LIMIT_PATTERNS = re.compile(
    r"\b(429|403|too many requests|rate.?limit|temporarily blocked|forbidden|quota exceeded|retry after|cloudflare|turnstile)\b",
    re.IGNORECASE,
)


def looks_rate_limited(text: str) -> bool:
    """Check if an error log or output string indicates rate limiting."""
    if not text:
        return False
    return bool(RATE_LIMIT_PATTERNS.search(text))


class Backoff:
    """Exponential backoff with full jitter according to AWS/best practice guidelines."""

    def __init__(
        self,
        base_s: float = 1.0,
        multiplier: float = 2.0,
        max_attempts: int = 5,
        max_delay_s: float = 60.0,
        jitter: bool = True,
    ):
        self.base_s = base_s
        self.multiplier = multiplier
        self.max_attempts = max_attempts
        self.max_delay_s = max_delay_s
        self.jitter = jitter
        self.attempt = 0

    def reset(self) -> None:
        self.attempt = 0

    def next_delay(self) -> float:
        calculated = self.base_s * (self.multiplier**self.attempt)
        delay = min(self.max_delay_s, calculated)
        if self.jitter:
            delay = random.uniform(0.0, delay)
        self.attempt += 1
        return delay

    @property
    def exhausted(self) -> bool:
        return self.attempt >= self.max_attempts


class TelegramRateLimiter:
    """Async Rate Limiter enforcing Telegram API limits:

    - Global limit: max 30 ops/sec across all chats.
    - Per-chat limit: minimum interval between calls (e.g. 1.0s).
    - FloodWait Cooldown: dynamic penalty tracking on FloodWait.
    """

    def __init__(
        self,
        global_rate_limit: float = 30.0,  # max requests per second globally
        per_chat_interval: float = 1.0,    # min seconds between calls in same chat
    ):
        self.global_rate_limit = global_rate_limit
        self.per_chat_interval = per_chat_interval

        self._global_lock = asyncio.Lock()
        self._global_last_call = 0.0

        self._chat_locks: dict[int, asyncio.Lock] = {}
        self._chat_last_call: dict[int, float] = {}
        self._chat_floodwait_until: dict[int, float] = {}
        self._global_floodwait_until: float = 0.0

    def _get_chat_lock(self, chat_id: int) -> asyncio.Lock:
        if chat_id not in self._chat_locks:
            self._chat_locks[chat_id] = asyncio.Lock()
        return self._chat_locks[chat_id]

    async def acquire(self, chat_id: Optional[int] = None) -> None:
        """Paces execution according to Telegram rate limits."""
        now = time.time()

        # Check global FloodWait pause
        if now < self._global_floodwait_until:
            wait_s = self._global_floodwait_until - now
            log.debug("Global FloodWait pause active, waiting %.2fs", wait_s)
            await asyncio.sleep(wait_s)
            now = time.time()

        # Check per-chat FloodWait pause
        if chat_id is not None:
            flood_until = self._chat_floodwait_until.get(chat_id, 0.0)
            if now < flood_until:
                wait_s = flood_until - now
                log.debug("Chat %s FloodWait pause active, waiting %.2fs", chat_id, wait_s)
                await asyncio.sleep(wait_s)
                now = time.time()

        # Enforce global pacing (1 / 30 = ~0.033s between calls)
        async with self._global_lock:
            min_global_interval = 1.0 / self.global_rate_limit
            elapsed = time.time() - self._global_last_call
            if elapsed < min_global_interval:
                await asyncio.sleep(min_global_interval - elapsed)
            self._global_last_call = time.time()

        # Enforce per-chat pacing (min 1.0s between calls in same chat)
        if chat_id is not None:
            chat_lock = self._get_chat_lock(chat_id)
            async with chat_lock:
                last_call = self._chat_last_call.get(chat_id, 0.0)
                elapsed = time.time() - last_call
                if elapsed < self.per_chat_interval:
                    await asyncio.sleep(self.per_chat_interval - elapsed)
                self._chat_last_call[chat_id] = time.time()

    def notify_floodwait(self, seconds: int, chat_id: Optional[int] = None) -> None:
        """Register a FloodWait penalty so subsequent calls wait out the penalty."""
        until = time.time() + seconds + 1.0
        if chat_id is not None:
            self._chat_floodwait_until[chat_id] = max(self._chat_floodwait_until.get(chat_id, 0.0), until)
            log.warning("Registered FloodWait of %ss for chat %s", seconds, chat_id)
        else:
            self._global_floodwait_until = max(self._global_floodwait_until, until)
            log.warning("Registered global FloodWait of %ss", seconds)


# Global rate limiter instance for Telegram API calls
telegram_limiter = TelegramRateLimiter()
