from __future__ import annotations

import re

RATE_LIMIT_PATTERNS = re.compile(
    r"\b(429|403|too many requests|rate.?limit|temporarily blocked|forbidden)\b",
    re.IGNORECASE,
)


def looks_rate_limited(text: str) -> bool:
    return bool(RATE_LIMIT_PATTERNS.search(text))


class Backoff:
    def __init__(self, base_s: float, multiplier: float, max_attempts: int):
        self.base_s = base_s
        self.multiplier = multiplier
        self.max_attempts = max_attempts
        self.attempt = 0

    def reset(self) -> None:
        self.attempt = 0

    def next_delay(self) -> float:
        delay = self.base_s * (self.multiplier**self.attempt)
        self.attempt += 1
        return delay

    @property
    def exhausted(self) -> bool:
        return self.attempt >= self.max_attempts
