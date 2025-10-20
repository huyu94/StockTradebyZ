"""Tushare related utilities: rate limiter and helpers."""
from __future__ import annotations
import time
import random
from constants import BAN_PATTERNS
def cool_sleep(base_seconds: int) -> None:
    jitter = random.uniform(0.9, 1.2)
    sleep_s = max(1, int(base_seconds * jitter))
    time.sleep(sleep_s)


def looks_like_ip_ban(exc: Exception) -> bool:
    msg = (str(exc) or "").lower()
    return any(pat in msg for pat in BAN_PATTERNS)