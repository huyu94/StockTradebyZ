"""Tushare related utilities: rate limiter and helpers."""
from __future__ import annotations

import time

import random




def _cool_sleep(base_seconds: int) -> None:
    jitter = random.uniform(0.9, 1.2)
    sleep_s = max(1, int(base_seconds * jitter))
    time.sleep(sleep_s)
