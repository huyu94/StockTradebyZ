BAN_PATTERNS = (
    "访问频繁", "请稍后", "超过频率", "频繁访问",
    "too many requests", "429",
    "forbidden", "403",
    "max retries exceeded"
)

COOLDOWN_SECS = 600
DEFAULT_OVERLAP_DAYS = 3

OLDEST_STOCK_DATE = "2024-01-01"


__all__ = ["BAN_PATTERNS", "COOLDOWN_SECS", "DEFAULT_OVERLAP_DAYS", "OLDEST_STOCK_DATE"]
