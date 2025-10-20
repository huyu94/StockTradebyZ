class RateLimitError(RuntimeError):
    """表示命中限流/封禁，需要长时间冷却后重试。"""
    pass

class DataValidationError(ValueError):
    pass

__all__ = ["RateLimitError", "DataValidationError"]
