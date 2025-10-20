import threading
from collections import deque
from typing import Deque
import time

class TushareRateLimiter:
    def __init__(self, max_calls: int = 200, time_window: int = 60):
        self.max_calls = max_calls
        self.time_window = time_window
        self.calls: Deque[float] = deque()
        self.lock = threading.Lock()

    def wait_if_needed(self) -> None:
        with self.lock:
            now = time.time()
            # 清理超过时间窗口的记录
            while self.calls and self.calls[0] < now - self.time_window:
                self.calls.popleft()

            # 如果达到限制，等待
            if len(self.calls) >= self.max_calls:
                sleep_time = self.time_window - (now - self.calls[0]) + 1
                time.sleep(sleep_time)

                # 重新清理
                now = time.time()
                while self.calls and now - self.calls[0] >= self.time_window:
                    self.calls.popleft()

            # 记录本次调用
            self.calls.append(now)
