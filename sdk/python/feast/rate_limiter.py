import math
import threading
import time
from typing import Optional


class TokenBucketRateLimiter:
    def __init__(self, rate: float, interval: float = 1.0, percent_usage: float = 0.6):
        """
        Args:
            rate: Maximum tokens added per interval (writes per interval)
            interval: Refill interval in seconds
            percent_usage: Fraction of available tokens allowed for writing
        """
        self.rate = float(rate)
        self.interval = float(interval)
        self.max_tokens = float(rate)
        self.tokens = float(rate)
        self.last_refill = time.monotonic()
        self.lock = threading.Lock()
        self.cond = threading.Condition(self.lock)
        self.percent_usage = float(percent_usage)

    def _refill(self):
        """Refill tokens based on elapsed time."""
        now = time.monotonic()
        elapsed = now - self.last_refill
        if elapsed <= 0:
            return

        added = (self.rate * elapsed) / self.interval
        if added > 0:
            self.tokens = min(self.max_tokens, self.tokens + added)
            self.last_refill = now

    def get_available_tokens(self) -> int:
        """
        Return the current number of tokens available for use,
        considering percent_usage.
        """
        with self.lock:
            self._refill()
            return math.floor(self.tokens * self.percent_usage)

    def wait_for_tokens(self, num: int, timeout: Optional[float] = None) -> bool:
        """
        Block until `num` tokens are available, then consume them.
        """
        if num <= 0:
            return True

        end_time = None if timeout is None else (time.monotonic() + timeout)
        with self.cond:
            while True:
                self._refill()
                available = self.tokens * self.percent_usage
                if available >= num:
                    # Consume atomically
                    self.tokens -= num
                    self.cond.notify_all()
                    return True

                if end_time is not None:
                    remaining = end_time - time.monotonic()
                    if remaining <= 0:
                        return False
                    wait_time = min(0.05, remaining)
                else:
                    wait_time = 0.05
                self.cond.wait(wait_time)
