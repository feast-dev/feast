import time


class SlidingWindowRateLimiter:
    def __init__(self, max_calls, period):
        self.max_calls = max_calls
        self.period = period
        self.timestamps = [0] * max_calls
        self.index = 0

    def acquire(self):
        if self.max_calls == 0:
            return True
        now = time.time()
        window_start = now - self.period

        if self.timestamps[self.index] <= window_start:
            self.timestamps[self.index] = now
            self.index = (self.index + 1) % self.max_calls
            return True
        return False
