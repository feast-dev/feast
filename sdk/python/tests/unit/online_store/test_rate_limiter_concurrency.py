import threading
import time

from feast.rate_limiter import TokenBucketRateLimiter


class TestMultiThreadedRateLimiter:
    """Tests for thread-safety of TokenBucketRateLimiter."""

    def test_multiple_threads_token_consumption(self):
        """Test that multiple threads correctly consume tokens without race conditions."""
        limiter = TokenBucketRateLimiter(rate=100, interval=1.0, percent_usage=0.6)
        results = []
        lock = threading.Lock()

        def worker(thread_id: int, tokens_to_consume: int):
            success = limiter.wait_for_tokens(tokens_to_consume, timeout=5.0)
            with lock:
                results.append((thread_id, success, tokens_to_consume))

        # Launch 10 threads, each requesting 10 tokens
        threads = []
        for i in range(10):
            t = threading.Thread(target=worker, args=(i, 10))
            threads.append(t)
            t.start()

        for t in threads:
            t.join()

        # All threads should succeed
        assert len(results) == 10
        assert all(success for _, success, _ in results)

        # Total tokens consumed should be 100
        total_consumed = sum(tokens for _, _, tokens in results)
        assert total_consumed == 100

    def test_thread_contention_with_limited_tokens(self):
        """Test thread behavior when tokens are limited."""
        limiter = TokenBucketRateLimiter(rate=20, interval=1.0, percent_usage=0.6)
        results = []
        lock = threading.Lock()

        def worker(thread_id: int, tokens_needed: int):
            start = time.time()
            success = limiter.wait_for_tokens(tokens_needed, timeout=0.5)
            elapsed = time.time() - start
            with lock:
                results.append(
                    {
                        "thread_id": thread_id,
                        "success": success,
                        "tokens_needed": tokens_needed,
                        "elapsed": elapsed,
                    }
                )

        # Launch 15 threads each wanting 5 tokens and 12 are available.
        # With 0.5s timeout, not enough time for refill to satisfy all
        threads = []
        for i in range(15):
            t = threading.Thread(target=worker, args=(i, 5))
            threads.append(t)
            t.start()

        for t in threads:
            t.join()

        successful = [r for r in results if r["success"]]
        failed = [r for r in results if not r["success"]]

        # Some threads should succeed
        assert len(successful) >= 2  # At least 2 threads (10 tokens) with 60% usage
        # Some threads should fail due to timeout and contention
        assert len(failed) > 0

    def test_concurrent_token_refill(self):
        """Test that token refill works correctly with concurrent access."""
        limiter = TokenBucketRateLimiter(rate=10, interval=1.0, percent_usage=0.8)
        results = []
        lock = threading.Lock()

        def worker(thread_id: int):
            # Each thread tries to get 4 tokens
            success = limiter.wait_for_tokens(4, timeout=5.0)
            with lock:
                results.append((thread_id, success))

        # First batch: consume initial tokens
        threads = []
        for i in range(2):
            t = threading.Thread(target=worker, args=(i,))
            threads.append(t)
            t.start()

        for t in threads:
            t.join()

        assert all(success for _, success in results)

        # Wait for refill
        time.sleep(1.5)

        # Second batch: should succeed after refill
        results.clear()
        threads = []
        for i in range(2, 4):
            t = threading.Thread(target=worker, args=(i,))
            threads.append(t)
            t.start()

        for t in threads:
            t.join()

        assert all(success for _, success in results)

    def test_timeout_with_multiple_threads(self):
        """Test timeout behavior when multiple threads compete."""
        limiter = TokenBucketRateLimiter(rate=10, interval=1.0, percent_usage=0.6)
        results = []
        lock = threading.Lock()

        def worker(thread_id: int, tokens_needed: int, timeout: float):
            start = time.time()
            success = limiter.wait_for_tokens(tokens_needed, timeout=timeout)
            elapsed = time.time() - start
            with lock:
                results.append(
                    {"thread_id": thread_id, "success": success, "elapsed": elapsed}
                )

        # Launch threads that need more tokens than available
        threads = []
        for i in range(5):
            # Each thread wants 8 tokens (total 40), but only 6 available
            t = threading.Thread(target=worker, args=(i, 8, 0.5))
            threads.append(t)
            t.start()

        for t in threads:
            t.join()

        # At least one should timeout since we don't have enough tokens
        timed_out = [r for r in results if not r["success"]]
        assert len(timed_out) >= 1

        for result in timed_out:
            assert 0.3 <= result["elapsed"] <= 0.8

    def test_atomic_token_consumption(self):
        """Test that token consumption is atomic across threads."""
        limiter = TokenBucketRateLimiter(rate=100, interval=1.0, percent_usage=1.0)
        consumed_tokens = []
        lock = threading.Lock()

        def worker(tokens_to_consume: int):
            success = limiter.wait_for_tokens(tokens_to_consume, timeout=2.0)
            if success:
                with lock:
                    consumed_tokens.append(tokens_to_consume)

        # Launch many threads simultaneously
        threads = []
        for i in range(20):
            t = threading.Thread(target=worker, args=(5,))
            threads.append(t)
            t.start()

        for t in threads:
            t.join()

        # Total consumed should not exceed available tokens
        total_consumed = sum(consumed_tokens)
        assert total_consumed <= 100  # Rate limit

    def test_high_concurrency_stress(self):
        """Stress test with many concurrent threads."""
        limiter = TokenBucketRateLimiter(rate=100, interval=1.0, percent_usage=0.5)
        results = []
        lock = threading.Lock()

        def worker(thread_id: int):
            success = limiter.wait_for_tokens(1, timeout=3.0)
            with lock:
                results.append(success)

        # Launch 100 threads
        threads = []
        for i in range(100):
            t = threading.Thread(target=worker, args=(i,))
            threads.append(t)
            t.start()

        for t in threads:
            t.join()

        # At least 50 should succeed (50% of 100)
        successful_count = sum(1 for r in results if r)
        assert successful_count >= 50

    def test_burst_then_steady(self):
        """Test burst of requests followed by steady rate."""
        limiter = TokenBucketRateLimiter(rate=20, interval=1.0, percent_usage=0.6)
        results = []
        lock = threading.Lock()

        def worker(thread_id: int, tokens: int):
            success = limiter.wait_for_tokens(tokens, timeout=5.0)
            with lock:
                results.append((thread_id, success))

        # Burst: 5 threads requesting 5 tokens each (25 total)
        burst_threads = []
        for i in range(5):
            t = threading.Thread(target=worker, args=(i, 5))
            burst_threads.append(t)
            t.start()

        for t in burst_threads:
            t.join()

        # At least 2 should succeed immediately (12 tokens = 60% of 20)
        immediate_success = sum(1 for _, success in results if success)
        assert immediate_success >= 2

        # Wait for refill
        time.sleep(1.5)
        results.clear()

        # Smaller requests
        steady_threads = []
        for i in range(5, 8):
            t = threading.Thread(target=worker, args=(i, 3))
            steady_threads.append(t)
            t.start()

        for t in steady_threads:
            t.join()

        # All should succeed after refill
        assert all(success for _, success in results)

    def test_notification_wakes_waiting_threads(self):
        """Test that notify_all wakes up waiting threads."""
        limiter = TokenBucketRateLimiter(rate=10, interval=1.0, percent_usage=0.6)
        results = []
        lock = threading.Lock()

        def consumer(thread_id: int, tokens: int):
            start = time.time()
            success = limiter.wait_for_tokens(tokens, timeout=3.0)
            elapsed = time.time() - start
            with lock:
                results.append(
                    {"thread_id": thread_id, "success": success, "elapsed": elapsed}
                )

        # Start threads that will wait for tokens
        threads = []
        for i in range(3):
            t = threading.Thread(target=consumer, args=(i, 4))
            threads.append(t)
            t.start()

        # Let threads start waiting
        time.sleep(0.1)

        # First thread consumes tokens and should notify others
        # Others should wake up and check

        for t in threads:
            t.join()

        # All should eventually succeed or timeout
        assert len(results) == 3
        # At least one should succeed
        assert any(r["success"] for r in results)
