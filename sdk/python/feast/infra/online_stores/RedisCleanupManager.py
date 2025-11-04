import logging
import queue
import threading
import time
from typing import (
    Tuple
)
logger = logging.getLogger(__name__)

class RedisCleanupManager:
    """
    Background manager for async cleanup tasks in Redis.
    Supports TTL cleanup and ZSET size-based trimming.
    """

    def __init__(self, client, cleanup_interval: int = 10):
        """
        :param client: Redis client instance
        :param cleanup_interval: Polling interval for cleanup tasks (seconds)
        """
        self.client = client
        self.cleanup_interval = cleanup_interval
        self.queue: "queue.Queue[Tuple]" = queue.Queue()
        self.stop_event = threading.Event()
        self.worker_thread = threading.Thread(target=self._worker, daemon=True)
        self.worker_thread.start()

    def enqueue(self, job: tuple):
        """Push a cleanup job into the internal queue."""
        # Job format:
        # ("ttl_cleanup", zset_key, ttl_seconds)
        # ("zset_trim", zset_key, max_events)
        self.queue.put(job)

    def stop(self):
        """Stop the background worker."""
        self.stop_event.set()
        self.worker_thread.join()

    def _worker(self):
        """Worker thread that executes cleanup jobs asynchronously."""
        while not self.stop_event.is_set():
            try:
                job = self.queue.get(timeout=self.cleanup_interval)
                job_type = job[0]
                if job_type == "ttl_cleanup":
                    _, zset_key, ttl_seconds = job
                    self._run_ttl_cleanup(zset_key, ttl_seconds)
                elif job_type == "zset_trim":
                    _, zset_key, max_events = job
                    self._run_zset_trim(zset_key, max_events)
            except queue.Empty:
                continue
            except Exception as e:
                logger.exception(f"[RedisCleanupManager] Error during cleanup: {e}")

    def _run_ttl_cleanup(self, zset_key: str, ttl_seconds: int):
        """Remove expired members and their hashes based on TTL."""
        now = int(time.time())
        cutoff = now - ttl_seconds
        old_members = self.client.zrangebyscore(zset_key, 0, cutoff)
        if not old_members:
            return

        with self.client.pipeline(transaction=False) as pipe:
            for member in old_members:
                pipe.delete(member)  # remove hash
                pipe.zrem(zset_key, member)
            pipe.execute()
        logger.info(
            f"[RedisCleanupManager] TTL cleanup: removed {len(old_members)} expired entries from {zset_key}"
        )
        try:
            if self.client.zcard(zset_key) == 0:
                self.client.delete(zset_key)
                logger.info(
                    f"[RedisCleanupManager] Deleted empty ZSET index {zset_key} after TTL cleanup"
                )
        except Exception as e:
            logger.warning(
                f"[RedisCleanupManager] Could not delete empty ZSET {zset_key}: {e}"
            )

    def _run_zset_trim(self, zset_key: str, max_events: int):
        """Trim ZSET to keep only the latest N members."""
        current_size = self.client.zcard(zset_key)
        if current_size <= max_events:
            return

        num_to_remove = current_size - max_events
        old_members = self.client.zrange(zset_key, 0, num_to_remove - 1)
        if not old_members:
            return

        with self.client.pipeline(transaction=False) as pipe:
            for member in old_members:
                pipe.delete(member)  # remove corresponding hash
                pipe.zrem(zset_key, member)
            pipe.execute()

        logger.info(
            f"[RedisCleanupManager] Size cleanup: trimmed {len(old_members)} entries from {zset_key}"
        )
        try:
            if self.client.zcard(zset_key) == 0:
                self.client.delete(zset_key)
                logger.debug(
                    f"[RedisCleanupManager] Deleted empty ZSET index {zset_key} after size trim"
                )
        except Exception as e:
            logger.warning(
                f"[RedisCleanupManager] Could not delete empty ZSET {zset_key}: {e}"
            )
