import math
import time
from types import SimpleNamespace

import feast.infra.passthrough_provider as pt
from feast.infra.passthrough_provider import PassthroughProvider
from feast.repo_config import RepoConfig


class TestTokenBucketRateLimiter:
    def __init__(
        self, rate, refill_interval=1.0, usage_fraction=0.8, enforce_delay=False
    ):
        # keep floats to allow fractional refill behavior
        self.rate = float(rate)
        self.interval = float(refill_interval)
        self.max_tokens = float(rate)
        self.tokens = float(rate)
        self.last_refill = time.monotonic()
        self.usage_fraction = float(usage_fraction)
        self.wait_for_tokens_calls = []
        self.enforce_delay = enforce_delay

    def _refill(self):
        now = time.monotonic()
        elapsed = now - self.last_refill
        if elapsed <= 0:
            return
        added = (self.rate * elapsed) / self.interval
        if added > 0:
            self.tokens = min(self.max_tokens, self.tokens + added)
            self.last_refill = now

    def get_available_tokens(self):
        if self.enforce_delay:
            self._refill()
        return math.floor(self.tokens * self.usage_fraction)

    def wait_for_tokens(self, requested_tokens):
        self.wait_for_tokens_calls.append(requested_tokens)
        if requested_tokens <= 0:
            return True

        if self.enforce_delay:
            # Block until tokens are available
            while True:
                self._refill()
                available = self.tokens * self.usage_fraction
                if available >= requested_tokens:
                    # consume
                    self.tokens -= requested_tokens
                    return True
                time.sleep(0.05)
        else:
            self._refill()
            if self.tokens * self.usage_fraction >= requested_tokens:
                # consume proportionally from tokens
                self.tokens -= requested_tokens
                return True
            return False


def create_repo_config_with_rate_limit(write_rate_limit: int):
    repo_config = RepoConfig(project="test_project", registry="test_registry")
    repo_config.online_config = SimpleNamespace(write_rate_limit=write_rate_limit)
    return repo_config


def _test_limiter_factory(enforce_delay: bool = False):
    def _factory(*a, **k):
        # Extract rate
        if "rate" in k:
            rate = k["rate"]
        elif len(a) >= 1:
            rate = a[0]
        else:
            rate = None

        # Extract interval/refill_interval
        if "interval" in k:
            interval = k["interval"]
        elif "refill_interval" in k:
            interval = k["refill_interval"]
        elif len(a) >= 2:
            interval = a[1]
        else:
            interval = 1.0

        # Extract percent_usage/usage_fraction
        if "percent_usage" in k:
            usage = k["percent_usage"]
        elif "usage_fraction" in k:
            usage = k["usage_fraction"]
        elif len(a) >= 3:
            usage = a[2]
        else:
            usage = 0.8

        return TestTokenBucketRateLimiter(
            rate, interval, usage, enforce_delay=enforce_delay
        )

    return _factory


def test_batching_respects_token_bucket(monkeypatch):
    """Test that batching logic respects available tokens from the token bucket."""
    repo_config = create_repo_config_with_rate_limit(write_rate_limit=10)
    provider = PassthroughProvider(repo_config)
    monkeypatch.setattr(
        pt,
        "TokenBucketRateLimiter",
        _test_limiter_factory(),
    )
    batch_sizes = []

    def mock_online_write_batch(config, table, batch, progress):
        batch_sizes.append(len(batch))

    provider._online_store = SimpleNamespace(online_write_batch=mock_online_write_batch)

    feature_table = SimpleNamespace(name="feature_view", tags=None)

    records = [object() for _ in range(25)]
    provider.online_write_batch(repo_config, feature_table, data=records, progress=None)

    key = f"{repo_config.project}:{feature_table.name}"
    limiter = provider._write_token_limiters[key]
    expected_batch_size = int(limiter.rate * limiter.usage_fraction)
    assert all(size <= expected_batch_size for size in batch_sizes)
    assert sum(batch_sizes) == 25
    assert all(n <= expected_batch_size for n in limiter.wait_for_tokens_calls)


def test_write_rate_limiting_enforced(monkeypatch):
    """Test that rate limiting is enforced by measuring elapsed time."""
    repo_config = create_repo_config_with_rate_limit(write_rate_limit=5)  # 5 tokens/sec
    provider = PassthroughProvider(repo_config)
    monkeypatch.setattr(
        pt,
        "TokenBucketRateLimiter",
        _test_limiter_factory(enforce_delay=True),
    )
    batch_sizes = []

    def mock_online_write_batch(config, table, batch, progress):
        batch_sizes.append(len(batch))

    provider._online_store = SimpleNamespace(online_write_batch=mock_online_write_batch)

    feature_table = SimpleNamespace(name="feature_view", tags=None)

    records = [
        object() for _ in range(15)
    ]  # 15 records, should take at least 3 seconds at 5/sec
    start_time = time.time()
    provider.online_write_batch(repo_config, feature_table, data=records, progress=None)
    elapsed_time = time.time() - start_time
    assert elapsed_time >= 2.0, f"Rate limiting failed, elapsed: {elapsed_time}"
    assert sum(batch_sizes) == 15


def test_tag_override(monkeypatch):
    """Test that feature view tag 'write_rate_limit' overrides config."""
    repo_config = create_repo_config_with_rate_limit(write_rate_limit=1)
    provider = PassthroughProvider(repo_config)
    monkeypatch.setattr(
        pt,
        "TokenBucketRateLimiter",
        _test_limiter_factory(),
    )
    feature_table = SimpleNamespace(name="feature_view", tags={"write_rate_limit": "7"})
    records = [object() for _ in range(10)]
    provider._online_store = SimpleNamespace(online_write_batch=lambda *a, **k: None)
    provider.online_write_batch(repo_config, feature_table, data=records, progress=None)
    key = f"{repo_config.project}:{feature_table.name}"
    limiter = provider._write_token_limiters[key]
    assert limiter.rate == 7


def test_config_fallback(monkeypatch):
    """Test that project level config value is used if no tag is present."""
    repo_config = create_repo_config_with_rate_limit(write_rate_limit=3)
    provider = PassthroughProvider(repo_config)
    monkeypatch.setattr(
        pt,
        "TokenBucketRateLimiter",
        _test_limiter_factory(),
    )
    feature_table = SimpleNamespace(name="feature_view", tags=None)
    records = [object() for _ in range(5)]
    provider._online_store = SimpleNamespace(online_write_batch=lambda *a, **k: None)
    provider.online_write_batch(repo_config, feature_table, data=records, progress=None)
    key = f"{repo_config.project}:{feature_table.name}"
    limiter = provider._write_token_limiters[key]
    assert limiter.rate == 3


def test_fallback_to_zero(monkeypatch):
    """Test that fallback to zero disables rate limiting and skips limiter creation."""
    repo_config = create_repo_config_with_rate_limit(write_rate_limit=None)
    provider = PassthroughProvider(repo_config)
    monkeypatch.setattr(
        pt,
        "TokenBucketRateLimiter",
        _test_limiter_factory(),
    )
    feature_table = SimpleNamespace(name="feature_view", tags=None)
    records = [object() for _ in range(2)]
    called = {"flag": False}

    def online_write_batch(config, table, batch, progress):
        called["flag"] = True

    provider._online_store = SimpleNamespace(online_write_batch=online_write_batch)
    provider.online_write_batch(repo_config, feature_table, data=records, progress=None)

    # Ensure the write went through directly
    assert called["flag"] is True

    # Ensure no limiter instance was created for zero/None rate
    key = f"{repo_config.project}:{feature_table.name}"
    assert key not in provider._write_token_limiters


def test_invalid_tag(monkeypatch, caplog):
    """Test that invalid tag falls back to config and logs a warning."""
    repo_config = create_repo_config_with_rate_limit(write_rate_limit=4)
    provider = PassthroughProvider(repo_config)
    monkeypatch.setattr(
        pt,
        "TokenBucketRateLimiter",
        _test_limiter_factory(),
    )
    feature_table = SimpleNamespace(
        name="feature_view", tags={"write_rate_limit": "not_a_number"}
    )
    records = [object() for _ in range(3)]
    provider._online_store = SimpleNamespace(online_write_batch=lambda *a, **k: None)
    with caplog.at_level("WARNING"):
        provider.online_write_batch(
            repo_config, feature_table, data=records, progress=None
        )
    key = f"{repo_config.project}:{feature_table.name}"
    limiter = provider._write_token_limiters[key]
    assert limiter.rate == 4
    assert any("Invalid write_rate_limit" in r for r in caplog.text.splitlines())


def test_empty_data(monkeypatch):
    """Test that empty data does not break batching logic."""
    repo_config = create_repo_config_with_rate_limit(write_rate_limit=5)
    provider = PassthroughProvider(repo_config)
    monkeypatch.setattr(
        pt,
        "TokenBucketRateLimiter",
        _test_limiter_factory(),
    )
    feature_table = SimpleNamespace(name="feature_view", tags=None)
    provider._online_store = SimpleNamespace(online_write_batch=lambda *a, **k: None)
    provider.online_write_batch(repo_config, feature_table, data=[], progress=None)
    key = f"{repo_config.project}:{feature_table.name}"
    limiter = provider._write_token_limiters[key]
    assert limiter.rate == 5
