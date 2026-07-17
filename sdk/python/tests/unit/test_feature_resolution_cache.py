"""Tests for the feature resolution cache in feast.utils._get_cached_request_context."""

from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytest

import feast.utils as utils


@pytest.fixture(autouse=True)
def _clear_cache():
    """Reset the module-level cache before each test."""
    with utils._feature_resolution_cache_lock:
        utils._feature_resolution_cache.clear()
        utils._feature_resolution_registry_ts = None
    yield
    with utils._feature_resolution_cache_lock:
        utils._feature_resolution_cache.clear()
        utils._feature_resolution_registry_ts = None


def _make_registry(ts: datetime, ttl_seconds: int = 600, cache_valid: bool = True):
    reg = MagicMock()
    reg.cached_registry_proto_created = ts
    reg.cached_registry_proto_ttl = timedelta(seconds=ttl_seconds)
    reg.is_cache_valid.return_value = cache_valid
    return reg


def _make_context():
    """Return a plausible fake context tuple matching _get_online_request_context output."""
    return (
        ["fv:feat1"],  # feature_refs
        [],  # requested_on_demand_feature_views
        {},  # entity_name_to_join_key_map
        {},  # entity_type_map
        {"user_id"},  # join_keys_set
        [("fv_table", ["feat1"])],  # grouped_refs
        {"feat1"},  # requested_result_row_names
        set(),  # needed_request_data
        False,  # entityless_case
    )


class TestFeatureResolutionCache:
    @patch("feast.utils._get_online_request_context")
    def test_second_call_uses_cache(self, mock_ctx):
        """Identical feature requests within the same registry generation
        should only call _get_online_request_context once."""
        mock_ctx.return_value = _make_context()
        now = datetime.now(tz=timezone.utc)
        registry = _make_registry(now, ttl_seconds=600)
        features = ["fv:feat1"]

        utils._get_cached_request_context(registry, "proj", features, False)
        utils._get_cached_request_context(registry, "proj", features, False)

        mock_ctx.assert_called_once()

    @patch("feast.utils._get_online_request_context")
    def test_different_features_are_separate_cache_entries(self, mock_ctx):
        """Different feature lists should be cached independently."""
        mock_ctx.return_value = _make_context()
        now = datetime.now(tz=timezone.utc)
        registry = _make_registry(now, ttl_seconds=600)

        utils._get_cached_request_context(registry, "proj", ["fv:feat1"], False)
        utils._get_cached_request_context(registry, "proj", ["fv:feat2"], False)

        assert mock_ctx.call_count == 2

    @patch("feast.utils._get_online_request_context")
    def test_cache_cleared_on_registry_refresh(self, mock_ctx):
        """When registry.cached_registry_proto_created changes, the cache
        must be cleared and _get_online_request_context called again."""
        mock_ctx.return_value = _make_context()
        t1 = datetime.now(tz=timezone.utc)
        t2 = t1 + timedelta(seconds=30)

        registry = _make_registry(t1, ttl_seconds=600)
        utils._get_cached_request_context(registry, "proj", ["fv:feat1"], False)
        assert mock_ctx.call_count == 1

        registry.cached_registry_proto_created = t2
        utils._get_cached_request_context(registry, "proj", ["fv:feat1"], False)
        assert mock_ctx.call_count == 2

    @patch("feast.utils._get_online_request_context")
    def test_cache_bypassed_when_registry_ttl_expired(self, mock_ctx):
        """If the registry's own cache has expired (TTL), the feature
        resolution cache must NOT be used even if the timestamp matches."""
        mock_ctx.return_value = _make_context()
        past = datetime.now(tz=timezone.utc) - timedelta(seconds=120)
        registry = _make_registry(past, ttl_seconds=60, cache_valid=False)

        utils._get_cached_request_context(registry, "proj", ["fv:feat1"], False)
        utils._get_cached_request_context(registry, "proj", ["fv:feat1"], False)

        assert mock_ctx.call_count == 2

    @patch("feast.utils._get_online_request_context")
    def test_cache_works_with_infinite_ttl(self, mock_ctx):
        """TTL of 0 means infinite cache — should cache normally."""
        mock_ctx.return_value = _make_context()
        past = datetime.now(tz=timezone.utc) - timedelta(hours=24)
        registry = _make_registry(past, ttl_seconds=0)

        utils._get_cached_request_context(registry, "proj", ["fv:feat1"], False)
        utils._get_cached_request_context(registry, "proj", ["fv:feat1"], False)

        mock_ctx.assert_called_once()

    @patch("feast.utils._get_online_request_context")
    def test_no_cache_without_registry_timestamp(self, mock_ctx):
        """Registries without cached_registry_proto_created should bypass
        caching entirely."""
        mock_ctx.return_value = _make_context()
        registry = MagicMock(spec=[])

        utils._get_cached_request_context(registry, "proj", ["fv:feat1"], False)
        utils._get_cached_request_context(registry, "proj", ["fv:feat1"], False)

        assert mock_ctx.call_count == 2

    @patch("feast.utils._get_online_request_context")
    def test_returned_result_row_names_is_mutable_copy(self, mock_ctx):
        """requested_result_row_names is stored as frozenset in cache;
        each call to _prepare_entities_to_read_from_online_store should
        get a fresh mutable copy so mutations don't leak between requests."""
        mock_ctx.return_value = _make_context()
        now = datetime.now(tz=timezone.utc)
        registry = _make_registry(now, ttl_seconds=600)

        r1 = utils._get_cached_request_context(registry, "proj", ["fv:feat1"], False)
        r2 = utils._get_cached_request_context(registry, "proj", ["fv:feat1"], False)

        result_row_names_1 = r1[6]
        result_row_names_2 = r2[6]
        assert isinstance(result_row_names_1, frozenset)
        assert result_row_names_1 == result_row_names_2

    @patch("feast.utils._get_online_request_context")
    def test_feature_service_cached_separately(self, mock_ctx):
        """FeatureService objects should be cached by name."""
        mock_ctx.return_value = _make_context()
        now = datetime.now(tz=timezone.utc)
        registry = _make_registry(now, ttl_seconds=600)

        fs = MagicMock()
        fs.name = "credit_scoring_v1"

        utils._get_cached_request_context(registry, "proj", fs, False)
        utils._get_cached_request_context(registry, "proj", fs, False)

        mock_ctx.assert_called_once()

        utils._get_cached_request_context(registry, "proj", ["fv:feat1"], False)
        assert mock_ctx.call_count == 2

    @patch("feast.utils._get_online_request_context")
    def test_full_feature_names_flag_is_cache_key(self, mock_ctx):
        """full_feature_names=True vs False should be different cache entries."""
        mock_ctx.return_value = _make_context()
        now = datetime.now(tz=timezone.utc)
        registry = _make_registry(now, ttl_seconds=600)

        utils._get_cached_request_context(registry, "proj", ["fv:feat1"], False)
        utils._get_cached_request_context(registry, "proj", ["fv:feat1"], True)

        assert mock_ctx.call_count == 2
