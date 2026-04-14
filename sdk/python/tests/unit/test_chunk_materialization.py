"""Unit tests for time-chunked materialization in FeatureStore.

These tests verify:
* ``FeatureStore._resolve_chunk_size()`` priority chain
* ``FeatureStore.materialize()`` dispatches N chunk calls when chunked
* ``FeatureStore.materialize_incremental()`` dispatches N chunk calls when chunked
* Chunk boundaries are contiguous and cover the full requested time range
* ``ChunkFailureStrategy.CONTINUE`` skips failed chunks instead of raising
* Single-pass behaviour is preserved when ``chunk_size=None``
"""

from datetime import datetime, timedelta, timezone
from tempfile import mkstemp
from unittest.mock import patch

import pandas as pd
import pytest

from feast import FeatureStore
from feast.data_format import ParquetFormat
from feast.entity import Entity
from feast.feature_view import FeatureView
from feast.field import Field
from feast.infra.offline_stores.file_source import FileSource
from feast.infra.online_stores.sqlite import SqliteOnlineStoreConfig
from feast.repo_config import ChunkFailureStrategy, MaterializationConfig, RepoConfig
from feast.types import Int64
from feast.value_type import ValueType

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_store(
    materialization_config: MaterializationConfig = MaterializationConfig(),
) -> FeatureStore:
    """Return an in-process FeatureStore backed by temporary SQLite/file registry."""
    fd1, registry_path = mkstemp()
    fd2, online_store_path = mkstemp()
    return FeatureStore(
        config=RepoConfig(
            registry=registry_path,
            project="test_chunked_mat",
            provider="local",
            online_store=SqliteOnlineStoreConfig(path=online_store_path),
            entity_key_serialization_version=3,
            materialization=materialization_config.model_dump(),
        )
    )


def _make_file_source(df: pd.DataFrame, timestamp_field: str) -> "FileSource":
    import tempfile

    # We write to a persistent temp file so the FeatureStore can read it.
    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
        path = tmp.name
    df.to_parquet(path)
    return FileSource(
        file_format=ParquetFormat(),
        path=path,
        timestamp_field=timestamp_field,
    )


def _utc(dt: datetime) -> datetime:
    return dt.replace(tzinfo=timezone.utc)


# ---------------------------------------------------------------------------
# _resolve_chunk_size
# ---------------------------------------------------------------------------


class TestResolveChunkSize:
    """Tests for FeatureStore._resolve_chunk_size()."""

    def test_override_takes_highest_priority(self):
        """Call-time override should win over config."""
        store = _make_store(MaterializationConfig(chunk_size=timedelta(hours=6)))
        override = timedelta(hours=2)
        assert store._resolve_chunk_size(override) == override

    def test_config_chunk_size_used_when_no_override(self):
        """Config chunk_size should be returned when no override is given."""
        config_chunk = timedelta(hours=12)
        store = _make_store(MaterializationConfig(chunk_size=config_chunk))
        assert store._resolve_chunk_size(None) == config_chunk

    def test_none_returned_when_both_unset(self):
        """None should be returned (single-pass mode) when nothing is configured."""
        store = _make_store(MaterializationConfig())
        assert store._resolve_chunk_size(None) is None

    def test_override_none_falls_through_to_config(self):
        """Passing override=None should behave the same as not passing an override."""
        config_chunk = timedelta(minutes=30)
        store = _make_store(MaterializationConfig(chunk_size=config_chunk))
        assert store._resolve_chunk_size(None) == config_chunk


# ---------------------------------------------------------------------------
# Materialize – chunk dispatch
# ---------------------------------------------------------------------------


class TestMaterializeChunking:
    """Integration-style unit tests verifying chunk dispatch in materialize()."""

    def _apply_fv(self, store: FeatureStore, entity: Entity, fv: FeatureView):
        store.apply([entity, fv])

    def _setup(self, materialization_config=MaterializationConfig()):
        """Build a store with one FeatureView and return (store, entity, fv)."""
        store = _make_store(materialization_config)

        entity = Entity(
            name="driver_id", join_keys=["driver_id"], value_type=ValueType.INT64
        )

        # Build a minimal DataFrame with timestamps spread over 2 days
        now = datetime(2024, 1, 1, tzinfo=timezone.utc)
        df = pd.DataFrame(
            {
                "driver_id": [1, 2, 3],
                "feature_val": [10, 20, 30],
                "ts": [now, now + timedelta(hours=12), now + timedelta(hours=23)],
            }
        )
        file_source = _make_file_source(df, timestamp_field="ts")

        fv = FeatureView(
            name="driver_fv",
            schema=[Field(name="feature_val", dtype=Int64)],
            entities=[entity],
            source=file_source,
            ttl=timedelta(days=7),
        )
        store.apply([entity, fv])
        return store, entity, fv

    # -----------------------------------------------------------------------
    # No chunking (backward-compatible behaviour)
    # -----------------------------------------------------------------------

    def test_no_chunk_size_single_call(self):
        """Without chunk_size, materialize_single_feature_view is called once per FV."""
        store, _, _ = self._setup()
        start = _utc(datetime(2024, 1, 1))
        end = _utc(datetime(2024, 1, 3))

        with patch.object(
            store._get_provider().__class__,
            "materialize_single_feature_view",
            return_value=None,
        ) as mock_mat:
            store.materialize(start_date=start, end_date=end)
            assert mock_mat.call_count == 1
            kwargs = mock_mat.call_args.kwargs
            assert kwargs["start_date"] == start
            assert kwargs["end_date"] == end

    # -----------------------------------------------------------------------
    # Chunked – call-time override
    # -----------------------------------------------------------------------

    def test_chunk_size_override_splits_into_correct_number_of_calls(self):
        """materialize_single_feature_view should be called once per chunk."""
        store, _, _ = self._setup()
        start = _utc(datetime(2024, 1, 1))
        end = _utc(datetime(2024, 1, 3))  # 48 hours

        chunk_size = timedelta(hours=12)  # → 4 chunks

        with patch.object(
            store._get_provider().__class__,
            "materialize_single_feature_view",
            return_value=None,
        ) as mock_mat:
            store.materialize(start_date=start, end_date=end, chunk_size=chunk_size)
            assert mock_mat.call_count == 4

    def test_chunk_boundaries_are_contiguous(self):
        """Start of chunk N+1 must equal end of chunk N."""
        store, _, _ = self._setup()
        start = _utc(datetime(2024, 1, 1))
        end = _utc(datetime(2024, 1, 3))  # 48 hours, chunk=13h → 4 chunks

        collected: list = []

        def _capture(**kwargs):
            collected.append((kwargs["start_date"], kwargs["end_date"]))

        with patch.object(
            store._get_provider().__class__,
            "materialize_single_feature_view",
            side_effect=_capture,
        ):
            store.materialize(
                start_date=start, end_date=end, chunk_size=timedelta(hours=13)
            )

        assert collected[0][0] == start
        assert collected[-1][1] == end
        for i in range(len(collected) - 1):
            assert collected[i][1] == collected[i + 1][0]

    def test_chunk_size_from_config_is_used(self):
        """Config-level chunk_size should be respected when no override is given."""
        store, _, _ = self._setup(MaterializationConfig(chunk_size=timedelta(hours=12)))
        start = _utc(datetime(2024, 1, 1))
        end = _utc(datetime(2024, 1, 3))  # 48 hours → 4 chunks of 12h

        with patch.object(
            store._get_provider().__class__,
            "materialize_single_feature_view",
            return_value=None,
        ) as mock_mat:
            store.materialize(start_date=start, end_date=end)
            assert mock_mat.call_count == 4

    def test_call_time_chunk_size_overrides_config(self):
        """A chunk_size passed at call time must override the config value."""
        store, _, _ = self._setup(MaterializationConfig(chunk_size=timedelta(hours=12)))
        start = _utc(datetime(2024, 1, 1))
        end = _utc(datetime(2024, 1, 3))  # 48 hours

        # Override with 24h → 2 chunks instead of 4
        with patch.object(
            store._get_provider().__class__,
            "materialize_single_feature_view",
            return_value=None,
        ) as mock_mat:
            store.materialize(
                start_date=start, end_date=end, chunk_size=timedelta(hours=24)
            )
            assert mock_mat.call_count == 2

    # -----------------------------------------------------------------------
    # ChunkFailureStrategy.CONTINUE
    # -----------------------------------------------------------------------

    def test_continue_strategy_skips_failed_chunks(self):
        """With CONTINUE strategy, a failed chunk does not abort the run and
        all chunks are attempted.  Crucially, apply_materialization must only
        be called for the contiguous prefix of successful chunks to avoid
        advancing the watermark past the failed window."""
        store, _, _ = self._setup(
            MaterializationConfig(
                chunk_size=timedelta(hours=12),
                chunk_failure_strategy=ChunkFailureStrategy.CONTINUE,
            )
        )
        start = _utc(datetime(2024, 1, 1))
        end = _utc(datetime(2024, 1, 3))  # 4 chunks of 12h

        call_count = {"n": 0}

        def _fail_second(**kwargs):
            call_count["n"] += 1
            if call_count["n"] == 2:
                raise RuntimeError("Simulated chunk failure")

        with (
            patch.object(
                store._get_provider().__class__,
                "materialize_single_feature_view",
                side_effect=_fail_second,
            ),
            patch.object(store.registry, "apply_materialization") as mock_reg,
        ):
            import warnings

            with warnings.catch_warnings(record=True) as caught:
                warnings.simplefilter("always")
                store.materialize(start_date=start, end_date=end)

        # All 4 chunks are attempted (online store receives data for 3 of them)
        assert call_count["n"] == 4
        # A warning is emitted about the failure
        assert any("failed" in str(w.message).lower() for w in caught)
        # apply_materialization must only have been called for chunk 1 (the
        # contiguous prefix before the gap).  Chunks 3 and 4 succeed but must
        # NOT advance the watermark past the failed chunk 2 window.
        assert mock_reg.call_count == 1
        committed_end = mock_reg.call_args_list[0].args[3]
        assert committed_end == start + timedelta(hours=12)

    def test_continue_strategy_all_succeed_commits_all(self):
        """When all chunks succeed the watermark must reach the full end_date."""
        store, _, _ = self._setup(
            MaterializationConfig(
                chunk_size=timedelta(hours=12),
                chunk_failure_strategy=ChunkFailureStrategy.CONTINUE,
            )
        )
        start = _utc(datetime(2024, 1, 1))
        end = _utc(datetime(2024, 1, 3))  # 4 chunks

        with (
            patch.object(
                store._get_provider().__class__,
                "materialize_single_feature_view",
                return_value=None,
            ),
            patch.object(store.registry, "apply_materialization") as mock_reg,
        ):
            store.materialize(start_date=start, end_date=end)

        # All 4 chunks committed — no gap, so all advance the watermark
        assert mock_reg.call_count == 4

    def test_stop_strategy_reraises_on_first_failure(self):
        """Default STOP strategy must re-raise the exception from the first failing chunk."""
        store, _, _ = self._setup(MaterializationConfig(chunk_size=timedelta(hours=12)))
        start = _utc(datetime(2024, 1, 1))
        end = _utc(datetime(2024, 1, 3))  # 4 chunks

        call_count = {"n": 0}

        def _fail_first(**kwargs):
            call_count["n"] += 1
            if call_count["n"] == 1:
                raise RuntimeError("First chunk failure")

        with patch.object(
            store._get_provider().__class__,
            "materialize_single_feature_view",
            side_effect=_fail_first,
        ):
            with pytest.raises(RuntimeError, match="First chunk failure"):
                store.materialize(start_date=start, end_date=end)

        # Only 1 call was made (stopped on first failure)
        assert call_count["n"] == 1

    # -----------------------------------------------------------------------
    # apply_materialization checkpointing
    # -----------------------------------------------------------------------

    def test_apply_materialization_called_per_chunk(self):
        """registry.apply_materialization must be called once per successful chunk."""
        store, _, _ = self._setup()
        start = _utc(datetime(2024, 1, 1))
        end = _utc(datetime(2024, 1, 3))  # 48 hours → 2 chunks of 24h

        with (
            patch.object(
                store._get_provider().__class__,
                "materialize_single_feature_view",
                return_value=None,
            ),
            patch.object(store.registry, "apply_materialization") as mock_reg,
        ):
            store.materialize(
                start_date=start, end_date=end, chunk_size=timedelta(hours=24)
            )
            assert mock_reg.call_count == 2
            # First call's end_date should be start + 24h
            first_call_end = mock_reg.call_args_list[0].args[3]
            assert first_call_end == start + timedelta(hours=24)


# ---------------------------------------------------------------------------
# MaterializeIncremental – chunk dispatch
# ---------------------------------------------------------------------------


class TestMaterializeIncrementalChunking:
    """Sanity-check that materialize_incremental() also respects chunk_size."""

    def _setup(self, materialization_config=MaterializationConfig()):
        store = _make_store(materialization_config)
        entity = Entity(
            name="user_id", join_keys=["user_id"], value_type=ValueType.INT64
        )
        now = datetime(2024, 1, 1, tzinfo=timezone.utc)
        df = pd.DataFrame(
            {
                "user_id": [1, 2],
                "feat": [100, 200],
                "ts": [now, now + timedelta(hours=5)],
            }
        )
        file_source = _make_file_source(df, timestamp_field="ts")
        fv = FeatureView(
            name="user_fv",
            schema=[Field(name="feat", dtype=Int64)],
            entities=[entity],
            source=file_source,
            ttl=timedelta(days=1),
        )
        store.apply([entity, fv])

        # Seed materialization_intervals so incremental has a start_date
        start = _utc(datetime(2024, 1, 1))
        initial_end = _utc(datetime(2024, 1, 1, 0))
        store.registry.apply_materialization(fv, store.project, start, initial_end)

        return store, entity, fv

    def test_incremental_chunk_size_splits_calls(self):
        """materialize_incremental should honour chunk_size."""
        store, _, _ = self._setup()
        end = _utc(datetime(2024, 1, 3))  # 2 days from seed end (2024-01-01T00)

        with patch.object(
            store._get_provider().__class__,
            "materialize_single_feature_view",
            return_value=None,
        ) as mock_mat:
            store.materialize_incremental(end_date=end, chunk_size=timedelta(hours=24))
            # 48 hours / 24 h = 2 chunks
            assert mock_mat.call_count == 2

    def test_incremental_continue_watermark_does_not_advance_past_gap(self):
        """Regression test for the data-loss bug described in GitHub issue.

        Scenario (4 x 6-hour chunks, chunk 2 fails):
          chunk 1  [0h,  6h)  ✓
          chunk 2  [6h, 12h)  ✗  ← fails
          chunk 3  [12h,18h)  ✓
          chunk 4  [18h,24h)  ✓

        Expected: watermark advances only to 6h (end of chunk 1).
        The next materialize_incremental() starts from 6h and retries the
        failed window naturally, so data is never permanently lost.

        Bug (before fix): apply_materialization was called for chunks 3 and 4,
        advancing most_recent_end_time to 24h.  The 6–12 h window was then
        permanently skipped by subsequent incremental runs.
        """
        store, _, fv = self._setup(
            materialization_config=MaterializationConfig(
                chunk_size=timedelta(hours=6),
                chunk_failure_strategy=ChunkFailureStrategy.CONTINUE,
            )
        )
        # Seed: watermark starts at midnight
        seed_start = _utc(datetime(2024, 1, 1, 0))
        seed_end = _utc(datetime(2024, 1, 1, 0))
        store.registry.apply_materialization(fv, store.project, seed_start, seed_end)

        end = _utc(datetime(2024, 1, 2, 0))  # 24 hours → 4 chunks of 6h

        call_order = []

        def _fail_second(**kwargs):
            call_order.append(kwargs["start_date"])
            if len(call_order) == 2:
                raise RuntimeError("Chunk 2 failure")

        with (
            patch.object(
                store._get_provider().__class__,
                "materialize_single_feature_view",
                side_effect=_fail_second,
            ),
            patch.object(store.registry, "apply_materialization") as mock_reg,
        ):
            import warnings

            with warnings.catch_warnings(record=True):
                warnings.simplefilter("always")
                store.materialize_incremental(end_date=end)

        # All 4 chunks must have been attempted
        assert len(call_order) == 4

        # apply_materialization must only have been called once (chunk 1 only)
        assert mock_reg.call_count == 1
        committed_end = mock_reg.call_args_list[0].args[3]
        # Watermark should sit at end of chunk 1, i.e. seed_end + 6h
        assert committed_end == seed_end + timedelta(hours=6), (
            f"Watermark advanced past the gap to {committed_end}; "
            f"expected {seed_end + timedelta(hours=6)}"
        )
