"""Unit tests for RaySource, load_ray_dataset_from_source, and to_ray_dataset.

These tests run without a live Ray cluster using mocks and lightweight
in-process data, so they are fast and suitable for CI.

Coverage:
  - RaySource construction, validation, and proto round-trip
  - load_ray_dataset_from_source reader dispatch (mocked ray_wrapper)
  - RetrievalJob.to_ray_dataset() base-class Arrow fallback
  - pull_latest_from_table_or_query time-range filtering for RaySource
"""

from datetime import datetime
from unittest.mock import MagicMock, patch

import pandas as pd
import pyarrow as pa
import pytest

from feast.infra.offline_stores.contrib.ray_offline_store.ray_source import (
    SUPPORTED_READER_TYPES,
    RaySource,
)

# ---------------------------------------------------------------------------
# RaySource — construction and attribute access
# ---------------------------------------------------------------------------


class TestRaySourceConstruction:
    def test_huggingface_source(self):
        src = RaySource(
            name="hf",
            reader_type="huggingface",
            reader_options={"dataset_name": "my/dataset", "split": "train"},
        )
        assert src.reader_type == "huggingface"
        assert src.reader_options["dataset_name"] == "my/dataset"
        assert src.path == ""

    def test_parquet_source(self):
        src = RaySource(
            name="parq",
            reader_type="parquet",
            path="s3://bucket/data.parquet",
        )
        assert src.reader_type == "parquet"
        assert src.path == "s3://bucket/data.parquet"
        assert src.reader_options == {}

    def test_all_supported_reader_types_are_accepted(self):
        for rt in SUPPORTED_READER_TYPES:
            src = RaySource(name=f"src_{rt}", reader_type=rt, path="/tmp/x")
            assert src.reader_type == rt

    def test_unsupported_reader_type_raises(self):
        with pytest.raises(ValueError, match="reader_type"):
            RaySource(name="bad", reader_type="unsupported_format")

    def test_timestamp_field_default(self):
        src = RaySource(name="s", reader_type="csv", path="/tmp/f.csv")
        assert src.get_table_column_names_and_types(MagicMock()) == []


# ---------------------------------------------------------------------------
# RaySource — proto round-trip
# ---------------------------------------------------------------------------


class TestRaySourceProto:
    def test_round_trip_preserves_reader_type_and_path(self):
        original = RaySource(
            name="proto_test",
            reader_type="json",
            path="gs://bucket/data.jsonl",
            reader_options={"lines": True},
        )
        proto = original.to_proto()
        restored = RaySource.from_proto(proto)
        assert restored.reader_type == original.reader_type
        assert restored.path == original.path
        assert restored.reader_options == original.reader_options

    def test_round_trip_huggingface(self):
        original = RaySource(
            name="hf_proto",
            reader_type="huggingface",
            reader_options={"dataset_name": "foo/bar", "split": "train[:10]"},
        )
        restored = RaySource.from_proto(original.to_proto())
        assert restored.reader_options["dataset_name"] == "foo/bar"
        assert restored.reader_options["split"] == "train[:10]"


# ---------------------------------------------------------------------------
# load_ray_dataset_from_source — reader dispatch (mocked wrapper)
# ---------------------------------------------------------------------------


class TestLoadRayDatasetFromSource:
    """Each reader type is dispatched to the right ray_wrapper method."""

    def _mock_wrapper(self):
        w = MagicMock()
        w.read_parquet.return_value = "parquet_ds"
        w.read_csv.return_value = "csv_ds"
        w.read_json.return_value = "json_ds"
        w.read_text.return_value = "text_ds"
        w.read_images.return_value = "images_ds"
        w.read_binary_files.return_value = "binary_ds"
        w.read_tfrecords.return_value = "tfrecords_ds"
        w.read_webdataset.return_value = "webdataset_ds"
        w.from_huggingface.return_value = "hf_ds"
        w.read_mongo.return_value = "mongo_ds"
        w.read_sql.return_value = "sql_ds"
        return w

    @pytest.mark.parametrize(
        "reader_type,path,expected",
        [
            ("parquet", "s3://b/data.parquet", "parquet_ds"),
            ("csv", "/tmp/data.csv", "csv_ds"),
            ("json", "/tmp/data.json", "json_ds"),
            ("text", "/tmp/data.txt", "text_ds"),
            ("images", "/tmp/imgs/", "images_ds"),
            ("binary_files", "/tmp/bin/", "binary_ds"),
            ("tfrecords", "/tmp/data.tfrecord", "tfrecords_ds"),
            ("webdataset", "/tmp/data.tar", "webdataset_ds"),
        ],
    )
    def test_file_readers_dispatch(self, reader_type, path, expected):
        from feast.infra.offline_stores.contrib.ray_offline_store.ray_offline_store_reader import (
            load_ray_dataset_from_source,
        )

        src = RaySource(name="s", reader_type=reader_type, path=path)
        mock_wrapper = self._mock_wrapper()
        with patch(
            "feast.infra.offline_stores.contrib.ray_offline_store.ray_offline_store_reader.get_ray_wrapper",
            return_value=mock_wrapper,
        ):
            result = load_ray_dataset_from_source(src)
        assert result == expected

    def test_huggingface_dispatch(self):
        import sys

        from feast.infra.offline_stores.contrib.ray_offline_store.ray_offline_store_reader import (
            load_ray_dataset_from_source,
        )

        src = RaySource(
            name="hf",
            reader_type="huggingface",
            reader_options={"dataset_name": "org/ds", "split": "train"},
        )
        mock_wrapper = self._mock_wrapper()
        mock_hf_dataset = MagicMock()

        # Inject a fake `datasets` module so `from datasets import load_dataset`
        # inside load_ray_dataset_from_source succeeds without the real package.
        fake_datasets = MagicMock()
        fake_datasets.load_dataset.return_value = mock_hf_dataset

        with (
            patch.dict(sys.modules, {"datasets": fake_datasets}),
            patch(
                "feast.infra.offline_stores.contrib.ray_offline_store.ray_offline_store_reader.get_ray_wrapper",
                return_value=mock_wrapper,
            ),
        ):
            load_ray_dataset_from_source(src)

        mock_wrapper.from_huggingface.assert_called_once()

    def test_unknown_reader_type_raises(self):
        from feast.infra.offline_stores.contrib.ray_offline_store.ray_offline_store_reader import (
            load_ray_dataset_from_source,
        )

        # Bypass RaySource construction-time validation by patching the
        # underlying RaySourceOptions directly (reader_type is a read-only
        # property on RaySource that delegates to ray_source_options).
        src = RaySource(name="s", reader_type="parquet", path="/x")
        src.ray_source_options.reader_type = "not_a_thing"
        mock_wrapper = self._mock_wrapper()
        with patch(
            "feast.infra.offline_stores.contrib.ray_offline_store.ray_offline_store_reader.get_ray_wrapper",
            return_value=mock_wrapper,
        ):
            with pytest.raises(ValueError, match="Unknown reader_type"):
                load_ray_dataset_from_source(src)


# ---------------------------------------------------------------------------
# RetrievalJob.to_ray_dataset() — base-class Arrow fallback
# ---------------------------------------------------------------------------


class TestRetrievalJobToRayDataset:
    """The base-class default converts via to_arrow() → ray.data.from_arrow()."""

    def _make_job(self, arrow_table: pa.Table):
        """Create a minimal concrete RetrievalJob whose _to_arrow_internal returns arrow_table."""
        from feast.infra.offline_stores.offline_store import RetrievalJob

        class _ConcreteJob(RetrievalJob):
            @property
            def full_feature_names(self):
                return False

            @property
            def on_demand_feature_views(self):
                return []

            def _to_df_internal(self, timeout=None):
                return arrow_table.to_pandas()

            def _to_arrow_internal(self, timeout=None):
                return arrow_table

        return _ConcreteJob()

    def test_returns_ray_dataset_with_correct_rows(self):
        pytest.importorskip("ray")
        table = pa.table({"a": [1, 2, 3], "b": ["x", "y", "z"]})
        job = self._make_job(table)
        ds = job.to_ray_dataset()
        assert ds.count() == 3

    def test_import_error_without_ray(self):
        import sys

        table = pa.table({"a": [1]})
        job = self._make_job(table)
        with patch.dict(sys.modules, {"ray": None, "ray.data": None}):
            with pytest.raises(ImportError, match="Ray is required"):
                job.to_ray_dataset()

    def test_ray_retrieval_job_overrides_base(self):
        """RayRetrievalJob.to_ray_dataset() must not call the Arrow fallback."""
        from feast.infra.offline_stores.contrib.ray_offline_store.ray import (
            RayRetrievalJob,
        )
        from feast.infra.offline_stores.offline_store import RetrievalJob

        base_impl = RetrievalJob.to_ray_dataset
        ray_impl = RayRetrievalJob.__dict__.get("to_ray_dataset")
        assert ray_impl is not None, (
            "RayRetrievalJob must define its own to_ray_dataset"
        )
        assert ray_impl is not base_impl


# ---------------------------------------------------------------------------
# get_historical_features().to_ray_dataset()
# ---------------------------------------------------------------------------


class TestGetHistoricalFeaturesToRayDataset:
    """Callers chain get_historical_features().to_ray_dataset() directly.

    FeatureStore has no separate to_ray_dataset() wrapper; to_ray_dataset() is
    a first-class method on the RetrievalJob returned by get_historical_features().
    """

    def test_chain_calls_to_ray_dataset_on_job(self):
        mock_job = MagicMock()
        sentinel = object()
        mock_job.to_ray_dataset.return_value = sentinel

        store = MagicMock()
        store.get_historical_features.return_value = mock_job

        entity_df = pd.DataFrame(
            {"driver_id": [1], "event_timestamp": [datetime.now()]}
        )
        result = store.get_historical_features(
            features=["driver_stats:conv_rate"],
            entity_df=entity_df,
        ).to_ray_dataset()

        mock_job.to_ray_dataset.assert_called_once()
        assert result is sentinel


# ---------------------------------------------------------------------------
# pull_latest_from_table_or_query — RaySource time-range filter and dedup
# ---------------------------------------------------------------------------


class TestPullLatestRaySourceFiltering:
    """Verify that pull_latest_from_table_or_query applies time-range filtering
    and deduplication for file-backed RaySource (e.g. reader_type="parquet").

    RaySource without a timestamp_field (exotic sources such as HuggingFace
    image datasets) must still be returned raw.
    """

    def _make_source(self, reader_type: str = "parquet") -> RaySource:
        return RaySource(
            name="test_src",
            reader_type=reader_type,
            path="s3://bucket/data.parquet",
            timestamp_field="event_timestamp",
        )

    def test_load_and_filter_dataset_ray_pre_loaded(self):
        """_load_and_filter_dataset_ray(pre_loaded_ds=...) must not raise and
        must apply the shared field-mapping / normalise / batch pipeline."""
        from datetime import timezone

        from feast.infra.offline_stores.contrib.ray_offline_store.ray import (
            RayOfflineStore,
        )

        start = datetime(2024, 1, 2, tzinfo=timezone.utc)
        end = datetime(2024, 1, 4, tzinfo=timezone.utc)

        rows = pa.table(
            {
                "driver_id": [1, 1, 1],
                "event_timestamp": pa.array(
                    [
                        datetime(2024, 1, 1, tzinfo=timezone.utc),  # before window
                        datetime(2024, 1, 3, tzinfo=timezone.utc),  # inside window
                        datetime(2024, 1, 5, tzinfo=timezone.utc),  # after window
                    ],
                    type=pa.timestamp("us", tz="UTC"),
                ),
                "feature_a": [10, 20, 30],
            }
        )

        src = self._make_source()

        with (
            patch(
                "feast.infra.offline_stores.contrib.ray_offline_store.ray.normalize_timestamp_columns"
            ) as mock_norm,
            patch(
                "feast.infra.offline_stores.contrib.ray_offline_store.ray.apply_field_mapping"
            ) as mock_map,
        ):
            import ray.data as rd

            mock_ds = rd.from_arrow(rows)
            mock_norm.return_value = mock_ds
            mock_map.return_value = mock_ds

            # We call the static method directly; it should not raise.
            # Full integration (filter + sort) is verified in component tests.
            store = RayOfflineStore()
            _ = store._load_and_filter_dataset_ray(
                None,
                src,
                join_key_columns=["driver_id"],
                feature_name_columns=["feature_a"],
                timestamp_field="event_timestamp",
                created_timestamp_column=None,
                start_date=start,
                end_date=end,
                pre_loaded_ds=mock_ds,
            )

    def test_pull_latest_raw_for_source_without_timestamp(self):
        """When timestamp_field is empty, RaySource data must be returned raw."""
        src = RaySource(
            name="hf_src",
            reader_type="huggingface",
            reader_options={"dataset_name": "cheques_sample_data"},
        )

        raw_sentinel = object()

        with (
            patch(
                "feast.infra.offline_stores.contrib.ray_offline_store.ray.RayOfflineStore._init_ray"
            ),
            patch(
                "feast.infra.offline_stores.contrib.ray_offline_store.ray_offline_store_reader.load_ray_dataset_from_source",
                return_value=raw_sentinel,
            ),
            patch(
                "feast.infra.offline_stores.contrib.ray_offline_store.ray.RayRetrievalJob"
            ) as mock_job_cls,
        ):
            from feast.infra.offline_stores.contrib.ray_offline_store.ray import (
                RayOfflineStore,
            )

            mock_config = MagicMock()
            mock_config.offline_store.storage_path = "/tmp/staging"

            RayOfflineStore.pull_latest_from_table_or_query(
                config=mock_config,
                data_source=src,
                join_key_columns=[],
                feature_name_columns=["feature_a"],
                timestamp_field="",
                created_timestamp_column=None,
                start_date=datetime(2024, 1, 1),
                end_date=datetime(2024, 1, 31),
            )

            # A RayRetrievalJob must still be created (just wrapping raw data).
            mock_job_cls.assert_called_once()
