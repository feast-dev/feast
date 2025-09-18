from typing import Any, Dict, List, Optional, Union

import numpy as np
import pandas as pd
import pyarrow as pa
import ray
from ray.data import Dataset


class RemoteDatasetProxy:
    """Proxy class that executes Ray Data operations remotely on cluster workers."""

    def __init__(self, dataset_ref: Any):
        """Initialize with a reference to the remote dataset."""
        self._dataset_ref = dataset_ref

    def map_batches(self, func, **kwargs) -> "RemoteDatasetProxy":
        """Execute map_batches remotely on cluster workers."""

        @ray.remote
        def _remote_map_batches(dataset, function, batch_kwargs):
            return dataset.map_batches(function, **batch_kwargs)

        new_ref = _remote_map_batches.remote(self._dataset_ref, func, kwargs)
        return RemoteDatasetProxy(new_ref)

    def filter(self, fn) -> "RemoteDatasetProxy":
        """Execute filter remotely on cluster workers."""

        @ray.remote
        def _remote_filter(dataset, filter_fn):
            return dataset.filter(filter_fn)

        new_ref = _remote_filter.remote(self._dataset_ref, fn)
        return RemoteDatasetProxy(new_ref)

    def to_pandas(self) -> pd.DataFrame:
        """Execute to_pandas remotely and transfer result to client."""

        @ray.remote
        def _remote_to_pandas(dataset):
            return dataset.to_pandas()

        result_ref = _remote_to_pandas.remote(self._dataset_ref)
        return ray.get(result_ref)

    def to_arrow(self) -> pa.Table:
        """Execute to_arrow remotely and transfer result to client."""

        @ray.remote
        def _remote_to_arrow(dataset):
            arrow_refs = dataset.to_arrow_refs()
            if arrow_refs:
                tables = ray.get(arrow_refs)
                return pa.concat_tables(tables)
            else:
                return pa.Table.from_pydict({})

        result_ref = _remote_to_arrow.remote(self._dataset_ref)
        return ray.get(result_ref)

    def schema(self) -> Any:
        """Get dataset schema."""

        @ray.remote
        def _remote_schema(dataset):
            return dataset.schema()

        schema_ref = _remote_schema.remote(self._dataset_ref)
        return ray.get(schema_ref)

    def sort(self, key, descending=False) -> "RemoteDatasetProxy":
        """Execute sort remotely on cluster workers."""

        @ray.remote
        def _remote_sort(dataset, sort_key, desc):
            return dataset.sort(sort_key, descending=desc)

        new_ref = _remote_sort.remote(self._dataset_ref, key, descending)
        return RemoteDatasetProxy(new_ref)

    def limit(self, count) -> "RemoteDatasetProxy":
        """Execute limit remotely on cluster workers."""

        @ray.remote
        def _remote_limit(dataset, limit_count):
            return dataset.limit(limit_count)

        new_ref = _remote_limit.remote(self._dataset_ref, count)
        return RemoteDatasetProxy(new_ref)

    def union(self, other) -> "RemoteDatasetProxy":
        """Execute union remotely on cluster workers."""

        @ray.remote
        def _remote_union(dataset1, dataset2):
            return dataset1.union(dataset2)

        new_ref = _remote_union.remote(self._dataset_ref, other._dataset_ref)
        return RemoteDatasetProxy(new_ref)

    def materialize(self) -> "RemoteDatasetProxy":
        """Execute materialize remotely on cluster workers."""

        @ray.remote
        def _remote_materialize(dataset):
            return dataset.materialize()

        new_ref = _remote_materialize.remote(self._dataset_ref)
        return RemoteDatasetProxy(new_ref)

    def count(self) -> int:
        """Execute count remotely and return result."""

        @ray.remote
        def _remote_count(dataset):
            return dataset.count()

        result_ref = _remote_count.remote(self._dataset_ref)
        return ray.get(result_ref)

    def take(self, n=20) -> list:
        """Execute take remotely and return result."""

        @ray.remote
        def _remote_take(dataset, num):
            return dataset.take(num)

        result_ref = _remote_take.remote(self._dataset_ref, n)
        return ray.get(result_ref)

    def size_bytes(self) -> int:
        """Execute size_bytes remotely and return result."""

        @ray.remote
        def _remote_size_bytes(dataset):
            return dataset.size_bytes()

        result_ref = _remote_size_bytes.remote(self._dataset_ref)
        return ray.get(result_ref)

    def __getattr__(self, name):
        """Catch any method calls that we haven't explicitly implemented."""
        raise AttributeError(f"RemoteDatasetProxy has no attribute '{name}'")


def is_ray_data(data: Any) -> bool:
    """Check if data is a Ray Dataset or RemoteDatasetProxy."""
    return isinstance(data, (Dataset, RemoteDatasetProxy))


def normalize_timestamp_columns(
    data: Union[pd.DataFrame, Dataset, Any],
    columns: Union[str, List[str]],
    inplace: bool = False,
    exclude_columns: Optional[List[str]] = None,
) -> Union[pd.DataFrame, Dataset, Any]:
    column_list = [columns] if isinstance(columns, str) else columns
    exclude_columns = exclude_columns or []

    def apply_normalization(series: pd.Series) -> pd.Series:
        return (
            pd.to_datetime(series, utc=True, errors="coerce")
            .dt.floor("s")
            .astype("datetime64[ns, UTC]")
        )

    if is_ray_data(data):

        def normalize_batch(batch: pd.DataFrame) -> pd.DataFrame:
            for column in column_list:
                if (
                    not batch.empty
                    and column in batch.columns
                    and column not in exclude_columns
                ):
                    batch[column] = apply_normalization(batch[column])
            return batch

        return data.map_batches(normalize_batch, batch_format="pandas")
    else:
        assert isinstance(data, pd.DataFrame)
        if not inplace:
            data = data.copy()
        for column in column_list:
            if column in data.columns and column not in exclude_columns:
                data[column] = apply_normalization(data[column])
        return data


def ensure_timestamp_compatibility(
    data: Union[pd.DataFrame, Dataset, Any],
    timestamp_fields: List[str],
    inplace: bool = False,
) -> Union[pd.DataFrame, Dataset, Any]:
    from feast.utils import make_df_tzaware

    if is_ray_data(data):

        def ensure_compatibility(batch: pd.DataFrame) -> pd.DataFrame:
            batch = make_df_tzaware(batch)
            for field in timestamp_fields:
                if field in batch.columns:
                    batch[field] = (
                        pd.to_datetime(batch[field], utc=True, errors="coerce")
                        .dt.floor("s")
                        .astype("datetime64[ns, UTC]")
                    )
            return batch

        return data.map_batches(ensure_compatibility, batch_format="pandas")
    else:
        assert isinstance(data, pd.DataFrame)
        if not inplace:
            data = data.copy()
        from feast.utils import make_df_tzaware

        data = make_df_tzaware(data)
        for field in timestamp_fields:
            if field in data.columns:
                data = normalize_timestamp_columns(data, field, inplace=True)
        return data


def apply_field_mapping(
    data: Union[pd.DataFrame, Dataset, Any],
    field_mapping: Dict[str, str],
) -> Union[pd.DataFrame, Dataset, Any]:
    def rename_columns(df: pd.DataFrame) -> pd.DataFrame:
        return df.rename(columns=field_mapping)

    if is_ray_data(data):
        return data.map_batches(rename_columns, batch_format="pandas")
    else:
        assert isinstance(data, pd.DataFrame)
        return data.rename(columns=field_mapping)


def deduplicate_by_keys_and_timestamp(
    data: Union[pd.DataFrame, Dataset, Any],
    join_keys: List[str],
    timestamp_columns: List[str],
) -> Union[pd.DataFrame, Dataset, Any]:
    def deduplicate_batch(batch: pd.DataFrame) -> pd.DataFrame:
        if batch.empty:
            return batch
        sort_columns = join_keys + timestamp_columns
        available_columns = [col for col in sort_columns if col in batch.columns]
        if available_columns:
            sorted_batch = batch.sort_values(
                available_columns,
                ascending=[True] * len(join_keys) + [False] * len(timestamp_columns),
            )
            deduped_batch = sorted_batch.drop_duplicates(
                subset=join_keys,
                keep="first",
            )
            return deduped_batch
        return batch

    if is_ray_data(data):
        return data.map_batches(deduplicate_batch, batch_format="pandas")
    else:
        assert isinstance(data, pd.DataFrame)
        return deduplicate_batch(data)


def broadcast_join(
    entity_ds: Dataset,
    feature_df: pd.DataFrame,
    join_keys: List[str],
    timestamp_field: str,
    requested_feats: List[str],
    full_feature_names: bool = False,
    feature_view_name: Optional[str] = None,
    original_join_keys: Optional[List[str]] = None,
) -> Dataset:
    import ray

    def join_batch_with_features(batch: pd.DataFrame) -> pd.DataFrame:
        features = ray.get(feature_ref)
        if original_join_keys:
            feature_join_keys = original_join_keys
            entity_join_keys = join_keys
        else:
            feature_join_keys = join_keys
            entity_join_keys = join_keys
        feature_cols = [timestamp_field] + feature_join_keys + requested_feats
        available_feature_cols = [
            col for col in feature_cols if col in features.columns
        ]
        features_filtered = features[available_feature_cols].copy()

        batch = normalize_timestamp_columns(batch, timestamp_field, inplace=True)
        features_filtered = normalize_timestamp_columns(
            features_filtered, timestamp_field, inplace=True
        )
        if not entity_join_keys:
            batch_sorted = batch.sort_values(timestamp_field).reset_index(drop=True)
            features_sorted = features_filtered.sort_values(
                timestamp_field
            ).reset_index(drop=True)
            result = pd.merge_asof(
                batch_sorted,
                features_sorted,
                on=timestamp_field,
                direction="backward",
            )
        else:
            for key in entity_join_keys:
                if key not in batch.columns:
                    batch[key] = np.nan
            for key in feature_join_keys:
                if key not in features_filtered.columns:
                    features_filtered[key] = np.nan
            batch_clean = batch.dropna(
                subset=entity_join_keys + [timestamp_field]
            ).copy()
            features_clean = features_filtered.dropna(
                subset=feature_join_keys + [timestamp_field]
            ).copy()
            if batch_clean.empty or features_clean.empty:
                return batch.head(0)
            if timestamp_field in batch_clean.columns:
                batch_sorted = batch_clean.sort_values(
                    timestamp_field, ascending=True
                ).reset_index(drop=True)
            else:
                batch_sorted = batch_clean.reset_index(drop=True)
            right_sort_columns = [
                k for k in feature_join_keys if k in features_clean.columns
            ]
            if timestamp_field in features_clean.columns:
                right_sort_columns.append(timestamp_field)
            if right_sort_columns:
                features_clean = features_clean.drop_duplicates(
                    subset=right_sort_columns, keep="last"
                )
                features_sorted = features_clean.sort_values(
                    right_sort_columns, ascending=True
                ).reset_index(drop=True)
            else:
                features_sorted = features_clean.reset_index(drop=True)
            try:
                if feature_join_keys:
                    batch_dedup_cols = [
                        k for k in entity_join_keys if k in batch_sorted.columns
                    ]
                    if timestamp_field in batch_sorted.columns:
                        batch_dedup_cols.append(timestamp_field)
                    if batch_dedup_cols:
                        batch_sorted = batch_sorted.drop_duplicates(
                            subset=batch_dedup_cols, keep="last"
                        )
                    feature_dedup_cols = [
                        k for k in feature_join_keys if k in features_sorted.columns
                    ]
                    if timestamp_field in features_sorted.columns:
                        feature_dedup_cols.append(timestamp_field)
                    if feature_dedup_cols:
                        features_sorted = features_sorted.drop_duplicates(
                            subset=feature_dedup_cols, keep="last"
                        )
                if feature_join_keys:
                    if entity_join_keys == feature_join_keys:
                        result = pd.merge_asof(
                            batch_sorted,
                            features_sorted,
                            on=timestamp_field,
                            by=entity_join_keys,
                            direction="backward",
                            suffixes=("", "_right"),
                        )
                    else:
                        result = pd.merge_asof(
                            batch_sorted,
                            features_sorted,
                            on=timestamp_field,
                            left_by=entity_join_keys,
                            right_by=feature_join_keys,
                            direction="backward",
                            suffixes=("", "_right"),
                        )
                else:
                    result = pd.merge_asof(
                        batch_sorted,
                        features_sorted,
                        on=timestamp_field,
                        direction="backward",
                        suffixes=("", "_right"),
                    )
            except Exception:
                # fallback to manual join if needed
                result = batch_clean  # fallback logic can be expanded
        if full_feature_names and feature_view_name:
            for feat in requested_feats:
                if feat in result.columns:
                    new_name = f"{feature_view_name}__{feat}"
                    result[new_name] = result[feat]
                    result = result.drop(columns=[feat])
        return result

    feature_ref = ray.put(feature_df)
    return entity_ds.map_batches(join_batch_with_features, batch_format="pandas")


def distributed_windowed_join(
    entity_ds: Dataset,
    feature_ds: Dataset,
    join_keys: List[str],
    timestamp_field: str,
    requested_feats: List[str],
    window_size: Optional[str] = None,
    full_feature_names: bool = False,
    feature_view_name: Optional[str] = None,
    original_join_keys: Optional[List[str]] = None,
) -> Dataset:
    import pandas as pd

    def add_window_and_source(ds, timestamp_field, source_marker, window_size):
        def add_window_and_source_batch(batch: pd.DataFrame) -> pd.DataFrame:
            batch = batch.copy()
            if timestamp_field in batch.columns:
                batch["time_window"] = (
                    pd.to_datetime(batch[timestamp_field])
                    .dt.floor(window_size)
                    .astype("datetime64[ns, UTC]")
                )
            batch["_data_source"] = source_marker
            return batch

        return ds.map_batches(add_window_and_source_batch, batch_format="pandas")

    entity_windowed = add_window_and_source(
        entity_ds, timestamp_field, "entity", window_size or "1H"
    )
    feature_windowed = add_window_and_source(
        feature_ds, timestamp_field, "feature", window_size or "1H"
    )
    combined_ds = entity_windowed.union(feature_windowed)

    def windowed_point_in_time_logic(batch: pd.DataFrame) -> pd.DataFrame:
        if len(batch) == 0:
            return pd.DataFrame()
        result_chunks = []
        group_keys = ["time_window"] + join_keys
        for group_values, group_data in batch.groupby(group_keys):
            entity_data = group_data[group_data["_data_source"] == "entity"].copy()
            feature_data = group_data[group_data["_data_source"] == "feature"].copy()
            if len(entity_data) > 0 and len(feature_data) > 0:
                entity_clean = entity_data.drop(columns=["time_window", "_data_source"])
                feature_clean = feature_data.drop(
                    columns=["time_window", "_data_source"]
                )
                if join_keys:
                    merged = pd.merge_asof(
                        entity_clean.sort_values(join_keys + [timestamp_field]),
                        feature_clean.sort_values(join_keys + [timestamp_field]),
                        on=timestamp_field,
                        by=join_keys,
                        direction="backward",
                    )
                else:
                    merged = pd.merge_asof(
                        entity_clean.sort_values(timestamp_field),
                        feature_clean.sort_values(timestamp_field),
                        on=timestamp_field,
                        direction="backward",
                    )
                result_chunks.append(merged)
            elif len(entity_data) > 0:
                entity_clean = entity_data.drop(columns=["time_window", "_data_source"])
                for feat in requested_feats:
                    if feat not in entity_clean.columns:
                        entity_clean[feat] = np.nan
                result_chunks.append(entity_clean)
        if result_chunks:
            result = pd.concat(result_chunks, ignore_index=True)
            if full_feature_names and feature_view_name:
                for feat in requested_feats:
                    if feat in result.columns:
                        new_name = f"{feature_view_name}__{feat}"
                        result[new_name] = result[feat]
                        result = result.drop(columns=[feat])
            return result
        else:
            return pd.DataFrame()

    return combined_ds.map_batches(windowed_point_in_time_logic, batch_format="pandas")


def _build_required_columns(
    join_key_columns: List[str],
    feature_name_columns: List[str],
    timestamp_columns: List[str],
) -> List[str]:
    """
    Build list of required columns for data processing.
    Args:
        join_key_columns: List of join key columns
        feature_name_columns: List of feature columns
        timestamp_columns: List of timestamp columns
    Returns:
        List of all required columns
    """
    all_required_columns = join_key_columns + feature_name_columns + timestamp_columns
    if not join_key_columns:
        all_required_columns.append("__DUMMY_ENTITY_ID__")
    if "event_timestamp" not in all_required_columns:
        all_required_columns.append("event_timestamp")
    return all_required_columns
