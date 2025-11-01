import logging
from datetime import datetime, timedelta, timezone
from typing import List, Optional, Union

import dill
import pandas as pd
import pyarrow as pa
import ray
from ray.data import Dataset

from feast import BatchFeatureView, FeatureView, StreamFeatureView
from feast.aggregation import Aggregation
from feast.data_source import DataSource
from feast.feature_view_utils import get_transformation_function, has_transformation
from feast.infra.common.serde import SerializedArtifacts
from feast.infra.compute_engines.dag.context import ExecutionContext
from feast.infra.compute_engines.dag.model import DAGFormat
from feast.infra.compute_engines.dag.node import DAGNode
from feast.infra.compute_engines.dag.value import DAGValue
from feast.infra.compute_engines.ray.config import RayComputeEngineConfig
from feast.infra.compute_engines.ray.utils import (
    safe_batch_processor,
    write_to_online_store,
)
from feast.infra.compute_engines.utils import create_offline_store_retrieval_job
from feast.infra.ray_initializer import get_ray_wrapper
from feast.infra.ray_shared_utils import (
    apply_field_mapping,
    broadcast_join,
    distributed_windowed_join,
)

logger = logging.getLogger(__name__)

# Entity timestamp alias for historical feature retrieval
ENTITY_TS_ALIAS = "__entity_event_timestamp"


class RayReadNode(DAGNode):
    """
    Ray node for reading data from offline stores.
    """

    def __init__(
        self,
        name: str,
        source: DataSource,
        column_info,
        config: RayComputeEngineConfig,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
    ):
        super().__init__(name)
        self.source = source
        self.column_info = column_info
        self.config = config
        self.start_time = start_time
        self.end_time = end_time

    def execute(self, context: ExecutionContext) -> DAGValue:
        """Execute the read operation to load data from the offline store."""
        try:
            retrieval_job = create_offline_store_retrieval_job(
                data_source=self.source,
                column_info=self.column_info,
                context=context,
                start_time=self.start_time,
                end_time=self.end_time,
            )

            if hasattr(retrieval_job, "to_ray_dataset"):
                ray_dataset = retrieval_job.to_ray_dataset()
            else:
                try:
                    arrow_table = retrieval_job.to_arrow()
                    ray_wrapper = get_ray_wrapper()
                    ray_dataset = ray_wrapper.from_arrow(arrow_table)
                except Exception:
                    df = retrieval_job.to_df()
                    ray_wrapper = get_ray_wrapper()
                    ray_dataset = ray_wrapper.from_pandas(df)

            field_mapping = getattr(self.source, "field_mapping", None)
            if field_mapping:
                ray_dataset = apply_field_mapping(ray_dataset, field_mapping)

            return DAGValue(
                data=ray_dataset,
                format=DAGFormat.RAY,
                metadata={
                    "source": "offline_store",
                    "source_type": type(self.source).__name__,
                    "start_time": self.start_time,
                    "end_time": self.end_time,
                },
            )

        except Exception as e:
            logger.error(f"Ray read node failed: {e}")
            raise


class RayJoinNode(DAGNode):
    """
    Ray node for joining entity dataframes with feature data.
    """

    def __init__(
        self,
        name: str,
        column_info,
        config: RayComputeEngineConfig,
        is_historical_retrieval: bool = False,
    ):
        super().__init__(name)
        self.column_info = column_info
        self.config = config
        self.is_historical_retrieval = is_historical_retrieval

    def execute(self, context: ExecutionContext) -> DAGValue:
        """Execute the join operation."""
        input_value = self.get_single_input_value(context)
        input_value.assert_format(DAGFormat.RAY)
        feature_dataset: Dataset = input_value.data

        # If this is not a historical retrieval, just return the feature data
        if not self.is_historical_retrieval or context.entity_df is None:
            return DAGValue(
                data=feature_dataset,
                format=DAGFormat.RAY,
                metadata={"joined": False},
            )

        entity_df = context.entity_df
        if isinstance(entity_df, pd.DataFrame):
            ray_wrapper = get_ray_wrapper()
            entity_dataset = ray_wrapper.from_pandas(entity_df)
        else:
            entity_dataset = entity_df

        join_keys = self.column_info.join_keys
        timestamp_col = self.column_info.timestamp_column
        requested_feats = getattr(self.column_info, "feature_cols", [])

        # Check if the feature dataset contains aggregated features (from aggregation node)
        # If so, we don't need point-in-time join logic - just simple join on entity keys
        is_aggregated = (
            input_value.metadata.get("aggregated", False)
            if input_value.metadata
            else False
        )

        feature_size = feature_dataset.size_bytes()

        if is_aggregated:
            # For aggregated features, do simple join on entity keys
            feature_df = feature_dataset.to_pandas()
            feature_ref = ray.put(feature_df)

            @safe_batch_processor
            def join_with_aggregated_features(batch: pd.DataFrame) -> pd.DataFrame:
                features = ray.get(feature_ref)
                if join_keys:
                    result = pd.merge(
                        batch,
                        features,
                        on=join_keys,
                        how="left",
                        suffixes=("", "_feature"),
                    )
                else:
                    result = batch.copy()
                return result

            joined_dataset = entity_dataset.map_batches(
                join_with_aggregated_features,
                batch_format="pandas",
                concurrency=self.config.max_workers or 12,
            )
        else:
            if feature_size <= self.config.broadcast_join_threshold_mb * 1024 * 1024:
                # Use broadcast join for small feature datasets
                joined_dataset = broadcast_join(
                    entity_dataset,
                    feature_dataset.to_pandas(),
                    join_keys,
                    timestamp_col,
                    requested_feats,
                )
            else:
                # Use distributed join for large datasets
                joined_dataset = distributed_windowed_join(
                    entity_dataset,
                    feature_dataset,
                    join_keys,
                    timestamp_col,
                    requested_feats,
                )

        return DAGValue(
            data=joined_dataset,
            format=DAGFormat.RAY,
            metadata={
                "joined": True,
                "join_keys": join_keys,
                "join_strategy": "broadcast"
                if feature_size <= self.config.broadcast_join_threshold_mb * 1024 * 1024
                else "distributed",
            },
        )


class RayFilterNode(DAGNode):
    """
    Ray node for filtering data based on TTL and custom conditions.
    """

    def __init__(
        self,
        name: str,
        column_info,
        config: RayComputeEngineConfig,
        ttl: Optional[timedelta] = None,
        filter_condition: Optional[str] = None,
    ):
        super().__init__(name)
        self.column_info = column_info
        self.config = config
        self.ttl = ttl
        self.filter_condition = filter_condition

    def execute(self, context: ExecutionContext) -> DAGValue:
        """Execute the filter operation."""
        input_value = self.get_single_input_value(context)
        input_value.assert_format(DAGFormat.RAY)
        dataset: Dataset = input_value.data

        @safe_batch_processor
        def apply_filters(batch: pd.DataFrame) -> pd.DataFrame:
            """Apply TTL and custom filters to the batch."""

            filtered_batch = batch.copy()

            # Apply TTL filter if specified
            if self.ttl:
                timestamp_col = self.column_info.timestamp_column
                if timestamp_col in filtered_batch.columns:
                    # Convert to datetime if not already
                    if not pd.api.types.is_datetime64_any_dtype(
                        filtered_batch[timestamp_col]
                    ):
                        filtered_batch[timestamp_col] = pd.to_datetime(
                            filtered_batch[timestamp_col]
                        )

                    # For historical retrieval, use entity timestamp for TTL calculation
                    if ENTITY_TS_ALIAS in filtered_batch.columns:
                        # Use entity timestamp for TTL calculation (historical retrieval)
                        if not pd.api.types.is_datetime64_any_dtype(
                            filtered_batch[ENTITY_TS_ALIAS]
                        ):
                            filtered_batch[ENTITY_TS_ALIAS] = pd.to_datetime(
                                filtered_batch[ENTITY_TS_ALIAS]
                            )

                        # Apply TTL filter with both upper and lower bounds:
                        # 1. feature.ts <= entity.event_timestamp (upper bound)
                        # 2. feature.ts >= entity.event_timestamp - ttl (lower bound)
                        upper_bound = filtered_batch[ENTITY_TS_ALIAS]
                        lower_bound = filtered_batch[ENTITY_TS_ALIAS] - self.ttl

                        filtered_batch = filtered_batch[
                            (filtered_batch[timestamp_col] <= upper_bound)
                            & (filtered_batch[timestamp_col] >= lower_bound)
                        ]
                    else:
                        # Use current time for TTL calculation (real-time retrieval)
                        # Check if timestamp column is timezone-aware
                        if isinstance(
                            filtered_batch[timestamp_col].dtype, pd.DatetimeTZDtype
                        ):
                            # Use timezone-aware current time
                            current_time = datetime.now(timezone.utc)
                        else:
                            # Use naive datetime
                            current_time = datetime.now()

                        ttl_threshold = current_time - self.ttl

                        # Apply TTL filter
                        filtered_batch = filtered_batch[
                            filtered_batch[timestamp_col] >= ttl_threshold
                        ]

            # Apply custom filter condition if specified
            if self.filter_condition:
                try:
                    filtered_batch = filtered_batch.query(self.filter_condition)
                except Exception as e:
                    logger.warning(f"Custom filter failed: {e}")

            return filtered_batch

        filtered_dataset = dataset.map_batches(apply_filters, batch_format="pandas")

        return DAGValue(
            data=filtered_dataset,
            format=DAGFormat.RAY,
            metadata={
                "filtered": True,
                "ttl": self.ttl,
                "filter_condition": self.filter_condition,
            },
        )


class RayAggregationNode(DAGNode):
    """
    Ray node for performing aggregations on feature data.
    """

    def __init__(
        self,
        name: str,
        aggregations: List[Aggregation],
        group_by_keys: List[str],
        timestamp_col: str,
        config: RayComputeEngineConfig,
    ):
        super().__init__(name)
        self.aggregations = aggregations
        self.group_by_keys = group_by_keys
        self.timestamp_col = timestamp_col
        self.config = config

    def execute(self, context: ExecutionContext) -> DAGValue:
        """Execute the aggregation operation."""
        input_value = self.get_single_input_value(context)
        input_value.assert_format(DAGFormat.RAY)
        dataset: Dataset = input_value.data

        # Convert aggregations to Ray's groupby format
        agg_dict = {}
        for agg in self.aggregations:
            feature_name = f"{agg.function}_{agg.column}"
            if agg.time_window:
                feature_name += f"_{int(agg.time_window.total_seconds())}s"

            if agg.function == "count":
                agg_dict[feature_name] = (agg.column, "count")
            elif agg.function == "sum":
                agg_dict[feature_name] = (agg.column, "sum")
            elif agg.function == "mean" or agg.function == "avg":
                agg_dict[feature_name] = (agg.column, "mean")
            elif agg.function == "min":
                agg_dict[feature_name] = (agg.column, "min")
            elif agg.function == "max":
                agg_dict[feature_name] = (agg.column, "max")
            elif agg.function == "std":
                agg_dict[feature_name] = (agg.column, "std")
            elif agg.function == "var":
                agg_dict[feature_name] = (agg.column, "var")
            else:
                raise ValueError(f"Unknown aggregation function: {agg.function}.")

        # Apply aggregations using pandas fallback (Ray's native groupby has compatibility issues)
        if self.group_by_keys and agg_dict:
            # Use pandas-based aggregation for entire dataset
            aggregated_dataset = self._fallback_pandas_aggregation(dataset, agg_dict)
        else:
            # No group keys or aggregations, return original dataset
            aggregated_dataset = dataset

        return DAGValue(
            data=aggregated_dataset,
            format=DAGFormat.RAY,
            metadata={
                "aggregated": True,
                "aggregations": len(self.aggregations),
                "group_by_keys": self.group_by_keys,
            },
        )

    def _fallback_pandas_aggregation(self, dataset: Dataset, agg_dict: dict) -> Dataset:
        """Fallback to pandas-based aggregation for the entire dataset."""
        # Convert entire dataset to pandas for aggregation
        df = dataset.to_pandas()

        if df.empty:
            return dataset

        # Group by the specified keys
        if self.group_by_keys:
            grouped = df.groupby(self.group_by_keys)
        else:
            # If no group keys, apply aggregations to entire dataset
            grouped = df.groupby(lambda x: 0)  # Dummy grouping

        # Apply each aggregation
        agg_results = []
        for feature_name, (column, function) in agg_dict.items():
            if column in df.columns:
                if function == "count":
                    result = grouped[column].count()
                elif function == "sum":
                    result = grouped[column].sum()
                elif function == "mean":
                    result = grouped[column].mean()
                elif function == "min":
                    result = grouped[column].min()
                elif function == "max":
                    result = grouped[column].max()
                elif function == "std":
                    result = grouped[column].std()
                elif function == "var":
                    result = grouped[column].var()
                else:
                    raise ValueError(f"Unknown aggregation function: {function}.")

                result.name = feature_name
                agg_results.append(result)

        # Combine aggregation results
        if agg_results:
            result_df = pd.concat(agg_results, axis=1)

            # Reset index to make group keys regular columns
            if self.group_by_keys:
                result_df = result_df.reset_index()

            # Convert back to Ray Dataset
            ray_wrapper = get_ray_wrapper()
            return ray_wrapper.from_pandas(result_df)
        else:
            return dataset


class RayDedupNode(DAGNode):
    """
    Ray node for deduplicating records.
    """

    def __init__(
        self,
        name: str,
        column_info,
        config: RayComputeEngineConfig,
    ):
        super().__init__(name)
        self.column_info = column_info
        self.config = config

    def execute(self, context: ExecutionContext) -> DAGValue:
        """Execute the deduplication operation."""
        input_value = self.get_single_input_value(context)
        input_value.assert_format(DAGFormat.RAY)
        dataset: Dataset = input_value.data

        @safe_batch_processor
        def deduplicate_batch(batch: pd.DataFrame) -> pd.DataFrame:
            """Remove duplicates from the batch."""
            # Get deduplication keys
            join_keys = self.column_info.join_keys
            timestamp_col = self.column_info.timestamp_column

            if join_keys:
                # Sort by join keys and timestamp (most recent first)
                sort_columns = join_keys + [timestamp_col]
                available_columns = [
                    col for col in sort_columns if col in batch.columns
                ]

                if available_columns:
                    # Sort and deduplicate
                    sorted_batch = batch.sort_values(
                        available_columns,
                        ascending=[True] * len(join_keys)
                        + [False],  # Recent timestamps first
                    )

                    # Keep first occurrence (most recent) for each join key combination
                    deduped_batch = sorted_batch.drop_duplicates(
                        subset=join_keys,
                        keep="first",
                    )

                    return deduped_batch

            return batch

        deduped_dataset = dataset.map_batches(deduplicate_batch, batch_format="pandas")

        return DAGValue(
            data=deduped_dataset,
            format=DAGFormat.RAY,
            metadata={"deduped": True},
        )


class RayTransformationNode(DAGNode):
    """
    Ray node for applying feature transformations.
    """

    def __init__(
        self,
        name: str,
        transformation,
        config: RayComputeEngineConfig,
    ):
        super().__init__(name)
        self.transformation = transformation
        self.transformation_name = getattr(transformation, "name", "unknown")
        self.config = config

    def execute(self, context: ExecutionContext) -> DAGValue:
        """Execute the transformation operation."""
        input_value = self.get_single_input_value(context)
        input_value.assert_format(DAGFormat.RAY)
        dataset: Dataset = input_value.data

        # Check transformation mode
        from feast.transformation.mode import TransformationMode

        transformation_mode = getattr(
            self.transformation, "mode", TransformationMode.PYTHON
        )
        is_ray_native = transformation_mode in (TransformationMode.RAY, "ray")
        if is_ray_native:
            transformation_func = None
            if hasattr(self.transformation, "udf") and callable(
                self.transformation.udf
            ):
                transformation_func = self.transformation.udf
            elif callable(self.transformation):
                transformation_func = self.transformation

            if transformation_func:
                transformed_dataset = transformation_func(dataset)
            else:
                logger.warning(
                    "No transformation function available in RAY mode, returning original dataset"
                )
                transformed_dataset = dataset
        else:
            transformation_serialized = None
            if hasattr(self.transformation, "udf") and callable(
                self.transformation.udf
            ):
                transformation_serialized = dill.dumps(self.transformation.udf)
            elif callable(self.transformation):
                transformation_serialized = dill.dumps(self.transformation)

            @safe_batch_processor
            def apply_transformation_with_serialized_udf(
                batch: pd.DataFrame,
            ) -> pd.DataFrame:
                """Apply the transformation using pre-serialized UDF."""
                if transformation_serialized:
                    transformation_func = dill.loads(transformation_serialized)
                    transformed_batch = transformation_func(batch)
                else:
                    logger.warning(
                        "No serialized transformation available, returning original batch"
                    )
                    transformed_batch = batch

                return transformed_batch

            transformed_dataset = dataset.map_batches(
                apply_transformation_with_serialized_udf,
                batch_format="pandas",
                concurrency=self.config.max_workers or 12,
            )

        return DAGValue(
            data=transformed_dataset,
            format=DAGFormat.RAY,
            metadata={
                "transformed": True,
                "transformation": self.transformation_name,
            },
        )


class RayDerivedReadNode(DAGNode):
    """
    Ray node for reading derived feature views after parent dependencies are materialized.
    This node ensures that parent feature views are fully materialized before reading from their sink_source.
    """

    def __init__(
        self,
        name: str,
        feature_view: FeatureView,
        parent_dependencies: List[DAGNode],
        config: RayComputeEngineConfig,
        column_info,
        is_materialization: bool = True,
    ):
        super().__init__(name)
        self.feature_view = feature_view
        self.config = config
        self.column_info = column_info
        self.is_materialization = is_materialization

        # Add parent dependencies to ensure they execute first
        for parent in parent_dependencies:
            self.add_input(parent)

    def execute(self, context: ExecutionContext) -> DAGValue:
        """Execute the derived read operation after parents are materialized."""
        parent_values = self.get_input_values(context)

        if not parent_values:
            raise ValueError(
                f"No parent data available for derived view {self.feature_view.name}"
            )

        parent_value = parent_values[0]
        parent_value.assert_format(DAGFormat.RAY)

        if has_transformation(self.feature_view):
            transformation_func = get_transformation_function(self.feature_view)
            if callable(transformation_func):

                def apply_transformation(batch: pd.DataFrame) -> pd.DataFrame:
                    return transformation_func(batch)

                transformed_dataset = parent_value.data.map_batches(
                    apply_transformation,
                    batch_format="pandas",
                    concurrency=self.config.max_workers or 12,
                )
                return DAGValue(
                    data=transformed_dataset,
                    format=DAGFormat.RAY,
                    metadata={
                        "source": "derived_from_parent",
                        "source_description": f"Transformed data from parent for {self.feature_view.name}",
                    },
                )

        return DAGValue(
            data=parent_value.data,
            format=DAGFormat.RAY,
            metadata={
                "source": "derived_from_parent",
                "source_description": f"Data from parent for {self.feature_view.name}",
            },
        )


class RayWriteNode(DAGNode):
    """
    Ray node for writing results to online/offline stores and sink_source paths.
    This node handles writing intermediate results for derived feature views.
    """

    def __init__(
        self,
        name: str,
        feature_view: Union[BatchFeatureView, StreamFeatureView, FeatureView],
        inputs=None,
        config: Optional[RayComputeEngineConfig] = None,
    ):
        super().__init__(name, inputs=inputs)
        self.feature_view = feature_view
        self.config = config

    def execute(self, context: ExecutionContext) -> DAGValue:
        """Execute the write operation."""
        input_value = self.get_single_input_value(context)
        input_value.assert_format(DAGFormat.RAY)
        dataset: Dataset = input_value.data

        serialized_artifacts = SerializedArtifacts.serialize(
            feature_view=self.feature_view, repo_config=context.repo_config
        )

        @safe_batch_processor
        def write_batch_with_serialized_artifacts(batch: pd.DataFrame) -> pd.DataFrame:
            """Write each batch using pre-serialized artifacts."""
            (
                feature_view,
                online_store,
                offline_store,
                repo_config,
            ) = serialized_artifacts.unserialize()

            arrow_table = pa.Table.from_pandas(batch)

            # Write to online store if enabled
            write_to_online_store(
                arrow_table=arrow_table,
                feature_view=feature_view,
                online_store=online_store,
                repo_config=repo_config,
            )

            # Write to offline store if enabled
            if getattr(feature_view, "offline", False):
                offline_store.offline_write_batch(
                    config=repo_config,
                    feature_view=feature_view,
                    table=arrow_table,
                    progress=lambda x: None,
                )

            return batch

        written_dataset = dataset.map_batches(
            write_batch_with_serialized_artifacts,
            batch_format="pandas",
            concurrency=self.config.max_workers if self.config else 12,
        )
        written_dataset = written_dataset.materialize()

        return DAGValue(
            data=written_dataset,
            format=DAGFormat.RAY,
            metadata={
                "written": True,
                "feature_view": self.feature_view.name,
                "online": getattr(self.feature_view, "online", False),
                "offline": getattr(self.feature_view, "offline", False),
                "batch_source_path": getattr(
                    getattr(self.feature_view, "batch_source", None), "path", "unknown"
                ),
            },
        )
