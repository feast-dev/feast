"""MlflowDatasetSource — first-class Feast DataSource for MLflow GenAI datasets.

Follows the PushSource pattern: the source is a pull/ingest handle that
requires a nested ``batch_source`` for historical reads and materialization.
Sync uses MLflow GenAI EvaluationDataset APIs; Feast does not introduce an
MLflow OfflineStore.
"""

from __future__ import annotations

import json
from typing import Any, Callable, Dict, Iterable, Optional, Tuple

from typeguard import typechecked

from feast.data_source import DataSource
from feast.errors import DataSourceNoNameException
from feast.protos.feast.core.DataSource_pb2 import DataSource as DataSourceProto
from feast.repo_config import RepoConfig
from feast.value_type import ValueType

_DATA_SOURCE_CLASS_TYPE = (
    "feast.infra.data_sources.mlflow.mlflow_dataset_source.MlflowDatasetSource"
)


@typechecked
class MlflowDatasetSource(DataSource):
    """References an MLflow GenAI EvaluationDataset for sync into Feast.

    Used as ``FeatureView.source`` or ``LabelView.source``.  Sync
    (``feast mlflow sync-dataset``) pulls records via
    ``mlflow.genai.datasets.get_dataset`` → ``to_df()``, then writes to the
    online store and the nested ``batch_source`` offline path.

    Args:
        name: Unique name of this data source within the project.
        dataset_name: MLflow GenAI dataset name (mutually exclusive with
            ``dataset_id`` when both are provided — ``dataset_id`` wins).
        dataset_id: MLflow GenAI dataset ID.
        batch_source: Batch DataSource used for historical retrieval and
            offline writes after sync. Required.
        tracking_uri: Optional MLflow tracking URI override. When unset,
            falls back to ``feature_store.yaml`` ``mlflow.tracking_uri`` or
            ``MLFLOW_TRACKING_URI``.
        timestamp_field: Event timestamp field for point-in-time joins.
        created_timestamp_column: Optional created-at column for dedup.
        field_mapping: Mapping from flattened MLflow columns (or
            dot-delimited paths) to Feast feature column names.
        description: Human-readable description.
        tags: Arbitrary metadata tags.
        owner: Owner email or identifier.
    """

    batch_source: DataSource
    dataset_name: Optional[str]
    dataset_id: Optional[str]
    tracking_uri: Optional[str]

    def __init__(
        self,
        *,
        name: Optional[str] = None,
        dataset_name: Optional[str] = None,
        dataset_id: Optional[str] = None,
        batch_source: DataSource,
        tracking_uri: Optional[str] = None,
        timestamp_field: Optional[str] = "",
        created_timestamp_column: Optional[str] = "",
        field_mapping: Optional[Dict[str, str]] = None,
        description: Optional[str] = "",
        tags: Optional[Dict[str, str]] = None,
        owner: Optional[str] = "",
    ):
        if not dataset_name and not dataset_id:
            raise ValueError("MlflowDatasetSource requires dataset_name or dataset_id")
        if name is None:
            raise DataSourceNoNameException()
        if batch_source is None:
            raise ValueError(
                f"A batch_source needs to be specified for MlflowDatasetSource `{name}`"
            )

        super().__init__(
            name=name,
            timestamp_field=timestamp_field
            or getattr(batch_source, "timestamp_field", "")
            or "",
            created_timestamp_column=created_timestamp_column
            or getattr(batch_source, "created_timestamp_column", "")
            or "",
            field_mapping=field_mapping,
            description=description,
            tags=tags,
            owner=owner,
        )
        self.dataset_name = dataset_name
        self.dataset_id = dataset_id
        self.tracking_uri = tracking_uri
        self.batch_source = batch_source

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, MlflowDatasetSource):
            return False
        return (
            super().__eq__(other)
            and self.dataset_name == other.dataset_name
            and self.dataset_id == other.dataset_id
            and self.tracking_uri == other.tracking_uri
            and self.batch_source == other.batch_source
        )

    def __hash__(self) -> int:
        return super().__hash__()

    def source_type(self) -> DataSourceProto.SourceType.ValueType:
        return DataSourceProto.CUSTOM_SOURCE

    def validate(self, config: RepoConfig) -> None:
        """Soft validation — config/schema only; does not contact MLflow."""
        if not self.dataset_name and not self.dataset_id:
            raise ValueError(
                f"MlflowDatasetSource '{self.name}' needs dataset_name or dataset_id"
            )
        if self.batch_source is None:
            raise ValueError(
                f"MlflowDatasetSource '{self.name}' requires a batch_source"
            )

    def get_table_column_names_and_types(
        self, config: RepoConfig
    ) -> Iterable[Tuple[str, str]]:
        return self.batch_source.get_table_column_names_and_types(config)

    def get_table_query_string(self) -> str:
        return self.batch_source.get_table_query_string()

    @staticmethod
    def source_datatype_to_feast_value_type() -> Callable[[str], ValueType]:
        from feast import type_map

        return type_map.pa_to_feast_value_type

    @staticmethod
    def from_proto(data_source: DataSourceProto) -> "MlflowDatasetSource":
        assert data_source.HasField("custom_options")
        options = json.loads(data_source.custom_options.configuration.decode("utf8"))
        batch_source = (
            DataSource.from_proto(data_source.batch_source)
            if data_source.HasField("batch_source")
            else None
        )
        if batch_source is None:
            raise ValueError(
                "MlflowDatasetSource proto is missing required batch_source"
            )
        return MlflowDatasetSource(
            name=data_source.name,
            dataset_name=options.get("dataset_name"),
            dataset_id=options.get("dataset_id"),
            tracking_uri=options.get("tracking_uri"),
            batch_source=batch_source,
            timestamp_field=data_source.timestamp_field,
            created_timestamp_column=data_source.created_timestamp_column,
            field_mapping=dict(data_source.field_mapping),
            description=data_source.description,
            tags=dict(data_source.tags),
            owner=data_source.owner,
        )

    def _to_proto_impl(self) -> DataSourceProto:
        config: Dict[str, Any] = {
            "dataset_name": self.dataset_name,
            "dataset_id": self.dataset_id,
            "tracking_uri": self.tracking_uri,
        }
        data_source_proto = DataSourceProto(
            name=self.name,
            type=DataSourceProto.CUSTOM_SOURCE,
            data_source_class_type=_DATA_SOURCE_CLASS_TYPE,
            field_mapping=self.field_mapping,
            description=self.description,
            tags=self.tags,
            owner=self.owner,
            custom_options=DataSourceProto.CustomSourceOptions(
                configuration=json.dumps(config).encode("utf8")
            ),
        )
        data_source_proto.timestamp_field = self.timestamp_field
        data_source_proto.created_timestamp_column = self.created_timestamp_column
        data_source_proto.batch_source.MergeFrom(self.batch_source.to_proto())
        return data_source_proto
