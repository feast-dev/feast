"""MLflow GenAI Dataset as a Feast DataSource (metadata container).

This source is used with a PushSource-backed FeatureView.
The ``feast datasets sync`` command reads from MLflow using this
config and pushes data into Feast stores.
"""

import json
from typing import Callable, Dict, Iterable, List, Optional, Tuple

from typeguard import typechecked

from feast.data_source import DataSource
from feast.protos.feast.core.DataSource_pb2 import DataSource as DataSourceProto
from feast.repo_config import RepoConfig
from feast.value_type import ValueType


@typechecked
class MlflowDatasetSource(DataSource):
    """DataSource backed by an MLflow GenAI EvaluationDataset.

    This source stores connection metadata only. Data ingestion is handled
    by the ``feast datasets sync`` CLI command, which reads from MLflow and
    pushes into Feast via ``write_to_online_store`` / ``push``.
    """

    def source_type(self) -> DataSourceProto.SourceType.ValueType:
        return DataSourceProto.CUSTOM_SOURCE

    def __init__(
        self,
        *,
        name: str,
        dataset_name: str,
        dataset_id: Optional[str] = None,
        experiment_ids: Optional[List[str]] = None,
        tracking_uri: Optional[str] = None,
        field_mapping: Optional[Dict[str, str]] = None,
        timestamp_field: str = "event_timestamp",
        record_id_field: str = "dataset_record_id",
        description: Optional[str] = "",
        tags: Optional[Dict[str, str]] = None,
        owner: Optional[str] = "",
    ):
        """Creates an MlflowDatasetSource object.

        Args:
            name: Unique name for this data source.
            dataset_name: MLflow GenAI dataset name (for ``get_dataset(name=...)``).
            dataset_id: Optional direct dataset ID in MLflow.
            experiment_ids: MLflow experiment IDs where dataset is linked.
            tracking_uri: Override tracking URI (falls back to mlflow.tracking_uri
                in feature_store.yaml or MLFLOW_TRACKING_URI env var).
            field_mapping: Maps MLflow nested paths to flat Feast column names.
            timestamp_field: Name of the event timestamp column after flattening.
            record_id_field: Name of the record ID column (entity key).
            description: Human-readable description.
            tags: Arbitrary metadata key-value pairs.
            owner: Owner of the data source.
        """
        self._mlflow_options = MlflowDatasetOptions(
            dataset_name=dataset_name,
            dataset_id=dataset_id,
            experiment_ids=experiment_ids,
            tracking_uri=tracking_uri,
            record_id_field=record_id_field,
        )

        super().__init__(
            name=name,
            timestamp_field=timestamp_field,
            field_mapping=field_mapping or {},
            description=description,
            tags=tags,
            owner=owner,
        )

    @property
    def dataset_name(self) -> str:
        return self._mlflow_options.dataset_name

    @property
    def dataset_id(self) -> Optional[str]:
        return self._mlflow_options.dataset_id

    @property
    def experiment_ids(self) -> Optional[List[str]]:
        return self._mlflow_options.experiment_ids

    @property
    def tracking_uri(self) -> Optional[str]:
        return self._mlflow_options.tracking_uri

    @property
    def record_id_field(self) -> str:
        return self._mlflow_options.record_id_field

    def __hash__(self):
        return super().__hash__()

    def __eq__(self, other):
        if not isinstance(other, MlflowDatasetSource):
            raise TypeError(
                "Comparisons should only involve MlflowDatasetSource class objects."
            )
        return super().__eq__(other) and self._mlflow_options == other._mlflow_options

    @staticmethod
    def from_proto(data_source: DataSourceProto):
        assert data_source.HasField("custom_options")

        config = json.loads(data_source.custom_options.configuration)

        return MlflowDatasetSource(
            name=data_source.name,
            dataset_name=config["dataset_name"],
            dataset_id=config.get("dataset_id"),
            experiment_ids=config.get("experiment_ids"),
            tracking_uri=config.get("tracking_uri"),
            record_id_field=config.get("record_id_field", "dataset_record_id"),
            field_mapping=dict(data_source.field_mapping),
            timestamp_field=data_source.timestamp_field,
            description=data_source.description,
            tags=dict(data_source.tags),
            owner=data_source.owner,
        )

    def _to_proto_impl(self) -> DataSourceProto:
        data_source_proto = DataSourceProto(
            name=self.name,
            type=DataSourceProto.CUSTOM_SOURCE,
            data_source_class_type=(
                "feast.infra.offline_stores.contrib.mlflow_source.MlflowDatasetSource"
            ),
            field_mapping=self.field_mapping,
            custom_options=self._mlflow_options.to_proto(),
            description=self.description,
            tags=self.tags,
            owner=self.owner,
        )
        data_source_proto.timestamp_field = self.timestamp_field
        return data_source_proto

    def validate(self, config: RepoConfig):
        pass

    @staticmethod
    def source_datatype_to_feast_value_type() -> Callable[[str], ValueType]:
        return lambda x: ValueType.STRING

    def get_table_column_names_and_types(
        self, config: RepoConfig
    ) -> Iterable[Tuple[str, str]]:
        # MLflow datasets have dynamic schemas; return empty.
        # Actual schema is defined by the FeatureView.
        return []

    def get_table_query_string(self) -> str:
        return f"mlflow_dataset:{self._mlflow_options.dataset_name}"


class MlflowDatasetOptions:
    """Stores MLflow-specific connection config for serialization."""

    def __init__(
        self,
        dataset_name: str,
        dataset_id: Optional[str] = None,
        experiment_ids: Optional[List[str]] = None,
        tracking_uri: Optional[str] = None,
        record_id_field: str = "dataset_record_id",
    ):
        self.dataset_name = dataset_name
        self.dataset_id = dataset_id
        self.experiment_ids = experiment_ids
        self.tracking_uri = tracking_uri
        self.record_id_field = record_id_field

    def __eq__(self, other):
        if not isinstance(other, MlflowDatasetOptions):
            return NotImplemented
        return (
            self.dataset_name == other.dataset_name
            and self.dataset_id == other.dataset_id
            and self.experiment_ids == other.experiment_ids
            and self.tracking_uri == other.tracking_uri
            and self.record_id_field == other.record_id_field
        )

    @classmethod
    def from_proto(cls, custom_options_proto: DataSourceProto.CustomSourceOptions):
        config = json.loads(custom_options_proto.configuration.decode("utf8"))
        return cls(
            dataset_name=config["dataset_name"],
            dataset_id=config.get("dataset_id"),
            experiment_ids=config.get("experiment_ids"),
            tracking_uri=config.get("tracking_uri"),
            record_id_field=config.get("record_id_field", "dataset_record_id"),
        )

    def to_proto(self) -> DataSourceProto.CustomSourceOptions:
        config = {
            "dataset_name": self.dataset_name,
            "dataset_id": self.dataset_id,
            "experiment_ids": self.experiment_ids,
            "tracking_uri": self.tracking_uri,
            "record_id_field": self.record_id_field,
        }
        return DataSourceProto.CustomSourceOptions(
            configuration=json.dumps(config).encode()
        )
