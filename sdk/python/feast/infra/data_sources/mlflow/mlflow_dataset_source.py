"""MlflowDatasetSource — dual-mode Feast DataSource for MLflow.

Supports two modes:
  1. **GenAI Dataset mode** (``dataset_name`` / ``dataset_id``):
     Reads from MLflow GenAI EvaluationDatasets via ``get_dataset().to_df()``.
     This is the primary path for the tracing → assessment → training flywheel.

  2. **Artifact mode** (``run_id`` + ``artifact_path``):
     Downloads tabular artifacts (Parquet/CSV) from MLflow runs via
     ``mlflow.artifacts.download_artifacts()``.  General-purpose path for
     arbitrary tabular data tracked in MLflow.

Both modes participate in ``get_historical_features()`` through the DuckDB
offline store reader.  The required ``batch_source`` is used for writeback
(``create_saved_dataset``, ``offline_write_batch``).
"""

from __future__ import annotations

import json
import logging
import tempfile
from typing import Any, Callable, Dict, Iterable, Optional, Tuple

from typeguard import typechecked

from feast.data_source import DataSource
from feast.errors import DataSourceNoNameException
from feast.protos.feast.core.DataSource_pb2 import DataSource as DataSourceProto
from feast.repo_config import RepoConfig
from feast.value_type import ValueType

logger = logging.getLogger(__name__)

_DATA_SOURCE_CLASS_TYPE = (
    "feast.infra.data_sources.mlflow.mlflow_dataset_source.MlflowDatasetSource"
)

_SUPPORTED_ARTIFACT_FORMATS = ("parquet", "csv")


@typechecked
class MlflowDatasetSource(DataSource):
    """Dual-mode Feast DataSource backed by MLflow.

    **GenAI Dataset mode** — set ``dataset_name`` or ``dataset_id``.
    Reads curated trace/assessment records via
    ``mlflow.genai.datasets.get_dataset() → to_df()``.

    **Artifact mode** — set ``run_id`` + ``artifact_path``.
    Downloads Parquet/CSV artifacts from MLflow runs.

    A ``batch_source`` is always required for writeback
    (``create_saved_dataset``, materialization).

    Args:
        name: Unique name of this data source within the project.
        dataset_name: MLflow GenAI dataset name (GenAI mode).
        dataset_id: MLflow GenAI dataset ID (GenAI mode).
        run_id: MLflow run ID containing the artifact (artifact mode).
        artifact_path: Path to the artifact within the run (artifact mode).
        artifact_format: Format of the artifact — ``"parquet"`` or ``"csv"``.
            Defaults to ``"parquet"``.
        batch_source: Batch DataSource for writeback and materialization.
            Required.
        tracking_uri: Optional MLflow tracking URI override.
        timestamp_field: Event timestamp field for point-in-time joins.
        created_timestamp_column: Optional created-at column for dedup.
        field_mapping: Mapping from source columns to Feast feature names.
        description: Human-readable description.
        tags: Arbitrary metadata tags.
        owner: Owner email or identifier.
    """

    batch_source: DataSource
    dataset_name: Optional[str]
    dataset_id: Optional[str]
    run_id: Optional[str]
    artifact_path: Optional[str]
    artifact_format: str
    tracking_uri: Optional[str]

    def __init__(
        self,
        *,
        name: Optional[str] = None,
        dataset_name: Optional[str] = None,
        dataset_id: Optional[str] = None,
        run_id: Optional[str] = None,
        artifact_path: Optional[str] = None,
        artifact_format: str = "parquet",
        batch_source: DataSource,
        tracking_uri: Optional[str] = None,
        timestamp_field: Optional[str] = "",
        created_timestamp_column: Optional[str] = "",
        field_mapping: Optional[Dict[str, str]] = None,
        description: Optional[str] = "",
        tags: Optional[Dict[str, str]] = None,
        owner: Optional[str] = "",
    ):
        has_genai = bool(dataset_name or dataset_id)
        has_artifact = bool(run_id and artifact_path)

        if not has_genai and not has_artifact:
            raise ValueError(
                "MlflowDatasetSource requires either (dataset_name/dataset_id) "
                "for GenAI Dataset mode, or (run_id + artifact_path) for "
                "artifact mode."
            )
        if has_genai and has_artifact:
            raise ValueError(
                "MlflowDatasetSource does not support setting both GenAI "
                "Dataset fields (dataset_name/dataset_id) and artifact fields "
                "(run_id/artifact_path) at the same time.  Use one mode."
            )
        if has_artifact and artifact_format not in _SUPPORTED_ARTIFACT_FORMATS:
            raise ValueError(
                f"Unsupported artifact_format '{artifact_format}'. "
                f"Supported: {_SUPPORTED_ARTIFACT_FORMATS}"
            )
        if name is None:
            raise DataSourceNoNameException()
        if batch_source is None:
            raise ValueError(
                f"A batch_source must be specified for MlflowDatasetSource '{name}'"
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
        self.run_id = run_id
        self.artifact_path = artifact_path
        self.artifact_format = artifact_format
        self.tracking_uri = tracking_uri
        self.batch_source = batch_source

    @property
    def is_genai_mode(self) -> bool:
        """True when this source reads from an MLflow GenAI Dataset."""
        return bool(self.dataset_name or self.dataset_id)

    @property
    def is_artifact_mode(self) -> bool:
        """True when this source reads a raw artifact from an MLflow run."""
        return bool(self.run_id and self.artifact_path)

    def get_effective_tracking_uri(self) -> Optional[str]:
        """Resolve the tracking URI using config → env var fallback."""
        from feast.mlflow_integration.config import resolve_tracking_uri

        return resolve_tracking_uri(self.tracking_uri)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, MlflowDatasetSource):
            return False
        return (
            super().__eq__(other)
            and self.dataset_name == other.dataset_name
            and self.dataset_id == other.dataset_id
            and self.run_id == other.run_id
            and self.artifact_path == other.artifact_path
            and self.artifact_format == other.artifact_format
            and self.tracking_uri == other.tracking_uri
            and self.batch_source == other.batch_source
        )

    def __hash__(self) -> int:
        return super().__hash__()

    def source_type(self) -> DataSourceProto.SourceType.ValueType:
        return DataSourceProto.CUSTOM_SOURCE

    def validate(self, config: RepoConfig) -> None:
        """Validate configuration without contacting MLflow."""
        if not self.is_genai_mode and not self.is_artifact_mode:
            raise ValueError(
                f"MlflowDatasetSource '{self.name}' requires either "
                f"dataset_name/dataset_id or run_id+artifact_path"
            )
        if self.batch_source is None:
            raise ValueError(
                f"MlflowDatasetSource '{self.name}' requires a batch_source"
            )
        if self.is_artifact_mode:
            if self.artifact_format not in _SUPPORTED_ARTIFACT_FORMATS:
                raise ValueError(
                    f"MlflowDatasetSource '{self.name}': unsupported "
                    f"artifact_format '{self.artifact_format}'. "
                    f"Supported: {_SUPPORTED_ARTIFACT_FORMATS}"
                )

    def get_table_column_names_and_types(
        self, config: RepoConfig
    ) -> Iterable[Tuple[str, str]]:
        """Return column names and types.

        In GenAI mode, fetches the dataset and reads the DataFrame schema.
        In artifact mode, delegates to the batch_source for schema info.
        Falls back to batch_source on any MLflow error.
        """
        if self.is_genai_mode:
            try:
                df = self._fetch_genai_dataframe()
                return [(str(col), str(df[col].dtype)) for col in df.columns]
            except Exception:
                logger.debug(
                    "Failed to fetch GenAI dataset schema for '%s', "
                    "falling back to batch_source",
                    self.name,
                    exc_info=True,
                )
        return self.batch_source.get_table_column_names_and_types(config)

    def get_table_query_string(self) -> str:
        return self.batch_source.get_table_query_string()

    @staticmethod
    def source_datatype_to_feast_value_type() -> Callable[[str], ValueType]:
        from feast import type_map

        return type_map.pa_to_feast_value_type

    def to_arrow(self):
        """Read MLflow data and return as a PyArrow Table.

        This is the generic fallback for offline stores that don't have
        native ibis integration.  Any store can call
        ``source.to_arrow()`` to materialize data from MLflow.
        """
        import pyarrow as pa

        from feast.infra.data_sources.mlflow.auth import (
            mlflow_token_scope,
            resolve_mlflow_token,
        )

        tracking_uri = self.get_effective_tracking_uri()
        token = resolve_mlflow_token()

        if self.is_genai_mode:
            try:
                import mlflow
                import mlflow.genai.datasets
            except ImportError as e:
                raise ImportError(
                    "Install feast[mlflow] to use MlflowDatasetSource "
                    "in GenAI Dataset mode."
                ) from e

            if tracking_uri:
                mlflow.set_tracking_uri(tracking_uri)

            with mlflow_token_scope(token):
                name = self.dataset_name or self.dataset_id
                dataset = mlflow.genai.datasets.get_dataset(name=name)
                df = dataset.to_df()

            try:
                from feast.mlflow_integration.dataset_sync import (
                    flatten_mlflow_dataset_df,
                )

                df = flatten_mlflow_dataset_df(
                    df, field_mapping=self.field_mapping or None
                )
            except ImportError:
                pass

            return pa.Table.from_pandas(df)
        else:
            try:
                import mlflow
            except ImportError as e:
                raise ImportError(
                    "Install feast[mlflow] to use MlflowDatasetSource in artifact mode."
                ) from e

            if tracking_uri:
                mlflow.set_tracking_uri(tracking_uri)

            with mlflow_token_scope(token):
                with tempfile.TemporaryDirectory(prefix="feast_mlflow_") as tmpdir:
                    local_path = mlflow.artifacts.download_artifacts(
                        run_id=self.run_id,
                        artifact_path=self.artifact_path,
                        dst_path=tmpdir,
                    )
                    if self.artifact_format == "parquet":
                        import pyarrow.parquet as pq

                        return pq.read_table(local_path)
                    elif self.artifact_format == "csv":
                        import pyarrow.csv as csv

                        return csv.read_csv(local_path)
                    else:
                        raise ValueError(
                            f"Unsupported artifact format: {self.artifact_format}"
                        )

    def _fetch_genai_dataframe(self):
        """Fetch GenAI dataset as a pandas DataFrame."""
        try:
            import mlflow.genai.datasets
        except ImportError as e:
            raise ImportError(
                "Install feast[mlflow] to use MlflowDatasetSource "
                "in GenAI Dataset mode."
            ) from e

        tracking_uri = self.get_effective_tracking_uri()
        if tracking_uri:
            import mlflow

            mlflow.set_tracking_uri(tracking_uri)

        name = self.dataset_name or self.dataset_id
        dataset = mlflow.genai.datasets.get_dataset(name=name)
        return dataset.to_df()

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
            run_id=options.get("run_id"),
            artifact_path=options.get("artifact_path"),
            artifact_format=options.get("artifact_format", "parquet"),
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
            "run_id": self.run_id,
            "artifact_path": self.artifact_path,
            "artifact_format": self.artifact_format,
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
