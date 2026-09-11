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
import threading
import time
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple

import pyarrow as pa
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

_SCHEMA_CACHE_TTL_SECONDS = 300
_ARROW_CACHE_TTL_SECONDS = 60


class _CacheEntry:
    """Thread-safe TTL cache entry for MLflow data."""

    __slots__ = ("_data", "_timestamp", "_ttl", "_lock")

    def __init__(self, ttl: float):
        self._data: Any = None
        self._timestamp: float = 0.0
        self._ttl = ttl
        self._lock = threading.Lock()

    def get(self) -> Optional[Any]:
        if self._data is not None and (time.monotonic() - self._timestamp) < self._ttl:
            return self._data
        return None

    def set(self, data: Any) -> None:
        with self._lock:
            self._data = data
            self._timestamp = time.monotonic()

    def invalidate(self) -> None:
        with self._lock:
            self._data = None
            self._timestamp = 0.0


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

        self._schema_cache = _CacheEntry(ttl=_SCHEMA_CACHE_TTL_SECONDS)
        self._arrow_cache = _CacheEntry(ttl=_ARROW_CACHE_TTL_SECONDS)

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

    def invalidate_cache(self) -> None:
        """Explicitly invalidate cached schema and data.

        Call after sync operations or when the underlying MLflow dataset
        has been updated.
        """
        self._schema_cache.invalidate()
        self._arrow_cache.invalidate()

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
        """Return column names and types without fetching the full dataset.

        In GenAI mode, uses MLflow's ``dataset.schema`` metadata property
        which is resolved without loading records. Falls back to a minimal
        record fetch if schema metadata is unavailable.

        In artifact mode, delegates to the batch_source for schema info.
        """
        cached = self._schema_cache.get()
        if cached is not None:
            return cached

        if self.is_genai_mode:
            result = self._introspect_genai_schema()
            if result is not None:
                self._schema_cache.set(result)
                return result

        result = list(self.batch_source.get_table_column_names_and_types(config))
        self._schema_cache.set(result)
        return result

    def _introspect_genai_schema(self) -> Optional[List[Tuple[str, str]]]:
        """Get schema from GenAI dataset metadata without full data fetch."""
        try:
            import mlflow.genai.datasets
        except ImportError:
            return None

        try:
            from feast.infra.data_sources.mlflow.auth import (
                mlflow_request_scope,
                resolve_mlflow_token,
            )

            tracking_uri = self.get_effective_tracking_uri()
            token = resolve_mlflow_token()

            with mlflow_request_scope(token, tracking_uri):
                name = self.dataset_name or self.dataset_id
                dataset = mlflow.genai.datasets.get_dataset(name=name)

                schema_json = dataset.schema
                if schema_json:
                    schema_data = json.loads(schema_json)
                    if isinstance(schema_data, list):
                        return [
                            (col.get("name", ""), col.get("type", "object"))
                            for col in schema_data
                            if "name" in col
                        ]
                    elif isinstance(schema_data, dict):
                        return [
                            (col_name, dtype) for col_name, dtype in schema_data.items()
                        ]

                df = dataset.to_df().head(1)

            try:
                from feast.mlflow_integration.dataset_sync import (
                    flatten_mlflow_dataset_df,
                )

                df = flatten_mlflow_dataset_df(
                    df, field_mapping=self.field_mapping or None
                )
            except ImportError:
                pass

            return [(str(col), str(df[col].dtype)) for col in df.columns]
        except Exception:
            logger.debug(
                "Failed to fetch GenAI dataset schema for '%s', "
                "falling back to batch_source",
                self.name,
                exc_info=True,
            )
            return None

    def get_table_query_string(self) -> str:
        return self.batch_source.get_table_query_string()

    @staticmethod
    def source_datatype_to_feast_value_type() -> Callable[[str], ValueType]:
        from feast import type_map

        return type_map.pa_to_feast_value_type

    def to_arrow(self, use_cache: bool = True) -> pa.Table:
        """Read MLflow data and return as a PyArrow Table.

        Uses a short-lived TTL cache to avoid redundant downloads within
        the same request cycle.  Pass ``use_cache=False`` to force a fresh
        fetch.

        This is the generic interface for offline stores to materialize
        data from MLflow.
        """
        if use_cache:
            cached = self._arrow_cache.get()
            if cached is not None:
                return cached

        table = self._fetch_arrow()
        self._arrow_cache.set(table)
        return table

    def _fetch_arrow(self) -> pa.Table:
        """Internal: download data from MLflow with retry and error classification."""
        import time

        from feast.infra.data_sources.mlflow.auth import (
            MlflowArtifactNotFoundError,
            MlflowAuthError,
            classify_mlflow_error,
            is_retryable_error,
            resolve_mlflow_token,
        )
        from feast.mlflow_integration.config import (
            DEFAULT_MAX_RETRIES,
            DEFAULT_RETRY_BACKOFF_FACTOR,
        )

        tracking_uri = self.get_effective_tracking_uri()
        token = resolve_mlflow_token()
        max_retries = DEFAULT_MAX_RETRIES
        backoff_factor = DEFAULT_RETRY_BACKOFF_FACTOR

        last_exc: Optional[Exception] = None
        for attempt in range(max_retries):
            try:
                if self.is_genai_mode:
                    return self._fetch_genai_arrow(token, tracking_uri)
                else:
                    return self._fetch_artifact_arrow(token, tracking_uri)
            except (MlflowArtifactNotFoundError, MlflowAuthError):
                raise
            except Exception as e:
                classified = classify_mlflow_error(e)
                if isinstance(
                    classified, (MlflowArtifactNotFoundError, MlflowAuthError)
                ):
                    raise classified from e
                if not is_retryable_error(e) or attempt == max_retries - 1:
                    last_exc = e
                    break
                wait = backoff_factor * (2**attempt)
                logger.warning(
                    "MLflow fetch attempt %d/%d failed for '%s': %s "
                    "(retrying in %.1fs)",
                    attempt + 1,
                    max_retries,
                    self.name,
                    e,
                    wait,
                )
                time.sleep(wait)
                last_exc = e

        from feast.infra.data_sources.mlflow.auth import MlflowConnectionError

        raise MlflowConnectionError(
            f"Failed to fetch data from MLflow for source '{self.name}' "
            f"after {max_retries} attempts. Last error: {last_exc}"
        ) from last_exc

    def _fetch_genai_arrow(
        self, token: Optional[str], tracking_uri: Optional[str]
    ) -> pa.Table:
        """Fetch GenAI dataset and return as materialized PyArrow Table."""
        try:
            import mlflow.genai.datasets
        except ImportError as e:
            raise ImportError(
                "Install feast[mlflow] to use MlflowDatasetSource "
                "in GenAI Dataset mode."
            ) from e

        from feast.infra.data_sources.mlflow.auth import (
            MlflowArtifactNotFoundError,
            classify_mlflow_error,
            mlflow_request_scope,
        )

        with mlflow_request_scope(token, tracking_uri):
            name = self.dataset_name or self.dataset_id
            try:
                dataset = mlflow.genai.datasets.get_dataset(name=name)
            except Exception as e:
                classified = classify_mlflow_error(e)
                if isinstance(classified, MlflowArtifactNotFoundError):
                    raise MlflowArtifactNotFoundError(
                        f"MLflow GenAI dataset '{name}' not found. "
                        f"Verify the dataset exists and has not been deleted. "
                        f"Source: '{self.name}'."
                    ) from e
                raise
            df = dataset.to_df()

        try:
            from feast.mlflow_integration.dataset_sync import (
                flatten_mlflow_dataset_df,
            )

            df = flatten_mlflow_dataset_df(df, field_mapping=self.field_mapping or None)
        except ImportError:
            pass

        return pa.Table.from_pandas(df)

    def _fetch_artifact_arrow(
        self, token: Optional[str], tracking_uri: Optional[str]
    ) -> pa.Table:
        """Download artifact and read into a materialized PyArrow Table."""
        try:
            import mlflow
        except ImportError as e:
            raise ImportError(
                "Install feast[mlflow] to use MlflowDatasetSource in artifact mode."
            ) from e

        import pyarrow.csv as pa_csv
        import pyarrow.parquet as pq

        from feast.infra.data_sources.mlflow.auth import (
            MlflowArtifactNotFoundError,
            classify_mlflow_error,
            mlflow_request_scope,
        )

        with mlflow_request_scope(token, tracking_uri):
            with tempfile.TemporaryDirectory(prefix="feast_mlflow_") as tmpdir:
                try:
                    local_path = mlflow.artifacts.download_artifacts(
                        run_id=self.run_id,
                        artifact_path=self.artifact_path,
                        dst_path=tmpdir,
                    )
                except Exception as e:
                    classified = classify_mlflow_error(e)
                    if isinstance(classified, MlflowArtifactNotFoundError):
                        raise MlflowArtifactNotFoundError(
                            f"MLflow artifact not found: run_id='{self.run_id}', "
                            f"artifact_path='{self.artifact_path}'. The run or "
                            f"artifact may have been deleted. "
                            f"Source: '{self.name}'."
                        ) from e
                    raise

                if self.artifact_format == "parquet":
                    return pq.read_table(local_path)
                elif self.artifact_format == "csv":
                    return pa_csv.read_csv(local_path)
                else:
                    raise ValueError(
                        f"Unsupported artifact format: {self.artifact_format}"
                    )

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
