import contextlib
import json
import tempfile
import uuid
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import (
    Any,
    Callable,
    ContextManager,
    Dict,
    Iterator,
    List,
    Literal,
    Optional,
    Tuple,
    Union,
)

import numpy as np
import pandas as pd
import pyarrow
import pyarrow.parquet
from pydantic import StrictStr, field_validator
from tenacity import Retrying, retry_if_exception_type, stop_after_delay, wait_fixed

from feast import flags_helper
from feast.data_source import DataSource
from feast.errors import (
    BigQueryJobCancelled,
    BigQueryJobStillRunning,
    EntityDFNotDateTime,
    EntitySQLEmptyResults,
    FeastProviderLoginError,
    InvalidEntityType,
)
from feast.feature_logging import LoggingConfig, LoggingSource
from feast.feature_view import DUMMY_ENTITY_ID, DUMMY_ENTITY_VAL, FeatureView
from feast.infra.offline_stores import offline_utils
from feast.infra.offline_stores.offline_store import (
    OfflineStore,
    RetrievalJob,
    RetrievalMetadata,
)
from feast.infra.registry.base_registry import BaseRegistry
from feast.monitoring.monitoring_utils import (
    MON_TABLE_FEATURE,
    MON_TABLE_FEATURE_SERVICE,
    MON_TABLE_FEATURE_VIEW,
    empty_categorical_metric,
    empty_numeric_metric,
    monitoring_table_meta,
    normalize_monitoring_row,
    opt_float,
)
from feast.on_demand_feature_view import OnDemandFeatureView
from feast.repo_config import FeastConfigBaseModel, RepoConfig
from feast.saved_dataset import SavedDatasetStorage
from feast.utils import _utc_now, get_user_agent

from .bigquery_source import (
    BigQueryLoggingDestination,
    BigQuerySource,
    SavedDatasetBigQueryStorage,
)
from .offline_utils import get_timestamp_filter_sql

try:
    from google.api_core import client_info as http_client_info
    from google.api_core.exceptions import NotFound
    from google.auth.exceptions import DefaultCredentialsError
    from google.cloud import bigquery
    from google.cloud.bigquery import Client, SchemaField, Table
    from google.cloud.storage import Client as StorageClient

except ImportError as e:
    from feast.errors import FeastExtrasDependencyImportError

    raise FeastExtrasDependencyImportError("gcp", str(e))

try:
    from google.cloud.bigquery._pyarrow_helpers import _ARROW_SCALAR_IDS_TO_BQ
except ImportError:
    try:
        from google.cloud.bigquery._pandas_helpers import (  # type: ignore
            ARROW_SCALAR_IDS_TO_BQ as _ARROW_SCALAR_IDS_TO_BQ,
        )
    except ImportError as e:
        raise FeastExtrasDependencyImportError("gcp", str(e))


def get_http_client_info():
    return http_client_info.ClientInfo(user_agent=get_user_agent())


class BigQueryOfflineStoreConfig(FeastConfigBaseModel):
    """Offline store config for GCP BigQuery"""

    type: Literal["bigquery"] = "bigquery"
    """ Offline store type selector"""

    dataset: StrictStr = "feast"
    """ (optional) BigQuery Dataset name for temporary tables """

    project_id: Optional[StrictStr] = None
    """ (optional) GCP project name used for the BigQuery offline store """
    billing_project_id: Optional[StrictStr] = None
    """ (optional) GCP project name used to run the bigquery jobs at """
    location: Optional[StrictStr] = None
    """ (optional) GCP location name used for the BigQuery offline store.
    Examples of location names include ``US``, ``EU``, ``us-central1``, ``us-west4``.
    If a location is not specified, the location defaults to the ``US`` multi-regional location.
    For more information on BigQuery data locations see: https://cloud.google.com/bigquery/docs/locations
    """

    gcs_staging_location: Optional[str] = None
    """ (optional) GCS location used for offloading BigQuery results as parquet files."""

    table_create_disposition: Literal["CREATE_NEVER", "CREATE_IF_NEEDED"] = (
        "CREATE_IF_NEEDED"
    )
    """ (optional) Specifies whether the job is allowed to create new tables. The default value is CREATE_IF_NEEDED.
    Custom constraint for table_create_disposition. To understand more, see:
    https://cloud.google.com/bigquery/docs/reference/rest/v2/Job#JobConfigurationLoad.FIELDS.create_disposition
    """

    @field_validator("billing_project_id")
    def project_id_exists(cls, v, values, **kwargs):
        if v and not values.data["project_id"]:
            raise ValueError(
                "please specify project_id if billing_project_id is specified"
            )
        return v


class BigQueryOfflineStore(OfflineStore):
    @staticmethod
    def pull_latest_from_table_or_query(
        config: RepoConfig,
        data_source: DataSource,
        join_key_columns: List[str],
        feature_name_columns: List[str],
        timestamp_field: str,
        created_timestamp_column: Optional[str],
        start_date: datetime,
        end_date: datetime,
    ) -> RetrievalJob:
        assert isinstance(config.offline_store, BigQueryOfflineStoreConfig)
        assert isinstance(data_source, BigQuerySource)
        from_expression = data_source.get_table_query_string()

        partition_by_join_key_string = ", ".join(
            BigQueryOfflineStore._escape_query_columns(join_key_columns)
        )
        if partition_by_join_key_string != "":
            partition_by_join_key_string = (
                "PARTITION BY " + partition_by_join_key_string
            )
        timestamps = [timestamp_field]
        if created_timestamp_column:
            timestamps.append(created_timestamp_column)
        timestamp_desc_string = " DESC, ".join(timestamps) + " DESC"
        field_string = ", ".join(
            BigQueryOfflineStore._escape_query_columns(join_key_columns)
            + BigQueryOfflineStore._escape_query_columns(feature_name_columns)
            + timestamps
        )
        project_id = (
            config.offline_store.billing_project_id or config.offline_store.project_id
        )
        client = _get_bigquery_client(
            project=project_id,
            location=config.offline_store.location,
        )
        timestamp_filter = get_timestamp_filter_sql(
            start_date,
            end_date,
            timestamp_field,
            date_partition_column=data_source.date_partition_column,
            quote_fields=False,
            cast_style="timestamp_func",
        )
        query = f"""
            SELECT
                {field_string}
                {f", {repr(DUMMY_ENTITY_VAL)} AS {DUMMY_ENTITY_ID}" if not join_key_columns else ""}
            FROM (
                SELECT {field_string},
                ROW_NUMBER() OVER({partition_by_join_key_string} ORDER BY {timestamp_desc_string}) AS _feast_row
                FROM {from_expression}
                WHERE {timestamp_filter}
            )
            WHERE _feast_row = 1
            """

        # When materializing a single feature view, we don't need full feature names. On demand transforms aren't materialized
        return BigQueryRetrievalJob(
            query=query,
            client=client,
            config=config,
            full_feature_names=False,
        )

    @staticmethod
    def pull_all_from_table_or_query(
        config: RepoConfig,
        data_source: DataSource,
        join_key_columns: List[str],
        feature_name_columns: List[str],
        timestamp_field: str,
        created_timestamp_column: Optional[str] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
    ) -> RetrievalJob:
        assert isinstance(config.offline_store, BigQueryOfflineStoreConfig)
        assert isinstance(data_source, BigQuerySource)
        from_expression = data_source.get_table_query_string()
        project_id = (
            config.offline_store.billing_project_id or config.offline_store.project_id
        )
        client = _get_bigquery_client(
            project=project_id,
            location=config.offline_store.location,
        )

        timestamp_fields = [timestamp_field]
        if created_timestamp_column:
            timestamp_fields.append(created_timestamp_column)
        field_string = ", ".join(
            BigQueryOfflineStore._escape_query_columns(join_key_columns)
            + BigQueryOfflineStore._escape_query_columns(feature_name_columns)
            + timestamp_fields
        )
        timestamp_filter = get_timestamp_filter_sql(
            start_date,
            end_date,
            timestamp_field,
            date_partition_column=data_source.date_partition_column,
            quote_fields=False,
            cast_style="timestamp_func",
        )
        query = f"""
            SELECT {field_string}
            FROM {from_expression}
            WHERE {timestamp_filter}
        """
        return BigQueryRetrievalJob(
            query=query,
            client=client,
            config=config,
            full_feature_names=False,
        )

    @staticmethod
    def get_historical_features(
        config: RepoConfig,
        feature_views: List[FeatureView],
        feature_refs: List[str],
        entity_df: Union[pd.DataFrame, str],
        registry: BaseRegistry,
        project: str,
        full_feature_names: bool = False,
    ) -> RetrievalJob:
        # TODO: Add entity_df validation in order to fail before interacting with BigQuery
        assert isinstance(config.offline_store, BigQueryOfflineStoreConfig)
        for fv in feature_views:
            assert isinstance(fv.batch_source, BigQuerySource)
        project_id = (
            config.offline_store.billing_project_id or config.offline_store.project_id
        )
        client = _get_bigquery_client(
            project=project_id,
            location=config.offline_store.location,
        )

        assert isinstance(config.offline_store, BigQueryOfflineStoreConfig)
        if config.offline_store.billing_project_id:
            dataset_project = str(config.offline_store.project_id)
        else:
            dataset_project = client.project
        table_reference = _get_table_reference_for_new_entity(
            client,
            dataset_project,
            config.offline_store.dataset,
            config.offline_store.location,
            config.offline_store.table_create_disposition,
        )

        entity_schema = _get_entity_schema(
            client=client,
            entity_df=entity_df,
        )

        entity_df_event_timestamp_col = (
            offline_utils.infer_event_timestamp_from_entity_df(entity_schema)
        )

        entity_df_event_timestamp_range = _get_entity_df_event_timestamp_range(
            entity_df,
            entity_df_event_timestamp_col,
            client,
        )

        @contextlib.contextmanager
        def query_generator() -> Iterator[str]:
            _upload_entity_df(
                client=client,
                table_name=table_reference,
                entity_df=entity_df,
            )

            expected_join_keys = offline_utils.get_expected_join_keys(
                project, feature_views, registry
            )

            offline_utils.assert_expected_columns_in_entity_df(
                entity_schema, expected_join_keys, entity_df_event_timestamp_col
            )

            # Build a query context containing all information required to template the BigQuery SQL query
            query_context = offline_utils.get_feature_view_query_context(
                feature_refs,
                feature_views,
                registry,
                project,
                entity_df_event_timestamp_range,
            )

            # Generate the BigQuery SQL query from the query context
            query = offline_utils.build_point_in_time_query(
                query_context,
                left_table_query_string=table_reference,
                entity_df_event_timestamp_col=entity_df_event_timestamp_col,
                entity_df_columns=entity_schema.keys(),
                query_template=MULTIPLE_FEATURE_VIEW_POINT_IN_TIME_JOIN,
                full_feature_names=full_feature_names,
            )

            try:
                yield query
            finally:
                # Asynchronously clean up the uploaded Bigquery table, which will expire
                # if cleanup fails
                client.delete_table(table=table_reference, not_found_ok=True)

        return BigQueryRetrievalJob(
            query=query_generator,
            client=client,
            config=config,
            full_feature_names=full_feature_names,
            on_demand_feature_views=OnDemandFeatureView.get_requested_odfvs(
                feature_refs, project, registry
            ),
            metadata=RetrievalMetadata(
                features=feature_refs,
                keys=list(entity_schema.keys() - {entity_df_event_timestamp_col}),
                min_event_timestamp=entity_df_event_timestamp_range[0],
                max_event_timestamp=entity_df_event_timestamp_range[1],
            ),
        )

    @staticmethod
    def write_logged_features(
        config: RepoConfig,
        data: Union[pyarrow.Table, Path],
        source: LoggingSource,
        logging_config: LoggingConfig,
        registry: BaseRegistry,
    ):
        destination = logging_config.destination
        assert isinstance(destination, BigQueryLoggingDestination)
        project_id = (
            config.offline_store.billing_project_id or config.offline_store.project_id
        )
        client = _get_bigquery_client(
            project=project_id,
            location=config.offline_store.location,
        )

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            schema=arrow_schema_to_bq_schema(source.get_schema(registry)),
            create_disposition=config.offline_store.table_create_disposition,
            time_partitioning=bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field=source.get_log_timestamp_column(),
            ),
        )

        if isinstance(data, Path):
            for file in data.iterdir():
                with file.open("rb") as f:
                    client.load_table_from_file(
                        file_obj=f,
                        destination=destination.table,
                        job_config=job_config,
                    ).result()

            return

        with tempfile.TemporaryFile() as parquet_temp_file:
            # In Pyarrow v13.0, the parquet version was upgraded to v2.6 from v2.4.
            # Set the coerce_timestamps to "us"(microseconds) for backward compatibility.
            pyarrow.parquet.write_table(
                table=data,
                where=parquet_temp_file,
                coerce_timestamps="us",
                allow_truncated_timestamps=True,
            )

            parquet_temp_file.seek(0)

            client.load_table_from_file(
                file_obj=parquet_temp_file,
                destination=destination.table,
                job_config=job_config,
            ).result()

    @staticmethod
    def offline_write_batch(
        config: RepoConfig,
        feature_view: FeatureView,
        table: pyarrow.Table,
        progress: Optional[Callable[[int], Any]],
    ):
        assert isinstance(config.offline_store, BigQueryOfflineStoreConfig)
        assert isinstance(feature_view.batch_source, BigQuerySource)

        pa_schema, column_names = offline_utils.get_pyarrow_schema_from_batch_source(
            config, feature_view.batch_source, timestamp_unit="ns"
        )
        if column_names != table.column_names:
            raise ValueError(
                f"The input pyarrow table has schema {table.schema} with the incorrect columns {table.column_names}. "
                f"The schema is expected to be {pa_schema} with the columns (in this exact order) to be {column_names}."
            )

        if table.schema != pa_schema:
            table = offline_utils.cast_arrow_table_to_schema(table, pa_schema)
        project_id = (
            config.offline_store.billing_project_id or config.offline_store.project_id
        )
        client = _get_bigquery_client(
            project=project_id,
            location=config.offline_store.location,
        )

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            schema=arrow_schema_to_bq_schema(pa_schema),
            create_disposition=config.offline_store.table_create_disposition,
            write_disposition="WRITE_APPEND",  # Default but included for clarity
        )

        with tempfile.TemporaryFile() as parquet_temp_file:
            # In Pyarrow v13.0, the parquet version was upgraded to v2.6 from v2.4.
            # Set the coerce_timestamps to "us"(microseconds) for backward compatibility.
            pyarrow.parquet.write_table(
                table=table,
                where=parquet_temp_file,
                coerce_timestamps="us",
                allow_truncated_timestamps=True,
            )

            parquet_temp_file.seek(0)

            client.load_table_from_file(
                file_obj=parquet_temp_file,
                destination=feature_view.batch_source.table,
                job_config=job_config,
            ).result()

    @staticmethod
    def _escape_query_columns(columns: List[str]) -> List[str]:
        return [f"`{x}`" for x in columns]

    @staticmethod
    def compute_monitoring_metrics(
        config: RepoConfig,
        data_source: DataSource,
        feature_columns: List[Tuple[str, str]],
        timestamp_field: str,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        histogram_bins: int = 20,
        top_n: int = 10,
    ) -> List[Dict[str, Any]]:
        assert isinstance(config.offline_store, BigQueryOfflineStoreConfig)
        assert isinstance(data_source, BigQuerySource)
        return _bq_compute_monitoring_metrics(
            config,
            data_source,
            feature_columns,
            timestamp_field,
            start_date=start_date,
            end_date=end_date,
            histogram_bins=histogram_bins,
            top_n=top_n,
        )

    @staticmethod
    def get_monitoring_max_timestamp(
        config: RepoConfig,
        data_source: DataSource,
        timestamp_field: str,
    ) -> Optional[datetime]:
        assert isinstance(config.offline_store, BigQueryOfflineStoreConfig)
        assert isinstance(data_source, BigQuerySource)
        return _bq_get_monitoring_max_timestamp(config, data_source, timestamp_field)

    @staticmethod
    def ensure_monitoring_tables(config: RepoConfig) -> None:
        assert isinstance(config.offline_store, BigQueryOfflineStoreConfig)
        _bq_ensure_monitoring_tables(config)

    @staticmethod
    def save_monitoring_metrics(
        config: RepoConfig,
        metric_type: str,
        metrics: List[Dict[str, Any]],
    ) -> None:
        if not metrics:
            return
        assert isinstance(config.offline_store, BigQueryOfflineStoreConfig)
        _bq_save_monitoring_metrics(config, metric_type, metrics)

    @staticmethod
    def query_monitoring_metrics(
        config: RepoConfig,
        project: str,
        metric_type: str,
        filters: Optional[Dict[str, Any]] = None,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
    ) -> List[Dict[str, Any]]:
        assert isinstance(config.offline_store, BigQueryOfflineStoreConfig)
        return _bq_query_monitoring_metrics(
            config, project, metric_type, filters, start_date, end_date
        )

    @staticmethod
    def clear_monitoring_baseline(
        config: RepoConfig,
        project: str,
        feature_view_name: Optional[str] = None,
        feature_name: Optional[str] = None,
        data_source_type: Optional[str] = None,
    ) -> None:
        assert isinstance(config.offline_store, BigQueryOfflineStoreConfig)
        _bq_clear_monitoring_baseline(
            config, project, feature_view_name, feature_name, data_source_type
        )


# ------------------------------------------------------------------ #
#  BigQuery monitoring metrics (native)
# ------------------------------------------------------------------ #


def _bq_monitoring_table_fqn(config: RepoConfig, table_name: str) -> str:
    assert isinstance(config.offline_store, BigQueryOfflineStoreConfig)
    project_id = config.offline_store.project_id
    if not project_id:
        client = _get_bigquery_client(
            project=config.offline_store.billing_project_id,
            location=config.offline_store.location,
        )
        project_id = client.project
    return f"`{project_id}.{config.offline_store.dataset}.{table_name}`"


def _bq_scalar_param_type(column: str) -> str:
    if column == "is_baseline":
        return "BOOL"
    if column == "metric_date":
        return "DATE"
    if column == "computed_at":
        return "TIMESTAMP"
    if column in {
        "row_count",
        "null_count",
        "total_row_count",
        "total_features",
        "features_with_nulls",
        "total_feature_views",
    }:
        return "INT64"
    if column in {
        "null_rate",
        "mean",
        "stddev",
        "min_val",
        "max_val",
        "p50",
        "p75",
        "p90",
        "p95",
        "p99",
        "avg_null_rate",
        "max_null_rate",
    }:
        return "FLOAT64"
    return "STRING"


def _bq_merge_row(
    config: RepoConfig,
    table_fqn: str,
    columns: List[str],
    pk_columns: List[str],
    row: Dict[str, Any],
) -> None:
    project_id = (
        config.offline_store.billing_project_id or config.offline_store.project_id
    )
    client = _get_bigquery_client(
        project=project_id,
        location=config.offline_store.location,
    )
    non_pk = [c for c in columns if c not in pk_columns]
    params: List[Any] = []
    using_parts: List[str] = []
    on_parts: List[str] = []
    merge_idx = 0
    for col in columns:
        p = f"p{merge_idx}"
        merge_idx += 1
        val = row.get(col)
        if col == "histogram" and val is not None and not isinstance(val, str):
            val = json.dumps(val)
        param_type = _bq_scalar_param_type(col)
        params.append(bigquery.ScalarQueryParameter(p, param_type, val))
        using_parts.append(f"@{p} AS {col}")
        on_parts.append(f"T.{col} = S.{col}")
    update_set = ", ".join(f"{c} = S.{c}" for c in non_pk)
    merge_sql = f"""
MERGE {table_fqn} T
USING (SELECT {", ".join(using_parts)}) S
ON {" AND ".join(on_parts)}
WHEN MATCHED THEN UPDATE SET {update_set}
WHEN NOT MATCHED THEN INSERT ({", ".join(columns)}) VALUES ({", ".join(f"S.{c}" for c in columns)})
"""
    job_config = bigquery.QueryJobConfig(query_parameters=params)
    client.query(merge_sql, job_config=job_config).result()


def _bq_save_monitoring_metrics(
    config: RepoConfig,
    metric_type: str,
    metrics: List[Dict[str, Any]],
) -> None:
    table_short, columns, pk_columns = monitoring_table_meta(metric_type)
    table_fqn = _bq_monitoring_table_fqn(config, table_short)
    for row in metrics:
        _bq_merge_row(config, table_fqn, columns, pk_columns, row)


def _bq_query_monitoring_metrics(
    config: RepoConfig,
    project: str,
    metric_type: str,
    filters: Optional[Dict[str, Any]] = None,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
) -> List[Dict[str, Any]]:
    table_short, columns, _ = monitoring_table_meta(metric_type)
    table_fqn = _bq_monitoring_table_fqn(config, table_short)
    project_id = (
        config.offline_store.billing_project_id or config.offline_store.project_id
    )
    client = _get_bigquery_client(
        project=project_id,
        location=config.offline_store.location,
    )
    params: List[Any] = [
        bigquery.ScalarQueryParameter("project", "STRING", project),
    ]
    conditions = ["project_id = @project"]
    if filters:
        for key, value in filters.items():
            if value is not None:
                conditions.append(f"`{key}` = @{key}")
                params.append(
                    bigquery.ScalarQueryParameter(
                        key, _bq_scalar_param_type(key), value
                    )
                )
    if start_date:
        conditions.append("metric_date >= @start_date")
        params.append(bigquery.ScalarQueryParameter("start_date", "DATE", start_date))
    if end_date:
        conditions.append("metric_date <= @end_date")
        params.append(bigquery.ScalarQueryParameter("end_date", "DATE", end_date))
    col_list = ", ".join(f"`{c}`" for c in columns)
    where_sql = " AND ".join(conditions)
    sql = (
        f"SELECT {col_list} FROM {table_fqn} WHERE {where_sql} ORDER BY metric_date ASC"
    )
    job_config = bigquery.QueryJobConfig(query_parameters=params)
    job = client.query(sql, job_config=job_config)
    job.result()
    results: List[Dict[str, Any]] = []
    for r in job:
        record = {columns[i]: r[i] for i in range(len(columns))}
        results.append(normalize_monitoring_row(record))
    return results


def _bq_clear_monitoring_baseline(
    config: RepoConfig,
    project: str,
    feature_view_name: Optional[str] = None,
    feature_name: Optional[str] = None,
    data_source_type: Optional[str] = None,
) -> None:
    table_fqn = _bq_monitoring_table_fqn(config, MON_TABLE_FEATURE)
    project_id = (
        config.offline_store.billing_project_id or config.offline_store.project_id
    )
    client = _get_bigquery_client(
        project=project_id,
        location=config.offline_store.location,
    )
    params: List[Any] = [
        bigquery.ScalarQueryParameter("project", "STRING", project),
    ]
    conditions = ["project_id = @project", "is_baseline = TRUE"]
    if feature_view_name:
        conditions.append("feature_view_name = @feature_view_name")
        params.append(
            bigquery.ScalarQueryParameter(
                "feature_view_name", "STRING", feature_view_name
            )
        )
    if feature_name:
        conditions.append("feature_name = @feature_name")
        params.append(
            bigquery.ScalarQueryParameter("feature_name", "STRING", feature_name)
        )
    if data_source_type:
        conditions.append("data_source_type = @data_source_type")
        params.append(
            bigquery.ScalarQueryParameter(
                "data_source_type", "STRING", data_source_type
            )
        )
    sql = f"UPDATE {table_fqn} SET is_baseline = FALSE WHERE {' AND '.join(conditions)}"
    job_config = bigquery.QueryJobConfig(query_parameters=params)
    client.query(sql, job_config=job_config).result()


def _bq_ensure_monitoring_tables(config: RepoConfig) -> None:
    project_id = (
        config.offline_store.billing_project_id or config.offline_store.project_id
    )
    client = _get_bigquery_client(
        project=project_id,
        location=config.offline_store.location,
    )
    ds = config.offline_store.dataset
    proj = config.offline_store.project_id or client.project
    feature_ddl = f"""
CREATE TABLE IF NOT EXISTS `{proj}.{ds}.{MON_TABLE_FEATURE}` (
  project_id STRING NOT NULL,
  feature_view_name STRING NOT NULL,
  feature_name STRING NOT NULL,
  metric_date DATE NOT NULL,
  granularity STRING NOT NULL,
  data_source_type STRING NOT NULL,
  computed_at TIMESTAMP NOT NULL,
  is_baseline BOOL NOT NULL,
  feature_type STRING NOT NULL,
  row_count INT64,
  null_count INT64,
  null_rate FLOAT64,
  mean FLOAT64,
  stddev FLOAT64,
  min_val FLOAT64,
  max_val FLOAT64,
  p50 FLOAT64,
  p75 FLOAT64,
  p90 FLOAT64,
  p95 FLOAT64,
  p99 FLOAT64,
  histogram STRING
)
PRIMARY KEY (project_id, feature_view_name, feature_name, metric_date, granularity, data_source_type) NOT ENFORCED
"""
    view_ddl = f"""
CREATE TABLE IF NOT EXISTS `{proj}.{ds}.{MON_TABLE_FEATURE_VIEW}` (
  project_id STRING NOT NULL,
  feature_view_name STRING NOT NULL,
  metric_date DATE NOT NULL,
  granularity STRING NOT NULL,
  data_source_type STRING NOT NULL,
  computed_at TIMESTAMP NOT NULL,
  is_baseline BOOL NOT NULL,
  total_row_count INT64,
  total_features INT64,
  features_with_nulls INT64,
  avg_null_rate FLOAT64,
  max_null_rate FLOAT64
)
PRIMARY KEY (project_id, feature_view_name, metric_date, granularity, data_source_type) NOT ENFORCED
"""
    service_ddl = f"""
CREATE TABLE IF NOT EXISTS `{proj}.{ds}.{MON_TABLE_FEATURE_SERVICE}` (
  project_id STRING NOT NULL,
  feature_service_name STRING NOT NULL,
  metric_date DATE NOT NULL,
  granularity STRING NOT NULL,
  data_source_type STRING NOT NULL,
  computed_at TIMESTAMP NOT NULL,
  is_baseline BOOL NOT NULL,
  total_feature_views INT64,
  total_features INT64,
  avg_null_rate FLOAT64,
  max_null_rate FLOAT64
)
PRIMARY KEY (project_id, feature_service_name, metric_date, granularity, data_source_type) NOT ENFORCED
"""
    for ddl in (feature_ddl, view_ddl, service_ddl):
        client.query(ddl).result()


def _bq_get_monitoring_max_timestamp(
    config: RepoConfig,
    data_source: BigQuerySource,
    timestamp_field: str,
) -> Optional[datetime]:
    from_expression = data_source.get_table_query_string()
    ts_col = f"`{timestamp_field}`"
    sql = f"SELECT MAX({ts_col}) AS _max_ts FROM {from_expression}"
    project_id = (
        config.offline_store.billing_project_id or config.offline_store.project_id
    )
    client = _get_bigquery_client(
        project=project_id,
        location=config.offline_store.location,
    )
    job = client.query(sql)
    job.result()
    rows = list(job)
    if not rows or rows[0][0] is None:
        return None
    val = rows[0][0]
    if isinstance(val, datetime):
        return val if val.tzinfo else val.replace(tzinfo=timezone.utc)
    if isinstance(val, date):
        return datetime.combine(val, datetime.min.time(), tzinfo=timezone.utc)
    return val  # type: ignore[no-any-return]


def _bq_numeric_histogram(
    config: RepoConfig,
    from_expression: str,
    col_name: str,
    ts_filter: str,
    bins: int,
    min_val: float,
    max_val: float,
) -> Dict[str, Any]:
    q_col = f"`{col_name}`"
    project_id = (
        config.offline_store.billing_project_id or config.offline_store.project_id
    )
    client = _get_bigquery_client(
        project=project_id,
        location=config.offline_store.location,
    )
    if min_val == max_val:
        sql = (
            f"SELECT COUNT(*) AS cnt FROM {from_expression} AS _src "
            f"WHERE {q_col} IS NOT NULL AND {ts_filter}"
        )
        job = client.query(sql)
        job.result()
        hrows = list(job)
        cnt = int(hrows[0][0]) if hrows else 0
        return {"bins": [min_val, max_val], "counts": [cnt], "bin_width": 0.0}

    bin_width = (max_val - min_val) / bins
    sql = f"""
SELECT
  LEAST(
    GREATEST(
      CAST(FLOOR((CAST({q_col} AS FLOAT64) - {min_val}) / {bin_width}) AS INT64) + 1,
      1
    ),
    {bins}
  ) AS bucket,
  COUNT(*) AS cnt
FROM {from_expression} AS _src
WHERE {q_col} IS NOT NULL AND {ts_filter}
GROUP BY bucket
ORDER BY bucket
"""
    job = client.query(sql)
    job.result()
    rows = list(job)
    counts = [0] * bins
    for bucket, cnt in rows:
        b = int(bucket)
        if 1 <= b <= bins:
            counts[b - 1] += int(cnt)
    bin_edges = [min_val + i * bin_width for i in range(bins + 1)]
    return {
        "bins": [float(b) for b in bin_edges],
        "counts": counts,
        "bin_width": float(bin_width),
    }


def _bq_numeric_stats(
    config: RepoConfig,
    from_expression: str,
    feature_names: List[str],
    ts_filter: str,
    histogram_bins: int,
) -> List[Dict[str, Any]]:
    project_id = (
        config.offline_store.billing_project_id or config.offline_store.project_id
    )
    client = _get_bigquery_client(
        project=project_id,
        location=config.offline_store.location,
    )
    select_parts: List[str] = ["COUNT(*) AS _row_count"]
    for i, col in enumerate(feature_names):
        q = f"`{col}`"
        c = f"CAST({q} AS FLOAT64)"
        select_parts.extend(
            [
                f"COUNT({q}) AS c{i}_nn",
                f"AVG({c}) AS c{i}_avg",
                f"STDDEV_SAMP({c}) AS c{i}_stddev",
                f"MIN({c}) AS c{i}_min",
                f"MAX({c}) AS c{i}_max",
                f"APPROX_QUANTILES({c}, 100)[OFFSET(50)] AS c{i}_p50",
                f"APPROX_QUANTILES({c}, 100)[OFFSET(75)] AS c{i}_p75",
                f"APPROX_QUANTILES({c}, 100)[OFFSET(90)] AS c{i}_p90",
                f"APPROX_QUANTILES({c}, 100)[OFFSET(95)] AS c{i}_p95",
                f"APPROX_QUANTILES({c}, 100)[OFFSET(99)] AS c{i}_p99",
            ]
        )
    query = (
        f"SELECT {', '.join(select_parts)} "
        f"FROM {from_expression} AS _src WHERE {ts_filter}"
    )
    job = client.query(query)
    job.result()
    rows = list(job)
    if not rows:
        return [empty_numeric_metric(n) for n in feature_names]
    row = rows[0]
    row_count = row["_row_count"] or 0
    results: List[Dict[str, Any]] = []
    for i, col in enumerate(feature_names):
        base = f"c{i}_"
        non_null = row[f"{base}nn"] or 0
        null_count = int(row_count) - int(non_null)
        min_v = opt_float(row[f"{base}min"])
        max_v = opt_float(row[f"{base}max"])
        result: Dict[str, Any] = {
            "feature_name": col,
            "feature_type": "numeric",
            "row_count": int(row_count),
            "null_count": null_count,
            "null_rate": null_count / row_count if row_count > 0 else 0.0,
            "mean": opt_float(row[f"{base}avg"]),
            "stddev": opt_float(row[f"{base}stddev"]),
            "min_val": min_v,
            "max_val": max_v,
            "p50": opt_float(row[f"{base}p50"]),
            "p75": opt_float(row[f"{base}p75"]),
            "p90": opt_float(row[f"{base}p90"]),
            "p95": opt_float(row[f"{base}p95"]),
            "p99": opt_float(row[f"{base}p99"]),
            "histogram": None,
        }
        if min_v is not None and max_v is not None and non_null and int(non_null) > 0:
            result["histogram"] = _bq_numeric_histogram(
                config,
                from_expression,
                col,
                ts_filter,
                histogram_bins,
                min_v,
                max_v,
            )
        results.append(result)
    return results


def _bq_categorical_stats(
    config: RepoConfig,
    from_expression: str,
    col_name: str,
    ts_filter: str,
    top_n: int,
) -> Dict[str, Any]:
    q_col = f"`{col_name}`"
    project_id = (
        config.offline_store.billing_project_id or config.offline_store.project_id
    )
    client = _get_bigquery_client(
        project=project_id,
        location=config.offline_store.location,
    )
    query = f"""
WITH filtered AS (
  SELECT * FROM {from_expression} AS _src WHERE {ts_filter}
)
SELECT
  (SELECT COUNT(*) FROM filtered) AS row_count,
  (SELECT COUNT(*) - COUNT({q_col}) FROM filtered) AS null_count,
  (SELECT COUNT(DISTINCT {q_col}) FROM filtered WHERE {q_col} IS NOT NULL) AS unique_count,
  CAST({q_col} AS STRING) AS value,
  COUNT(*) AS cnt
FROM filtered
WHERE {q_col} IS NOT NULL
GROUP BY CAST({q_col} AS STRING)
ORDER BY cnt DESC
LIMIT {int(top_n)}
"""
    job = client.query(query)
    job.result()
    rows = list(job)
    if not rows:
        return empty_categorical_metric(col_name)
    row_count = rows[0]["row_count"]
    null_count = rows[0]["null_count"]
    unique_count = rows[0]["unique_count"]
    top_entries = [{"value": r["value"], "count": r["cnt"]} for r in rows]
    top_total = sum(e["count"] for e in top_entries)
    other_count = (row_count - null_count) - top_total
    return {
        "feature_name": col_name,
        "feature_type": "categorical",
        "row_count": row_count,
        "null_count": null_count,
        "null_rate": null_count / row_count if row_count > 0 else 0.0,
        "mean": None,
        "stddev": None,
        "min_val": None,
        "max_val": None,
        "p50": None,
        "p75": None,
        "p90": None,
        "p95": None,
        "p99": None,
        "histogram": {
            "values": top_entries,
            "other_count": max(other_count, 0),
            "unique_count": unique_count,
        },
    }


def _bq_compute_monitoring_metrics(
    config: RepoConfig,
    data_source: BigQuerySource,
    feature_columns: List[Tuple[str, str]],
    timestamp_field: str,
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None,
    histogram_bins: int = 20,
    top_n: int = 10,
) -> List[Dict[str, Any]]:
    from_expression = data_source.get_table_query_string()
    ts_filter = get_timestamp_filter_sql(
        start_date,
        end_date,
        timestamp_field,
        date_partition_column=data_source.date_partition_column,
        quote_fields=False,
        cast_style="timestamp_func",
    )
    numeric_features = [n for n, t in feature_columns if t == "numeric"]
    categorical_features = [n for n, t in feature_columns if t == "categorical"]
    results: List[Dict[str, Any]] = []
    if numeric_features:
        results.extend(
            _bq_numeric_stats(
                config,
                from_expression,
                numeric_features,
                ts_filter,
                histogram_bins,
            )
        )
    for col_name in categorical_features:
        results.append(
            _bq_categorical_stats(config, from_expression, col_name, ts_filter, top_n)
        )
    return results


class BigQueryRetrievalJob(RetrievalJob):
    def __init__(
        self,
        query: Union[str, Callable[[], ContextManager[str]]],
        client: bigquery.Client,
        config: RepoConfig,
        full_feature_names: bool,
        on_demand_feature_views: Optional[List[OnDemandFeatureView]] = None,
        metadata: Optional[RetrievalMetadata] = None,
    ):
        if not isinstance(query, str):
            self._query_generator = query
        else:

            @contextlib.contextmanager
            def query_generator() -> Iterator[str]:
                assert isinstance(query, str)
                yield query

            self._query_generator = query_generator
        self.client = client
        self.config = config
        self._full_feature_names = full_feature_names
        self._on_demand_feature_views = on_demand_feature_views or []
        self._metadata = metadata
        if self.config.offline_store.gcs_staging_location:
            self._gcs_path = (
                self.config.offline_store.gcs_staging_location
                + f"/{self.config.project}/export/"
                + str(uuid.uuid4())
            )
        else:
            self._gcs_path = None

    @property
    def full_feature_names(self) -> bool:
        return self._full_feature_names

    @property
    def on_demand_feature_views(self) -> List[OnDemandFeatureView]:
        return self._on_demand_feature_views

    def _to_df_internal(self, timeout: Optional[int] = None) -> pd.DataFrame:
        with self._query_generator() as query:
            query_job = self._execute_query(query=query, timeout=timeout)
            assert query_job
            return query_job.to_dataframe(create_bqstorage_client=True)

    def to_sql(self) -> str:
        """Returns the underlying SQL query."""
        with self._query_generator() as query:
            return query

    def to_bigquery(
        self,
        job_config: Optional[bigquery.QueryJobConfig] = None,
        timeout: Optional[int] = 1800,
        retry_cadence: Optional[int] = 10,
    ) -> str:
        """
        Synchronously executes the underlying query and exports the result to a BigQuery table. The
        underlying BigQuery job runs for a limited amount of time (the default is 30 minutes).

        Args:
            job_config (optional): A bigquery.QueryJobConfig to specify options like the destination table, dry run, etc.
            timeout (optional): The time limit of the BigQuery job in seconds. Defaults to 30 minutes.
            retry_cadence (optional): The number of seconds for setting how long the job should checked for completion.

        Returns:
            Returns the destination table name or None if job_config.dry_run is True.
        """

        if not job_config:
            today = date.today().strftime("%Y%m%d")
            rand_id = str(uuid.uuid4())[:7]
            if self.config.offline_store.billing_project_id:
                path = f"{self.config.offline_store.project_id}.{self.config.offline_store.dataset}.historical_{today}_{rand_id}"
            else:
                path = f"{self.client.project}.{self.config.offline_store.dataset}.historical_{today}_{rand_id}"
            job_config = bigquery.QueryJobConfig(destination=path)

        if not job_config.dry_run and self.on_demand_feature_views:
            job = self.client.load_table_from_dataframe(
                self.to_df(), job_config.destination
            )
            job.result()
            print(f"Done writing to '{job_config.destination}'.")
            return str(job_config.destination)

        with self._query_generator() as query:
            dest = job_config.destination
            # because setting destination for scripts is not valid
            # remove destination attribute if provided
            job_config.destination = None
            bq_job = self._execute_query(query, job_config, timeout)

            if not job_config.dry_run:
                assert bq_job
                config = bq_job.to_api_repr()["configuration"]
                # get temp table created by BQ
                tmp_dest = config["query"]["destinationTable"]
                temp_dest_table = f"{tmp_dest['projectId']}.{tmp_dest['datasetId']}.{tmp_dest['tableId']}"

                # persist temp table
                sql = f"CREATE TABLE `{dest}` AS SELECT * FROM `{temp_dest_table}`"
                self._execute_query(sql, timeout=timeout)

            print(f"Done writing to '{dest}'.")
            return str(dest)

    def _to_arrow_internal(self, timeout: Optional[int] = None) -> pyarrow.Table:
        with self._query_generator() as query:
            q = self._execute_query(query=query, timeout=timeout)
            assert q
            return q.to_arrow()

    def _execute_query(
        self, query, job_config=None, timeout: Optional[int] = None
    ) -> Optional[bigquery.job.query.QueryJob]:
        bq_job = self.client.query(query, job_config=job_config)

        if job_config and job_config.dry_run:
            print(
                "This query will process {} bytes.".format(bq_job.total_bytes_processed)
            )
            return None

        block_until_done(client=self.client, bq_job=bq_job, timeout=timeout or 1800)
        return bq_job

    def persist(
        self,
        storage: SavedDatasetStorage,
        allow_overwrite: Optional[bool] = False,
        timeout: Optional[int] = None,
    ):
        assert isinstance(storage, SavedDatasetBigQueryStorage)

        self.to_bigquery(
            bigquery.QueryJobConfig(destination=storage.bigquery_options.table),
            timeout=timeout,
        )

    @property
    def metadata(self) -> Optional[RetrievalMetadata]:
        return self._metadata

    def supports_remote_storage_export(self) -> bool:
        return self._gcs_path is not None

    def to_remote_storage(self) -> List[str]:
        if not self._gcs_path:
            raise ValueError(
                "gcs_staging_location needs to be specified for the big query "
                "offline store when executing `to_remote_storage()`"
            )

        table = self.to_bigquery()

        job_config = bigquery.job.ExtractJobConfig()
        job_config.destination_format = "PARQUET"

        extract_job = self.client.extract_table(
            table,
            destination_uris=[f"{self._gcs_path}/*.parquet"],
            location=self.config.offline_store.location,
            job_config=job_config,
        )
        extract_job.result()

        bucket: str
        prefix: str
        if self.config.offline_store.billing_project_id:
            storage_client = StorageClient(project=self.config.offline_store.project_id)
        else:
            storage_client = StorageClient(project=self.client.project)
        bucket, prefix = self._gcs_path[len("gs://") :].split("/", 1)
        if prefix.startswith("/"):
            prefix = prefix[1:]

        blobs = storage_client.list_blobs(bucket, prefix=prefix)
        results = []
        for b in blobs:
            results.append(f"gs://{b.bucket.name}/{b.name}")
        return results


def block_until_done(
    client: Client,
    bq_job: Union[bigquery.job.query.QueryJob, bigquery.job.load.LoadJob],
    timeout: int = 1800,
    retry_cadence: float = 1,
):
    """
    Waits for bq_job to finish running, up to a maximum amount of time specified by the timeout parameter (defaulting to 30 minutes).

    Args:
        client: A bigquery.client.Client to monitor the bq_job.
        bq_job: The bigquery.job.QueryJob that blocks until done runnning.
        timeout: An optional number of seconds for setting the time limit of the job.
        retry_cadence: An optional number of seconds for setting how long the job should checked for completion.

    Raises:
        BigQueryJobStillRunning exception if the function has blocked longer than 30 minutes.
        BigQueryJobCancelled exception to signify when that the job has been cancelled (i.e. from timeout or KeyboardInterrupt).
    """

    # For test environments, retry more aggressively
    if flags_helper.is_test():
        retry_cadence = 0.1

    def _wait_until_done(bq_job):
        if client.get_job(bq_job).state in ["PENDING", "RUNNING"]:
            raise BigQueryJobStillRunning(job_id=bq_job.job_id)

    try:
        retryer = Retrying(
            wait=wait_fixed(retry_cadence),
            stop=stop_after_delay(timeout),
            retry=retry_if_exception_type(BigQueryJobStillRunning),
            reraise=True,
        )
        retryer(_wait_until_done, bq_job)

    finally:
        if client.get_job(bq_job).state in ["PENDING", "RUNNING"]:
            client.cancel_job(bq_job.job_id)
            raise BigQueryJobCancelled(job_id=bq_job.job_id)

        # We explicitly set the timeout to None because `google-api-core` changed the default value and
        # breaks downstream libraries.
        # https://github.com/googleapis/python-api-core/issues/479
        if bq_job.exception(timeout=None):
            raise bq_job.exception(timeout=None)


def _get_table_reference_for_new_entity(
    client: Client,
    dataset_project: str,
    dataset_name: str,
    dataset_location: Optional[str],
    table_create_disposition: str,
) -> str:
    """Gets the table_id for the new entity to be uploaded."""

    # First create the BigQuery dataset if it doesn't exist
    dataset = bigquery.Dataset(f"{dataset_project}.{dataset_name}")
    dataset.location = dataset_location if dataset_location else "US"

    try:
        client.get_dataset(dataset.reference)
    except NotFound as nfe:
        # Only create the dataset if it does not exist
        if table_create_disposition == "CREATE_NEVER":
            raise ValueError(
                f"Dataset {dataset_project}.{dataset_name} does not exist "
                f"and table_create_disposition is set to {table_create_disposition}."
            ) from nfe
        client.create_dataset(dataset, exists_ok=True)

    table_name = offline_utils.get_temp_entity_table_name()

    return f"{dataset_project}.{dataset_name}.{table_name}"


def _upload_entity_df(
    client: Client,
    table_name: str,
    entity_df: Union[pd.DataFrame, str],
) -> Table:
    """Uploads a Pandas entity dataframe into a BigQuery table and returns the resulting table"""
    job: Union[bigquery.job.query.QueryJob, bigquery.job.load.LoadJob]

    if isinstance(entity_df, str):
        job = client.query(f"CREATE TABLE `{table_name}` AS ({entity_df})")

    elif isinstance(entity_df, pd.DataFrame):
        # Drop the index so that we don't have unnecessary columns
        entity_df.reset_index(drop=True, inplace=True)
        job = client.load_table_from_dataframe(entity_df, table_name)
    else:
        raise InvalidEntityType(type(entity_df))

    block_until_done(client, job)

    # Ensure that the table expires after some time
    table = client.get_table(table=table_name)
    table.expires = _utc_now() + timedelta(minutes=30)
    client.update_table(table, ["expires"])

    return table


def _get_entity_schema(
    client: Client, entity_df: Union[pd.DataFrame, str]
) -> Dict[str, np.dtype]:
    if isinstance(entity_df, str):
        entity_df_sample = (
            client.query(f"SELECT * FROM ({entity_df}) LIMIT 0").result().to_dataframe()
        )

        entity_schema = dict(zip(entity_df_sample.columns, entity_df_sample.dtypes))
    elif isinstance(entity_df, pd.DataFrame):
        entity_schema = dict(zip(entity_df.columns, entity_df.dtypes))
    else:
        raise InvalidEntityType(type(entity_df))

    return entity_schema


def _get_entity_df_event_timestamp_range(
    entity_df: Union[pd.DataFrame, str],
    entity_df_event_timestamp_col: str,
    client: Client,
) -> Tuple[datetime, datetime]:
    if type(entity_df) is str:
        job = client.query(
            f"SELECT MIN({entity_df_event_timestamp_col}) AS min, MAX({entity_df_event_timestamp_col}) AS max "
            f"FROM ({entity_df})"
        )
        res = next(job.result())
        entity_df_event_timestamp_range = (
            res.get("min"),
            res.get("max"),
        )
        if (
            entity_df_event_timestamp_range[0] is None
            or entity_df_event_timestamp_range[1] is None
        ):
            raise EntitySQLEmptyResults(entity_df)
        if type(entity_df_event_timestamp_range[0]) != datetime:
            raise EntityDFNotDateTime()
    elif isinstance(entity_df, pd.DataFrame):
        entity_df_event_timestamp = entity_df.loc[
            :, entity_df_event_timestamp_col
        ].infer_objects()
        if pd.api.types.is_string_dtype(entity_df_event_timestamp):
            entity_df_event_timestamp = pd.to_datetime(
                entity_df_event_timestamp, utc=True
            )
        entity_df_event_timestamp_range = (
            entity_df_event_timestamp.min().to_pydatetime(),
            entity_df_event_timestamp.max().to_pydatetime(),
        )
    else:
        raise InvalidEntityType(type(entity_df))

    return entity_df_event_timestamp_range


def _get_bigquery_client(
    project: Optional[str] = None, location: Optional[str] = None
) -> bigquery.Client:
    try:
        client = bigquery.Client(
            project=project, location=location, client_info=get_http_client_info()
        )
    except DefaultCredentialsError as e:
        raise FeastProviderLoginError(
            str(e)
            + '\nIt may be necessary to run "gcloud auth application-default login" if you would like to use your '
            "local Google Cloud account"
        )
    except EnvironmentError as e:
        raise FeastProviderLoginError(
            "GCP error: "
            + str(e)
            + "\nIt may be necessary to set a default GCP project by running "
            '"gcloud config set project your-project"'
        )

    return client


def arrow_schema_to_bq_schema(arrow_schema: pyarrow.Schema) -> List[SchemaField]:
    bq_schema = []

    for field in arrow_schema:
        if pyarrow.types.is_struct(field.type) or pyarrow.types.is_map(field.type):
            detected_mode = "NULLABLE"
            detected_type = "STRING"
        elif pyarrow.types.is_list(field.type):
            detected_mode = "REPEATED"
            detected_type = _ARROW_SCALAR_IDS_TO_BQ.get(
                field.type.value_type.id, "STRING"
            )
        else:
            detected_mode = "NULLABLE"
            detected_type = _ARROW_SCALAR_IDS_TO_BQ.get(field.type.id, "STRING")

        bq_schema.append(
            SchemaField(name=field.name, field_type=detected_type, mode=detected_mode)
        )

    return bq_schema


# TODO: Optimizations
#   * Use GENERATE_UUID() instead of ROW_NUMBER(), or join on entity columns directly
#   * Precompute ROW_NUMBER() so that it doesn't have to be recomputed for every query on entity_dataframe
#   * Create temporary tables instead of keeping all tables in memory

# Note: Keep this in sync with sdk/python/feast/infra/offline_stores/redshift.py:MULTIPLE_FEATURE_VIEW_POINT_IN_TIME_JOIN

MULTIPLE_FEATURE_VIEW_POINT_IN_TIME_JOIN = """
/*
 Compute a deterministic hash for the `left_table_query_string` that will be used throughout
 all the logic as the field to GROUP BY the data
*/
CREATE TEMP TABLE entity_dataframe AS (
    SELECT *,
        {{entity_df_event_timestamp_col}} AS entity_timestamp
        {% for featureview in featureviews %}
            {% if featureview.entities %}
            ,CONCAT(
                {% for entity in featureview.entities %}
                    CAST({{entity}} AS STRING),
                {% endfor %}
                CAST({{entity_df_event_timestamp_col}} AS STRING)
            ) AS {{featureview.name}}__entity_row_unique_id
            {% else %}
            ,CAST({{entity_df_event_timestamp_col}} AS STRING) AS {{featureview.name}}__entity_row_unique_id
            {% endif %}
        {% endfor %}
    FROM `{{ left_table_query_string }}`
);

{% for featureview in featureviews %}
CREATE TEMP TABLE {{ featureview.name }}__cleaned AS (
    WITH {{ featureview.name }}__entity_dataframe AS (
        SELECT
            {{ featureview.entities | join(', ')}}{% if featureview.entities %},{% else %}{% endif %}
            entity_timestamp,
            {{featureview.name}}__entity_row_unique_id
        FROM entity_dataframe
        GROUP BY
            {{ featureview.entities | join(', ')}}{% if featureview.entities %},{% else %}{% endif %}
            entity_timestamp,
            {{featureview.name}}__entity_row_unique_id
    ),

    /*
    This query template performs the point-in-time correctness join for a single feature set table
    to the provided entity table.

    1. We first join the current feature_view to the entity dataframe that has been passed.
    This JOIN has the following logic:
        - For each row of the entity dataframe, only keep the rows where the `timestamp_field`
        is less than the one provided in the entity dataframe
        - If there a TTL for the current feature_view, also keep the rows where the `timestamp_field`
        is higher the the one provided minus the TTL
        - For each row, Join on the entity key and retrieve the `entity_row_unique_id` that has been
        computed previously

    The output of this CTE will contain all the necessary information and already filtered out most
    of the data that is not relevant.
    */

    {{ featureview.name }}__subquery AS (
        SELECT
            {{ featureview.timestamp_field }} as event_timestamp,
            {{ featureview.created_timestamp_column ~ ' as created_timestamp,' if featureview.created_timestamp_column else '' }}
            {{ featureview.entity_selections | join(', ')}}{% if featureview.entity_selections %},{% else %}{% endif %}
            {% for feature in featureview.features %}
                {{ feature | backticks }} as {% if full_feature_names %}
                {{ featureview.name }}__{{featureview.field_mapping.get(feature, feature)}}{% else %}
                {{ featureview.field_mapping.get(feature, feature) | backticks }}{% endif %}
                {% if loop.last %}{% else %}, {% endif %}
            {% endfor %}
        FROM {{ featureview.table_subquery }}
        WHERE {{ featureview.timestamp_field }} <= '{{ featureview.max_event_timestamp }}'
        {% if featureview.ttl == 0 %}{% else %}
        AND {{ featureview.timestamp_field }} >= '{{ featureview.min_event_timestamp }}'
        {% endif %}
        {% if featureview.date_partition_column %}
        AND {{ featureview.date_partition_column | backticks }} <= '{{ featureview.max_event_timestamp[:10] }}'
        {% if featureview.min_event_timestamp %}
        AND {{ featureview.date_partition_column | backticks }} >= '{{ featureview.min_event_timestamp[:10] }}'
        {% endif %}
        {% endif %}
    ),

    {{ featureview.name }}__base AS (
        SELECT
            subquery.*,
            entity_dataframe.entity_timestamp,
            entity_dataframe.{{featureview.name}}__entity_row_unique_id
        FROM {{ featureview.name }}__subquery AS subquery
        INNER JOIN {{ featureview.name }}__entity_dataframe AS entity_dataframe
        ON TRUE
            AND subquery.event_timestamp <= entity_dataframe.entity_timestamp

            {% if featureview.ttl == 0 %}{% else %}
            AND subquery.event_timestamp >= Timestamp_sub(entity_dataframe.entity_timestamp, interval {{ featureview.ttl }} second)
            {% endif %}

            {% for entity in featureview.entities %}
            AND subquery.{{ entity }} = entity_dataframe.{{ entity }}
            {% endfor %}
    ),

    /*
    2. If the `created_timestamp_column` has been set, we need to
    deduplicate the data first. This is done by calculating the
    `MAX(created_at_timestamp)` for each event_timestamp.
    We then join the data on the next CTE
    */
    {% if featureview.created_timestamp_column %}
    {{ featureview.name }}__dedup AS (
        SELECT
            {{featureview.name}}__entity_row_unique_id,
            event_timestamp,
            MAX(created_timestamp) as created_timestamp
        FROM {{ featureview.name }}__base
        GROUP BY {{featureview.name}}__entity_row_unique_id, event_timestamp
    ),
    {% endif %}

    /*
    3. The data has been filtered during the first CTE "*__base"
    Thus we only need to compute the latest timestamp of each feature.
    */
    {{ featureview.name }}__latest AS (
    SELECT
        event_timestamp,
        {% if featureview.created_timestamp_column %}created_timestamp,{% endif %}
        {{featureview.name}}__entity_row_unique_id
    FROM
    (
        SELECT *,
            ROW_NUMBER() OVER(
                PARTITION BY {{featureview.name}}__entity_row_unique_id
                ORDER BY event_timestamp DESC{% if featureview.created_timestamp_column %},created_timestamp DESC{% endif %}
            ) AS row_number
        FROM {{ featureview.name }}__base
        {% if featureview.created_timestamp_column %}
            INNER JOIN {{ featureview.name }}__dedup
            USING ({{featureview.name}}__entity_row_unique_id, event_timestamp, created_timestamp)
        {% endif %}
    )
    WHERE row_number = 1
)

/*
 4. Once we know the latest value of each feature for a given timestamp,
 we can join again the data back to the original "base" dataset
*/

    SELECT base.*
    FROM {{ featureview.name }}__base as base
    INNER JOIN {{ featureview.name }}__latest
    USING(
        {{featureview.name}}__entity_row_unique_id,
        event_timestamp
        {% if featureview.created_timestamp_column %}
            ,created_timestamp
        {% endif %}
    )
);


{% endfor %}
/*
 Joins the outputs of multiple time travel joins to a single table.
 The entity_dataframe dataset being our source of truth here.
 */

SELECT {{ final_output_feature_names | backticks | join(', ')}}
FROM entity_dataframe
{% for featureview in featureviews %}
LEFT JOIN (
    SELECT
        {{featureview.name}}__entity_row_unique_id
        {% for feature in featureview.features %}
            ,{% if full_feature_names %}{{ featureview.name }}__{{featureview.field_mapping.get(feature, feature)}}{% else %}{{ featureview.field_mapping.get(feature, feature) | backticks }}{% endif %}
        {% endfor %}
    FROM {{ featureview.name }}__cleaned
) USING ({{featureview.name}}__entity_row_unique_id)
{% endfor %}
"""
