import json
import os
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

import duckdb
import ibis
import pandas as pd
import pyarrow
import pyarrow as pa
import pyarrow.parquet as pq
from ibis.expr.types import Table
from pydantic import StrictStr

from feast.data_format import DeltaFormat, ParquetFormat
from feast.data_source import DataSource
from feast.errors import SavedDatasetLocationAlreadyExists
from feast.feature_logging import LoggingConfig, LoggingSource
from feast.feature_view import FeatureView
from feast.infra.offline_stores.file_source import FileSource
from feast.infra.offline_stores.ibis import (
    get_historical_features_ibis,
    offline_write_batch_ibis,
    pull_all_from_table_or_query_ibis,
    pull_latest_from_table_or_query_ibis,
    write_logged_features_ibis,
)
from feast.infra.offline_stores.offline_store import OfflineStore, RetrievalJob
from feast.infra.offline_stores.offline_utils import get_timestamp_filter_sql
from feast.infra.registry.base_registry import BaseRegistry
from feast.monitoring.monitoring_utils import (
    FEATURE_METRICS_COLUMNS,
    FEATURE_METRICS_PK,
    FEATURE_SERVICE_METRICS_COLUMNS,
    FEATURE_SERVICE_METRICS_PK,
    FEATURE_VIEW_METRICS_COLUMNS,
    FEATURE_VIEW_METRICS_PK,
    empty_categorical_metric,
    empty_numeric_metric,
    normalize_monitoring_row,
    opt_float,
)
from feast.repo_config import FeastConfigBaseModel, RepoConfig


def _read_data_source(data_source: DataSource, repo_path: str) -> Table:
    assert isinstance(data_source, FileSource)

    if isinstance(data_source.file_format, ParquetFormat):
        return ibis.read_parquet(data_source.path)
    elif isinstance(data_source.file_format, DeltaFormat):
        if data_source.s3_endpoint_override:
            storage_options = {
                "AWS_ENDPOINT_URL": data_source.s3_endpoint_override,
            }
            return ibis.read_delta(data_source.path, storage_options=storage_options)
        return ibis.read_delta(data_source.path)


def _write_data_source(
    table: Table,
    data_source: DataSource,
    repo_path: str,
    mode: str = "append",
    allow_overwrite: bool = False,
):
    assert isinstance(data_source, FileSource)

    file_options = data_source.file_options

    absolute_path = FileSource.get_uri_for_file_path(
        repo_path=repo_path, uri=file_options.uri
    )

    if (
        mode == "overwrite"
        and not allow_overwrite
        and os.path.exists(str(absolute_path))
    ):
        raise SavedDatasetLocationAlreadyExists(location=file_options.uri)

    if isinstance(data_source.file_format, ParquetFormat):
        if mode == "overwrite":
            table = table.to_pyarrow()

            filesystem, path = FileSource.create_filesystem_and_path(
                str(absolute_path),
                file_options.s3_endpoint_override,
            )

            if path.endswith(".parquet"):
                pyarrow.parquet.write_table(table, where=path, filesystem=filesystem)
            else:
                # otherwise assume destination is directory
                pyarrow.parquet.write_to_dataset(
                    table, root_path=path, filesystem=filesystem
                )
        elif mode == "append":
            table = table.to_pyarrow()
            prev_table = ibis.read_parquet(file_options.uri).to_pyarrow()
            if table.schema != prev_table.schema:
                table = table.cast(prev_table.schema)
            new_table = pyarrow.concat_tables([table, prev_table])
            ibis.memtable(new_table).to_parquet(file_options.uri)
    elif isinstance(data_source.file_format, DeltaFormat):
        storage_options = {
            "AWS_ENDPOINT_URL": str(data_source.s3_endpoint_override),
        }

        if mode == "append":
            from deltalake import DeltaTable

            prev_schema = (
                DeltaTable(file_options.uri, storage_options=storage_options)
                .schema()
                .to_pyarrow()
            )
            table = table.cast(ibis.Schema.from_pyarrow(prev_schema))
            write_mode = "append"
        elif mode == "overwrite":
            write_mode = (
                "overwrite"
                if allow_overwrite and os.path.exists(file_options.uri)
                else "error"
            )

        table.to_delta(
            file_options.uri, mode=write_mode, storage_options=storage_options
        )


# ------------------------------------------------------------------ #
#  DuckDB monitoring (Parquet-backed)
# ------------------------------------------------------------------ #

MONITORING_DIR = "feast_monitoring"
FEATURE_METRICS_FILE = "feature_metrics.parquet"
VIEW_METRICS_FILE = "feature_view_metrics.parquet"
SERVICE_METRICS_FILE = "feature_service_metrics.parquet"


def _duckdb_monitoring_base(config: RepoConfig) -> str:
    base = config.repo_path
    return str(base) if base else "."


def _duckdb_monitoring_path(config: RepoConfig, filename: str) -> str:
    return os.path.join(_duckdb_monitoring_base(config), MONITORING_DIR, filename)


def _duckdb_parquet_from_expression(config: RepoConfig, data_source: FileSource) -> str:
    absolute_path = FileSource.get_uri_for_file_path(
        repo_path=_duckdb_monitoring_base(config),
        uri=data_source.file_options.uri,
    )
    return str(absolute_path).replace("'", "''")


def _duckdb_quote_ident(name: str) -> str:
    return f'"{name}"'


def _duckdb_ts_where(ts_filter: str) -> str:
    return f"({ts_filter})" if (ts_filter and ts_filter.strip()) else "1=1"


def _duckdb_numeric_stats(
    conn: duckdb.DuckDBPyConnection,
    from_expr: str,
    feature_names: List[str],
    ts_filter: str,
    histogram_bins: int,
) -> List[Dict[str, Any]]:
    select_parts = ["COUNT(*)"]
    for col in feature_names:
        q = _duckdb_quote_ident(col)
        c = f"CAST({q} AS DOUBLE)"
        select_parts.extend(
            [
                f"COUNT({q})",
                f"AVG({c})",
                f"STDDEV_SAMP({c})",
                f"MIN({c})",
                f"MAX({c})",
                f"PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY {c})",
                f"PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY {c})",
                f"PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY {c})",
                f"PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY {c})",
                f"PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY {c})",
            ]
        )

    tw = _duckdb_ts_where(ts_filter)
    query = f"SELECT {', '.join(select_parts)} FROM {from_expr} AS _src WHERE {tw}"
    row = conn.execute(query).fetchone()

    if row is None:
        return [empty_numeric_metric(n) for n in feature_names]

    row_count = row[0]
    results: List[Dict[str, Any]] = []

    for i, col in enumerate(feature_names):
        base = 1 + i * 10
        non_null = row[base] or 0
        null_count = row_count - non_null

        min_val = opt_float(row[base + 3])
        max_val = opt_float(row[base + 4])

        result: Dict[str, Any] = {
            "feature_name": col,
            "feature_type": "numeric",
            "row_count": row_count,
            "null_count": null_count,
            "null_rate": null_count / row_count if row_count > 0 else 0.0,
            "mean": opt_float(row[base + 1]),
            "stddev": opt_float(row[base + 2]),
            "min_val": min_val,
            "max_val": max_val,
            "p50": opt_float(row[base + 5]),
            "p75": opt_float(row[base + 6]),
            "p90": opt_float(row[base + 7]),
            "p95": opt_float(row[base + 8]),
            "p99": opt_float(row[base + 9]),
            "histogram": None,
        }

        if min_val is not None and max_val is not None and non_null > 0:
            result["histogram"] = _duckdb_numeric_histogram(
                conn,
                from_expr,
                col,
                ts_filter,
                histogram_bins,
                min_val,
                max_val,
            )

        results.append(result)

    return results


def _duckdb_numeric_histogram(
    conn: duckdb.DuckDBPyConnection,
    from_expr: str,
    col_name: str,
    ts_filter: str,
    bins: int,
    min_val: float,
    max_val: float,
) -> Dict[str, Any]:
    q_col = _duckdb_quote_ident(col_name)

    tw = _duckdb_ts_where(ts_filter)
    if min_val == max_val:
        cnt = conn.execute(
            f"SELECT COUNT(*) FROM {from_expr} AS _src "
            f"WHERE {q_col} IS NOT NULL AND {tw}"
        ).fetchone()[0]
        return {"bins": [min_val, max_val], "counts": [cnt], "bin_width": 0.0}

    upper = max_val + (max_val - min_val) * 1e-10
    bin_width = (max_val - min_val) / bins

    query = (
        f"SELECT width_bucket(CAST({q_col} AS DOUBLE), {min_val}, {upper}, {bins}) AS bucket, "
        f"COUNT(*) AS cnt "
        f"FROM {from_expr} AS _src "
        f"WHERE {q_col} IS NOT NULL AND {tw} "
        f"GROUP BY bucket ORDER BY bucket"
    )
    rows = conn.execute(query).fetchall()

    counts = [0] * bins
    for bucket, cnt in rows:
        b = int(bucket)
        if 1 <= b <= bins:
            counts[b - 1] = cnt

    bin_edges = [min_val + i * bin_width for i in range(bins + 1)]
    return {
        "bins": [float(b) for b in bin_edges],
        "counts": counts,
        "bin_width": float(bin_width),
    }


def _duckdb_categorical_stats(
    conn: duckdb.DuckDBPyConnection,
    from_expr: str,
    col_name: str,
    ts_filter: str,
    top_n: int,
) -> Dict[str, Any]:
    q_col = _duckdb_quote_ident(col_name)

    tw = _duckdb_ts_where(ts_filter)
    query = (
        f"WITH filtered AS ("
        f"  SELECT * FROM {from_expr} AS _src WHERE {tw}"
        f") "
        f"SELECT "
        f"  (SELECT COUNT(*) FROM filtered) AS row_count, "
        f"  (SELECT COUNT(*) - COUNT({q_col}) FROM filtered) AS null_count, "
        f"  (SELECT COUNT(DISTINCT {q_col}) FROM filtered "
        f"   WHERE {q_col} IS NOT NULL) AS unique_count, "
        f"  CAST({q_col} AS VARCHAR) AS value, COUNT(*) AS cnt "
        f"FROM filtered WHERE {q_col} IS NOT NULL "
        f"GROUP BY {q_col} "
        f"ORDER BY cnt DESC LIMIT {int(top_n)}"
    )

    rows = conn.execute(query).fetchall()

    if not rows:
        return empty_categorical_metric(col_name)

    row_count = rows[0][0]
    null_count = rows[0][1]
    unique_count = rows[0][2]

    top_entries = [{"value": r[3], "count": r[4]} for r in rows]
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


def _duckdb_mon_table_meta(metric_type: str):
    if metric_type == "feature":
        return FEATURE_METRICS_FILE, FEATURE_METRICS_COLUMNS, FEATURE_METRICS_PK
    if metric_type == "feature_view":
        return VIEW_METRICS_FILE, FEATURE_VIEW_METRICS_COLUMNS, FEATURE_VIEW_METRICS_PK
    if metric_type == "feature_service":
        return (
            SERVICE_METRICS_FILE,
            FEATURE_SERVICE_METRICS_COLUMNS,
            FEATURE_SERVICE_METRICS_PK,
        )
    raise ValueError(f"Unknown metric_type '{metric_type}'")


def _duckdb_read_parquet_if_exists(path: str) -> Optional[pa.Table]:
    if not os.path.isfile(path):
        return None
    return pq.read_table(path)


def _duckdb_parquet_upsert(
    path: str,
    columns: List[str],
    pk_cols: List[str],
    new_rows: List[Dict[str, Any]],
) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)

    prepared: List[Dict[str, Any]] = []
    for row in new_rows:
        r = dict(row)
        if (
            "histogram" in r
            and r["histogram"] is not None
            and not isinstance(r["histogram"], str)
        ):
            r["histogram"] = json.dumps(r["histogram"])
        prepared.append(r)

    new_df = pd.DataFrame(prepared, columns=columns)
    existing = _duckdb_read_parquet_if_exists(path)
    if existing is not None:
        old_df = existing.to_pandas()
        combined = pd.concat([old_df, new_df], ignore_index=True)
    else:
        combined = new_df

    combined = combined.drop_duplicates(subset=pk_cols, keep="last")
    table = pa.Table.from_pandas(combined, preserve_index=False)
    pq.write_table(table, path)


def _duckdb_parquet_query(
    path: str,
    columns: List[str],
    project: str,
    filters: Optional[Dict[str, Any]],
    start_date: Optional[date],
    end_date: Optional[date],
) -> List[Dict[str, Any]]:
    tab = _duckdb_read_parquet_if_exists(path)
    if tab is None or tab.num_rows == 0:
        return []

    df = tab.to_pandas()
    df = df[df["project_id"] == project]
    if filters:
        for key, value in filters.items():
            if value is not None:
                df = df[df[key] == value]
    if start_date is not None:
        df = df[df["metric_date"] >= start_date]
    if end_date is not None:
        df = df[df["metric_date"] <= end_date]
    df = df.sort_values("metric_date", ascending=True)

    results = []
    for _, row in df.iterrows():
        record = {c: row.get(c) for c in columns}
        normalize_monitoring_row(record)
        for key in ("metric_date", "computed_at"):
            val = record.get(key)
            if (
                val is not None
                and not isinstance(val, str)
                and hasattr(val, "isoformat")
            ):
                record[key] = val.isoformat()
        results.append(record)

    return results


def _duckdb_sql_from_expression(config: RepoConfig, data_source: FileSource) -> str:
    p = _duckdb_parquet_from_expression(config, data_source)
    if isinstance(data_source.file_format, ParquetFormat):
        return f"read_parquet('{p}')"
    if isinstance(data_source.file_format, DeltaFormat):
        return f"delta_scan('{p}')"
    raise NotImplementedError(
        "DuckDB monitoring compute supports Parquet and Delta file sources only."
    )


class DuckDBOfflineStoreConfig(FeastConfigBaseModel):
    type: StrictStr = "duckdb"
    # """ Offline store type selector"""

    staging_location: Optional[str] = None

    staging_location_endpoint_override: Optional[str] = None


class DuckDBOfflineStore(OfflineStore):
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
        return pull_latest_from_table_or_query_ibis(
            config=config,
            data_source=data_source,
            join_key_columns=join_key_columns,
            feature_name_columns=feature_name_columns,
            timestamp_field=timestamp_field,
            created_timestamp_column=created_timestamp_column,
            start_date=start_date,
            end_date=end_date,
            data_source_reader=_read_data_source,
            data_source_writer=_write_data_source,
            staging_location=config.offline_store.staging_location,
            staging_location_endpoint_override=config.offline_store.staging_location_endpoint_override,
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
        return get_historical_features_ibis(
            config=config,
            feature_views=feature_views,
            feature_refs=feature_refs,
            entity_df=entity_df,
            registry=registry,
            project=project,
            full_feature_names=full_feature_names,
            data_source_reader=_read_data_source,
            data_source_writer=_write_data_source,
            staging_location=config.offline_store.staging_location,
            staging_location_endpoint_override=config.offline_store.staging_location_endpoint_override,
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
        return pull_all_from_table_or_query_ibis(
            config=config,
            data_source=data_source,
            join_key_columns=join_key_columns,
            feature_name_columns=feature_name_columns,
            timestamp_field=timestamp_field,
            created_timestamp_column=created_timestamp_column,
            start_date=start_date,
            end_date=end_date,
            data_source_reader=_read_data_source,
            data_source_writer=_write_data_source,
            staging_location=config.offline_store.staging_location,
            staging_location_endpoint_override=config.offline_store.staging_location_endpoint_override,
        )

    @staticmethod
    def offline_write_batch(
        config: RepoConfig,
        feature_view: FeatureView,
        table: pyarrow.Table,
        progress: Optional[Callable[[int], Any]],
    ):
        offline_write_batch_ibis(
            config=config,
            feature_view=feature_view,
            table=table,
            progress=progress,
            data_source_writer=_write_data_source,
        )

    @staticmethod
    def write_logged_features(
        config: RepoConfig,
        data: Union[pyarrow.Table, Path],
        source: LoggingSource,
        logging_config: LoggingConfig,
        registry: BaseRegistry,
    ):
        write_logged_features_ibis(
            config=config,
            data=data,
            source=source,
            logging_config=logging_config,
            registry=registry,
        )

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
        assert isinstance(config.offline_store, DuckDBOfflineStoreConfig)
        assert isinstance(data_source, FileSource)

        from_expr = _duckdb_sql_from_expression(config, data_source)
        ts_filter = get_timestamp_filter_sql(
            start_date,
            end_date,
            timestamp_field,
            tz=timezone.utc,
            cast_style="timestamp",
            date_time_separator=" ",
        )

        numeric_features = [n for n, t in feature_columns if t == "numeric"]
        categorical_features = [n for n, t in feature_columns if t == "categorical"]
        results: List[Dict[str, Any]] = []

        conn = duckdb.connect()
        if numeric_features:
            results.extend(
                _duckdb_numeric_stats(
                    conn,
                    from_expr,
                    numeric_features,
                    ts_filter,
                    histogram_bins,
                )
            )
        for col_name in categorical_features:
            results.append(
                _duckdb_categorical_stats(
                    conn,
                    from_expr,
                    col_name,
                    ts_filter,
                    top_n,
                )
            )
        conn.close()
        return results

    @staticmethod
    def get_monitoring_max_timestamp(
        config: RepoConfig,
        data_source: DataSource,
        timestamp_field: str,
    ) -> Optional[datetime]:
        assert isinstance(config.offline_store, DuckDBOfflineStoreConfig)
        assert isinstance(data_source, FileSource)

        from_expr = _duckdb_sql_from_expression(config, data_source)
        ts_col = _duckdb_quote_ident(timestamp_field)
        conn = duckdb.connect()
        row = conn.execute(
            f"SELECT MAX({ts_col}) AS m FROM {from_expr} AS _src"
        ).fetchone()
        conn.close()

        if row is None or row[0] is None:
            return None
        val = row[0]
        if isinstance(val, datetime):
            return val if val.tzinfo else val.replace(tzinfo=timezone.utc)
        if isinstance(val, date):
            return datetime.combine(val, datetime.min.time(), tzinfo=timezone.utc)
        return None

    @staticmethod
    def ensure_monitoring_tables(config: RepoConfig) -> None:
        assert isinstance(config.offline_store, DuckDBOfflineStoreConfig)
        base = os.path.join(_duckdb_monitoring_base(config), MONITORING_DIR)
        os.makedirs(base, exist_ok=True)

        tables = [
            (FEATURE_METRICS_FILE, FEATURE_METRICS_COLUMNS),
            (VIEW_METRICS_FILE, FEATURE_VIEW_METRICS_COLUMNS),
            (SERVICE_METRICS_FILE, FEATURE_SERVICE_METRICS_COLUMNS),
        ]
        for fname, columns in tables:
            path = _duckdb_monitoring_path(config, fname)
            if not os.path.isfile(path):
                os.makedirs(os.path.dirname(path), exist_ok=True)
                pd.DataFrame(columns=columns).to_parquet(path, index=False)

    @staticmethod
    def save_monitoring_metrics(
        config: RepoConfig,
        metric_type: str,
        metrics: List[Dict[str, Any]],
    ) -> None:
        if not metrics:
            return
        assert isinstance(config.offline_store, DuckDBOfflineStoreConfig)

        fname, columns, pk = _duckdb_mon_table_meta(metric_type)
        path = _duckdb_monitoring_path(config, fname)
        _duckdb_parquet_upsert(path, columns, pk, metrics)

    @staticmethod
    def query_monitoring_metrics(
        config: RepoConfig,
        project: str,
        metric_type: str,
        filters: Optional[Dict[str, Any]] = None,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
    ) -> List[Dict[str, Any]]:
        assert isinstance(config.offline_store, DuckDBOfflineStoreConfig)

        fname, columns, _ = _duckdb_mon_table_meta(metric_type)
        path = _duckdb_monitoring_path(config, fname)
        return _duckdb_parquet_query(
            path, columns, project, filters, start_date, end_date
        )

    @staticmethod
    def clear_monitoring_baseline(
        config: RepoConfig,
        project: str,
        feature_view_name: Optional[str] = None,
        feature_name: Optional[str] = None,
        data_source_type: Optional[str] = None,
    ) -> None:
        assert isinstance(config.offline_store, DuckDBOfflineStoreConfig)

        path = _duckdb_monitoring_path(config, FEATURE_METRICS_FILE)
        tab = _duckdb_read_parquet_if_exists(path)
        if tab is None or tab.num_rows == 0:
            return

        df = tab.to_pandas()
        mask = df["project_id"] == project
        if feature_view_name is not None:
            mask = mask & (df["feature_view_name"] == feature_view_name)
        if feature_name is not None:
            mask = mask & (df["feature_name"] == feature_name)
        if data_source_type is not None:
            mask = mask & (df["data_source_type"] == data_source_type)
        mask = mask & (df["is_baseline"].isin([True, 1]))
        df.loc[mask, "is_baseline"] = False
        pq.write_table(pa.Table.from_pandas(df, preserve_index=False), path)
