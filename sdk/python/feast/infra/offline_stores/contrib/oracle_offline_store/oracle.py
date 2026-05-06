import json
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Literal, Optional, Tuple, Union

import ibis
import pandas as pd
import pyarrow
from ibis.expr.types import Table
from pydantic import StrictInt, StrictStr, model_validator

from feast.data_source import DataSource
from feast.feature_logging import LoggingConfig, LoggingSource
from feast.feature_view import FeatureView
from feast.infra.offline_stores.contrib.oracle_offline_store.oracle_source import (
    OracleSource,
)
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
    MON_TABLE_FEATURE,
    MON_TABLE_FEATURE_SERVICE,
    MON_TABLE_FEATURE_VIEW,
    empty_categorical_metric,
    empty_numeric_metric,
    monitoring_table_meta,
    normalize_monitoring_row,
    opt_float,
)
from feast.repo_config import FeastConfigBaseModel, RepoConfig
from feast.utils import compute_non_entity_date_range


def get_ibis_connection(config: RepoConfig):
    """Create an ibis Oracle connection from the offline store config."""
    offline_config = config.offline_store
    assert isinstance(offline_config, OracleOfflineStoreConfig)

    kwargs = {}
    if offline_config.service_name:
        kwargs["service_name"] = offline_config.service_name
    if offline_config.sid:
        kwargs["sid"] = offline_config.sid
    if offline_config.database:
        kwargs["database"] = offline_config.database
    if offline_config.dsn:
        kwargs["dsn"] = offline_config.dsn

    return ibis.oracle.connect(
        user=offline_config.user,
        password=offline_config.password,
        host=offline_config.host,
        port=offline_config.port,
        **kwargs,
    )


def _read_oracle_table(con, data_source: DataSource) -> Table:
    """Read an Oracle table via ibis.

    Column names are returned exactly as Oracle stores them.  The user is
    expected to reference columns using the same casing shown by Oracle
    (e.g. ``USER_ID`` for unquoted identifiers, ``CamelCase`` for quoted).
    """
    assert isinstance(data_source, OracleSource)
    table = con.table(data_source.table_ref)

    # Cast Oracle DATE columns (ibis date → timestamp) to preserve time.
    casts = {}
    for col_name in table.columns:
        if table[col_name].type().is_date():
            casts[col_name] = table[col_name].cast("timestamp")
    if casts:
        table = table.mutate(**casts)

    return table


def _build_data_source_reader(config: RepoConfig, con=None):
    """Build a reader that returns Oracle-backend ibis tables."""
    if con is None:
        con = get_ibis_connection(config)

    def _read_data_source(data_source: DataSource, repo_path: str = "") -> Table:
        return _read_oracle_table(con, data_source)

    return _read_data_source


def _build_data_source_writer(config: RepoConfig, con=None):
    """Build a function that writes data to an Oracle table via ibis."""
    if con is None:
        con = get_ibis_connection(config)

    def _write_data_source(
        table: Table,
        data_source: DataSource,
        repo_path: str = "",
        mode: str = "append",
        allow_overwrite: bool = False,
    ):
        assert isinstance(data_source, OracleSource)
        table_ref = data_source.table_ref

        if mode == "overwrite":
            if not allow_overwrite:
                raise ValueError(
                    f"Table '{table_ref}' already exists. "
                    f"Set allow_overwrite=True to truncate and replace data."
                )
            con.truncate_table(table_ref)

        con.insert(table_name=table_ref, obj=table.to_pandas())

    return _write_data_source


class OracleOfflineStoreConfig(FeastConfigBaseModel):
    """Offline store config for Oracle Database"""

    type: Literal["oracle"] = "oracle"
    """Offline store type selector"""

    user: StrictStr
    """Oracle database user"""

    password: StrictStr
    """Oracle database password"""

    host: StrictStr = "localhost"
    """Oracle database host"""

    port: StrictInt = 1521
    """Oracle database port"""

    service_name: Optional[StrictStr] = None
    """Oracle service name (mutually exclusive with sid and dsn)"""

    sid: Optional[StrictStr] = None
    """Oracle SID (mutually exclusive with service_name and dsn)"""

    database: Optional[StrictStr] = None
    """Oracle database name"""

    dsn: Optional[StrictStr] = None
    """Oracle DSN string (mutually exclusive with service_name and sid)"""

    @model_validator(mode="after")
    def _validate_connection_params(self):
        exclusive = [
            f for f in ("service_name", "sid", "dsn") if getattr(self, f) is not None
        ]
        if len(exclusive) > 1:
            raise ValueError(
                f"Only one of 'service_name', 'sid', or 'dsn' may be set, "
                f"but got: {', '.join(exclusive)}"
            )
        return self


def _build_entity_df_from_feature_sources(
    con,
    feature_views: List[FeatureView],
    start_date: datetime,
    end_date: datetime,
) -> pd.DataFrame:
    """Build a synthetic entity DataFrame by scanning each feature source within a date range."""
    entity_dfs = []
    for fv in feature_views:
        source = fv.batch_source
        assert source is not None, f"Feature view '{fv.name}' has no batch_source"
        table = _read_oracle_table(con, source)
        ts_col = source.timestamp_field
        join_keys = [e.name for e in fv.entity_columns]
        cols = join_keys + [ts_col]
        sub = table.filter(
            (table[ts_col] >= ibis.literal(start_date))
            & (table[ts_col] <= ibis.literal(end_date))
        ).select(cols)
        sub = sub.rename({"event_timestamp": ts_col})
        entity_dfs.append(sub.execute())

    return pd.concat(entity_dfs, ignore_index=True).drop_duplicates()


# ------------------------------------------------------------------ #
#  Oracle monitoring helpers
# ------------------------------------------------------------------ #


def _oracle_quote_ident(name: str) -> str:
    return f'"{name}"'


def _oracle_ts_where(ts_filter: str) -> str:
    return f"({ts_filter})" if (ts_filter and ts_filter.strip()) else "1=1"


def _oracle_fetchall(con, sql: str):
    cur = con.raw_sql(sql)
    try:
        return cur.fetchall()
    finally:
        if hasattr(cur, "close"):
            cur.close()


def _oracle_exec(con, sql: str) -> None:
    cur = con.raw_sql(sql)
    try:
        pass
    finally:
        if hasattr(cur, "close"):
            cur.close()


def _oracle_numeric_histogram(
    con,
    from_expression: str,
    col_name: str,
    ts_filter: str,
    bins: int,
    min_val: float,
    max_val: float,
) -> Dict[str, Any]:
    q_col = _oracle_quote_ident(col_name)

    if min_val == max_val:
        tw = _oracle_ts_where(ts_filter)
        cnt_row = _oracle_fetchall(
            con,
            f"SELECT COUNT(*) FROM {from_expression} _src "
            f"WHERE {q_col} IS NOT NULL AND {tw}",
        )
        cnt = (cnt_row[0][0] if cnt_row else 0) or 0
        return {"bins": [min_val, max_val], "counts": [cnt], "bin_width": 0.0}

    upper = max_val + (max_val - min_val) * 1e-10
    bin_width = (max_val - min_val) / bins
    bw = bin_width if bin_width != 0 else 1e-300

    tw = _oracle_ts_where(ts_filter)
    query = (
        f"SELECT bucket, COUNT(*) AS cnt FROM ("
        f"  SELECT "
        f"    CASE WHEN {q_col} IS NULL THEN NULL "
        f"    WHEN {min_val} = {upper} THEN 1 "
        f"    ELSE LEAST(GREATEST("
        f"      FLOOR((CAST({q_col} AS NUMBER) - {min_val}) / {bw}) + 1, "
        f"      1), {bins}) "
        f"    END AS bucket "
        f"  FROM {from_expression} _src "
        f"  WHERE {q_col} IS NOT NULL AND {tw}"
        f") sub "
        f"WHERE bucket IS NOT NULL "
        f"GROUP BY bucket ORDER BY bucket"
    )

    rows = _oracle_fetchall(con, query)
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


def _oracle_numeric_stats(
    con,
    from_expression: str,
    feature_names: List[str],
    ts_filter: str,
    histogram_bins: int,
) -> List[Dict[str, Any]]:
    select_parts = ["COUNT(*)"]
    for col in feature_names:
        q = _oracle_quote_ident(col)
        c = f"CAST({q} AS NUMBER)"
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

    tw = _oracle_ts_where(ts_filter)
    query = f"SELECT {', '.join(select_parts)} FROM {from_expression} _src WHERE {tw}"

    row = (_oracle_fetchall(con, query) or [None])[0]

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
            result["histogram"] = _oracle_numeric_histogram(
                con,
                from_expression,
                col,
                ts_filter,
                histogram_bins,
                min_val,
                max_val,
            )

        results.append(result)

    return results


def _oracle_categorical_stats(
    con,
    from_expression: str,
    col_name: str,
    ts_filter: str,
    top_n: int,
) -> Dict[str, Any]:
    q_col = _oracle_quote_ident(col_name)

    tw = _oracle_ts_where(ts_filter)
    query = (
        f"WITH filtered AS ("
        f"  SELECT * FROM {from_expression} _src WHERE {tw}"
        f") "
        f"SELECT "
        f"  (SELECT COUNT(*) FROM filtered) AS row_count, "
        f"  (SELECT COUNT(*) - COUNT({q_col}) FROM filtered) AS null_count, "
        f"  (SELECT COUNT(DISTINCT {q_col}) FROM filtered "
        f"   WHERE {q_col} IS NOT NULL) AS unique_count, "
        f"  TO_CHAR({q_col}) AS value, COUNT(*) AS cnt "
        f"FROM filtered WHERE {q_col} IS NOT NULL "
        f"GROUP BY {q_col} "
        f"ORDER BY cnt DESC "
        f"FETCH FIRST {int(top_n)} ROWS ONLY"
    )

    rows = _oracle_fetchall(con, query)

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


def _oracle_escape_literal(val: Any) -> str:
    if val is None:
        return "NULL"
    if isinstance(val, bool):
        return "1" if val else "0"
    if isinstance(val, (int, float)) and not isinstance(val, bool):
        return str(val)
    if isinstance(val, datetime):
        s = val.isoformat(sep=" ", timespec="seconds")
        return f"TIMESTAMP '{s}'"
    if isinstance(val, date):
        return f"DATE '{val.isoformat()}'"
    s = str(val).replace("'", "''")
    return f"'{s}'"


def _oracle_merge_metric_row(
    con, table: str, columns: List[str], pk_cols: List[str], row: Dict[str, Any]
) -> None:
    def qid(c: str) -> str:
        return f'"{c}"'

    non_pk = [c for c in columns if c not in pk_cols]
    vals = []
    for c in columns:
        v = row.get(c)
        if c == "histogram" and v is not None and not isinstance(v, str):
            v = json.dumps(v)
        vals.append(_oracle_escape_literal(v))

    join_cond = " AND ".join(f"t.{qid(c)} = s.{qid(c)}" for c in pk_cols)
    insert_cols = ", ".join(qid(c) for c in columns)
    insert_vals = ", ".join(f"s.{qid(c)}" for c in columns)
    update_set = ", ".join(f"t.{qid(c)} = s.{qid(c)}" for c in non_pk)

    src_select = ", ".join(
        f"{vals[i]} AS {qid(columns[i])}" for i in range(len(columns))
    )

    sql = (
        f"MERGE INTO {table} t "
        f"USING (SELECT {src_select} FROM dual) s "
        f"ON ({join_cond}) "
        f"WHEN MATCHED THEN UPDATE SET {update_set} "
        f"WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})"
    )
    _oracle_exec(con, sql)


def _oracle_try_execute_ddl(con, ddl: str) -> None:
    """Run DDL; ignore ORA-00955 (name already used)."""
    escaped = ddl.strip().replace("'", "''")
    plsql = (
        "BEGIN\n"
        f"  EXECUTE IMMEDIATE '{escaped}';\n"
        "EXCEPTION\n"
        "  WHEN OTHERS THEN\n"
        "    IF SQLCODE != -955 THEN RAISE;\n"
        "    END IF;\n"
        "END;"
    )
    _oracle_exec(con, plsql)


class OracleOfflineStore(OfflineStore):
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
        con = get_ibis_connection(config)

        return pull_latest_from_table_or_query_ibis(
            config=config,
            data_source=data_source,
            join_key_columns=join_key_columns,
            feature_name_columns=feature_name_columns,
            timestamp_field=timestamp_field,
            created_timestamp_column=created_timestamp_column,
            start_date=start_date,
            end_date=end_date,
            data_source_reader=_build_data_source_reader(config, con=con),
            data_source_writer=_build_data_source_writer(config, con=con),
        )

    @staticmethod
    def get_historical_features(
        config: RepoConfig,
        feature_views: List[FeatureView],
        feature_refs: List[str],
        entity_df: Optional[Union[pd.DataFrame, str]],
        registry: BaseRegistry,
        project: str,
        full_feature_names: bool = False,
        **kwargs,
    ) -> RetrievalJob:
        if not feature_views:
            raise ValueError("feature_views must not be empty")

        # Single connection reused across the entire call.
        con = get_ibis_connection(config)

        # Handle non-entity retrieval mode (start_date/end_date only)
        if entity_df is None:
            start_date, end_date = compute_non_entity_date_range(
                feature_views,
                start_date=kwargs.get("start_date"),
                end_date=kwargs.get("end_date"),
            )
            entity_df = _build_entity_df_from_feature_sources(
                con, feature_views, start_date, end_date
            )

        # If entity_df is a SQL string, execute it to get a DataFrame
        if isinstance(entity_df, str):
            entity_df = con.sql(entity_df).execute()

        return get_historical_features_ibis(
            config=config,
            feature_views=feature_views,
            feature_refs=feature_refs,
            entity_df=entity_df,
            registry=registry,
            project=project,
            full_feature_names=full_feature_names,
            data_source_reader=_build_data_source_reader(config, con=con),
            data_source_writer=_build_data_source_writer(config, con=con),
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
        con = get_ibis_connection(config)

        return pull_all_from_table_or_query_ibis(
            config=config,
            data_source=data_source,
            join_key_columns=join_key_columns,
            feature_name_columns=feature_name_columns,
            timestamp_field=timestamp_field,
            created_timestamp_column=created_timestamp_column,
            start_date=start_date,
            end_date=end_date,
            data_source_reader=_build_data_source_reader(config, con=con),
            data_source_writer=_build_data_source_writer(config, con=con),
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
            data_source_writer=_build_data_source_writer(config),
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
        assert isinstance(config.offline_store, OracleOfflineStoreConfig)
        assert isinstance(data_source, OracleSource)

        from_expression = data_source.get_table_query_string()
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

        con = get_ibis_connection(config)

        if numeric_features:
            results.extend(
                _oracle_numeric_stats(
                    con,
                    from_expression,
                    numeric_features,
                    ts_filter,
                    histogram_bins,
                )
            )

        for col_name in categorical_features:
            results.append(
                _oracle_categorical_stats(
                    con,
                    from_expression,
                    col_name,
                    ts_filter,
                    top_n,
                )
            )

        return results

    @staticmethod
    def get_monitoring_max_timestamp(
        config: RepoConfig,
        data_source: DataSource,
        timestamp_field: str,
    ) -> Optional[datetime]:
        assert isinstance(config.offline_store, OracleOfflineStoreConfig)
        assert isinstance(data_source, OracleSource)

        from_expression = data_source.get_table_query_string()
        ts_col = _oracle_quote_ident(timestamp_field)

        con = get_ibis_connection(config)
        rows = _oracle_fetchall(
            con,
            f"SELECT MAX({ts_col}) FROM {from_expression} _src",
        )
        row = rows[0] if rows else None

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
        assert isinstance(config.offline_store, OracleOfflineStoreConfig)
        con = get_ibis_connection(config)

        _oracle_try_execute_ddl(
            con,
            f"""
            CREATE TABLE {MON_TABLE_FEATURE} (
              project_id         VARCHAR2(255) NOT NULL,
              feature_view_name  VARCHAR2(255) NOT NULL,
              feature_name       VARCHAR2(255) NOT NULL,
              metric_date        DATE NOT NULL,
              granularity        VARCHAR2(20) DEFAULT 'daily' NOT NULL,
              data_source_type   VARCHAR2(50) DEFAULT 'batch' NOT NULL,
              computed_at        TIMESTAMP WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL,
              is_baseline        NUMBER(1) DEFAULT 0 NOT NULL,
              feature_type       VARCHAR2(50) NOT NULL,
              row_count          NUMBER,
              null_count         NUMBER,
              null_rate          NUMBER,
              mean               NUMBER,
              stddev             NUMBER,
              min_val            NUMBER,
              max_val            NUMBER,
              p50                NUMBER,
              p75                NUMBER,
              p90                NUMBER,
              p95                NUMBER,
              p99                NUMBER,
              histogram          VARCHAR2(4000),
              CONSTRAINT pk_fm PRIMARY KEY (project_id, feature_view_name,
                feature_name, metric_date, granularity, data_source_type)
            )
            """,
        )

        _oracle_try_execute_ddl(
            con,
            f"""
            CREATE TABLE {MON_TABLE_FEATURE_VIEW} (
              project_id         VARCHAR2(255) NOT NULL,
              feature_view_name  VARCHAR2(255) NOT NULL,
              metric_date        DATE NOT NULL,
              granularity        VARCHAR2(20) DEFAULT 'daily' NOT NULL,
              data_source_type   VARCHAR2(50) DEFAULT 'batch' NOT NULL,
              computed_at        TIMESTAMP WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL,
              is_baseline        NUMBER(1) DEFAULT 0 NOT NULL,
              total_row_count    NUMBER,
              total_features     NUMBER,
              features_with_nulls NUMBER,
              avg_null_rate      NUMBER,
              max_null_rate      NUMBER,
              CONSTRAINT pk_fvm PRIMARY KEY (project_id, feature_view_name,
                metric_date, granularity, data_source_type)
            )
            """,
        )

        _oracle_try_execute_ddl(
            con,
            f"""
            CREATE TABLE {MON_TABLE_FEATURE_SERVICE} (
              project_id           VARCHAR2(255) NOT NULL,
              feature_service_name VARCHAR2(255) NOT NULL,
              metric_date          DATE NOT NULL,
              granularity          VARCHAR2(20) DEFAULT 'daily' NOT NULL,
              data_source_type     VARCHAR2(50) DEFAULT 'batch' NOT NULL,
              computed_at          TIMESTAMP WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL,
              is_baseline          NUMBER(1) DEFAULT 0 NOT NULL,
              total_feature_views  NUMBER,
              total_features       NUMBER,
              avg_null_rate        NUMBER,
              max_null_rate        NUMBER,
              CONSTRAINT pk_fsm PRIMARY KEY (project_id, feature_service_name,
                metric_date, granularity, data_source_type)
            )
            """,
        )

    @staticmethod
    def save_monitoring_metrics(
        config: RepoConfig,
        metric_type: str,
        metrics: List[Dict[str, Any]],
    ) -> None:
        if not metrics:
            return
        assert isinstance(config.offline_store, OracleOfflineStoreConfig)

        table, columns, pk_columns = monitoring_table_meta(metric_type)
        con = get_ibis_connection(config)
        for row in metrics:
            _oracle_merge_metric_row(con, table, columns, pk_columns, row)

    @staticmethod
    def query_monitoring_metrics(
        config: RepoConfig,
        project: str,
        metric_type: str,
        filters: Optional[Dict[str, Any]] = None,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
    ) -> List[Dict[str, Any]]:
        assert isinstance(config.offline_store, OracleOfflineStoreConfig)

        table, columns, _ = monitoring_table_meta(metric_type)

        conditions = [
            f"{_oracle_quote_ident('project_id')} = {_oracle_escape_literal(project)}"
        ]
        if filters:
            for key, value in filters.items():
                if value is not None:
                    conditions.append(
                        f"{_oracle_quote_ident(key)} = {_oracle_escape_literal(value)}"
                    )
        if start_date:
            conditions.append(
                f"{_oracle_quote_ident('metric_date')} >= {_oracle_escape_literal(start_date)}"
            )
        if end_date:
            conditions.append(
                f"{_oracle_quote_ident('metric_date')} <= {_oracle_escape_literal(end_date)}"
            )

        where_sql = " AND ".join(conditions)
        col_list = ", ".join(_oracle_quote_ident(c) for c in columns)

        con = get_ibis_connection(config)
        rows = _oracle_fetchall(
            con,
            f"SELECT {col_list} FROM {table} WHERE {where_sql} "
            f"ORDER BY {_oracle_quote_ident('metric_date')} ASC",
        )

        results = []
        for row in rows:
            record = dict(zip(columns, row))
            results.append(normalize_monitoring_row(record))

        return results

    @staticmethod
    def clear_monitoring_baseline(
        config: RepoConfig,
        project: str,
        feature_view_name: Optional[str] = None,
        feature_name: Optional[str] = None,
        data_source_type: Optional[str] = None,
    ) -> None:
        assert isinstance(config.offline_store, OracleOfflineStoreConfig)

        conditions = [
            f"{_oracle_quote_ident('project_id')} = {_oracle_escape_literal(project)}"
        ]
        if feature_view_name:
            conditions.append(
                f"{_oracle_quote_ident('feature_view_name')} = "
                f"{_oracle_escape_literal(feature_view_name)}"
            )
        if feature_name:
            conditions.append(
                f"{_oracle_quote_ident('feature_name')} = "
                f"{_oracle_escape_literal(feature_name)}"
            )
        if data_source_type:
            conditions.append(
                f"{_oracle_quote_ident('data_source_type')} = "
                f"{_oracle_escape_literal(data_source_type)}"
            )
        conditions.append(f"{_oracle_quote_ident('is_baseline')} = 1")

        where_sql = " AND ".join(conditions)
        con = get_ibis_connection(config)
        _oracle_exec(
            con,
            f"UPDATE {MON_TABLE_FEATURE} SET {_oracle_quote_ident('is_baseline')} = 0 "
            f"WHERE {where_sql}",
        )
