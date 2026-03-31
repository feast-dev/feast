import json
import logging
from datetime import date, datetime
from typing import Any, Dict, List, Optional

from psycopg import sql

from feast.infra.utils.postgres.connection_utils import _get_conn
from feast.infra.utils.postgres.postgres_config import PostgreSQLConfig

logger = logging.getLogger(__name__)

VALID_GRANULARITIES = ("daily", "weekly", "biweekly", "monthly", "quarterly")

_FEATURE_METRICS_TABLE = "feast_monitoring_feature_metrics"
_FEATURE_VIEW_METRICS_TABLE = "feast_monitoring_feature_view_metrics"
_FEATURE_SERVICE_METRICS_TABLE = "feast_monitoring_feature_service_metrics"

_FEATURE_METRICS_COLUMNS = [
    "project_id",
    "feature_view_name",
    "feature_name",
    "metric_date",
    "granularity",
    "data_source_type",
    "computed_at",
    "is_baseline",
    "feature_type",
    "row_count",
    "null_count",
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
    "histogram",
]

_FEATURE_VIEW_METRICS_COLUMNS = [
    "project_id",
    "feature_view_name",
    "metric_date",
    "granularity",
    "data_source_type",
    "computed_at",
    "is_baseline",
    "total_row_count",
    "total_features",
    "features_with_nulls",
    "avg_null_rate",
    "max_null_rate",
]

_FEATURE_SERVICE_METRICS_COLUMNS = [
    "project_id",
    "feature_service_name",
    "metric_date",
    "granularity",
    "data_source_type",
    "computed_at",
    "is_baseline",
    "total_feature_views",
    "total_features",
    "avg_null_rate",
    "max_null_rate",
]


class MonitoringStore:
    def __init__(self, config: PostgreSQLConfig):
        self._config = config

    def ensure_tables(self) -> None:
        with _get_conn(self._config) as conn, conn.cursor() as cur:
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {_FEATURE_METRICS_TABLE} (
                    project_id        VARCHAR(255) NOT NULL,
                    feature_view_name VARCHAR(255) NOT NULL,
                    feature_name      VARCHAR(255) NOT NULL,
                    metric_date       DATE         NOT NULL,
                    granularity       VARCHAR(20)  NOT NULL DEFAULT 'daily',
                    data_source_type  VARCHAR(50)  NOT NULL DEFAULT 'batch',
                    computed_at       TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
                    is_baseline       BOOLEAN      NOT NULL DEFAULT FALSE,
                    feature_type      VARCHAR(50)  NOT NULL,
                    row_count         BIGINT,
                    null_count        BIGINT,
                    null_rate         DOUBLE PRECISION,
                    mean              DOUBLE PRECISION,
                    stddev            DOUBLE PRECISION,
                    min_val           DOUBLE PRECISION,
                    max_val           DOUBLE PRECISION,
                    p50               DOUBLE PRECISION,
                    p75               DOUBLE PRECISION,
                    p90               DOUBLE PRECISION,
                    p95               DOUBLE PRECISION,
                    p99               DOUBLE PRECISION,
                    histogram         JSONB,
                    PRIMARY KEY (project_id, feature_view_name, feature_name,
                                 metric_date, granularity, data_source_type)
                );
                CREATE INDEX IF NOT EXISTS idx_fm_feature_metrics_project
                    ON {_FEATURE_METRICS_TABLE} (project_id);
                CREATE INDEX IF NOT EXISTS idx_fm_feature_metrics_view
                    ON {_FEATURE_METRICS_TABLE} (project_id, feature_view_name);
                CREATE INDEX IF NOT EXISTS idx_fm_feature_metrics_date
                    ON {_FEATURE_METRICS_TABLE} (metric_date);
                CREATE INDEX IF NOT EXISTS idx_fm_feature_metrics_granularity
                    ON {_FEATURE_METRICS_TABLE} (granularity);
                CREATE INDEX IF NOT EXISTS idx_fm_feature_metrics_source_type
                    ON {_FEATURE_METRICS_TABLE} (data_source_type);
                CREATE INDEX IF NOT EXISTS idx_fm_feature_metrics_baseline
                    ON {_FEATURE_METRICS_TABLE} (project_id, feature_view_name, feature_name)
                    WHERE is_baseline = TRUE;
            """)

            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {_FEATURE_VIEW_METRICS_TABLE} (
                    project_id        VARCHAR(255) NOT NULL,
                    feature_view_name VARCHAR(255) NOT NULL,
                    metric_date       DATE         NOT NULL,
                    granularity       VARCHAR(20)  NOT NULL DEFAULT 'daily',
                    data_source_type  VARCHAR(50)  NOT NULL DEFAULT 'batch',
                    computed_at       TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
                    is_baseline       BOOLEAN      NOT NULL DEFAULT FALSE,
                    total_row_count   BIGINT,
                    total_features    INTEGER,
                    features_with_nulls INTEGER,
                    avg_null_rate     DOUBLE PRECISION,
                    max_null_rate     DOUBLE PRECISION,
                    PRIMARY KEY (project_id, feature_view_name, metric_date,
                                 granularity, data_source_type)
                );
            """)

            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {_FEATURE_SERVICE_METRICS_TABLE} (
                    project_id           VARCHAR(255) NOT NULL,
                    feature_service_name VARCHAR(255) NOT NULL,
                    metric_date          DATE         NOT NULL,
                    granularity          VARCHAR(20)  NOT NULL DEFAULT 'daily',
                    data_source_type     VARCHAR(50)  NOT NULL DEFAULT 'batch',
                    computed_at          TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
                    is_baseline          BOOLEAN      NOT NULL DEFAULT FALSE,
                    total_feature_views  INTEGER,
                    total_features       INTEGER,
                    avg_null_rate        DOUBLE PRECISION,
                    max_null_rate        DOUBLE PRECISION,
                    PRIMARY KEY (project_id, feature_service_name, metric_date,
                                 granularity, data_source_type)
                );
            """)
            conn.commit()

    def save_feature_metrics(self, metrics: List[Dict[str, Any]]) -> None:
        if not metrics:
            return
        self._upsert(
            _FEATURE_METRICS_TABLE,
            _FEATURE_METRICS_COLUMNS,
            [
                "project_id",
                "feature_view_name",
                "feature_name",
                "metric_date",
                "granularity",
                "data_source_type",
            ],
            metrics,
        )

    def save_feature_view_metrics(self, metrics: List[Dict[str, Any]]) -> None:
        if not metrics:
            return
        self._upsert(
            _FEATURE_VIEW_METRICS_TABLE,
            _FEATURE_VIEW_METRICS_COLUMNS,
            [
                "project_id",
                "feature_view_name",
                "metric_date",
                "granularity",
                "data_source_type",
            ],
            metrics,
        )

    def save_feature_service_metrics(self, metrics: List[Dict[str, Any]]) -> None:
        if not metrics:
            return
        self._upsert(
            _FEATURE_SERVICE_METRICS_TABLE,
            _FEATURE_SERVICE_METRICS_COLUMNS,
            [
                "project_id",
                "feature_service_name",
                "metric_date",
                "granularity",
                "data_source_type",
            ],
            metrics,
        )

    def get_feature_metrics(
        self,
        project: str,
        feature_view_name: Optional[str] = None,
        feature_name: Optional[str] = None,
        granularity: Optional[str] = None,
        data_source_type: Optional[str] = None,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
    ) -> List[Dict[str, Any]]:
        return self._query_metrics(
            _FEATURE_METRICS_TABLE,
            _FEATURE_METRICS_COLUMNS,
            project=project,
            filters={
                "feature_view_name": feature_view_name,
                "feature_name": feature_name,
                "granularity": granularity,
                "data_source_type": data_source_type,
            },
            start_date=start_date,
            end_date=end_date,
        )

    def get_feature_view_metrics(
        self,
        project: str,
        feature_view_name: Optional[str] = None,
        granularity: Optional[str] = None,
        data_source_type: Optional[str] = None,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
    ) -> List[Dict[str, Any]]:
        return self._query_metrics(
            _FEATURE_VIEW_METRICS_TABLE,
            _FEATURE_VIEW_METRICS_COLUMNS,
            project=project,
            filters={
                "feature_view_name": feature_view_name,
                "granularity": granularity,
                "data_source_type": data_source_type,
            },
            start_date=start_date,
            end_date=end_date,
        )

    def get_feature_service_metrics(
        self,
        project: str,
        feature_service_name: Optional[str] = None,
        granularity: Optional[str] = None,
        data_source_type: Optional[str] = None,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
    ) -> List[Dict[str, Any]]:
        return self._query_metrics(
            _FEATURE_SERVICE_METRICS_TABLE,
            _FEATURE_SERVICE_METRICS_COLUMNS,
            project=project,
            filters={
                "feature_service_name": feature_service_name,
                "granularity": granularity,
                "data_source_type": data_source_type,
            },
            start_date=start_date,
            end_date=end_date,
        )

    def get_baseline(
        self,
        project: str,
        feature_view_name: Optional[str] = None,
        feature_name: Optional[str] = None,
        data_source_type: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        return self._query_metrics(
            _FEATURE_METRICS_TABLE,
            _FEATURE_METRICS_COLUMNS,
            project=project,
            filters={
                "feature_view_name": feature_view_name,
                "feature_name": feature_name,
                "data_source_type": data_source_type,
                "is_baseline": True,
            },
        )

    def clear_baseline(
        self,
        project: str,
        feature_view_name: Optional[str] = None,
        feature_name: Optional[str] = None,
        data_source_type: Optional[str] = None,
    ) -> None:
        conditions = [sql.SQL("project_id = %s")]
        params: list = [project]

        if feature_view_name:
            conditions.append(sql.SQL("feature_view_name = %s"))
            params.append(feature_view_name)
        if feature_name:
            conditions.append(sql.SQL("feature_name = %s"))
            params.append(feature_name)
        if data_source_type:
            conditions.append(sql.SQL("data_source_type = %s"))
            params.append(data_source_type)

        conditions.append(sql.SQL("is_baseline = TRUE"))

        query = sql.SQL("UPDATE {} SET is_baseline = FALSE WHERE {}").format(
            sql.Identifier(_FEATURE_METRICS_TABLE),
            sql.SQL(" AND ").join(conditions),
        )

        with _get_conn(self._config) as conn, conn.cursor() as cur:
            cur.execute(query, params)
            conn.commit()

    # -- Private helpers --

    def _upsert(
        self,
        table: str,
        columns: List[str],
        pk_columns: List[str],
        rows: List[Dict[str, Any]],
    ) -> None:
        non_pk_columns = [c for c in columns if c not in pk_columns]

        col_identifiers = sql.SQL(", ").join(sql.Identifier(c) for c in columns)
        placeholders = sql.SQL(", ").join(sql.Placeholder() for _ in columns)
        update_clause = sql.SQL(", ").join(
            sql.SQL("{} = EXCLUDED.{}").format(sql.Identifier(c), sql.Identifier(c))
            for c in non_pk_columns
        )
        pk_identifiers = sql.SQL(", ").join(sql.Identifier(c) for c in pk_columns)

        query = sql.SQL(
            "INSERT INTO {} ({}) VALUES ({}) ON CONFLICT ({}) DO UPDATE SET {}"
        ).format(
            sql.Identifier(table),
            col_identifiers,
            placeholders,
            pk_identifiers,
            update_clause,
        )

        with _get_conn(self._config) as conn, conn.cursor() as cur:
            for row in rows:
                values = []
                for col in columns:
                    val = row.get(col)
                    if col == "histogram" and val is not None:
                        val = json.dumps(val)
                    values.append(val)
                cur.execute(query, values)
            conn.commit()

    def _query_metrics(
        self,
        table: str,
        columns: List[str],
        project: str,
        filters: Optional[Dict[str, Any]] = None,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
    ) -> List[Dict[str, Any]]:
        conditions = [sql.SQL("project_id = %s")]
        params: list = [project]

        if filters:
            for key, value in filters.items():
                if value is not None:
                    conditions.append(sql.SQL("{} = %s").format(sql.Identifier(key)))
                    params.append(value)

        if start_date:
            conditions.append(sql.SQL("metric_date >= %s"))
            params.append(start_date)
        if end_date:
            conditions.append(sql.SQL("metric_date <= %s"))
            params.append(end_date)

        col_identifiers = sql.SQL(", ").join(sql.Identifier(c) for c in columns)
        query = sql.SQL("SELECT {} FROM {} WHERE {} ORDER BY metric_date ASC").format(
            col_identifiers,
            sql.Identifier(table),
            sql.SQL(" AND ").join(conditions),
        )

        with _get_conn(self._config) as conn, conn.cursor() as cur:
            conn.read_only = True
            cur.execute(query, params)
            rows = cur.fetchall()

        results = []
        for row in rows:
            record = dict(zip(columns, row))
            if "histogram" in record and isinstance(record["histogram"], str):
                record["histogram"] = json.loads(record["histogram"])
            if "metric_date" in record and isinstance(record["metric_date"], date):
                record["metric_date"] = record["metric_date"].isoformat()
            if "computed_at" in record and isinstance(record["computed_at"], datetime):
                record["computed_at"] = record["computed_at"].isoformat()
            results.append(record)

        return results
