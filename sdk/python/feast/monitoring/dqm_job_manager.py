import json
import logging
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from psycopg import sql

from feast.infra.utils.postgres.connection_utils import _get_conn
from feast.infra.utils.postgres.postgres_config import PostgreSQLConfig

logger = logging.getLogger(__name__)

_JOBS_TABLE = "feast_monitoring_jobs"

JOB_STATUS_PENDING = "pending"
JOB_STATUS_RUNNING = "running"
JOB_STATUS_COMPLETED = "completed"
JOB_STATUS_FAILED = "failed"


class DQMJobManager:
    def __init__(self, config: PostgreSQLConfig):
        self._config = config

    def ensure_table(self) -> None:
        with _get_conn(self._config) as conn, conn.cursor() as cur:
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {_JOBS_TABLE} (
                    job_id            VARCHAR(36) PRIMARY KEY,
                    project_id        VARCHAR(255) NOT NULL,
                    feature_view_name VARCHAR(255),
                    job_type          VARCHAR(50)  NOT NULL,
                    status            VARCHAR(20)  NOT NULL DEFAULT 'pending',
                    parameters        JSONB,
                    created_at        TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
                    started_at        TIMESTAMPTZ,
                    completed_at      TIMESTAMPTZ,
                    error_message     TEXT,
                    result_summary    JSONB
                );
                CREATE INDEX IF NOT EXISTS idx_fm_jobs_status
                    ON {_JOBS_TABLE} (status);
                CREATE INDEX IF NOT EXISTS idx_fm_jobs_project
                    ON {_JOBS_TABLE} (project_id);
            """)
            conn.commit()

    def submit(
        self,
        project: str,
        job_type: str,
        feature_view_name: Optional[str] = None,
        parameters: Optional[Dict[str, Any]] = None,
    ) -> str:
        job_id = str(uuid.uuid4())
        now = datetime.now(timezone.utc)

        with _get_conn(self._config) as conn, conn.cursor() as cur:
            cur.execute(
                sql.SQL(
                    "INSERT INTO {} (job_id, project_id, feature_view_name, "
                    "job_type, status, parameters, created_at) "
                    "VALUES (%s, %s, %s, %s, %s, %s, %s)"
                ).format(sql.Identifier(_JOBS_TABLE)),
                (
                    job_id,
                    project,
                    feature_view_name,
                    job_type,
                    JOB_STATUS_PENDING,
                    json.dumps(parameters) if parameters else None,
                    now,
                ),
            )
            conn.commit()

        return job_id

    def get_job(self, job_id: str) -> Optional[Dict[str, Any]]:
        columns = [
            "job_id",
            "project_id",
            "feature_view_name",
            "job_type",
            "status",
            "parameters",
            "created_at",
            "started_at",
            "completed_at",
            "error_message",
            "result_summary",
        ]
        col_sql = sql.SQL(", ").join(sql.Identifier(c) for c in columns)

        with _get_conn(self._config) as conn, conn.cursor() as cur:
            conn.read_only = True
            cur.execute(
                sql.SQL("SELECT {} FROM {} WHERE job_id = %s").format(
                    col_sql, sql.Identifier(_JOBS_TABLE)
                ),
                (job_id,),
            )
            row = cur.fetchone()

        if row is None:
            return None

        record = dict(zip(columns, row))
        for key in ("parameters", "result_summary"):
            if isinstance(record.get(key), str):
                record[key] = json.loads(record[key])
        for key in ("created_at", "started_at", "completed_at"):
            if isinstance(record.get(key), datetime):
                record[key] = record[key].isoformat()
        return record

    def update_status(
        self,
        job_id: str,
        status: str,
        error_message: Optional[str] = None,
        result_summary: Optional[Dict[str, Any]] = None,
    ) -> None:
        now = datetime.now(timezone.utc)
        sets = [sql.SQL("status = %s")]
        params: list = [status]

        if status == JOB_STATUS_RUNNING:
            sets.append(sql.SQL("started_at = %s"))
            params.append(now)
        elif status in (JOB_STATUS_COMPLETED, JOB_STATUS_FAILED):
            sets.append(sql.SQL("completed_at = %s"))
            params.append(now)

        if error_message is not None:
            sets.append(sql.SQL("error_message = %s"))
            params.append(error_message)

        if result_summary is not None:
            sets.append(sql.SQL("result_summary = %s"))
            params.append(json.dumps(result_summary))

        params.append(job_id)

        query = sql.SQL("UPDATE {} SET {} WHERE job_id = %s").format(
            sql.Identifier(_JOBS_TABLE),
            sql.SQL(", ").join(sets),
        )

        with _get_conn(self._config) as conn, conn.cursor() as cur:
            cur.execute(query, params)
            conn.commit()

    def execute_job(self, job_id: str, monitoring_service) -> Dict[str, Any]:
        """Execute a DQM job synchronously. Manages status transitions."""
        job = self.get_job(job_id)
        if job is None:
            raise ValueError(f"Failed to find DQM job '{job_id}'")

        self.update_status(job_id, JOB_STATUS_RUNNING)

        try:
            params = job.get("parameters") or {}
            job_type = job["job_type"]
            project = job["project_id"]

            if job_type == "auto_compute":
                result = monitoring_service.auto_compute(
                    project=project,
                    feature_view_name=job.get("feature_view_name"),
                )
            elif job_type == "baseline":
                result = monitoring_service.compute_baseline(
                    project=project,
                    feature_view_name=job.get("feature_view_name"),
                    feature_names=params.get("feature_names"),
                )
            elif job_type == "compute":
                from datetime import date as date_type

                result = monitoring_service.compute_metrics(
                    project=project,
                    feature_view_name=job.get("feature_view_name"),
                    feature_names=params.get("feature_names"),
                    start_date=date_type.fromisoformat(params["start_date"])
                    if params.get("start_date")
                    else None,
                    end_date=date_type.fromisoformat(params["end_date"])
                    if params.get("end_date")
                    else None,
                    granularity=params.get("granularity", "daily"),
                )
            else:
                raise ValueError(f"Unknown job type '{job_type}'")

            self.update_status(job_id, JOB_STATUS_COMPLETED, result_summary=result)
            return result

        except Exception as e:
            self.update_status(job_id, JOB_STATUS_FAILED, error_message=str(e))
            raise
