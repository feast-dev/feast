import json
import logging
import uuid
from datetime import date, datetime, timezone
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)

JOB_STATUS_PENDING = "pending"
JOB_STATUS_RUNNING = "running"
JOB_STATUS_COMPLETED = "completed"
JOB_STATUS_FAILED = "failed"


class DQMJobManager:
    """DQM job manager that persists jobs via the offline store abstraction."""

    def __init__(self, offline_store, config):
        self._offline_store = offline_store
        self._config = config

    def ensure_table(self) -> None:
        self._offline_store.ensure_monitoring_tables(self._config)

    def submit(
        self,
        project: str,
        job_type: str,
        feature_view_name: Optional[str] = None,
        parameters: Optional[Dict[str, Any]] = None,
    ) -> str:
        job_id = str(uuid.uuid4())
        now = datetime.now(timezone.utc)
        row = {
            "job_id": job_id,
            "project_id": project,
            "feature_view_name": feature_view_name,
            "job_type": job_type,
            "status": JOB_STATUS_PENDING,
            "parameters": json.dumps(parameters) if parameters else None,
            "metric_date": now.date(),
            "started_at": None,
            "completed_at": None,
            "error_message": None,
            "result_summary": None,
        }
        self._offline_store.save_monitoring_metrics(self._config, "job", [row])
        return job_id

    def get_job(self, job_id: str) -> Optional[Dict[str, Any]]:
        rows = self._offline_store.query_monitoring_metrics(
            config=self._config,
            project="",
            metric_type="job",
            filters={"job_id": job_id},
        )
        if not rows:
            return None
        record = rows[0]
        for key in ("parameters", "result_summary"):
            val = record.get(key)
            if isinstance(val, str):
                try:
                    record[key] = json.loads(val)
                except (json.JSONDecodeError, TypeError):
                    pass
        return record

    def update_status(
        self,
        job_id: str,
        status: str,
        error_message: Optional[str] = None,
        result_summary: Optional[Dict[str, Any]] = None,
    ) -> None:
        job = self.get_job(job_id)
        if job is None:
            return

        now = datetime.now(timezone.utc)
        job["status"] = status

        if status == JOB_STATUS_RUNNING:
            job["started_at"] = now
        elif status in (JOB_STATUS_COMPLETED, JOB_STATUS_FAILED):
            job["completed_at"] = now

        if error_message is not None:
            job["error_message"] = error_message
        if result_summary is not None:
            job["result_summary"] = json.dumps(result_summary)

        if "parameters" in job and not isinstance(job["parameters"], str):
            job["parameters"] = (
                json.dumps(job["parameters"]) if job["parameters"] else None
            )

        if isinstance(job.get("metric_date"), str):
            job["metric_date"] = date.fromisoformat(job["metric_date"])

        self._offline_store.save_monitoring_metrics(self._config, "job", [job])

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
                result = monitoring_service.compute_metrics(
                    project=project,
                    feature_view_name=job.get("feature_view_name"),
                    feature_names=params.get("feature_names"),
                    start_date=date.fromisoformat(params["start_date"])
                    if params.get("start_date")
                    else None,
                    end_date=date.fromisoformat(params["end_date"])
                    if params.get("end_date")
                    else None,
                    granularity=params.get("granularity", "daily"),
                    set_baseline=params.get("set_baseline", False),
                )
            else:
                raise ValueError(f"Unknown job type '{job_type}'")

            self.update_status(job_id, JOB_STATUS_COMPLETED, result_summary=result)
            return result

        except Exception as e:
            self.update_status(job_id, JOB_STATUS_FAILED, error_message=str(e))
            raise
