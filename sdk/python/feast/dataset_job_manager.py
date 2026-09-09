import threading
import uuid
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Callable, Dict, List, Optional


class JobStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


@dataclass
class DatasetJob:
    job_id: str
    name: str
    project: str
    status: JobStatus = JobStatus.PENDING
    created_at: str = ""
    completed_at: Optional[str] = None
    error: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "job_id": self.job_id,
            "dataset_name": self.name,
            "project": self.project,
            "status": self.status.value,
            "created_at": self.created_at,
            "completed_at": self.completed_at,
            "error": self.error,
        }


class DatasetJobManager:
    """Thread-safe in-memory job manager for dataset creation jobs."""

    def __init__(self, max_workers: int = 2):
        self._jobs: Dict[str, DatasetJob] = {}
        self._lock = threading.Lock()
        self._executor = ThreadPoolExecutor(
            max_workers=max_workers, thread_name_prefix="dataset-job"
        )

    def submit_job(
        self,
        name: str,
        project: str,
        task_fn: Callable[[DatasetJob], None],
        metadata: Optional[Dict[str, Any]] = None,
    ) -> DatasetJob:
        job_id = str(uuid.uuid4())
        job = DatasetJob(
            job_id=job_id,
            name=name,
            project=project,
            status=JobStatus.PENDING,
            created_at=datetime.now(timezone.utc).isoformat(),
            metadata=metadata or {},
        )

        with self._lock:
            self._jobs[job_id] = job

        self._executor.submit(self._run_job, job, task_fn)
        return job

    def _run_job(self, job: DatasetJob, task_fn: Callable[[DatasetJob], None]):
        with self._lock:
            job.status = JobStatus.RUNNING

        try:
            task_fn(job)
            with self._lock:
                job.status = JobStatus.COMPLETED
                job.completed_at = datetime.now(timezone.utc).isoformat()
        except Exception as e:
            with self._lock:
                job.status = JobStatus.FAILED
                job.completed_at = datetime.now(timezone.utc).isoformat()
                job.error = str(e)

    def get_job(self, job_id: str) -> Optional[DatasetJob]:
        with self._lock:
            return self._jobs.get(job_id)

    def list_jobs(
        self,
        project: Optional[str] = None,
        status: Optional[JobStatus] = None,
    ) -> List[DatasetJob]:
        with self._lock:
            jobs = list(self._jobs.values())

        if project:
            jobs = [j for j in jobs if j.project == project]
        if status:
            jobs = [j for j in jobs if j.status == status]

        return sorted(jobs, key=lambda j: j.created_at, reverse=True)

    def cleanup_completed(self, max_age_seconds: int = 3600):
        """Remove completed/failed jobs older than max_age_seconds."""
        now = datetime.now(timezone.utc)
        with self._lock:
            to_remove = []
            for job_id, job in self._jobs.items():
                if job.status in (JobStatus.COMPLETED, JobStatus.FAILED):
                    if job.completed_at:
                        completed = datetime.fromisoformat(job.completed_at)
                        if (now - completed).total_seconds() > max_age_seconds:
                            to_remove.append(job_id)
            for job_id in to_remove:
                del self._jobs[job_id]

    def shutdown(self):
        self._executor.shutdown(wait=False)


# Singleton instance for use across the REST API
_dataset_job_manager: Optional[DatasetJobManager] = None
_manager_lock = threading.Lock()


def get_dataset_job_manager() -> DatasetJobManager:
    global _dataset_job_manager
    if _dataset_job_manager is None:
        with _manager_lock:
            if _dataset_job_manager is None:
                _dataset_job_manager = DatasetJobManager()
    return _dataset_job_manager
