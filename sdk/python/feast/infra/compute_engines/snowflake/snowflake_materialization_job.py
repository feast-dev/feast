from dataclasses import dataclass
from typing import Optional

from feast.infra.common.materialization_job import (
    MaterializationJob,
    MaterializationJobStatus,
)


@dataclass
class SnowflakeMaterializationJob(MaterializationJob):
    def __init__(
        self,
        job_id: str,
        status: MaterializationJobStatus,
        error: Optional[BaseException] = None,
    ) -> None:
        super().__init__()
        self._job_id: str = job_id
        self._status: MaterializationJobStatus = status
        self._error: Optional[BaseException] = error

    def status(self) -> MaterializationJobStatus:
        return self._status

    def error(self) -> Optional[BaseException]:
        return self._error

    def should_be_retried(self) -> bool:
        return False

    def job_id(self) -> str:
        return self._job_id

    def url(self) -> Optional[str]:
        return None
