import time

from feast.pyspark.abc import RetrievalJobParameters, SparkJobStatus, SparkJob
from feast.pyspark.launchers.gcloud import DataprocClusterLauncher

from .fixtures.job_parameters import customer_entity  # noqa: F401
from .fixtures.job_parameters import customer_feature  # noqa: F401
from .fixtures.job_parameters import dataproc_retrieval_job_params  # noqa: F401
from .fixtures.launchers import dataproc_launcher  # noqa: F401


def wait_for_job_status(job: SparkJob, expected_status: SparkJobStatus, max_retry: int = 4, retry_interval: int = 5):
    for i in range(max_retry):
        if job.get_status() == expected_status:
            return
        time.sleep(retry_interval)
    raise ValueError(f"Timeout waiting for job status to become {expected_status.name}")

def test_dataproc_job_api(
    dataproc_launcher: DataprocClusterLauncher,  # noqa: F811
    dataproc_retrieval_job_params: RetrievalJobParameters,  # noqa: F811
):
    job = dataproc_launcher.historical_feature_retrieval(dataproc_retrieval_job_params)
    job_id = job.get_id()
    retrieved_job = dataproc_launcher.get_job_by_id(job_id)
    assert retrieved_job.get_id() == job_id
    status = retrieved_job.get_status()
    assert status in [
        SparkJobStatus.STARTING,
        SparkJobStatus.IN_PROGRESS,
        SparkJobStatus.COMPLETED,
    ]
    active_job_ids = [
        job.get_id() for job in dataproc_launcher.list_jobs(include_terminated=False)
    ]
    assert job_id in active_job_ids
    wait_for_job_status(retrieved_job, SparkJobStatus.IN_PROGRESS)
    retrieved_job.cancel()
    assert retrieved_job.get_status() == SparkJobStatus.FAILED
