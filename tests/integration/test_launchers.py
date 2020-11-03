from time import sleep

from feast.pyspark.abc import RetrievalJobParameters, SparkJobStatus
from feast.pyspark.launchers.gcloud import DataprocClusterLauncher

from .fixtures.job_parameters import customer_entity  # noqa: F401
from .fixtures.job_parameters import customer_feature  # noqa: F401
from .fixtures.job_parameters import dataproc_retrieval_job_params  # noqa: F401
from .fixtures.launchers import dataproc_launcher  # noqa: F401


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
    retrieved_job.cancel()
    assert retrieved_job.get_status() == SparkJobStatus.FAILED
