import pytest

from feast.pyspark.launchers.gcloud import DataprocClusterLauncher


@pytest.fixture
def dataproc_launcher(pytestconfig) -> DataprocClusterLauncher:
    cluster_name = pytestconfig.getoption("--dataproc-cluster-name")
    region = pytestconfig.getoption("--dataproc-region")
    project_id = pytestconfig.getoption("--dataproc-project")
    staging_location = pytestconfig.getoption("--dataproc-staging-location")
    executor_instances = pytestconfig.getoption("dataproc_executor_instances")
    executor_cores = pytestconfig.getoption("dataproc_executor_cores")
    executor_memory = pytestconfig.getoption("dataproc_executor_memory")
    return DataprocClusterLauncher(
        cluster_name=cluster_name,
        staging_location=staging_location,
        region=region,
        project_id=project_id,
        executor_instances=executor_instances,
        executor_cores=executor_cores,
        executor_memory=executor_memory,
        additional_options={}
    )
