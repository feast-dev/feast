import pytest

from feast.pyspark.launchers.gcloud import DataprocClusterLauncher


@pytest.fixture
def dataproc_launcher(pytestconfig) -> DataprocClusterLauncher:
    cluster_name = pytestconfig.getoption("--dataproc-cluster-name")
    region = pytestconfig.getoption("--dataproc-region")
    project_id = pytestconfig.getoption("--dataproc-project")
    staging_location = pytestconfig.getoption("--dataproc-staging-location")
    return DataprocClusterLauncher(
        cluster_name=cluster_name,
        staging_location=staging_location,
        region=region,
        project_id=project_id,
    )
