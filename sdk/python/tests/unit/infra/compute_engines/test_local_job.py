from unittest.mock import MagicMock

import pytest

from feast.infra.common.materialization_job import MaterializationJobStatus
from feast.infra.compute_engines.local.job import (
    LocalMaterializationJob,
    LocalRetrievalJob,
)


class TestLocalMaterializationJob:
    def test_status(self):
        job = LocalMaterializationJob(
            job_id="test-job-1",
            status=MaterializationJobStatus.SUCCEEDED,
        )
        assert job.status() == MaterializationJobStatus.SUCCEEDED

    def test_job_id(self):
        job = LocalMaterializationJob(
            job_id="test-job-1",
            status=MaterializationJobStatus.RUNNING,
        )
        assert job.job_id() == "test-job-1"

    def test_error_none_by_default(self):
        job = LocalMaterializationJob(
            job_id="test-job-1",
            status=MaterializationJobStatus.SUCCEEDED,
        )
        assert job.error() is None

    def test_error_stored(self):
        err = RuntimeError("something failed")
        job = LocalMaterializationJob(
            job_id="test-job-1",
            status=MaterializationJobStatus.ERROR,
            error=err,
        )
        assert job.error() is err

    def test_should_not_be_retried(self):
        job = LocalMaterializationJob(
            job_id="test-job-1",
            status=MaterializationJobStatus.ERROR,
        )
        assert job.should_be_retried() is False

    def test_url_is_none(self):
        job = LocalMaterializationJob(
            job_id="test-job-1",
            status=MaterializationJobStatus.SUCCEEDED,
        )
        assert job.url() is None


class TestLocalRetrievalJob:
    def test_full_feature_names(self):
        job = LocalRetrievalJob(
            plan=None,
            context=MagicMock(),
            full_feature_names=True,
        )
        assert job.full_feature_names is True

    def test_full_feature_names_false(self):
        job = LocalRetrievalJob(
            plan=None,
            context=MagicMock(),
            full_feature_names=False,
        )
        assert job.full_feature_names is False

    def test_error_none_by_default(self):
        job = LocalRetrievalJob(plan=None, context=MagicMock())
        assert job.error() is None

    def test_error_stored(self):
        err = ValueError("bad data")
        job = LocalRetrievalJob(plan=None, context=MagicMock(), error=err)
        assert job.error() is err

    def test_on_demand_feature_views_default_empty(self):
        job = LocalRetrievalJob(plan=None, context=MagicMock())
        assert job.on_demand_feature_views == []

    def test_metadata_default_none(self):
        job = LocalRetrievalJob(plan=None, context=MagicMock())
        assert job.metadata is None

    def test_to_remote_storage_raises(self):
        job = LocalRetrievalJob(plan=None, context=MagicMock())
        with pytest.raises(NotImplementedError, match="Remote storage"):
            job.to_remote_storage()

    def test_to_sql_raises(self):
        job = LocalRetrievalJob(plan=None, context=MagicMock())
        with pytest.raises(NotImplementedError, match="SQL generation"):
            job.to_sql()
