from datetime import datetime
from unittest.mock import MagicMock

from feast.infra.common.materialization_job import (
    MaterializationJobStatus,
    MaterializationTask,
)


class TestMaterializationJobStatus:
    def test_all_statuses_defined(self):
        expected = {
            "WAITING",
            "RUNNING",
            "AVAILABLE",
            "ERROR",
            "CANCELLING",
            "CANCELLED",
            "SUCCEEDED",
            "PAUSED",
            "RETRYING",
        }
        actual = {s.name for s in MaterializationJobStatus}
        assert actual == expected


class TestMaterializationTask:
    def test_creation(self):
        mock_fv = MagicMock()
        task = MaterializationTask(
            project="my_project",
            feature_view=mock_fv,
            start_time=datetime(2024, 1, 1),
            end_time=datetime(2024, 1, 2),
        )
        assert task.project == "my_project"
        assert task.feature_view is mock_fv
        assert task.start_time == datetime(2024, 1, 1)
        assert task.end_time == datetime(2024, 1, 2)

    def test_default_only_latest(self):
        mock_fv = MagicMock()
        task = MaterializationTask(
            project="p",
            feature_view=mock_fv,
            start_time=datetime(2024, 1, 1),
            end_time=datetime(2024, 1, 2),
        )
        assert task.only_latest is True

    def test_default_disable_event_timestamp(self):
        mock_fv = MagicMock()
        task = MaterializationTask(
            project="p",
            feature_view=mock_fv,
            start_time=datetime(2024, 1, 1),
            end_time=datetime(2024, 1, 2),
        )
        assert task.disable_event_timestamp is False
