from feast.sdk.utils.bq_util import TrainingDatasetCreator, get_table_name
from feast.specs.StorageSpec_pb2 import StorageSpec
import pytest


def test_get_table_name():
    project_name = "my_project"
    dataset_name = "my_dataset"
    feature_id = "myentity.none.feature1"
    storage_spec = StorageSpec(id="BIGQUERY1", type="bigquery",
                                   options={"project": project_name,
                                            "dataset": dataset_name})
    assert get_table_name(feature_id, storage_spec) == \
           "my_project.my_dataset.myentity_none"


def test_get_table_name_not_bq():
    feature_id = "myentity.none.feature1"
    storage_spec = StorageSpec(id="REDIS1", type="redis")
    with pytest.raises(ValueError, match="storage spec is not BigQuery storage spec"):
        get_table_name(feature_id, storage_spec)


class TestTrainingDatasetCreator(object):
    def test_simple(self):
        t = TrainingDatasetCreator()
        assert t._sql_template is not None

    def test_group_features(self):
        f = [("entity.none.feature1", "project.dataset.table1"),
             ("entity.none.feature2", "project.dataset.table1"),
             ("entity.none.feature3", "project.dataset.table1"),
             ("entity.minute.feature1", "project.dataset.table1"),
             ("entity.hour.feature1", "project.dataset.table1"),
             ("entity.day.feature1", "project.dataset.table1"),
             ("entity.second.feature1", "project.dataset.table1")]

        t = TrainingDatasetCreator()
        feature_groups = t._group_features(f)
        assert feature_groups[0].granularity == "second"
        assert feature_groups[1].granularity == "minute"
        assert feature_groups[2].granularity == "hour"
        assert feature_groups[3].granularity == "day"
        assert feature_groups[4].granularity == "none"

        for feature_group in feature_groups:
            assert feature_group.table_id == "project.dataset.table1"

        assert len(feature_groups[0].features) == 1
        assert len(feature_groups[1].features) == 1
        assert len(feature_groups[2].features) == 1
        assert len(feature_groups[3].features) == 1
        assert len(feature_groups[4].features) == 3

    def test_create_query(self):
        f = [("entity.none.feature1", "project.dataset.table1"),
             ("entity.none.feature2", "project.dataset.table1"),
             ("entity.none.feature3", "project.dataset.table1"),
             ("entity.minute.feature1", "project.dataset.table1"),
             ("entity.hour.feature1", "project.dataset.table1"),
             ("entity.day.feature1", "project.dataset.table1"),
             ("entity.second.feature1", "project.dataset.table1")]
        start = "2018-12-01"
        end = "2018-12-02"

        t = TrainingDatasetCreator()
        query = t._create_query(f, start, end, None)
        assert "LIMIT" not in query
