from feast.sdk.resources.feature_group import FeatureGroup

class TestFeatureGroupSpec(object):
   def test_read_from_yaml(self):
        feature_group = FeatureGroup.from_yaml(
            "tests/sample/valid_feature_group.yaml")
        assert feature_group.id == "my_fg"
        assert feature_group.serving_store.id == "REDIS1"
        assert feature_group.warehouse_store.id == "BIGQUERY1"
        assert feature_group.tags == ["tag1", "tag2"]