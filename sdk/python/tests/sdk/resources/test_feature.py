from feast.sdk.resources.feature import Feature
from feast.sdk.resources.storage import Datastore
from feast.sdk.utils.types import ValueType, Granularity

class TestFeature(object):
    def dummy_feature(self):
        warehouse_data_store = Datastore(id = "BIGQUERY1", options = {})
        serving_data_store = Datastore(id = "REDIS1", options = {})
        my_feature = Feature(name = "my_feature", entity = "my_entity", granularity = Granularity.NONE, value_type = ValueType.BYTES,
            owner = "feast@web.com", description = "test feature", uri = "github.com/feature_repo", warehouse_store = warehouse_data_store, 
            serving_store = serving_data_store)
        return my_feature

    def test_set_name(self):
        my_feature = self.dummy_feature()
        new_name = "my_feature_new"
        my_feature.name = new_name
        assert my_feature.name == new_name
        assert my_feature.id == "my_entity.none.my_feature_new"
    
    def test_set_granularity(self):
        my_feature = self.dummy_feature()
        my_feature.granularity = Granularity.DAY
        assert my_feature.granularity == Granularity.DAY
        assert my_feature.id == "my_entity.day.my_feature"
    
    def test_set_entity(self):
        my_feature = self.dummy_feature()
        new_entity = "new_entity"
        my_feature.entity = new_entity
        assert my_feature.entity == new_entity
        assert my_feature.id == "new_entity.none.my_feature"
    
    def test_read_from_yaml(self):
        feature = Feature.from_yaml("tests/sample/valid_feature.yaml")
        assert feature.id == "myentity.none.feature_bool_redis1"
        assert feature.name == "feature_bool_redis1"
        assert feature.entity == "myentity"
        assert feature.owner == "bob@example.com"
        assert feature.description == "test entity."
        assert feature.value_type == ValueType.BOOL
        assert feature.granularity == Granularity.NONE
        assert feature.uri == "https://github.com/bob/example"
        assert feature.serving_store.id == "REDIS1"
        assert feature.warehouse_store.id == "BIGQUERY1"
