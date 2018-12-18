from feast.sdk.resources.entity import Entity
from feast.sdk.resources.feature import Feature
from feast.types.Granularity_pb2 import Granularity
from feast.types.Value_pb2 import ValueType
import feast.specs.FeatureSpec_pb2 as feature_pb

from feast.sdk.client import Client

myEntity = Entity("test", "test description", ["tag1", "tag2"])
myEntity.dump("test.yaml")

entity2 = Entity.from_yaml_file("test.yaml")
entity2.dump("entity.yaml")

warehouse_data_store = feature_pb.DataStore(id = "BIGQUERY1", options = {})
serving_data_store = feature_pb.DataStore(id = "REDIS1", options = {})
my_feature = Feature(name = "my_feature", entity = "my_entity", granularity = Granularity.NONE, value_type = ValueType.BYTES,
    owner = "feast@web.com", description = "test feature", uri = "github.com/feature_repo", warehouse_store = warehouse_data_store, 
    serving_store = serving_data_store)
print(my_feature.serving_store)

my_feature.options = {"test": "test", "test2": "test2"}
print(my_feature)

cli = Client("localhost:8433")
cli.apply(my_feature)