import assertpy
import grpc_testing
import pytest
from google.protobuf.empty_pb2 import Empty

from feast import Entity, FeatureStore, RepoConfig
from feast.protos.feast.core import Entity_pb2
from feast.protos.feast.registry import RegistryServer_pb2, RegistryServer_pb2_grpc
from feast.registry_server import RegistryServer


def call_registry_server(server, method: str, request=None):
    service = RegistryServer_pb2.DESCRIPTOR.services_by_name["RegistryServer"]
    rpc = server.invoke_unary_unary(
        service.methods_by_name[method], (), request if request else Empty(), None
    )

    return rpc.termination()


@pytest.fixture
def registry_server(environment):
    store: FeatureStore = environment.feature_store

    servicer = RegistryServer(store=store)

    return grpc_testing.server_from_dictionary(
        {RegistryServer_pb2.DESCRIPTOR.services_by_name["RegistryServer"]: servicer},
        grpc_testing.strict_real_time(),
    )


def test_registry_server_get_entity(environment, registry_server):
    store: FeatureStore = environment.feature_store
    entity = Entity(name="driver", join_keys=["driver_id"])
    store.apply(entity)

    expected = store.get_entity(entity.name)

    get_entity_request = RegistryServer_pb2.GetEntityRequest(
        name=entity.name, project=store.project, allow_cache=False
    )
    response, trailing_metadata, code, details = call_registry_server(
        registry_server, "GetEntity", get_entity_request
    )
    response_entity = Entity.from_proto(response)

    assertpy.assert_that(response_entity).is_equal_to(expected)


def test_registry_server_proto(environment, registry_server):
    store: FeatureStore = environment.feature_store
    entity = Entity(name="driver", join_keys=["driver_id"])
    store.apply(entity)

    expected = store.registry.proto()
    response, trailing_metadata, code, details = call_registry_server(
        registry_server, "Proto"
    )

    assertpy.assert_that(response).is_equal_to(expected)
