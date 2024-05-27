import assertpy
import grpc_testing
import pytest

from feast import Entity, FeatureStore
from feast.infra.registry.remote import RemoteRegistry, RemoteRegistryConfig
from feast.protos.feast.registry import RegistryServer_pb2, RegistryServer_pb2_grpc
from feast.registry_server import RegistryServer


class GrpcMockChannel:
    def __init__(self, service, servicer):
        self.service = service
        self.test_server = grpc_testing.server_from_dictionary(
            {service: servicer},
            grpc_testing.strict_real_time(),
        )

    def unary_unary(
        self, method: str, request_serializer=None, response_deserializer=None
    ):
        method_name = method.split("/")[-1]
        method_descriptor = self.service.methods_by_name[method_name]

        def handler(request):
            rpc = self.test_server.invoke_unary_unary(
                method_descriptor, (), request, None
            )

            response, trailing_metadata, code, details = rpc.termination()
            return response

        return handler


@pytest.fixture
def mock_remote_registry(environment):
    store: FeatureStore = environment.feature_store
    registry = RemoteRegistry(
        registry_config=RemoteRegistryConfig(path=""), project=None, repo_path=None
    )
    mock_channel = GrpcMockChannel(
        RegistryServer_pb2.DESCRIPTOR.services_by_name["RegistryServer"],
        RegistryServer(registry=store._registry),
    )
    registry.stub = RegistryServer_pb2_grpc.RegistryServerStub(mock_channel)
    return registry


def test_registry_server_get_entity(environment, mock_remote_registry):
    store: FeatureStore = environment.feature_store
    entity = Entity(name="driver", join_keys=["driver_id"])
    store.apply(entity)

    expected = store.get_entity(entity.name)
    response_entity = mock_remote_registry.get_entity(entity.name, store.project)

    assertpy.assert_that(response_entity).is_equal_to(expected)


def test_registry_server_proto(environment, mock_remote_registry):
    store: FeatureStore = environment.feature_store
    entity = Entity(name="driver", join_keys=["driver_id"])
    store.apply(entity)

    expected = store.registry.proto()
    response = mock_remote_registry.proto()

    assertpy.assert_that(response).is_equal_to(expected)
