from datetime import datetime, timedelta, timezone

import grpc
import pytest
from grpc_health.v1 import health_pb2, health_pb2_grpc

from feast.entity import Entity
from feast.feature_store import FeatureStore
from feast.grpc_error_interceptor import ErrorInterceptor
from feast.permissions.auth_model import AuthConfig, OidcAuthConfig
from feast.permissions.server.grpc import (
    WRITE_RPC_METHODS,
    AuthInterceptor,
    WritePathAuthInterceptor,
    is_write_rpc,
)
from feast.permissions.server.utils import AuthManagerType
from feast.protos.feast.registry import RegistryServer_pb2, RegistryServer_pb2_grpc
from feast.registry_server import _grpc_interceptors, start_server
from feast.repo_config import RepoConfig
from feast.value_type import ValueType
from feast.wait import wait_retry_backoff
from tests.unit.permissions.auth.rsa_jwt import (
    AUDIENCE,
    CLIENT_ID,
    ISSUER,
    encode_jwt,
    generate_rsa_keypair,
    patch_oidc_jwks,
)
from tests.utils.http_server import check_port_open, free_port

PROJECT = "test_project"


@pytest.fixture(scope="module")
def rsa_keypair():
    return generate_rsa_keypair()


def _oidc_write_auth_config() -> dict:
    return {
        "type": "oidc",
        "client_id": CLIENT_ID,
        "auth_discovery_url": "https://example/.well-known/openid-configuration",
        "write_auth_only": True,
        "issuer": ISSUER,
        "audience": AUDIENCE,
    }


def _store(tmp_path, auth: dict) -> FeatureStore:
    return FeatureStore(
        config=RepoConfig(
            registry=str(tmp_path / "registry.db"),
            project=PROJECT,
            provider="local",
            entity_key_serialization_version=3,
            offline_store={"type": "file"},
            online_store={"type": "sqlite", "path": str(tmp_path / "online.db")},
            auth=auth,
        )
    )


def _start_registry(store: FeatureStore):
    port = free_port()
    server = start_server(store, port, wait_for_termination=False)
    wait_retry_backoff(
        lambda: (None, check_port_open("localhost", port)),
        timeout_secs=10,
    )
    channel = grpc.insecure_channel(f"localhost:{port}")
    stub = RegistryServer_pb2_grpc.RegistryServerStub(channel)
    health_stub = health_pb2_grpc.HealthStub(channel)
    return server, channel, stub, health_stub


def _apply_request(name: str = "user") -> RegistryServer_pb2.ApplyEntityRequest:
    entity = Entity(name=name, join_keys=["user_id"], value_type=ValueType.STRING)
    return RegistryServer_pb2.ApplyEntityRequest(
        entity=entity.to_proto(), project=PROJECT, commit=True
    )


def _auth_metadata(token: str):
    return (("authorization", f"Bearer {token}"),)


def test_write_rpc_allowlist_is_registry_writes_only():
    writes = {
        "ApplyEntity",
        "DeleteEntity",
        "ApplyDataSource",
        "DeleteDataSource",
        "ApplyFeatureView",
        "DeleteFeatureView",
        "ApplyFeatureService",
        "DeleteFeatureService",
        "ApplySavedDataset",
        "DeleteSavedDataset",
        "ApplyValidationReference",
        "DeleteValidationReference",
        "ApplyPermission",
        "DeletePermission",
        "ApplyProject",
        "DeleteProject",
        "ApplyMaterialization",
        "UpdateInfra",
        "Commit",
    }
    assert {method.rsplit("/", 1)[-1] for method in WRITE_RPC_METHODS} == writes
    for name in writes:
        assert is_write_rpc(f"/feast.registry.RegistryServer/{name}")

    for method in (
        "/feast.registry.RegistryServer/GetEntity",
        "/feast.registry.RegistryServer/ListEntities",
        "/feast.registry.RegistryServer/GetInfra",
        "/feast.registry.RegistryServer/Proto",
        "/feast.registry.RegistryServer/Refresh",
        "/feast.registry.RegistryServer/ExpediaSearchProjects",
        "/feast.registry.RegistryServer/ExpediaSearchFeatureViews",
        "/grpc.health.v1.Health/Check",
        "/grpc.reflection.v1alpha.ServerReflection/ServerReflectionInfo",
        "/feast.serving.ServingService/GetOnlineFeatures",
    ):
        assert not is_write_rpc(method)


def test_grpc_interceptor_selection_by_flag():
    none = _grpc_interceptors(AuthManagerType.NONE, AuthConfig())
    assert len(none) == 1
    assert isinstance(none[0], ErrorInterceptor)

    stock_oidc = OidcAuthConfig(
        type="oidc",
        auth_discovery_url="https://example/.well-known/openid-configuration",
        client_id=CLIENT_ID,
    )
    stock = _grpc_interceptors(AuthManagerType.OIDC, stock_oidc)
    assert isinstance(stock[0], AuthInterceptor)
    assert isinstance(stock[1], ErrorInterceptor)

    write_oidc = OidcAuthConfig(
        type="oidc",
        auth_discovery_url="https://example/.well-known/openid-configuration",
        client_id=CLIENT_ID,
        write_auth_only=True,
    )
    write_only = _grpc_interceptors(AuthManagerType.OIDC, write_oidc)
    assert isinstance(write_only[0], WritePathAuthInterceptor)
    assert isinstance(write_only[1], ErrorInterceptor)


def test_no_auth_apply_without_token_succeeds(tmp_path):
    store = _store(tmp_path, {"type": "no_auth"})
    server, channel, stub, _ = _start_registry(store)
    try:
        stub.ApplyEntity(_apply_request())
        got = stub.GetEntity(
            RegistryServer_pb2.GetEntityRequest(name="user", project=PROJECT)
        )
        assert got.spec.name == "user"
    finally:
        channel.close()
        server.stop(grace=None)


def test_write_auth_apply_without_token_is_unauthenticated(
    tmp_path, rsa_keypair, monkeypatch
):
    _, public_key = rsa_keypair
    patch_oidc_jwks(monkeypatch, public_key)
    store = _store(tmp_path, _oidc_write_auth_config())
    server, channel, stub, _ = _start_registry(store)
    try:
        with pytest.raises(grpc.RpcError) as exc:
            stub.ApplyEntity(_apply_request())
        assert exc.value.code() == grpc.StatusCode.UNAUTHENTICATED
    finally:
        channel.close()
        server.stop(grace=None)


def test_write_auth_valid_jwt_apply_succeeds_without_permissions(
    tmp_path, rsa_keypair, monkeypatch
):
    private_key, public_key = rsa_keypair
    patch_oidc_jwks(monkeypatch, public_key)
    store = _store(tmp_path, _oidc_write_auth_config())
    server, channel, stub, health_stub = _start_registry(store)
    try:
        token = encode_jwt(private_key)
        stub.ApplyEntity(_apply_request(), metadata=_auth_metadata(token))

        listed = stub.ListEntities(
            RegistryServer_pb2.ListEntitiesRequest(project=PROJECT)
        )
        assert [entity.spec.name for entity in listed.entities] == ["user"]

        got = stub.GetEntity(
            RegistryServer_pb2.GetEntityRequest(name="user", project=PROJECT)
        )
        assert got.spec.name == "user"

        health = health_stub.Check(health_pb2.HealthCheckRequest())
        assert health.status == health_pb2.HealthCheckResponse.SERVING
    finally:
        channel.close()
        server.stop(grace=None)


@pytest.mark.parametrize(
    "token_factory",
    [
        pytest.param(
            lambda private_key: encode_jwt(
                private_key, exp=datetime.now(timezone.utc) - timedelta(minutes=5)
            ),
            id="expired",
        ),
        pytest.param(
            lambda private_key: encode_jwt(private_key, aud="wrong-audience"),
            id="wrong_aud",
        ),
        pytest.param(
            lambda private_key: encode_jwt(generate_rsa_keypair()[0]),
            id="bad_sig",
        ),
    ],
)
def test_write_auth_invalid_jwt_is_unauthenticated(
    tmp_path, rsa_keypair, monkeypatch, token_factory
):
    private_key, public_key = rsa_keypair
    patch_oidc_jwks(monkeypatch, public_key)
    store = _store(tmp_path, _oidc_write_auth_config())
    server, channel, stub, _ = _start_registry(store)
    try:
        token = token_factory(private_key)
        with pytest.raises(grpc.RpcError) as exc:
            stub.ApplyEntity(_apply_request(), metadata=_auth_metadata(token))
        assert exc.value.code() == grpc.StatusCode.UNAUTHENTICATED
    finally:
        channel.close()
        server.stop(grace=None)


def test_write_auth_reads_and_health_without_metadata(
    tmp_path, rsa_keypair, monkeypatch
):
    private_key, public_key = rsa_keypair
    patch_oidc_jwks(monkeypatch, public_key)
    store = _store(tmp_path, _oidc_write_auth_config())
    server, channel, stub, health_stub = _start_registry(store)
    try:
        stub.ApplyEntity(
            _apply_request(), metadata=_auth_metadata(encode_jwt(private_key))
        )

        listed = stub.ListEntities(
            RegistryServer_pb2.ListEntitiesRequest(project=PROJECT)
        )
        assert len(listed.entities) == 1

        got = stub.GetEntity(
            RegistryServer_pb2.GetEntityRequest(name="user", project=PROJECT)
        )
        assert got.spec.name == "user"

        stub.Refresh(RegistryServer_pb2.RefreshRequest(project=PROJECT))

        health = health_stub.Check(health_pb2.HealthCheckRequest())
        assert health.status == health_pb2.HealthCheckResponse.SERVING
    finally:
        channel.close()
        server.stop(grace=None)
