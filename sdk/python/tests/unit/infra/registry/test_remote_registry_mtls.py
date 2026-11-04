"""
Tests for mTLS (mutual TLS) support on RemoteRegistry.

Covers two deployment patterns:

1. **Direct connection** — client connects straight to the service.  The
   server cert's SAN includes the connection target so ``authority`` is
   optional (gRPC defaults it to the target host).

2. **IAP-tunnel / proxy** — ``gcloud compute start-iap-tunnel`` forwards
   traffic through ``localhost``.  The server cert's SAN is the real service
   hostname (not ``localhost``), so the client *must* set ``authority`` to
   match — otherwise TLS hostname verification fails.
"""

import tempfile
from collections.abc import Generator
from concurrent import futures

import grpc
import pytest
from google.protobuf.empty_pb2 import Empty

from feast.protos.feast.core import Project_pb2, Registry_pb2
from feast.protos.feast.registry import RegistryServer_pb2, RegistryServer_pb2_grpc
from tests.utils.ssl_certifcates_util import generate_mtls_certs

# The hostname the server cert is issued for.
# Deliberately *not* localhost — the whole point of the authority override.
SERVICE_HOSTNAME = "feature-registry.example.com"


# ---------------------------------------------------------------------------
# Minimal gRPC servicer — just enough to prove the channel works
# ---------------------------------------------------------------------------


class _StubRegistryServicer(RegistryServer_pb2_grpc.RegistryServerServicer):
    """Returns a single dummy project so list_projects() has something to assert on."""

    def ListProjects(
        self,
        request: RegistryServer_pb2.ListProjectsRequest,
        context: grpc.ServicerContext,
    ) -> RegistryServer_pb2.ListProjectsResponse:
        spec = Project_pb2.ProjectSpec(
            name="test_project",
            description="created by mTLS test",
        )
        project = Project_pb2.Project(spec=spec)
        return RegistryServer_pb2.ListProjectsResponse(projects=[project])

    def Proto(
        self,
        request: Empty,
        context: grpc.ServicerContext,
    ) -> Registry_pb2.Registry:
        return Registry_pb2.Registry()


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _generate_certs_and_start_server(
    tmpdir: str, server_san: str
) -> tuple[dict[str, str], grpc.Server, int]:
    """Helper: generate mTLS certs and start a gRPC server requiring client auth."""
    certs = generate_mtls_certs(tmpdir, server_san=server_san)

    with open(certs["ca_cert"], "rb") as f:
        ca_cert = f.read()
    with open(certs["server_key"], "rb") as f:
        server_key = f.read()
    with open(certs["server_cert"], "rb") as f:
        server_cert = f.read()

    server_creds = grpc.ssl_server_credentials(
        [(server_key, server_cert)],
        root_certificates=ca_cert,
        require_client_auth=True,
    )

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=2))
    RegistryServer_pb2_grpc.add_RegistryServerServicer_to_server(
        _StubRegistryServicer(), server
    )
    port = server.add_secure_port("localhost:0", server_creds)
    server.start()
    return certs, server, port


# -- Fixtures for IAP-tunnel tests (SAN ≠ localhost) --


@pytest.fixture(scope="module")
def mtls_certs() -> Generator[tuple[dict[str, str], int], None, None]:
    """Certs where the server SAN is SERVICE_HOSTNAME (not localhost)."""
    with tempfile.TemporaryDirectory() as tmpdir:
        certs, server, port = _generate_certs_and_start_server(tmpdir, SERVICE_HOSTNAME)
        yield certs, port
        server.stop(grace=0)


@pytest.fixture(scope="module")
def mtls_server(mtls_certs: tuple[dict[str, str], int]) -> int:
    return mtls_certs[1]


@pytest.fixture(scope="module")
def mtls_certs_only(mtls_certs: tuple[dict[str, str], int]) -> dict[str, str]:
    return mtls_certs[0]


# -- Fixtures for direct-connection tests (SAN = localhost) --


@pytest.fixture(scope="module")
def direct_mtls() -> Generator[tuple[dict[str, str], int], None, None]:
    """Certs where the server SAN includes localhost (direct connection)."""
    with tempfile.TemporaryDirectory() as tmpdir:
        certs, server, port = _generate_certs_and_start_server(tmpdir, "localhost")
        yield certs, port
        server.stop(grace=0)


@pytest.fixture(scope="module")
def direct_server(direct_mtls: tuple[dict[str, str], int]) -> int:
    return direct_mtls[1]


@pytest.fixture(scope="module")
def direct_certs(direct_mtls: tuple[dict[str, str], int]) -> dict[str, str]:
    return direct_mtls[0]


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestMtlsDirectConnection:
    """
    Direct connection to the registry — no proxy or tunnel.

    The server cert's SAN includes ``localhost`` so gRPC's default authority
    (derived from the connection target) already matches.  The ``authority``
    field should be optional.

    Config equivalent:
        registry:
          registry_type: remote
          path: feature-registry.example.com:443
          cert: /etc/tls/internal-client/ca.crt
          client_cert: /etc/tls/internal-client/tls.crt
          client_key: /etc/tls/internal-client/tls.key
    """

    def test_mtls_without_authority(
        self, direct_server: int, direct_certs: dict[str, str]
    ) -> None:
        """authority is not set — gRPC defaults it to 'localhost', which matches the SAN."""
        from feast.infra.registry.remote import RemoteRegistry, RemoteRegistryConfig

        config = RemoteRegistryConfig(
            registry_type="remote",
            path=f"localhost:{direct_server}",
            cert=direct_certs["ca_cert"],
            client_cert=direct_certs["client_cert"],
            client_key=direct_certs["client_key"],
        )
        registry = RemoteRegistry(config, project="test", repo_path=None)
        try:
            projects = registry.list_projects()
            assert len(projects) == 1
            assert projects[0].name == "test_project"
        finally:
            registry.close()

    def test_mtls_with_explicit_authority_matching_san(
        self, direct_server: int, direct_certs: dict[str, str]
    ) -> None:
        """authority is explicitly set to the same host — should also work."""
        from feast.infra.registry.remote import RemoteRegistry, RemoteRegistryConfig

        config = RemoteRegistryConfig(
            registry_type="remote",
            path=f"localhost:{direct_server}",
            cert=direct_certs["ca_cert"],
            client_cert=direct_certs["client_cert"],
            client_key=direct_certs["client_key"],
            authority="localhost",
        )
        registry = RemoteRegistry(config, project="test", repo_path=None)
        try:
            projects = registry.list_projects()
            assert len(projects) == 1
            assert projects[0].name == "test_project"
        finally:
            registry.close()


class TestMtlsIapTunnel:
    """
    Simulates ``gcloud compute start-iap-tunnel`` to a remote registry.

    The server cert's SAN is ``feature-registry.example.com`` — it does NOT
    include ``localhost``.  The ``authority`` field is mandatory.

    Config equivalent:
        registry:
          registry_type: remote
          path: localhost:8443
          cert: /etc/tls/internal-client/ca.crt
          client_cert: /etc/tls/internal-client/tls.crt
          client_key: /etc/tls/internal-client/tls.key
          authority: feature-registry.example.com
    """

    def test_list_projects_via_mtls_with_authority(
        self, mtls_server: int, mtls_certs_only: dict[str, str]
    ) -> None:
        """The primary IAP-tunnel scenario: all four fields set."""
        from feast.infra.registry.remote import RemoteRegistry, RemoteRegistryConfig

        config = RemoteRegistryConfig(
            registry_type="remote",
            path=f"localhost:{mtls_server}",
            cert=mtls_certs_only["ca_cert"],
            client_cert=mtls_certs_only["client_cert"],
            client_key=mtls_certs_only["client_key"],
            authority=SERVICE_HOSTNAME,
        )
        registry = RemoteRegistry(config, project="test", repo_path=None)
        try:
            projects = registry.list_projects()
            assert len(projects) == 1
            assert projects[0].name == "test_project"
            assert projects[0].description == "created by mTLS test"
        finally:
            registry.close()

    def test_proto_via_mtls_with_authority(
        self, mtls_server: int, mtls_certs_only: dict[str, str]
    ) -> None:
        """Proto() is the simplest RPC — verify the channel works for it too."""
        from feast.infra.registry.remote import RemoteRegistry, RemoteRegistryConfig

        config = RemoteRegistryConfig(
            registry_type="remote",
            path=f"localhost:{mtls_server}",
            cert=mtls_certs_only["ca_cert"],
            client_cert=mtls_certs_only["client_cert"],
            client_key=mtls_certs_only["client_key"],
            authority=SERVICE_HOSTNAME,
        )
        registry = RemoteRegistry(config, project="test", repo_path=None)
        try:
            proto = registry.proto()
            assert proto is not None
        finally:
            registry.close()


class TestMtlsRejections:
    """Verify mTLS enforcement — connections fail without proper credentials."""

    def test_rejected_without_client_cert(
        self, mtls_server: int, mtls_certs_only: dict[str, str]
    ) -> None:
        """Server requires mTLS; connecting with only server-TLS must fail."""
        from feast.infra.registry.remote import RemoteRegistry, RemoteRegistryConfig

        config = RemoteRegistryConfig(
            registry_type="remote",
            path=f"localhost:{mtls_server}",
            cert=mtls_certs_only["ca_cert"],
            is_tls=True,
            authority=SERVICE_HOSTNAME,
        )
        registry = RemoteRegistry(config, project="test", repo_path=None)
        try:
            with pytest.raises(grpc.RpcError):
                registry.list_projects()
        finally:
            registry.close()

    def test_fails_without_authority_when_san_mismatch(
        self, mtls_server: int, mtls_certs_only: dict[str, str]
    ) -> None:
        """
        Without authority override, the gRPC client checks the server cert SAN
        against 'localhost' — which doesn't match — so the handshake fails.
        This proves the authority field is necessary for the IAP-tunnel case.
        """
        from feast.infra.registry.remote import RemoteRegistry, RemoteRegistryConfig

        config = RemoteRegistryConfig(
            registry_type="remote",
            path=f"localhost:{mtls_server}",
            cert=mtls_certs_only["ca_cert"],
            client_cert=mtls_certs_only["client_cert"],
            client_key=mtls_certs_only["client_key"],
            # No authority — should fail because SAN is SERVICE_HOSTNAME, not localhost
        )
        registry = RemoteRegistry(config, project="test", repo_path=None)
        try:
            with pytest.raises(grpc.RpcError):
                registry.list_projects()
        finally:
            registry.close()


class TestMtlsConfigValidation:
    """Config validation catches mismatched cert/key before any I/O."""

    def test_client_cert_without_client_key_raises(self) -> None:
        from feast.infra.registry.remote import RemoteRegistry, RemoteRegistryConfig

        config = RemoteRegistryConfig(
            registry_type="remote",
            path="localhost:0",
            cert="/dev/null",
            client_cert="/dev/null",
        )
        with pytest.raises(
            ValueError, match="Both client_cert and client_key must be provided"
        ):
            RemoteRegistry(config, project="test", repo_path=None)

    def test_client_key_without_client_cert_raises(self) -> None:
        from feast.infra.registry.remote import RemoteRegistry, RemoteRegistryConfig

        config = RemoteRegistryConfig(
            registry_type="remote",
            path="localhost:0",
            cert="/dev/null",
            client_key="/dev/null",
        )
        with pytest.raises(
            ValueError, match="Both client_cert and client_key must be provided"
        ):
            RemoteRegistry(config, project="test", repo_path=None)
