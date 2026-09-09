from unittest.mock import MagicMock, patch

import pytest

# Skip the entire module when the optional cassandra-driver is not installed.
pytest.importorskip("cassandra", reason="cassandra-driver not installed")

from cassandra.cluster import EXEC_PROFILE_DEFAULT  # noqa: E402

from feast.infra.online_stores.cassandra_online_store import (  # noqa: E402
    cassandra_online_store as _cos,
)
from feast.repo_config import RepoConfig  # noqa: E402

CassandraInvalidConfig = _cos.CassandraInvalidConfig
CassandraOnlineStore = _cos.CassandraOnlineStore
CassandraOnlineStoreConfig = _cos.CassandraOnlineStoreConfig
E_CASSANDRA_DC_CONFIG_CONFLICT = _cos.E_CASSANDRA_DC_CONFIG_CONFLICT
E_CASSANDRA_DC_CONFIG_EMPTY = _cos.E_CASSANDRA_DC_CONFIG_EMPTY
E_CASSANDRA_DC_DEFAULT_NOT_FOUND = _cos.E_CASSANDRA_DC_DEFAULT_NOT_FOUND
E_CASSANDRA_DC_ROUTING_INVALID = _cos.E_CASSANDRA_DC_ROUTING_INVALID


_DCConfig = CassandraOnlineStoreConfig.CassandraDatacenterConfig
_LBConfig = CassandraOnlineStoreConfig.CassandraLoadBalancingPolicy
_RoutingConfig = CassandraOnlineStoreConfig.CassandraRoutingConfig

# Path to patch so tests never open a real Cassandra connection
_CLUSTER_PATH = (
    "feast.infra.online_stores.cassandra_online_store.cassandra_online_store.Cluster"
)


#  helpers


def _dc(name: str, hosts=None, rf=None, rs=None) -> _DCConfig:
    return _DCConfig(
        name=name,
        hosts=hosts or ["192.168.0.1"],
        replication_factor=rf,
        replication_strategy=rs,
    )


def _lb(
    local_dc: str = "dc1",
    policy: str = "DCAwareRoundRobinPolicy",
) -> _LBConfig:
    return _LBConfig(local_dc=local_dc, load_balancing_policy=policy)


def _routing(read_dc=None, write_dc=None) -> _RoutingConfig:
    return _RoutingConfig(read_dc=read_dc, write_dc=write_dc)


def _repo_config(online_store_cfg) -> RepoConfig:
    return RepoConfig(
        registry="dummy_registry.db",
        project="test_project",
        provider="local",
        entity_key_serialization_version=3,
        online_store=online_store_cfg,
    )


# Fixture: mock out the cassandra cluster conn


@pytest.fixture
def mock_cluster():
    """Yields (MockClusterClass, mock_session) with Cluster fully patched."""
    with patch(_CLUSTER_PATH) as mock_cls:
        mock_session = MagicMock()
        mock_cls.return_value.connect.return_value = mock_session
        yield mock_cls, mock_session


# Section 1 — Config-parsing tests (no DB connection, no mocking needed)


class TestCassandraConfigParsing:
    """Validate that the Pydantic config models parse correctly."""

    def test_single_dc_hosts_config_parses(self):
        cfg = CassandraOnlineStoreConfig(hosts=["192.168.1.1"], keyspace="ks")
        assert cfg.hosts == ["192.168.1.1"]
        assert cfg.keyspace == "ks"
        assert cfg.datacenters is None
        assert cfg.routing is None

    def test_datacenters_field_parses_correctly(self):
        cfg = CassandraOnlineStoreConfig(
            keyspace="ks",
            datacenters=[
                _dc("dc1", ["192.168.1.1"]),
                _dc("dc2", ["10.0.0.1"]),
            ],
        )
        assert len(cfg.datacenters) == 2
        assert cfg.datacenters[0].name == "dc1"
        assert cfg.datacenters[0].hosts == ["192.168.1.1"]
        assert cfg.datacenters[1].name == "dc2"
        assert cfg.datacenters[1].hosts == ["10.0.0.1"]

    def test_datacenter_replication_fields_parse(self):
        cfg = CassandraOnlineStoreConfig(
            keyspace="ks",
            datacenters=[
                _dc(
                    "dc1",
                    ["192.168.1.1"],
                    rf=3,
                    rs="NetworkTopologyStrategy",
                ),
            ],
        )
        assert cfg.datacenters[0].replication_factor == 3
        assert cfg.datacenters[0].replication_strategy == "NetworkTopologyStrategy"

    def test_datacenter_replication_fields_default_to_none(self):
        cfg = CassandraOnlineStoreConfig(
            keyspace="ks",
            datacenters=[_dc("dc1", ["192.168.1.1"])],
        )
        assert cfg.datacenters[0].replication_factor is None
        assert cfg.datacenters[0].replication_strategy is None

    def test_routing_block_parses(self):
        cfg = CassandraOnlineStoreConfig(
            keyspace="ks",
            datacenters=[
                _dc("dc1", ["192.168.1.1"]),
                _dc("dc2", ["10.0.0.1"]),
            ],
            routing=_routing(read_dc="dc2", write_dc="dc1"),
        )
        assert cfg.routing.read_dc == "dc2"
        assert cfg.routing.write_dc == "dc1"

    def test_routing_fields_default_to_none(self):
        cfg = CassandraOnlineStoreConfig(
            keyspace="ks",
            datacenters=[_dc("dc1", ["192.168.1.1"])],
            routing=_routing(),
        )
        assert cfg.routing.read_dc is None
        assert cfg.routing.write_dc is None

    def test_astra_db_config_parses(self):
        cfg = CassandraOnlineStoreConfig(
            secure_bundle_path="/path/to/bundle.zip",
            keyspace="ks",
            username="client_id",
            password="pw",  # pragma: allowlist secret
        )
        assert cfg.secure_bundle_path == "/path/to/bundle.zip"
        assert cfg.datacenters is None


# Section 2 — _get_session: validation errors (Branch B early exits)
# These should be raised BEFORE any driver connection is made.


class TestCassandraMultiDCValidation:
    """
    Validation errors that _get_session raises before touching the driver.
    """

    def test_datacenters_and_hosts_are_mutually_exclusive(self, mock_cluster):
        store = CassandraOnlineStore()
        cfg = _repo_config(
            CassandraOnlineStoreConfig(
                keyspace="ks",
                hosts=["192.168.1.1"],
                datacenters=[_dc("dc1", ["10.0.0.1"])],
            )
        )
        with pytest.raises(
            CassandraInvalidConfig, match=E_CASSANDRA_DC_CONFIG_CONFLICT
        ):
            store._get_session(cfg)

    def test_datacenters_and_secure_bundle_are_mutually_exclusive(self, mock_cluster):
        store = CassandraOnlineStore()
        cfg = _repo_config(
            CassandraOnlineStoreConfig(
                keyspace="ks",
                secure_bundle_path="/path/bundle.zip",
                datacenters=[_dc("dc1", ["10.0.0.1"])],
            )
        )
        with pytest.raises(
            CassandraInvalidConfig, match=E_CASSANDRA_DC_CONFIG_CONFLICT
        ):
            store._get_session(cfg)

    def test_empty_datacenters_list_raises(self, mock_cluster):
        store = CassandraOnlineStore()
        cfg = _repo_config(CassandraOnlineStoreConfig(keyspace="ks", datacenters=[]))
        with pytest.raises(CassandraInvalidConfig, match=E_CASSANDRA_DC_CONFIG_EMPTY):
            store._get_session(cfg)

    def test_load_balancing_local_dc_not_in_datacenters_raises(self, mock_cluster):
        store = CassandraOnlineStore()
        cfg = _repo_config(
            CassandraOnlineStoreConfig(
                keyspace="ks",
                datacenters=[
                    _dc("dc1", ["192.168.1.1"]),
                    _dc("dc2", ["10.0.0.1"]),
                ],
                load_balancing=_lb(local_dc="dc99"),  # dc99 doesn't exist
            )
        )
        with pytest.raises(
            CassandraInvalidConfig,
            match=E_CASSANDRA_DC_DEFAULT_NOT_FOUND,
        ):
            store._get_session(cfg)

    def test_routing_read_dc_not_in_datacenters_raises(self, mock_cluster):
        store = CassandraOnlineStore()
        cfg = _repo_config(
            CassandraOnlineStoreConfig(
                keyspace="ks",
                datacenters=[
                    _dc("dc1", ["192.168.1.1"]),
                    _dc("dc2", ["10.0.0.1"]),
                ],
                load_balancing=_lb(local_dc="dc1"),
                routing=_routing(read_dc="dc99"),  # dc99 doesn't exist
            )
        )
        with pytest.raises(
            CassandraInvalidConfig, match=E_CASSANDRA_DC_ROUTING_INVALID
        ):
            store._get_session(cfg)

    def test_routing_write_dc_not_in_datacenters_raises(self, mock_cluster):
        store = CassandraOnlineStore()
        cfg = _repo_config(
            CassandraOnlineStoreConfig(
                keyspace="ks",
                datacenters=[
                    _dc("dc1", ["192.168.1.1"]),
                    _dc("dc2", ["10.0.0.1"]),
                ],
                load_balancing=_lb(local_dc="dc1"),
                routing=_routing(write_dc="dc99"),  # dc99 doesn't exist
            )
        )
        with pytest.raises(
            CassandraInvalidConfig, match=E_CASSANDRA_DC_ROUTING_INVALID
        ):
            store._get_session(cfg)


# Section 3 — _get_session: successful multi-DC session creation (Branch B)


class TestCassandraMultiDCSession:
    """
    Happy-path tests for Branch B — one execution profile per datacenter.
    """

    def test_execution_profiles_created_for_each_dc(self, mock_cluster):
        mock_cls, mock_session = mock_cluster
        store = CassandraOnlineStore()
        cfg = _repo_config(
            CassandraOnlineStoreConfig(
                keyspace="ks",
                datacenters=[
                    _dc("dc1", ["192.168.1.1"]),
                    _dc("dc2", ["10.0.0.1"]),
                ],
                load_balancing=_lb(local_dc="dc1"),
            )
        )
        store._get_session(cfg)

        profiles = mock_cls.call_args.kwargs["execution_profiles"]
        assert "dc1" in profiles
        assert "dc2" in profiles
        assert EXEC_PROFILE_DEFAULT in profiles
        assert store._dc_execution_profiles == ["dc1", "dc2"]

    def test_default_dc_from_load_balancing_local_dc(self, mock_cluster):
        """
        EXEC_PROFILE_DEFAULT must point to the DC named by
        load_balancing.local_dc.
        """
        mock_cls, mock_session = mock_cluster
        store = CassandraOnlineStore()
        cfg = _repo_config(
            CassandraOnlineStoreConfig(
                keyspace="ks",
                datacenters=[
                    _dc("dc1", ["192.168.1.1"]),
                    _dc("dc2", ["10.0.0.1"]),
                ],
                load_balancing=_lb(local_dc="dc2"),  # dc2 is default
            )
        )
        store._get_session(cfg)

        profiles = mock_cls.call_args.kwargs["execution_profiles"]
        assert profiles[EXEC_PROFILE_DEFAULT] is profiles["dc2"]

    def test_default_dc_fallback_to_first_entry_when_no_load_balancing(
        self, mock_cluster
    ):
        """
        When load_balancing is absent, the first datacenter entry is
        the default.
        """
        mock_cls, mock_session = mock_cluster
        store = CassandraOnlineStore()
        cfg = _repo_config(
            CassandraOnlineStoreConfig(
                keyspace="ks",
                datacenters=[
                    _dc("dc1", ["192.168.1.1"]),
                    _dc("dc2", ["10.0.0.1"]),
                ],
                # no load_balancing block
            )
        )
        store._get_session(cfg)

        profiles = mock_cls.call_args.kwargs["execution_profiles"]
        assert profiles[EXEC_PROFILE_DEFAULT] is profiles["dc1"]

    def test_all_dc_hosts_merged_into_cluster_contact_points(self, mock_cluster):
        """
        Cluster() must receive the union of all DC hosts as contact points.
        """
        mock_cls, mock_session = mock_cluster
        store = CassandraOnlineStore()
        cfg = _repo_config(
            CassandraOnlineStoreConfig(
                keyspace="ks",
                datacenters=[
                    _dc("dc1", ["192.168.1.1", "192.168.1.2"]),
                    _dc("dc2", ["10.0.0.1"]),
                ],
                load_balancing=_lb(local_dc="dc1"),
            )
        )
        store._get_session(cfg)

        contact_points = mock_cls.call_args.args[0]
        assert set(contact_points) == {
            "192.168.1.1",
            "192.168.1.2",
            "10.0.0.1",
        }

    def test_routing_read_dc_sets_read_execution_profile(self, mock_cluster):
        mock_cls, mock_session = mock_cluster
        store = CassandraOnlineStore()
        cfg = _repo_config(
            CassandraOnlineStoreConfig(
                keyspace="ks",
                datacenters=[
                    _dc("dc1", ["192.168.1.1"]),
                    _dc("dc2", ["10.0.0.1"]),
                ],
                load_balancing=_lb(local_dc="dc1"),
                routing=_routing(read_dc="dc2"),
            )
        )
        store._get_session(cfg)
        assert store._read_execution_profile == "dc2"

    def test_routing_write_dc_sets_write_execution_profile(self, mock_cluster):
        mock_cls, mock_session = mock_cluster
        store = CassandraOnlineStore()
        cfg = _repo_config(
            CassandraOnlineStoreConfig(
                keyspace="ks",
                datacenters=[
                    _dc("dc1", ["192.168.1.1"]),
                    _dc("dc2", ["10.0.0.1"]),
                ],
                load_balancing=_lb(local_dc="dc1"),
                routing=_routing(write_dc="dc1"),
            )
        )
        store._get_session(cfg)
        assert store._write_execution_profile == "dc1"

    def test_no_routing_block_uses_default_dc_for_both_ops(self, mock_cluster):
        """Without a routing block, both read and write use the default DC."""
        mock_cls, mock_session = mock_cluster
        store = CassandraOnlineStore()
        cfg = _repo_config(
            CassandraOnlineStoreConfig(
                keyspace="ks",
                datacenters=[
                    _dc("dc1", ["192.168.1.1"]),
                    _dc("dc2", ["10.0.0.1"]),
                ],
                load_balancing=_lb(local_dc="dc1"),
            )
        )
        store._get_session(cfg)
        assert store._read_execution_profile == "dc1"
        assert store._write_execution_profile == "dc1"

    def test_partial_routing_missing_read_dc_falls_back_to_default(self, mock_cluster):
        """routing.read_dc omitted → reads use default DC."""
        mock_cls, mock_session = mock_cluster
        store = CassandraOnlineStore()
        cfg = _repo_config(
            CassandraOnlineStoreConfig(
                keyspace="ks",
                datacenters=[
                    _dc("dc1", ["192.168.1.1"]),
                    _dc("dc2", ["10.0.0.1"]),
                ],
                load_balancing=_lb(local_dc="dc1"),
                routing=_routing(write_dc="dc2"),  # only write_dc given
            )
        )
        store._get_session(cfg)
        # read_dc not given → falls back to default_dc "dc1"
        assert store._read_execution_profile == "dc1"
        assert store._write_execution_profile == "dc2"

    def test_session_is_cached_and_cluster_created_once(self, mock_cluster):
        """Calling _get_session twice returns the same session object."""
        mock_cls, mock_session = mock_cluster
        store = CassandraOnlineStore()
        cfg = _repo_config(
            CassandraOnlineStoreConfig(
                keyspace="ks",
                datacenters=[_dc("dc1", ["192.168.1.1"])],
                load_balancing=_lb(local_dc="dc1"),
            )
        )
        s1 = store._get_session(cfg)
        s2 = store._get_session(cfg)

        assert s1 is s2
        assert mock_cls.call_count == 1  # Cluster() instantiated exactly once

    def test_single_dc_in_datacenters_list_works(self, mock_cluster):
        """A single-entry datacenters list is valid (not an error)."""
        mock_cls, mock_session = mock_cluster
        store = CassandraOnlineStore()
        cfg = _repo_config(
            CassandraOnlineStoreConfig(
                keyspace="ks",
                datacenters=[_dc("dc1", ["192.168.1.1"])],
                load_balancing=_lb(local_dc="dc1"),
            )
        )
        session = store._get_session(cfg)
        assert session is mock_session


# Section 4 — Backward compatibility: Branch A (single-DC / Astra DB)


class TestCassandraSingleDCBackwardCompat:
    """
    The original Branch A path must be unaffected by the multi-DC changes.
    In particular, _read_execution_profile and _write_execution_profile must
    stay at their class-level EXEC_PROFILE_DEFAULT sentinel in Branch A.
    """

    def test_branch_a_hosts_creates_session(self, mock_cluster):
        mock_cls, mock_session = mock_cluster
        store = CassandraOnlineStore()
        cfg = _repo_config(
            CassandraOnlineStoreConfig(hosts=["127.0.0.1"], port=9042, keyspace="ks")
        )
        session = store._get_session(cfg)

        assert session is mock_session
        # Branch A leaves the sentinel defaults untouched
        assert store._read_execution_profile is EXEC_PROFILE_DEFAULT
        assert store._write_execution_profile is EXEC_PROFILE_DEFAULT

    def test_branch_a_astra_db_creates_session(self, mock_cluster):
        mock_cls, mock_session = mock_cluster
        store = CassandraOnlineStore()
        cfg = _repo_config(
            CassandraOnlineStoreConfig(
                secure_bundle_path="/path/bundle.zip",
                keyspace="ks",
                username="client_id",
                password="pw",  # pragma: allowlist secret
            )
        )
        session = store._get_session(cfg)

        assert session is mock_session
        assert store._read_execution_profile is EXEC_PROFILE_DEFAULT
        assert store._write_execution_profile is EXEC_PROFILE_DEFAULT

    def test_branch_a_with_load_balancing_creates_session(self, mock_cluster):
        mock_cls, mock_session = mock_cluster
        store = CassandraOnlineStore()
        cfg = _repo_config(
            CassandraOnlineStoreConfig(
                hosts=["127.0.0.1"],
                keyspace="ks",
                load_balancing=_lb(local_dc="datacenter1"),
            )
        )
        session = store._get_session(cfg)

        assert session is mock_session
        # Branch A does not touch execution profile attributes
        assert store._read_execution_profile is EXEC_PROFILE_DEFAULT
        assert store._write_execution_profile is EXEC_PROFILE_DEFAULT
