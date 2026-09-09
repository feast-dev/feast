import time
from typing import Dict

from testcontainers.core.container import DockerContainer
from testcontainers.core.network import Network
from testcontainers.core.waiting_utils import wait_for_logs

from tests.universal.feature_repos.universal.online_store_creator import (
    OnlineStoreCreator,
)

_SCYLLA_IMAGE = "scylladb/scylla:2026.1.4"
_VECTOR_STORE_IMAGE = "scylladb/vector-store:1.7.0"


class ScyllaDBOnlineStoreCreator(OnlineStoreCreator):
    """
    Starts a ScyllaDB + Vector Store stack for integration tests.

    Two containers share a Docker network:
    - scylladb/scylla:2026.1.4     — CQL data node (exposes port 9042)
    - scylladb/vector-store:1.7.0  — ANN indexing service (port 6080)

    Both regular read/write and vector search tests run against this stack
    with no external cloud dependency.
    """

    def __init__(self, project_name: str, **kwargs):
        super().__init__(project_name)
        self.network = Network()
        self.scylla = (
            DockerContainer(_SCYLLA_IMAGE)
            .with_network(self.network)
            .with_network_aliases("scylla")
            .with_command(
                "--smp 1 --memory 1G --overprovisioned 1 "
                "--vector-store-primary-uri http://vector-store:6080 "
                "--broadcast-rpc-address 127.0.0.1"
            )
            .with_exposed_ports("9042")
        )
        self.vector_store = (
            DockerContainer(_VECTOR_STORE_IMAGE)
            .with_network(self.network)
            .with_network_aliases("vector-store")
            .with_env("VECTOR_STORE_URI", "0.0.0.0:6080")
            .with_env("VECTOR_STORE_SCYLLADB_URI", "scylla:9042")
        )

    def create_online_store(self) -> Dict[str, object]:
        self.network.create()
        self.scylla.start()
        wait_for_logs(
            container=self.scylla,
            predicate="Starting listening for CQL clients",
            timeout=120,
        )
        time.sleep(5)  # allow CQL port to be fully ready
        self.vector_store.start()
        wait_for_logs(
            container=self.vector_store,
            predicate="6080",
            timeout=60,
        )
        keyspace = "feast_keyspace"
        self.scylla.exec(
            f'cqlsh -e "CREATE KEYSPACE IF NOT EXISTS \\"{keyspace}\\"'
            " WITH replication = {'class': 'NetworkTopologyStrategy', 'datacenter1': 1}"
            " AND tablets = {'enabled': true};\""
        )
        exposed_port = int(self.scylla.get_exposed_port("9042"))
        return {
            "type": "scylladb",
            "hosts": ["127.0.0.1"],
            "port": exposed_port,
            "keyspace": keyspace,
            "local_dc": "datacenter1",
        }

    def teardown(self):
        self.vector_store.stop()
        self.scylla.stop()
        self.network.remove()
