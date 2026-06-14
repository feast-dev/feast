import os
from typing import Dict

from tests.universal.feature_repos.universal.online_store_creator import (
    OnlineStoreCreator,
)


class ScyllaDBOnlineStoreCreator(OnlineStoreCreator):
    """
    Online store creator for ScyllaDB integration tests.

    Reads connection details from environment variables:

        SCYLLA_HOSTS      comma-separated contact points (required)
        SCYLLA_KEYSPACE   keyspace name (default: feast_test)
        SCYLLA_USERNAME   username (optional)
        SCYLLA_PASSWORD   password (optional)
        SCYLLA_LOCAL_DC   datacenter name (optional, required for vector search)
    """

    def __init__(self, project_name: str, **kwargs):
        super().__init__(project_name)

    def create_online_store(self) -> Dict[str, object]:
        hosts_env = os.environ.get("SCYLLA_HOSTS", "")
        if not hosts_env:
            raise RuntimeError(
                "Set SCYLLA_HOSTS (comma-separated) to run ScyllaDB integration tests."
            )
        store: Dict[str, object] = {
            "type": "scylladb",
            "hosts": [h.strip() for h in hosts_env.split(",")],
            "keyspace": os.environ.get("SCYLLA_KEYSPACE", "feast_test"),
        }
        username = os.environ.get("SCYLLA_USERNAME")
        password = os.environ.get("SCYLLA_PASSWORD")
        local_dc = os.environ.get("SCYLLA_LOCAL_DC")
        if username:
            store["username"] = username
        if password:
            store["password"] = password
        if local_dc:
            store["local_dc"] = local_dc
        return store

    def teardown(self):
        pass
