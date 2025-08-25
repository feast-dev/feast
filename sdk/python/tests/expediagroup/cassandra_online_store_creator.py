from typing import Dict

from testcontainers.cassandra import CassandraContainer
from testcontainers.core.waiting_utils import wait_for_logs

from tests.integration.feature_repos.universal.online_store_creator import (
    OnlineStoreCreator,
)


class EGCassandraOnlineStoreCreator(OnlineStoreCreator):
    def __init__(self, project_name: str, **kwargs):
        super().__init__(project_name)
        with CassandraContainer("cassandra:4.1.4") as cassandra_container:
            self.container = cassandra_container

    def create_online_store(self) -> Dict[str, str]:
        self.container.start()
        log_string_to_wait_for = "Startup complete"
        wait_for_logs(
            container=self.container, predicate=log_string_to_wait_for, timeout=120
        )
        host = self.container.get_container_host_ip()
        exposed_port = int(self.container.get_exposed_port(self.container.CQL_PORT))

        return {
            "alias": "default",
            "type": "eg-cassandra",
            "hosts": [host],
            "port": exposed_port,
            "keyspace": "feast_keyspace",
            "username": None,
            "password": None,
            "table_name_format_version": 1,
        }

    def teardown(self):
        self.container.stop()
