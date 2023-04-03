#
#  Copyright 2019 The Feast Authors
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      https://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#

import time
from typing import Dict

from testcontainers.core.container import DockerContainer
from testcontainers.core.waiting_utils import wait_for_logs

from tests.integration.feature_repos.universal.online_store_creator import (
    OnlineStoreCreator,
)


class CassandraOnlineStoreCreator(OnlineStoreCreator):
    def __init__(self, project_name: str, **kwargs):
        super().__init__(project_name)
        self.container = DockerContainer("library/cassandra:4.0.4").with_exposed_ports(
            "9042"
        )

    def create_online_store(self) -> Dict[str, object]:
        self.container.start()
        log_string_to_wait_for = "Startup complete"
        # on a modern machine it takes about 45-60 seconds for the container
        # to start accepting CQL requests:
        wait_for_logs(
            container=self.container, predicate=log_string_to_wait_for, timeout=90
        )
        keyspace_name = "feast_keyspace"
        keyspace_creation_command = f"create KEYSPACE \"{keyspace_name}\" WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}};"
        self.container.exec(f'cqlsh -e "{keyspace_creation_command}"')
        time.sleep(2)
        exposed_port = int(self.container.get_exposed_port("9042"))
        return {
            "type": "cassandra",
            "hosts": ["127.0.0.1"],
            "port": exposed_port,
            "keyspace": keyspace_name,
        }

    def teardown(self):
        self.container.stop()
