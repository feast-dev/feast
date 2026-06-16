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

from typing import Any, Dict

from testcontainers.core.container import DockerContainer
from testcontainers.core.waiting_utils import wait_for_logs

from tests.universal.feature_repos.universal.online_store_creator import (
    OnlineStoreCreator,
)

# Aerospike Community Edition — pin a known-good server image so CI stays
# deterministic; bump when Feast's Aerospike server baseline moves.
AEROSPIKE_CE_IMAGE = "aerospike/aerospike-server:8.0.0.10_1"

# The Aerospike server prints this line once the default "test" namespace has
# finished starting up and the service is accepting client connections.
_READY_LOG_MARKER = "migrations: complete"


class AerospikeOnlineStoreCreator(OnlineStoreCreator):
    """Spin up a single-node Aerospike CE cluster for universal online-store tests.

    The CE image ships a pre-configured namespace called ``test``, which we
    reuse as the Feast namespace (avoiding the need to mount a custom
    ``aerospike.conf``).
    """

    def __init__(self, project_name: str, **kwargs):
        super().__init__(project_name)
        self.container = DockerContainer(AEROSPIKE_CE_IMAGE).with_exposed_ports("3000")

    def create_online_store(self) -> Dict[str, Any]:
        self.container.start()
        wait_for_logs(container=self.container, predicate=_READY_LOG_MARKER, timeout=60)
        exposed_port = int(self.container.get_exposed_port("3000"))
        return {
            "type": "aerospike",
            "hosts": [("127.0.0.1", exposed_port)],
            "namespace": "test",
        }

    def teardown(self):
        self.container.stop()
