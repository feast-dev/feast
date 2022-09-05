import os
from typing import Dict

from google.cloud import bigtable
from testcontainers.core.container import DockerContainer
from testcontainers.core.waiting_utils import wait_for_logs

from tests.integration.feature_repos.universal.online_store_creator import (
    OnlineStoreCreator,
)


# TODO: deduplicate code between this and the datastore creator
class BigTableOnlineStoreCreator(OnlineStoreCreator):
    def __init__(self, project_name: str, **kwargs):
        super().__init__(project_name)
        self.container = (
            DockerContainer(
                "gcr.io/google.com/cloudsdktool/cloud-sdk:380.0.0-emulators"
            )
            .with_command(
                "gcloud beta emulators bigtable start --project test-project --host-port 0.0.0.0:8082"
            )
            .with_exposed_ports("8082")
        )

    def create_online_store(self) -> Dict[str, str]:
        self.container.start()
        log_string_to_wait_for = r"\[bigtable\] Dev App Server is now running"
        wait_for_logs(
            container=self.container, predicate=log_string_to_wait_for, timeout=10
        )
        exposed_port = self.container.get_exposed_port("8082")
        os.environ[bigtable.client.BIGTABLE_EMULATOR] = f"0.0.0.0:{exposed_port}"
        return {"type": "bigtable", "project_id": "test-project"}

    def teardown(self):
        del os.environ[bigtable.client.BIGTABLE_EMULATOR]
        self.container.stop()
