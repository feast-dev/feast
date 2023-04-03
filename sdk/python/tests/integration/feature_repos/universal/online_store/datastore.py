import os
from typing import Dict

from google.cloud import datastore
from testcontainers.core.container import DockerContainer
from testcontainers.core.waiting_utils import wait_for_logs

from tests.integration.feature_repos.universal.online_store_creator import (
    OnlineStoreCreator,
)


class DatastoreOnlineStoreCreator(OnlineStoreCreator):
    def __init__(self, project_name: str, **kwargs):
        super().__init__(project_name)
        self.container = (
            DockerContainer(
                "gcr.io/google.com/cloudsdktool/cloud-sdk:380.0.0-emulators"
            )
            .with_command(
                "gcloud beta emulators datastore start --project test-project --host-port 0.0.0.0:8081"
            )
            .with_exposed_ports("8081")
        )

    def create_online_store(self) -> Dict[str, str]:
        self.container.start()
        log_string_to_wait_for = r"\[datastore\] Dev App Server is now running"
        wait_for_logs(
            container=self.container, predicate=log_string_to_wait_for, timeout=10
        )
        exposed_port = self.container.get_exposed_port("8081")
        os.environ[datastore.client.DATASTORE_EMULATOR_HOST] = f"0.0.0.0:{exposed_port}"
        return {"type": "datastore", "project_id": "test-project"}

    def teardown(self):
        del os.environ[datastore.client.DATASTORE_EMULATOR_HOST]
        self.container.stop()
