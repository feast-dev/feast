import os
from typing import Dict

from google.cloud import bigtable
from testcontainers.core.container import DockerContainer
from testcontainers.core.waiting_utils import wait_for_logs

from tests.integration.feature_repos.universal.online_store_creator import (
    OnlineStoreCreator,
)


class BigtableOnlineStoreCreator(OnlineStoreCreator):
    gcp_project = "test-project"
    host = "0.0.0.0"
    port = "8086"
    bt_instance = "test-instance"

    def __init__(self, project_name: str, **kwargs):
        super().__init__(project_name)
        self.container = (
            DockerContainer(
                "gcr.io/google.com/cloudsdktool/cloud-sdk:380.0.0-emulators"
            )
            .with_command(
                f"gcloud beta emulators bigtable start --project {self.gcp_project} --host-port {self.host}:{self.port}"
            )
            .with_exposed_ports(self.port)
        )

    def create_online_store(self) -> Dict[str, str]:
        self.container.start()
        log_string_to_wait_for = r"\[bigtable\] Cloud Bigtable emulator running"
        wait_for_logs(
            container=self.container, predicate=log_string_to_wait_for, timeout=10
        )
        exposed_port = self.container.get_exposed_port(self.port)
        os.environ[bigtable.client.BIGTABLE_EMULATOR] = f"{self.host}:{exposed_port}"
        return {
            "type": "bigtable",
            "project_id": self.gcp_project,
            "instance": self.bt_instance,
        }

    def teardown(self):
        del os.environ[bigtable.client.BIGTABLE_EMULATOR]
        self.container.stop()
