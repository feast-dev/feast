import time
from typing import Dict

import requests
from testcontainers.core.container import DockerContainer
from testcontainers.core.waiting_utils import wait_for_logs

from tests.integration.feature_repos.universal.online_store_creator import (
    OnlineStoreCreator,
)


class CouchbaseOnlineStoreCreator(OnlineStoreCreator):
    def __init__(self, project_name: str, **kwargs):
        super().__init__(project_name)
        # Using the latest Couchbase Enterprise version
        self.container = DockerContainer(
            "couchbase/server:enterprise-7.6.3"
        ).with_exposed_ports(
            "8091",  # REST/HTTP interface - mgmt
            "8092",  # Views - C api
            "8093",  # Query - n1ql
            "8094",  # Search
            "8095",  # Analytics
            "8096",  # Eventing
            "11210",  # Key-Value
        )
        self.username = "Administrator"
        self.password = "password"
        self.bucket_name = f"feast_{project_name}"

    def create_online_store(self) -> Dict[str, object]:
        self.container.start()

        # Wait for Couchbase server to be ready
        log_string_to_wait_for = "Starting Couchbase Server"
        wait_for_logs(
            container=self.container, predicate=log_string_to_wait_for, timeout=120
        )

        # Get the exposed ports
        rest_port = self.container.get_exposed_port("8091")
        views_port = self.container.get_exposed_port("8092")
        query_port = self.container.get_exposed_port("8093")
        kv_port = self.container.get_exposed_port("11210")
        base_url = f"http://127.0.0.1:{rest_port}"

        port_map = {
            "rest": rest_port,
            "views": views_port,
            "query": query_port,
            "kv": kv_port,
        }

        # Wait for the server to be fully available
        self._wait_for_server_ready(base_url)

        # Initialize the cluster
        self._initialize_cluster(base_url, port_map)

        # Create bucket
        self._create_bucket(base_url)

        # Wait for the credentials to be valid
        time.sleep(5)

        # Return the configuration for Feast
        return {
            "type": "couchbase",
            "connection_string": "couchbase://127.0.0.1",
            "user": self.username,
            "password": self.password,
            "bucket_name": self.bucket_name,
            "kv_port": int(kv_port),
        }

    def _wait_for_server_ready(self, base_url: str, timeout: int = 120):
        start_time = time.time()
        while True:
            try:
                response = requests.get(f"{base_url}/pools")
                if response.status_code == 200:
                    break
            except requests.RequestException:
                pass

            if time.time() - start_time > timeout:
                raise TimeoutError(
                    f"Couchbase server failed to start after {timeout} seconds"
                )

            time.sleep(1)

    def _initialize_cluster(self, base_url: str, ports: Dict[str, int]):
        # Initialize services
        services_data = {"services": "kv,n1ql,index"}
        requests.post(f"{base_url}/node/controller/setupServices", data=services_data)

        # Initialize memory quotas
        quota_data = {"memoryQuota": "256", "indexMemoryQuota": "256"}
        requests.post(f"{base_url}/pools/default", data=quota_data)

        # Set administrator credentials
        credentials_data = {
            "username": self.username,
            "password": self.password,
            "port": "SAME",
        }
        requests.post(f"{base_url}/settings/web", data=credentials_data)

        # Initialize index storage mode
        index_data = {"storageMode": "memory_optimized"}
        requests.post(
            f"{base_url}/settings/indexes",
            data=index_data,
            auth=(self.username, self.password),
        )

        # Set up alternate addresses
        payload = {
            "hostname": "127.0.0.1",
            "kv": ports["kv"],  # KV service port
            "n1ql": ports["query"],  # Query service port
            "capi": ports["views"],  # Views service port
            "mgmt": ports["rest"],  # REST/HTTP interface port
        }

        requests.put(
            f"{base_url}/node/controller/setupAlternateAddresses/external",
            data=payload,
            auth=(self.username, self.password),
        )

    def _create_bucket(self, base_url: str):
        bucket_data = {
            "name": self.bucket_name,
            "bucketType": "couchbase",
            "ramQuotaMB": "128",
            "durabilityMinLevel": "none",
        }

        requests.post(
            f"{base_url}/pools/default/buckets",
            data=bucket_data,
            auth=(self.username, self.password),
        )

    def teardown(self):
        self.container.stop()
