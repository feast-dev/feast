import subprocess
import time
from typing import Dict

from testcontainers.core.container import DockerContainer

from tests.integration.feature_repos.universal.online_store_creator import (
    OnlineStoreCreator,
)


class SingleStoreOnlineStoreCreator(OnlineStoreCreator):
    def __init__(self, project_name: str, **kwargs):
        super().__init__(project_name)
        self.container = (
            DockerContainer("ghcr.io/singlestore-labs/singlestoredb-dev:latest")
            .with_exposed_ports(3306)
            .with_env("USER", "root")
            .with_env("ROOT_PASSWORD", "test")
            # this license key is authorized solely for use in SingleStore Feast tests and is subject to strict usage restrictions
            # if you want a free SingleStore license for your own use please visit https://www.singlestore.com/cloud-trial/
            .with_env(
                "LICENSE_KEY",
                "BGIxODZiYTg1YWUxYjRlODRhYzRjMGFmYTA1OTkxYzgyAAAAAAAAAAABAAAAAAAAACgwNQIZANx4NIXJ7CWvKYYb3wIyRXxBY7fdAnLeSwIYLy2Q0jA124GAkl04yuGrD59Zpv85DVYXAA==",
            )
        )

    def create_online_store(self) -> Dict[str, str]:
        self.container.start()
        time.sleep(30)
        exposed_port = self.container.get_exposed_port("3306")
        command = f"mysql -uroot -ptest -P {exposed_port} -e 'CREATE DATABASE feast;'"
        subprocess.run(command, shell=True, check=True)
        return {
            "type": "singlestore",
            "user": "root",
            "password": "test",
            "database": "feast",
            "port": exposed_port,
        }

    def teardown(self):
        self.container.stop()
