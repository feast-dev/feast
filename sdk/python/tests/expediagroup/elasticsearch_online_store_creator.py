import logging

from testcontainers.elasticsearch import ElasticSearchContainer

from tests.integration.feature_repos.universal.online_store_creator import (
    OnlineStoreCreator,
)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ElasticsearchOnlineStoreCreator(OnlineStoreCreator):
    def __init__(self, project_name: str):
        super().__init__(project_name)
        self.es_port = 9200
        self.elasticsearch_container = ElasticSearchContainer(
            image="docker.elastic.co/elasticsearch/elasticsearch:8.8.2",
            port=self.es_port,
        )

    def create_online_store(self):
        # Start the container
        self.elasticsearch_container.start()
        elasticsearch_host = self.elasticsearch_container.get_container_host_ip()
        elasticsearch_http_port = self.elasticsearch_container.get_exposed_port(
            self.es_port
        )
        return {
            "host": elasticsearch_host,
            "port": elasticsearch_http_port,
            "username": "",
            "password": "",
            "token": "",
        }

    def teardown(self):
        self.elasticsearch_container.stop()
