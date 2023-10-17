import logging

from elasticsearch import Elasticsearch
from testcontainers.elasticsearch import ElasticsearchContainer
from tests.integration.feature_repos.universal.online_store_creator import (
    OnlineStoreCreator,
)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ElasticsearchOnlineCreator(OnlineStoreCreator):
    def __init__(self, project_name: str, index_name: str, es_host: str, es_port: int):
        super().__init__(project_name)
        self.elasticsearch_container = ElasticsearchContainer("docker.elastic.co/elasticsearch/elasticsearch:8.10.4")
        # Obtain the host and ports for Elasticsearch
        elasticsearch_host = self.elasticsearch_container.get_container_host_ip()
        elasticsearch_http_port = self.elasticsearch_container.get_exposed_port(9200)

        self.es = Elasticsearch([{"host": elasticsearch_host, "port": elasticsearch_http_port}])

    def create_online_store(self):
        # Start the container
        self.elasticsearch_container.start()
        self.es.start()
        # Check if Elasticsearch is running and obtain cluster information
        if self.es.ping():
            cluster_info = self.es.cluster.health()
            logger.info(f"Elasticsearch cluster status: {cluster_info['status']}")
        else:
            logger.info("Elasticsearch is not available.")

    def teardown(self):
        self.es.close()
        self.elasticsearch_container.stop()
