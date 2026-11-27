import os
from typing import Any

from tests.universal.feature_repos.universal.online_store_creator import (
    OnlineStoreCreator,
)


class PineconeOnlineStoreCreator(OnlineStoreCreator):
    def __init__(self, project_name: str, **kwargs):
        super().__init__(project_name)

    def create_online_store(self) -> dict[str, Any]:
        api_key = os.environ.get("PINECONE_API_KEY", "")
        return {
            "type": "pinecone",
            "api_key": api_key,
            "index_name": f"feast-test-{self.project_name}",
            "embedding_dim": 2,
            "metric": "cosine",
            "vector_enabled": True,
            "cloud": "aws",
            "region": "us-east-1",
        }

    def teardown(self):
        pass
