# Copyright 2019 The Feast Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
from typing import Optional

import numpy as np

from feast import FeatureStore, FeatureView
from feast.online_response import OnlineResponse


class FeastVectorStore:
    """
    Feast-based vector store implementation with support for text, image, and multi-modal search.
    This class provides a convenient interface for vector similarity search using Feast's
    vector database integrations. It supports:
    - Text similarity search using text embeddings
    - Image similarity search using image embeddings
    - Multi-modal search combining text and image queries
    """

    def __init__(self, repo_path: str, rag_view: FeatureView, features: list[str]):
        """Initialize the Feast vector store.

        Args:
            repo_path: Path to the Feast repo
            rag_view: Feature view
            features: List of feature names to retrieve
        """
        self._store = None  # Lazy load
        self._store_repo_path = repo_path
        self.rag_view = rag_view
        self.features = features

    @property
    def store(self):
        if self._store is None:
            self._store = FeatureStore(repo_path=self._store_repo_path)
            self._store.apply([self.rag_view])
            # TODO: Add validation to ensure store type is one of supported types e.g. pgvector, elasticsearch, milvus
        return self._store

    def query(
        self,
        query_vector: Optional[np.ndarray] = None,
        query_string: Optional[str] = None,
        query_image_bytes: Optional[bytes] = None,
        top_k: int = 10,
    ) -> OnlineResponse:
        """Query the Feast vector store with support for text, image, and multi-modal search.

        Args:
            query_vector: Optional vector to use for similarity search (text embeddings)
            query_string: Optional string query for keyword/semantic search
            query_image_bytes: Optional image bytes for image similarity search
            top_k: Number of results to return

        Returns:
            An OnlineResponse
        """
        query_list = query_vector.tolist() if query_vector is not None else None

        distance_metric = None
        for field in self.rag_view.schema:
            if hasattr(field, "vector_index") and field.vector_index:
                if hasattr(field, "vector_search_metric"):
                    distance_metric = field.vector_search_metric
                    break

        return self.store.retrieve_online_documents_v2(
            features=self.features,
            query=query_list,
            query_string=query_string,
            query_image_bytes=query_image_bytes,
            top_k=top_k,
            distance_metric=distance_metric,
        )
