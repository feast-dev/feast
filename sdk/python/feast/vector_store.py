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
from abc import ABC, abstractmethod
from typing import List, Optional, Any, Dict, cast

import numpy as np
from feast import FeatureStore, FeatureView


class VectorStore(ABC):
    """Abstract base class for vector stores used in RAG retrieval."""
    
    @abstractmethod
    def query(
        self,
        query_vector: Optional[np.ndarray] = None,
        query_string: Optional[str] = None,
        top_k: int = 10,
    ) -> List[Dict[str, Any]]:
        """Query the vector store for similar vectors.
        
        Args:
            query_vector: Optional vector to use for similarity search
            query_string: Optional text query for semantic search
            top_k: Number of results to return
            
        Returns:
            List of dictionaries containing the retrieved documents and their metadata
        """
        pass


class FeastVectorStore(VectorStore):
    """Feast-based vector store implementation."""
    
    def __init__(self, store: FeatureStore, rag_view: FeatureView, features: List[str]):
        """Initialize the Feast vector store.
        
        Args:
            store: Feast feature store instance
            rag_view: Feature view containing the vector data
            features: List of feature names to retrieve
        """
        self.store = store
        self.rag_view = rag_view
        self.store.apply([rag_view])
        self.features = features

    def query(
        self,
        query_vector: Optional[np.ndarray] = None,
        query_string: Optional[str] = None,
        top_k: int = 10,
    ) -> List[Dict[str, Any]]:
        """Query the Feast vector store.
        
        Args:
            query_vector: Optional vector to use for similarity search
            query_string: Optional text query for semantic search
            top_k: Number of results to return
            
        Returns:
            List of dictionaries containing the retrieved documents and their metadata
        """
        distance_metric = "COSINE" if query_vector is not None else None
        query_list = query_vector.tolist() if query_vector is not None else None

        response = self.store.retrieve_online_documents_v2(
            features=self.features,
            query=query_list,
            query_string=query_string,
            top_k=top_k,
            distance_metric=distance_metric,
        ).to_dict()
        
        results: List[Dict[str, Any]] = []
        # response_dict = cast(Dict[str, List[Any]], response.to_dict())
        
        for feature_name in self.features:
            short_name = feature_name.split(":")[-1]
            feature_values = response[short_name]
            for i, value in enumerate(feature_values):
                if i >= len(results):
                    results.append({})
                results[i][short_name] = value
            
        return results
