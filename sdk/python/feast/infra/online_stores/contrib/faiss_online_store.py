import logging
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple, Union

import faiss
import numpy as np
from google.protobuf.timestamp_pb2 import Timestamp

from feast import Entity, FeatureView, RepoConfig
from feast.infra.online_stores.online_store import OnlineStore
from feast.protos.feast.types.EntityKey_pb2 import EntityKey
from feast.protos.feast.types.Value_pb2 import Value
from feast.repo_config import FeastConfigBaseModel


class FaissOnlineStoreConfig(FeastConfigBaseModel):
    dimension: int
    index_path: str
    index_type: str = "IVFFlat"
    nlist: int = 100


class InMemoryStore:
    def __init__(self):
        self.feature_names: List[str] = []
        self.entity_keys: Dict[Tuple[str, ...], int] = {}

    def update(self,
               feature_names: List[str],
               entity_keys: Dict[Tuple[str, ...], int]):
        self.feature_names = feature_names
        self.entity_keys = entity_keys

    def delete(self,
               entity_keys: List[Tuple[str, ...]]):
        for entity_key in entity_keys:
            if entity_key in self.entity_keys:
                del self.entity_keys[entity_key]

    def read(self,
             entity_keys: List[Tuple[str, ...]]) -> List[Optional[int]]:
        return [self.entity_keys.get(entity_key) for entity_key in entity_keys]

    def teardown(self):
        self.feature_names = []
        self.entity_keys = {}


class FaissOnlineStore(OnlineStore):
    _index: Optional[faiss.IndexIVFFlat] = None
    _in_memory_store: InMemoryStore = InMemoryStore()
    _config: Optional[FaissOnlineStoreConfig] = None
    _logger: logging.Logger = logging.getLogger(__name__)

    def _get_index(self,
                   config: RepoConfig) -> faiss.IndexIVFFlat:
        if self._index is None or self._config is None:
            raise ValueError("Index is not initialized")
        return self._index

    def update(
            self,
            config: RepoConfig,
            tables_to_delete: Sequence[FeatureView],
            tables_to_keep: Sequence[FeatureView],
            entities_to_delete: Sequence[Entity],
            entities_to_keep: Sequence[Entity],
            partial: bool,
    ):
        feature_views = tables_to_keep
        if not feature_views:
            return

        feature_names = [f.name for f in feature_views[0].features]
        dimension = len(feature_names)

        self._config = FaissOnlineStoreConfig(**config.online_store.dict())
        if self._index is None or not partial:
            quantizer = faiss.IndexFlatL2(dimension)
            self._index = faiss.IndexIVFFlat(quantizer, dimension, self._config.nlist)
            self._index.train(np.random.rand(self._config.nlist * 100, dimension).astype(np.float32))
            self._in_memory_store = InMemoryStore()

        self._in_memory_store.update(feature_names, {})

    def teardown(
            self,
            config: RepoConfig,
            tables: Sequence[FeatureView],
            entities: Sequence[Entity],
    ):
        self._index = None
        self._in_memory_store.teardown()

    def online_read(
            self,
            config: RepoConfig,
            table: FeatureView,
            entity_keys: List[EntityKey],
            requested_features: Optional[List[str]] = None,
    ) -> List[Tuple[Optional[datetime], Optional[Dict[str, Value]]]]:
        if self._index is None:
            return [(None, None)] * len(entity_keys)

        results: List[Tuple[Optional[datetime], Optional[Dict[str, Value]]]] = []
        for entity_key in entity_keys:
            entity_key_tuple = tuple(
                f"{field.name}:{field.value.string_val}"
                for field in entity_key.join_keys
            )
            idx = self._in_memory_store.entity_keys.get(entity_key_tuple, -1)
            if idx == -1:
                results.append((None, None))
            else:
                feature_vector = self._index.reconstruct(int(idx))
                feature_dict = {
                    name: Value(double_val=value)
                    for name, value in zip(
                        self._in_memory_store.feature_names, feature_vector
                    )
                }
                results.append((None, feature_dict))
        return results

    def online_write_batch(
            self,
            config: RepoConfig,
            table: FeatureView,
            data: List[Tuple[EntityKey, Dict[str, Value], datetime, Optional[datetime]]],
            progress: Optional[Callable[[int], Any]],
    ) -> None:
        if self._index is None:
            self._logger.warning("Index is not initialized. Skipping write operation.")
            return

        feature_vectors = []
        entity_key_tuples = []

        for entity_key, feature_dict, _, _ in data:
            entity_key_tuple = tuple(
                f"{field.name}:{field.value.string_val}"
                for field in entity_key.join_keys
            )
            feature_vector = np.array(
                [
                    feature_dict[name].double_val
                    for name in self._in_memory_store.feature_names
                ],
                dtype=np.float32,
            )

            feature_vectors.append(feature_vector)
            entity_key_tuples.append(entity_key_tuple)

        feature_vectors_array = np.array(feature_vectors)

        existing_indices = [
            self._in_memory_store.entity_keys.get(ekt, -1) for ekt in entity_key_tuples
        ]
        mask = np.array(existing_indices) != -1
        if np.any(mask):
            self._index.remove_ids(
                np.array([idx for idx in existing_indices if idx != -1])
            )

        new_indices = np.arange(
            self._index.ntotal, self._index.ntotal + len(feature_vectors_array)
        )
        self._index.add(feature_vectors_array)

        for ekt, idx in zip(entity_key_tuples, new_indices):
            self._in_memory_store.entity_keys[ekt] = idx

        if progress:
            progress(len(data))

    def retrieve_online_documents(
            self,
            config: RepoConfig,
            table: FeatureView,
            requested_feature: str,
            embedding: List[float],
            top_k: int,
            distance_metric: Optional[str] = None,
    ) -> List[
        Tuple[
            Optional[datetime],
            Optional[Value],
            Optional[Value],
            Optional[Value],
        ]
    ]:
        if self._index is None:
            self._logger.warning("Index is not initialized. Returning empty result.")
            return []

        query_vector = np.array(embedding, dtype=np.float32).reshape(1, -1)
        distances, indices = self._index.search(query_vector, top_k)

        results: List[
            Tuple[
                Optional[datetime],
                Optional[Value],
                Optional[Value],
                Optional[Value],
            ]
        ] = []
        for i, idx in enumerate(indices[0]):
            if idx == -1:
                continue

            feature_vector = self._index.reconstruct(int(idx))

            timestamp = Timestamp()
            timestamp.GetCurrentTime()

            feature_value = Value(string_val=",".join(map(str, feature_vector)))
            vector_value = Value(string_val=",".join(map(str, feature_vector)))
            distance_value = Value(float_val=distances[0][i])

            results.append(
                (
                    timestamp.ToDatetime(),
                    feature_value,
                    vector_value,
                    distance_value,
                )
            )

        return results

    async def online_read_async(
            self,
            config: RepoConfig,
            table: FeatureView,
            entity_keys: List[EntityKey],
            requested_features: Optional[List[str]] = None,
    ) -> List[Tuple[Optional[datetime], Optional[Dict[str, Value]]]]:
        # Implement async read if needed
        raise NotImplementedError("Async read is not implemented for FaissOnlineStore")
