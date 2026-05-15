import logging
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

import faiss
import numpy as np
from google.protobuf.timestamp_pb2 import Timestamp

from feast import Entity, FeatureView, RepoConfig
from feast.infra.key_encoding_utils import serialize_entity_key
from feast.infra.online_stores.helpers import compute_table_id
from feast.infra.online_stores.online_store import OnlineStore
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from feast.repo_config import FeastConfigBaseModel


class FaissOnlineStoreConfig(FeastConfigBaseModel):
    dimension: int
    index_path: str
    index_type: str = "IVFFlat"
    nlist: int = 100


class InMemoryStore:
    def __init__(self):
        self.feature_names: List[str] = []
        self.entity_keys: Dict[str, int] = {}

    def update(self, feature_names: List[str], entity_keys: Dict[str, int]):
        self.feature_names = feature_names
        self.entity_keys = entity_keys

    def delete(self, entity_keys: List[str]):
        for entity_key in entity_keys:
            if entity_key in self.entity_keys:
                del self.entity_keys[entity_key]

    def read(self, entity_keys: List[str]) -> List[Optional[int]]:
        return [self.entity_keys.get(entity_key) for entity_key in entity_keys]

    def teardown(self):
        self.feature_names = []
        self.entity_keys = {}


def _table_id(project: str, table: FeatureView, enable_versioning: bool = False) -> str:
    return compute_table_id(project, table, enable_versioning)


class FaissOnlineStore(OnlineStore):
    _logger: logging.Logger = logging.getLogger(__name__)

    def __init__(self):
        super().__init__()
        self._indices: Dict[str, faiss.IndexIVFFlat] = {}
        self._in_memory_stores: Dict[str, InMemoryStore] = {}
        self._config: Optional[FaissOnlineStoreConfig] = None

    def _get_index(self, table_key: str) -> Optional[faiss.IndexIVFFlat]:
        return self._indices.get(table_key)

    def update(
        self,
        config: RepoConfig,
        tables_to_delete: Sequence[FeatureView],
        tables_to_keep: Sequence[FeatureView],
        entities_to_delete: Sequence[Entity],
        entities_to_keep: Sequence[Entity],
        partial: bool,
    ):
        self._config = FaissOnlineStoreConfig(**config.online_store.dict())
        versioning = config.registry.enable_online_feature_view_versioning

        for table in tables_to_delete:
            table_key = _table_id(config.project, table, versioning)
            self._indices.pop(table_key, None)
            self._in_memory_stores.pop(table_key, None)

        for table in tables_to_keep:
            table_key = _table_id(config.project, table, versioning)
            feature_names = [f.name for f in table.features]
            dimension = len(feature_names)

            if table_key not in self._indices or not partial:
                quantizer = faiss.IndexFlatL2(dimension)
                index = faiss.IndexIVFFlat(quantizer, dimension, self._config.nlist)
                index.train(
                    np.random.rand(self._config.nlist * 100, dimension).astype(
                        np.float32
                    )
                )
                self._indices[table_key] = index
                self._in_memory_stores[table_key] = InMemoryStore()

            self._in_memory_stores[table_key].update(feature_names, {})

    def teardown(
        self,
        config: RepoConfig,
        tables: Sequence[FeatureView],
        entities: Sequence[Entity],
    ):
        versioning = config.registry.enable_online_feature_view_versioning
        for table in tables:
            table_key = _table_id(config.project, table, versioning)
            self._indices.pop(table_key, None)
            store = self._in_memory_stores.pop(table_key, None)
            if store is not None:
                store.teardown()

    def online_read(
        self,
        config: RepoConfig,
        table: FeatureView,
        entity_keys: List[EntityKeyProto],
        requested_features: Optional[List[str]] = None,
    ) -> List[Tuple[Optional[datetime], Optional[Dict[str, ValueProto]]]]:
        versioning = config.registry.enable_online_feature_view_versioning
        table_key = _table_id(config.project, table, versioning)
        index = self._get_index(table_key)
        in_memory_store = self._in_memory_stores.get(table_key)

        if index is None or in_memory_store is None:
            return [(None, None)] * len(entity_keys)

        results: List[Tuple[Optional[datetime], Optional[Dict[str, Any]]]] = []
        for entity_key in entity_keys:
            serialized_key = serialize_entity_key(
                entity_key, config.entity_key_serialization_version
            ).hex()
            idx = in_memory_store.entity_keys.get(serialized_key, -1)
            if idx == -1:
                results.append((None, None))
            else:
                feature_vector = index.reconstruct(int(idx))
                feature_dict = {
                    name: ValueProto(double_val=value)
                    for name, value in zip(
                        in_memory_store.feature_names, feature_vector
                    )
                }
                results.append((None, feature_dict))
        return results

    def online_write_batch(
        self,
        config: RepoConfig,
        table: FeatureView,
        data: List[
            Tuple[EntityKeyProto, Dict[str, ValueProto], datetime, Optional[datetime]]
        ],
        progress: Optional[Callable[[int], Any]],
    ) -> None:
        versioning = config.registry.enable_online_feature_view_versioning
        table_key = _table_id(config.project, table, versioning)
        index = self._get_index(table_key)
        in_memory_store = self._in_memory_stores.get(table_key)

        if index is None or in_memory_store is None:
            self._logger.warning(
                "Index for table '%s' is not initialized. Skipping write operation.",
                table_key,
            )
            return

        feature_vectors = []
        serialized_keys = []

        for entity_key, feature_dict, _, _ in data:
            serialized_key = serialize_entity_key(
                entity_key, config.entity_key_serialization_version
            ).hex()
            feature_vector = np.array(
                [
                    feature_dict[name].double_val
                    for name in in_memory_store.feature_names
                ],
                dtype=np.float32,
            )

            feature_vectors.append(feature_vector)
            serialized_keys.append(serialized_key)

        feature_vectors_array = np.array(feature_vectors)

        existing_indices = [
            in_memory_store.entity_keys.get(sk, -1) for sk in serialized_keys
        ]
        mask = np.array(existing_indices) != -1
        if np.any(mask):
            index.remove_ids(np.array([idx for idx in existing_indices if idx != -1]))

        new_indices = np.arange(index.ntotal, index.ntotal + len(feature_vectors_array))
        index.add(feature_vectors_array)

        for sk, idx in zip(serialized_keys, new_indices):
            in_memory_store.entity_keys[sk] = idx

        if progress:
            progress(len(data))

    def retrieve_online_documents(
        self,
        config: RepoConfig,
        table: FeatureView,
        requested_features: List[str],
        embedding: List[float],
        top_k: int,
        distance_metric: Optional[str] = None,
    ) -> List[
        Tuple[
            Optional[datetime],
            Optional[EntityKeyProto],
            Optional[ValueProto],
            Optional[ValueProto],
            Optional[ValueProto],
        ]
    ]:
        versioning = config.registry.enable_online_feature_view_versioning
        table_key = _table_id(config.project, table, versioning)
        index = self._get_index(table_key)

        if index is None:
            self._logger.warning("Index is not initialized. Returning empty result.")
            return []

        query_vector = np.array(embedding, dtype=np.float32).reshape(1, -1)
        distances, indices = index.search(query_vector, top_k)

        results: List[
            Tuple[
                Optional[datetime],
                Optional[EntityKeyProto],
                Optional[ValueProto],
                Optional[ValueProto],
                Optional[ValueProto],
            ]
        ] = []
        for i, idx in enumerate(indices[0]):
            if idx == -1:
                continue

            feature_vector = index.reconstruct(int(idx))

            timestamp = Timestamp()
            timestamp.GetCurrentTime()
            entity_value = EntityKeyProto()
            feature_value = ValueProto(string_val=",".join(map(str, feature_vector)))
            vector_value = ValueProto(string_val=",".join(map(str, feature_vector)))
            distance_value = ValueProto(float_val=distances[0][i])

            results.append(
                (
                    timestamp.ToDatetime(),
                    entity_value,
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
        entity_keys: List[EntityKeyProto],
        requested_features: Optional[List[str]] = None,
    ) -> List[Tuple[Optional[datetime], Optional[Dict[str, ValueProto]]]]:
        raise NotImplementedError("Async read is not implemented for FaissOnlineStore")
