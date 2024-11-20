from datetime import datetime
from typing import Any, Callable, Dict, List, Literal, Optional, Sequence, Tuple

from pydantic import StrictStr
from pymilvus import (
    Collection,
    CollectionSchema,
    DataType,
    FieldSchema,
    connections,
)

from feast import Entity
from feast.feature_view import FeatureView
from feast.infra.infra_object import InfraObject
from feast.infra.key_encoding_utils import (
    deserialize_entity_key,
    serialize_entity_key,
)
from feast.infra.online_stores.online_store import OnlineStore
from feast.infra.online_stores.vector_store import VectorStoreConfig
from feast.protos.feast.core.InfraObject_pb2 import InfraObject as InfraObjectProto
from feast.protos.feast.core.Registry_pb2 import Registry as RegistryProto
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from feast.repo_config import FeastConfigBaseModel, RepoConfig
from feast.utils import (
    _build_retrieve_online_document_record,
    to_naive_utc,
)


class MilvusOnlineStoreConfig(FeastConfigBaseModel, VectorStoreConfig):
    """
    Configuration for the Milvus online store.
    NOTE: The class *must* end with the `OnlineStoreConfig` suffix.
    """

    type: Literal["milvus"] = "milvus"

    host: Optional[StrictStr] = "localhost"
    port: Optional[int] = 19530
    index_type: Optional[str] = "IVF_FLAT"
    metric_type: Optional[str] = "L2"
    embedding_dim: Optional[int] = 128
    vector_enabled: Optional[bool] = True


class MilvusOnlineStore(OnlineStore):
    """
    Milvus implementation of the online store interface.

    Attributes:
        _collections: Dictionary to cache Milvus collections.
    """

    _collections: Dict[str, Collection] = {}

    def _connect(self, config: RepoConfig):
        connections.connect(
            alias="feast",
            host=config.online_store.host,
            port=str(config.online_store.port),
        )

    def _get_collection(self, config: RepoConfig, table: FeatureView) -> Collection:
        collection_name = _table_id(config.project, table)
        if collection_name not in self._collections:
            self._connect(config)

            fields = [
                FieldSchema(
                    name="pk", dtype=DataType.INT64, is_primary=True, auto_id=True
                ),
                FieldSchema(name="entity_key", dtype=DataType.VARCHAR, max_length=512),
                FieldSchema(
                    name="feature_name", dtype=DataType.VARCHAR, max_length=256
                ),
                FieldSchema(name="value", dtype=DataType.BINARY_VECTOR, dim=8 * 1024),
                FieldSchema(
                    name="vector_value",
                    dtype=DataType.FLOAT_VECTOR,
                    dim=config.online_store.embedding_dim,
                ),
                FieldSchema(name="event_ts", dtype=DataType.INT64),
                FieldSchema(name="created_ts", dtype=DataType.INT64),
            ]
            schema = CollectionSchema(
                fields=fields, description="Feast feature view data"
            )
            collection = Collection(
                name=collection_name, schema=schema, using="default"
            )
            if not collection.has_index():
                index_params = {
                    "index_type": config.online_store.index_type,
                    "metric_type": config.online_store.metric_type,
                    "params": {"nlist": 128},
                }
                collection.create_index(
                    field_name="vector_value", index_params=index_params
                )
            collection.load()
            self._collections[collection_name] = collection
        return self._collections[collection_name]

    def online_write_batch(
        self,
        config: RepoConfig,
        table: FeatureView,
        data: List[
            Tuple[
                EntityKeyProto,
                Dict[str, ValueProto],
                datetime,
                Optional[datetime],
            ]
        ],
        progress: Optional[Callable[[int], Any]],
    ) -> None:
        collection = self._get_collection(config, table)
        entity_keys = []
        feature_names = []
        values = []
        vector_values = []
        event_tss = []
        created_tss = []

        for entity_key, values_dict, timestamp, created_ts in data:
            entity_key_str = serialize_entity_key(
                entity_key,
                entity_key_serialization_version=config.entity_key_serialization_version,
            ).hex()
            timestamp_int = int(to_naive_utc(timestamp).timestamp() * 1e6)
            created_ts_int = (
                int(to_naive_utc(created_ts).timestamp() * 1e6) if created_ts else 0
            )
            for feature_name, val in values_dict.items():
                entity_keys.append(entity_key_str)
                feature_names.append(feature_name)
                values.append(val.SerializeToString())
                if config.online_store.vector_enabled:
                    vector_values.append(val.float_list_val.val)
                else:
                    vector_values.append([0.0] * config.online_store.embedding_dim)
                event_tss.append(timestamp_int)
                created_tss.append(created_ts_int)
            if progress:
                progress(1)

        if entity_keys:
            insert_data = {
                "entity_key": entity_keys,
                "feature_name": feature_names,
                "value": values,
                "vector_value": vector_values,
                "event_ts": event_tss,
                "created_ts": created_tss,
            }
            collection.insert(insert_data)
            collection.flush()

    def online_read(
        self,
        config: RepoConfig,
        table: FeatureView,
        entity_keys: List[EntityKeyProto],
        requested_features: Optional[List[str]] = None,
    ) -> List[Tuple[Optional[datetime], Optional[Dict[str, ValueProto]]]]:
        collection = self._get_collection(config, table)
        results = []

        for entity_key in entity_keys:
            entity_key_str = serialize_entity_key(
                entity_key,
                entity_key_serialization_version=config.entity_key_serialization_version,
            ).hex()
            expr = f"entity_key == '{entity_key_str}'"
            if requested_features:
                features_str = ", ".join([f"'{f}'" for f in requested_features])
                expr += f" && feature_name in [{features_str}]"

            res = collection.query(
                expr,
                output_fields=["feature_name", "value", "event_ts"],
                consistency_level="Strong",
            )

            res_dict = {}
            res_ts = None
            for r in res:
                feature_name = r["feature_name"]
                val_bin = r["value"]
                val = ValueProto()
                val.ParseFromString(val_bin)
                res_dict[feature_name] = val
                res_ts = datetime.fromtimestamp(r["event_ts"] / 1e6)
            if not res_dict:
                results.append((None, None))
            else:
                results.append((res_ts, res_dict))
        return results

    def update(
        self,
        config: RepoConfig,
        tables_to_delete: Sequence[FeatureView],
        tables_to_keep: Sequence[FeatureView],
        entities_to_delete: Sequence[Entity],
        entities_to_keep: Sequence[Entity],
        partial: bool,
    ):
        self._connect(config)
        for table in tables_to_keep:
            self._get_collection(config, table)
        for table in tables_to_delete:
            collection_name = _table_id(config.project, table)
            collection = Collection(name=collection_name)
            if collection.exists():
                collection.drop()
                self._collections.pop(collection_name, None)

    def plan(
        self, config: RepoConfig, desired_registry_proto: RegistryProto
    ) -> List[InfraObject]:
        project = config.project

        infra_objects: List[InfraObject] = [
            MilvusTable(
                host=config.online_store.host,
                port=config.online_store.port,
                name=_table_id(project, FeatureView.from_proto(view)),
            )
            for view in [
                *desired_registry_proto.feature_views,
                *desired_registry_proto.stream_feature_views,
            ]
        ]
        return infra_objects

    def teardown(
        self,
        config: RepoConfig,
        tables: Sequence[FeatureView],
        entities: Sequence[Entity],
    ):
        self._connect(config)
        for table in tables:
            collection_name = _table_id(config.project, table)
            collection = Collection(name=collection_name)
            if collection.exists():
                collection.drop()
                self._collections.pop(collection_name, None)

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
            Optional[EntityKeyProto],
            Optional[ValueProto],
            Optional[ValueProto],
            Optional[ValueProto],
        ]
    ]:
        collection = self._get_collection(config, table)
        if not config.online_store.vector_enabled:
            raise ValueError("Vector search is not enabled in the online store config")

        search_params = {
            "metric_type": distance_metric or config.online_store.metric_type,
            "params": {"nprobe": 10},
        }
        expr = f"feature_name == '{requested_feature}'"

        results = collection.search(
            data=[embedding],
            anns_field="vector_value",
            param=search_params,
            limit=top_k,
            expr=expr,
            output_fields=["entity_key", "value", "event_ts"],
            consistency_level="Strong",
        )

        result_list = []
        for hits in results:
            for hit in hits:
                entity_key_str = hit.entity.get("entity_key")
                val_bin = hit.entity.get("value")
                val = ValueProto()
                val.ParseFromString(val_bin)
                distance = hit.distance
                event_ts = datetime.fromtimestamp(hit.entity.get("event_ts") / 1e6)
                entity_key = deserialize_entity_key(
                    bytes.fromhex(entity_key_str),
                    config.entity_key_serialization_version,
                )
                result_list.append(
                    _build_retrieve_online_document_record(
                        entity_key,
                        val.SerializeToString(),
                        embedding,
                        distance,
                        event_ts,
                        config.entity_key_serialization_version,
                    )
                )
        return result_list


def _table_id(project: str, table: FeatureView) -> str:
    return f"{project}_{table.name}"


class MilvusTable(InfraObject):
    """
    A Milvus collection managed by Feast.

    Attributes:
        host: The host of the Milvus server.
        port: The port of the Milvus server.
        name: The name of the collection.
    """

    host: str
    port: int

    def __init__(self, host: str, port: int, name: str):
        super().__init__(name)
        self.host = host
        self.port = port
        self._connect()

    def _connect(self):
        return connections.connect(alias="default", host=self.host, port=str(self.port))

    def to_infra_object_proto(self) -> InfraObjectProto:
        # Implement serialization if needed
        pass

    def update(self):
        # Implement update logic if needed
        pass

    def teardown(self):
        collection = Collection(name=self.name)
        if collection.exists():
            collection.drop()
