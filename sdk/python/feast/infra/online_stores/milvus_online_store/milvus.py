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
from feast.type_map import PROTO_VALUE_TO_VALUE_TYPE_MAP
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
from feast.types import VALUE_TYPES_TO_FEAST_TYPES, PrimitiveFeastType, Array, ValueType

PROTO_TO_MILVUS_TYPE_MAPPING = {
    PROTO_VALUE_TO_VALUE_TYPE_MAP['bytes_val']: DataType.STRING,
    PROTO_VALUE_TO_VALUE_TYPE_MAP['bool_val']: DataType.BOOL,
    PROTO_VALUE_TO_VALUE_TYPE_MAP['string_val']: DataType.STRING,
    PROTO_VALUE_TO_VALUE_TYPE_MAP['float_val']: DataType.FLOAT,
    PROTO_VALUE_TO_VALUE_TYPE_MAP['double_val']: DataType.DOUBLE,
    PROTO_VALUE_TO_VALUE_TYPE_MAP['int32_val']: DataType.INT32,
    PROTO_VALUE_TO_VALUE_TYPE_MAP['int64_val']: DataType.INT64,
    PROTO_VALUE_TO_VALUE_TYPE_MAP['float_list_val']: DataType.FLOAT_VECTOR,
    PROTO_VALUE_TO_VALUE_TYPE_MAP['int32_list_val']: DataType.FLOAT_VECTOR,
    PROTO_VALUE_TO_VALUE_TYPE_MAP['int64_list_val']: DataType.FLOAT_VECTOR,
    PROTO_VALUE_TO_VALUE_TYPE_MAP['double_list_val']: DataType.FLOAT_VECTOR,
    PROTO_VALUE_TO_VALUE_TYPE_MAP['bool_list_val']: DataType.BINARY_VECTOR,
}

FEAST_PRIMITIVE_TO_MILVUS_TYPE_MAPPING = {}

for value_type, feast_type in VALUE_TYPES_TO_FEAST_TYPES.items():
    if isinstance(feast_type, PrimitiveFeastType):
        milvus_type = PROTO_TO_MILVUS_TYPE_MAPPING.get(value_type)
        if milvus_type:
            FEAST_PRIMITIVE_TO_MILVUS_TYPE_MAPPING[feast_type] = milvus_type
    elif isinstance(feast_type, Array):
        base_type = feast_type.base_type
        base_value_type = base_type.to_value_type()
        if base_value_type in [ValueType.INT32, ValueType.INT64, ValueType.FLOAT, ValueType.DOUBLE]:
            FEAST_PRIMITIVE_TO_MILVUS_TYPE_MAPPING[feast_type] = DataType.FLOAT_VECTOR
        elif base_value_type == ValueType.BOOL:
            FEAST_PRIMITIVE_TO_MILVUS_TYPE_MAPPING[feast_type] = DataType.BINARY_VECTOR

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

    _conn: Optional[connections] = None
    _collections: Dict[str, Collection] = {}

    def _connect(self, config: RepoConfig) -> connections:
        if not self._conn:
            self._conn = connections.connect(
                alias="feast",
                host=config.online_store.host,
                port=str(config.online_store.port),
            )
        self._conn = connections.connect(
            alias="feast",
            host=config.online_store.host,
            port=str(config.online_store.port),
        )
        return self._conn

    def _get_collection(self, config: RepoConfig, table: FeatureView) -> Collection:
        collection_name = _table_id(config.project, table)
        if collection_name not in self._collections:
            self._connect(config)

            # Create a composite key by combining entity fields
            composite_key_name = '_'.join([field.name for field in table.entity_columns]) + "_pk"

            fields = [
                FieldSchema(name=composite_key_name, dtype=DataType.VARCHAR, max_length=512, is_primary=True),
                FieldSchema(name="event_ts", dtype=DataType.INT64),
                FieldSchema(name="created_ts", dtype=DataType.INT64),
            ]
            fields_to_exclude = [field.name for field in table.entity_columns] + ['event_ts', 'created_ts']
            fields_to_add = [f for f in table.schema if f.name not in fields_to_exclude]
            for field in fields_to_add:
                dtype = FEAST_PRIMITIVE_TO_MILVUS_TYPE_MAPPING.get(field.dtype)
                if dtype:
                    if dtype == DataType.FLOAT_VECTOR:
                        fields.append(FieldSchema(name=field.name, dtype=dtype, dim=config.online_store.embedding_dim))

                    else:
                        fields.append(FieldSchema(name=field.name, dtype=dtype))

            schema = CollectionSchema(fields=fields, description="Feast feature view data")
            collection = Collection(name=collection_name, schema=schema, using="feast")
            if not collection.has_index():
                index_params = {
                    "index_type": config.online_store.index_type,
                    "metric_type": config.online_store.metric_type,
                    "params": {"nlist": 128},
                }
            for vector_field in schema.fields:
                if vector_field.dtype in [DataType.FLOAT_VECTOR, DataType.BINARY_VECTOR]:
                    collection.create_index(field_name=vector_field.name, index_params=index_params)
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
        numeric_vector_list_types = [k for k in PROTO_VALUE_TO_VALUE_TYPE_MAP.keys() if k is not None and 'list' in k and 'string' not in k]

        entity_batch_to_insert = []
        for entity_key, values_dict, timestamp, created_ts in data:
            entity_key_str = serialize_entity_key(
                entity_key,
                entity_key_serialization_version=config.entity_key_serialization_version,
            ).hex()
            timestamp_int = int(to_naive_utc(timestamp).timestamp() * 1e6)
            created_ts_int = (
                int(to_naive_utc(created_ts).timestamp() * 1e6) if created_ts else 0
            )
            for feature_name in values_dict:
                for vector_list_type_name in numeric_vector_list_types:
                    vector_list = getattr(values_dict[feature_name], vector_list_type_name, None)
                    if vector_list:
                        vector_values = getattr(values_dict[feature_name], vector_list_type_name).val
                        if  vector_values != []:
                            # Note here we are over-writing the feature and collapsing the list into a single value
                            values_dict[feature_name] = vector_values

            single_entity_record = {
                "entity_key": entity_key_str,
                "event_ts": timestamp_int,
                "created_ts": created_ts_int,
            }
            single_entity_record.update(values_dict)
            entity_batch_to_insert.append(single_entity_record)

            if progress:
                progress(1)

        collection.insert(entity_batch_to_insert)
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
            if collection:
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
