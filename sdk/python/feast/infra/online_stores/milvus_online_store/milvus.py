from datetime import datetime
from typing import Any, Callable, Dict, List, Literal, Optional, Sequence, Tuple, Union

from pydantic import StrictStr
from pymilvus import (
    Collection,
    CollectionSchema,
    DataType,
    FieldSchema,
    MilvusClient,
)

from feast import Entity
from feast.feature_view import FeatureView
from feast.infra.infra_object import InfraObject
from feast.infra.key_encoding_utils import (
    serialize_entity_key,
)
from feast.infra.online_stores.online_store import OnlineStore
from feast.infra.online_stores.vector_store import VectorStoreConfig
from feast.protos.feast.core.InfraObject_pb2 import InfraObject as InfraObjectProto
from feast.protos.feast.core.Registry_pb2 import Registry as RegistryProto
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from feast.repo_config import FeastConfigBaseModel, RepoConfig
from feast.type_map import PROTO_VALUE_TO_VALUE_TYPE_MAP
from feast.types import (
    VALUE_TYPES_TO_FEAST_TYPES,
    Array,
    ComplexFeastType,
    PrimitiveFeastType,
    ValueType,
)
from feast.utils import (
    _build_retrieve_online_document_record,
    _serialize_vector_to_float_list,
    to_naive_utc,
)

PROTO_TO_MILVUS_TYPE_MAPPING: Dict[ValueType, DataType] = {
    PROTO_VALUE_TO_VALUE_TYPE_MAP["bytes_val"]: DataType.VARCHAR,
    PROTO_VALUE_TO_VALUE_TYPE_MAP["bool_val"]: DataType.BOOL,
    PROTO_VALUE_TO_VALUE_TYPE_MAP["string_val"]: DataType.VARCHAR,
    PROTO_VALUE_TO_VALUE_TYPE_MAP["float_val"]: DataType.FLOAT,
    PROTO_VALUE_TO_VALUE_TYPE_MAP["double_val"]: DataType.DOUBLE,
    PROTO_VALUE_TO_VALUE_TYPE_MAP["int32_val"]: DataType.INT32,
    PROTO_VALUE_TO_VALUE_TYPE_MAP["int64_val"]: DataType.INT64,
    PROTO_VALUE_TO_VALUE_TYPE_MAP["float_list_val"]: DataType.FLOAT_VECTOR,
    PROTO_VALUE_TO_VALUE_TYPE_MAP["int32_list_val"]: DataType.FLOAT_VECTOR,
    PROTO_VALUE_TO_VALUE_TYPE_MAP["int64_list_val"]: DataType.FLOAT_VECTOR,
    PROTO_VALUE_TO_VALUE_TYPE_MAP["double_list_val"]: DataType.FLOAT_VECTOR,
    PROTO_VALUE_TO_VALUE_TYPE_MAP["bool_list_val"]: DataType.BINARY_VECTOR,
}

FEAST_PRIMITIVE_TO_MILVUS_TYPE_MAPPING: Dict[
    Union[PrimitiveFeastType, Array, ComplexFeastType], DataType
] = {}

for value_type, feast_type in VALUE_TYPES_TO_FEAST_TYPES.items():
    if isinstance(feast_type, PrimitiveFeastType):
        milvus_type = PROTO_TO_MILVUS_TYPE_MAPPING.get(value_type)
        if milvus_type:
            FEAST_PRIMITIVE_TO_MILVUS_TYPE_MAPPING[feast_type] = milvus_type
    elif isinstance(feast_type, Array):
        base_type = feast_type.base_type
        base_value_type = base_type.to_value_type()
        if base_value_type in [
            ValueType.INT32,
            ValueType.INT64,
            ValueType.FLOAT,
            ValueType.DOUBLE,
        ]:
            FEAST_PRIMITIVE_TO_MILVUS_TYPE_MAPPING[feast_type] = DataType.FLOAT_VECTOR
        elif base_value_type == ValueType.STRING:
            FEAST_PRIMITIVE_TO_MILVUS_TYPE_MAPPING[feast_type] = DataType.VARCHAR
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
    nlist: Optional[int] = 128
    username: Optional[StrictStr] = ""
    password: Optional[StrictStr] = ""


class MilvusOnlineStore(OnlineStore):
    """
    Milvus implementation of the online store interface.

    Attributes:
        _collections: Dictionary to cache Milvus collections.
    """

    client: Optional[MilvusClient] = None
    _collections: Dict[str, Any] = {}

    def _connect(self, config: RepoConfig) -> MilvusClient:
        if not self.client:
            self.client = MilvusClient(
                url=f"{config.online_store.host}:{config.online_store.port}",
                token=f"{config.online_store.username}:{config.online_store.password}"
                if config.online_store.username and config.online_store.password
                else "",
            )
            print(
                f"Connected to Milvus at {config.online_store.host}:{config.online_store.port}"
            )
        return self.client

    def _get_collection(self, config: RepoConfig, table: FeatureView) -> Dict[str, Any]:
        self.client = self._connect(config)
        collection_name = _table_id(config.project, table)
        if collection_name not in self._collections:
            # Create a composite key by combining entity fields
            composite_key_name = (
                "_".join([field.name for field in table.entity_columns]) + "_pk"
            )

            fields = [
                FieldSchema(
                    name=composite_key_name,
                    dtype=DataType.VARCHAR,
                    max_length=512,
                    is_primary=True,
                ),
                FieldSchema(name="event_ts", dtype=DataType.INT64),
                FieldSchema(name="created_ts", dtype=DataType.INT64),
            ]
            fields_to_exclude = [
                "event_ts",
                "created_ts",
            ]
            fields_to_add = [f for f in table.schema if f.name not in fields_to_exclude]
            for field in fields_to_add:
                dtype = FEAST_PRIMITIVE_TO_MILVUS_TYPE_MAPPING.get(field.dtype)
                if dtype:
                    if dtype == DataType.FLOAT_VECTOR:
                        fields.append(
                            FieldSchema(
                                name=field.name,
                                dtype=dtype,
                                dim=config.online_store.embedding_dim,
                            )
                        )
                    elif dtype == DataType.VARCHAR:
                        fields.append(
                            FieldSchema(
                                name=field.name,
                                dtype=dtype,
                                max_length=512,
                            )
                        )
                    else:
                        fields.append(FieldSchema(name=field.name, dtype=dtype))

            schema = CollectionSchema(
                fields=fields, description="Feast feature view data"
            )
            self.client.drop_collection(collection_name)
            collection_exists = self.client.has_collection(
                collection_name=collection_name
            )
            print(f"Collection {collection_name} exists: {collection_exists}")
            if not collection_exists:
                self.client.create_collection(
                    collection_name=collection_name,
                    dimension=config.online_store.embedding_dim,
                    schema=schema,
                )
                index_params = self.client.prepare_index_params()
                for vector_field in schema.fields:
                    if vector_field.dtype in [
                        DataType.FLOAT_VECTOR,
                        DataType.BINARY_VECTOR,
                    ]:
                        self.client.create_index(
                            collection_name=collection_name,
                            field_name=vector_field.name,
                            index_params=index_params,
                            metric_type=config.online_store.metric_type,
                            index_type=config.online_store.index_type,
                            index_name=f"vector_index_{vector_field.name}",
                            params={"nlist": config.online_store.nlist},
                        )
            else:
                self.client.load_collection(collection_name)
            self._collections[collection_name] = self.client.describe_collection(
                collection_name
            )
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
        entity_batch_to_insert = []
        for entity_key, values_dict, timestamp, created_ts in data:
            # need to construct the composite primary key also need to handle the fact that entities are a list
            entity_key_str = serialize_entity_key(
                entity_key,
                entity_key_serialization_version=config.entity_key_serialization_version,
            ).hex()
            composite_key_name = (
                "_".join([str(value) for value in entity_key.join_keys]) + "_pk"
            )
            timestamp_int = int(to_naive_utc(timestamp).timestamp() * 1e6)
            created_ts_int = (
                int(to_naive_utc(created_ts).timestamp() * 1e6) if created_ts else 0
            )
            values_dict = _extract_proto_values_to_dict(values_dict)
            entity_dict = _extract_proto_values_to_dict(
                dict(zip(entity_key.join_keys, entity_key.entity_values))
            )
            values_dict.update(entity_dict)

            single_entity_record = {
                composite_key_name: entity_key_str,
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
        raise NotImplementedError

    def update(
        self,
        config: RepoConfig,
        tables_to_delete: Sequence[FeatureView],
        tables_to_keep: Sequence[FeatureView],
        entities_to_delete: Sequence[Entity],
        entities_to_keep: Sequence[Entity],
        partial: bool,
    ):
        self.client = self._connect(config)
        for table in tables_to_keep:
            self._collections = self._get_collection(config, table)

        for table in tables_to_delete:
            collection_name = _table_id(config.project, table)
            if self.collections.get(collection_name, None):
                self.client.delete(collection_name)
                self._collections.pop(collection_name, None)

    def plan(
        self, config: RepoConfig, desired_registry_proto: RegistryProto
    ) -> List[InfraObject]:
        raise NotImplementedError

    def teardown(
        self,
        config: RepoConfig,
        tables: Sequence[FeatureView],
        entities: Sequence[Entity],
    ):
        self._connect(config)
        for table in tables:
            collection_name = self._get_collection(config, table)
            if self.collections.get(collection_name, None):
                self.client.delete(collection_name)
                self._collections.pop(collection_name, None)

    def retrieve_online_documents(
        self,
        config: RepoConfig,
        table: FeatureView,
        requested_feature: Optional[str],
        requested_features: Optional[List[str]],
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
        collection_name = _table_id(config.project, table)
        collection = self._get_collection(config, table)
        if not config.online_store.vector_enabled:
            raise ValueError("Vector search is not enabled in the online store config")

        search_params = {
            "metric_type": distance_metric or config.online_store.metric_type,
            "params": {"nprobe": 10},
        }
        expr = f"feature_name == '{requested_feature}'"

        composite_key_name = (
            "_".join([str(field.name) for field in table.entity_columns]) + "_pk"
        )
        if requested_features:
            features_str = ", ".join([f"'{f}'" for f in requested_features])
            expr += f" && feature_name in [{features_str}]"

        output_fields = (
            [composite_key_name]
            + (requested_features if requested_features else [])
            + ["created_ts", "event_ts"]
        )
        print(output_fields, collection)
        assert all(
            field in [f["name"] for f in collection["fields"]]
            for field in output_fields
        ), f"field(s) [{[field for field in output_fields if field not in [f['name'] for f in collection['fields']]]}] not found in collection schema"
        # Note we choose the first vector field as the field to search on. Not ideal but it's something.
        ann_search_field = None
        for field in collection["fields"]:
            if (
                field["type"] in [DataType.FLOAT_VECTOR, DataType.BINARY_VECTOR]
                and field["name"] in output_fields
            ):
                ann_search_field = field["name"]
                break

        results = self.client.search(
            collection_name=collection_name,
            data=[embedding],
            anns_field=ann_search_field,
            search_params=search_params,
            limit=top_k,
            output_fields=output_fields,
        )

        result_list = []
        for hits in results:
            for hit in hits:
                single_record = {}
                for field in output_fields:
                    single_record[field] = hit.entity.get(field)

                entity_key_bytes = bytes.fromhex(hit.entity.get(composite_key_name))
                embedding = hit.entity.get(ann_search_field)
                serialized_embedding = _serialize_vector_to_float_list(embedding)
                distance = hit.distance
                event_ts = datetime.fromtimestamp(hit.entity.get("event_ts") / 1e6)
                prepared_result = _build_retrieve_online_document_record(
                    entity_key_bytes,
                    # This may have a bug
                    serialized_embedding.SerializeToString(),
                    embedding,
                    distance,
                    event_ts,
                    config.entity_key_serialization_version,
                )
                result_list.append(prepared_result)
        return result_list


def _table_id(project: str, table: FeatureView) -> str:
    return f"{project}_{table.name}"


def _extract_proto_values_to_dict(input_dict: Dict[str, Any]) -> Dict[str, Any]:
    numeric_vector_list_types = [
        k
        for k in PROTO_VALUE_TO_VALUE_TYPE_MAP.keys()
        if k is not None and "list" in k and "string" not in k
    ]
    output_dict = {}
    for feature_name, feature_values in input_dict.items():
        for proto_val_type in PROTO_VALUE_TO_VALUE_TYPE_MAP:
            if feature_values.HasField(proto_val_type):
                if proto_val_type in numeric_vector_list_types:
                    vector_values = getattr(feature_values, proto_val_type).val
                else:
                    vector_values = getattr(feature_values, proto_val_type)
                output_dict[feature_name] = vector_values
    return output_dict


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
        raise NotImplementedError

    def to_infra_object_proto(self) -> InfraObjectProto:
        # Implement serialization if needed
        raise NotImplementedError

    def update(self):
        # Implement update logic if needed
        raise NotImplementedError

    def teardown(self):
        collection = Collection(name=self.name)
        if collection.exists():
            collection.drop()
