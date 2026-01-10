import base64
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, List, Literal, Optional, Sequence, Tuple, Union

from pydantic import StrictStr
from pymilvus import (
    CollectionSchema,
    DataType,
    FieldSchema,
    MilvusClient,
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
from feast.protos.feast.core.Registry_pb2 import Registry as RegistryProto
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from feast.repo_config import FeastConfigBaseModel, RepoConfig
from feast.type_map import (
    PROTO_VALUE_TO_VALUE_TYPE_MAP,
    VALUE_TYPE_TO_PROTO_VALUE_MAP,
    feast_value_type_to_python_type,
)
from feast.types import (
    VALUE_TYPES_TO_FEAST_TYPES,
    Array,
    ComplexFeastType,
    PrimitiveFeastType,
    ValueType,
    from_feast_type,
)
from feast.utils import (
    _serialize_vector_to_float_list,
    to_naive_utc,
)

PROTO_TO_MILVUS_TYPE_MAPPING: Dict[ValueType, DataType] = {
    PROTO_VALUE_TO_VALUE_TYPE_MAP["bytes_val"]: DataType.VARCHAR,
    ValueType.IMAGE_BYTES: DataType.VARCHAR,
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
    path: Optional[StrictStr] = ""
    host: Optional[StrictStr] = "http://localhost"
    port: Optional[int] = 19530
    index_type: Optional[str] = "FLAT"
    metric_type: Optional[str] = "COSINE"
    embedding_dim: Optional[int] = 128
    vector_enabled: Optional[bool] = True
    text_search_enabled: Optional[bool] = False
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

    def _get_db_path(self, config: RepoConfig) -> str:
        assert (
            config.online_store.type == "milvus"
            or config.online_store.type.endswith("MilvusOnlineStore")
        )

        if config.repo_path and not Path(config.online_store.path).is_absolute():
            db_path = str(config.repo_path / config.online_store.path)
        else:
            db_path = config.online_store.path
        return db_path

    def _connect(self, config: RepoConfig) -> MilvusClient:
        if not self.client:
            if config.provider == "local" and config.online_store.path:
                db_path = self._get_db_path(config)
                print(f"Connecting to Milvus in local mode using {db_path}")
                self.client = MilvusClient(db_path)
            else:
                print(
                    f"Connecting to Milvus remotely at {config.online_store.host}:{config.online_store.port}"
                )
                self.client = MilvusClient(
                    uri=f"{config.online_store.host}:{config.online_store.port}",
                    token=f"{config.online_store.username}:{config.online_store.password}"
                    if config.online_store.username and config.online_store.password
                    else "",
                )
        return self.client

    def _get_or_create_collection(
        self, config: RepoConfig, table: FeatureView
    ) -> Dict[str, Any]:
        self.client = self._connect(config)
        vector_field_dict = {k.name: k for k in table.schema if k.vector_index}
        collection_name = _table_id(config.project, table)
        if collection_name not in self._collections:
            # Create a composite key by combining entity fields
            composite_key_name = _get_composite_key_name(table)

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
                    else:
                        fields.append(
                            FieldSchema(
                                name=field.name,
                                dtype=DataType.VARCHAR,
                                max_length=512,
                            )
                        )

            schema = CollectionSchema(
                fields=fields, description="Feast feature view data"
            )
            collection_exists = self.client.has_collection(
                collection_name=collection_name
            )
            if not collection_exists:
                self.client.create_collection(
                    collection_name=collection_name,
                    dimension=config.online_store.embedding_dim,
                    schema=schema,
                )
                index_params = self.client.prepare_index_params()
                for vector_field in schema.fields:
                    if (
                        vector_field.dtype
                        in [
                            DataType.FLOAT_VECTOR,
                            DataType.BINARY_VECTOR,
                        ]
                        and vector_field.name in vector_field_dict
                    ):
                        metric = vector_field_dict[
                            vector_field.name
                        ].vector_search_metric
                        index_params.add_index(
                            collection_name=collection_name,
                            field_name=vector_field.name,
                            metric_type=metric or config.online_store.metric_type,
                            index_type=config.online_store.index_type,
                            index_name=f"vector_index_{vector_field.name}",
                            params={"nlist": config.online_store.nlist},
                        )
                self.client.create_index(
                    collection_name=collection_name,
                    index_params=index_params,
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
        self.client = self._connect(config)
        collection = self._get_or_create_collection(config, table)
        vector_cols = [f.name for f in table.features if f.vector_index]
        entity_batch_to_insert = []
        unique_entities: dict[str, dict[str, Any]] = {}
        required_fields = {field["name"] for field in collection["fields"]}
        for entity_key, values_dict, timestamp, created_ts in data:
            # need to construct the composite primary key also need to handle the fact that entities are a list
            entity_key_str = serialize_entity_key(
                entity_key,
                entity_key_serialization_version=config.entity_key_serialization_version,
            ).hex()
            # to recover the entity key just run:
            # deserialize_entity_key(bytes.fromhex(entity_key_str), entity_key_serialization_version=3)
            composite_key_name = _get_composite_key_name(table)

            timestamp_int = int(to_naive_utc(timestamp).timestamp() * 1e6)
            created_ts_int = (
                int(to_naive_utc(created_ts).timestamp() * 1e6) if created_ts else 0
            )
            entity_dict = {
                join_key: feast_value_type_to_python_type(value)
                for join_key, value in zip(
                    entity_key.join_keys, entity_key.entity_values
                )
            }
            values_dict.update(entity_dict)
            values_dict = _extract_proto_values_to_dict(
                values_dict,
                vector_cols=vector_cols,
                serialize_to_string=True,
            )

            single_entity_record = {
                composite_key_name: entity_key_str,
                "event_ts": timestamp_int,
                "created_ts": created_ts_int,
            }
            single_entity_record.update(values_dict)
            # Ensure all required fields exist, setting missing ones to empty strings
            for field in required_fields:
                if field not in single_entity_record:
                    single_entity_record[field] = ""
            # Store only the latest event timestamp per entity
            if (
                entity_key_str not in unique_entities
                or unique_entities[entity_key_str]["event_ts"] < timestamp_int
            ):
                unique_entities[entity_key_str] = single_entity_record

            if progress:
                progress(1)

        entity_batch_to_insert = list(unique_entities.values())
        self.client.upsert(
            collection_name=collection["collection_name"],
            data=entity_batch_to_insert,
        )

    def online_read(
        self,
        config: RepoConfig,
        table: FeatureView,
        entity_keys: List[EntityKeyProto],
        requested_features: Optional[List[str]] = None,
        full_feature_names: bool = False,
    ) -> List[Tuple[Optional[datetime], Optional[Dict[str, ValueProto]]]]:
        self.client = self._connect(config)
        collection_name = _table_id(config.project, table)
        collection = self._get_or_create_collection(config, table)

        composite_key_name = _get_composite_key_name(table)

        output_fields = (
            [composite_key_name]
            + (requested_features if requested_features else [])
            + ["created_ts", "event_ts"]
        )
        assert all(
            field in [f["name"] for f in collection["fields"]]
            for field in output_fields
        ), (
            f"field(s) [{[field for field in output_fields if field not in [f['name'] for f in collection['fields']]]}] not found in collection schema"
        )
        composite_entities = []
        for entity_key in entity_keys:
            entity_key_str = serialize_entity_key(
                entity_key,
                entity_key_serialization_version=config.entity_key_serialization_version,
            ).hex()
            composite_entities.append(entity_key_str)

        query_filter_for_entities = (
            f"{composite_key_name} in ["
            + ", ".join([f"'{e}'" for e in composite_entities])
            + "]"
        )
        self.client.load_collection(collection_name)
        results = self.client.query(
            collection_name=collection_name,
            filter=query_filter_for_entities,
            output_fields=output_fields,
        )
        # Group hits by composite key.
        grouped_hits: Dict[str, Any] = {}
        for hit in results:
            key = hit.get(composite_key_name)
            grouped_hits.setdefault(key, []).append(hit)

        # Map the features to their Feast types.
        feature_name_feast_primitive_type_map = {
            f.name: f.dtype for f in table.features
        }
        if getattr(table, "write_to_online_store", False):
            feature_name_feast_primitive_type_map.update(
                {f.name: f.dtype for f in table.schema}
            )
        # Build a dictionary mapping composite key -> (res_ts, res)
        results_dict: Dict[
            str, Tuple[Optional[datetime], Optional[Dict[str, ValueProto]]]
        ] = {}

        # here we need to map the data stored as characters back into the protobuf value
        for hit in results:
            key = hit.get(composite_key_name)
            # Only take one hit per composite key (adjust if you need aggregation)
            if key not in results_dict:
                res = {}
                res_ts = None
                for field in output_fields:
                    val = ValueProto()
                    field_value = hit.get(field, None)
                    if field_value is None and ":" in field:
                        _, field_short = field.split(":", 1)
                        field_value = hit.get(field_short)

                    if field in ["created_ts", "event_ts"]:
                        res_ts = datetime.fromtimestamp(field_value / 1e6)
                    elif field == composite_key_name:
                        # We do not return the composite key value
                        pass
                    else:
                        feature_feast_primitive_type = (
                            feature_name_feast_primitive_type_map.get(
                                field, PrimitiveFeastType.INVALID
                            )
                        )
                        feature_fv_dtype = from_feast_type(feature_feast_primitive_type)
                        proto_attr = VALUE_TYPE_TO_PROTO_VALUE_MAP.get(feature_fv_dtype)
                        if proto_attr:
                            if proto_attr == "bytes_val":
                                setattr(val, proto_attr, field_value.encode())
                            elif proto_attr in [
                                "int32_val",
                                "int64_val",
                                "float_val",
                                "double_val",
                                "string_val",
                            ]:
                                setattr(
                                    val,
                                    proto_attr,
                                    type(getattr(val, proto_attr))(field_value),
                                )
                            elif proto_attr in [
                                "int32_list_val",
                                "int64_list_val",
                                "float_list_val",
                                "double_list_val",
                            ]:
                                getattr(val, proto_attr).val.extend(field_value)
                            else:
                                setattr(val, proto_attr, field_value)
                        else:
                            raise ValueError(
                                f"Unsupported ValueType: {feature_feast_primitive_type} with feature view value {field_value} for feature {field} with value type {proto_attr}"
                            )
                        # res[field] = val
                        key_to_use = field.split(":", 1)[-1] if ":" in field else field
                        res[key_to_use] = val
                results_dict[key] = (res_ts, res if res else None)

        # Map the results back into a list matching the original order of composite_keys.
        result_list = [
            results_dict.get(key, (None, None)) for key in composite_entities
        ]

        return result_list

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
            self._collections = self._get_or_create_collection(config, table)

        for table in tables_to_delete:
            collection_name = _table_id(config.project, table)
            if self._collections.get(collection_name, None):
                self.client.drop_collection(collection_name)
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
        self.client = self._connect(config)
        for table in tables:
            collection_name = _table_id(config.project, table)
            if self._collections.get(collection_name, None):
                self.client.drop_collection(collection_name)
                self._collections.pop(collection_name, None)

    def retrieve_online_documents_v2(
        self,
        config: RepoConfig,
        table: FeatureView,
        requested_features: List[str],
        embedding: Optional[List[float]],
        top_k: int,
        distance_metric: Optional[str] = None,
        query_string: Optional[str] = None,
    ) -> List[
        Tuple[
            Optional[datetime],
            Optional[EntityKeyProto],
            Optional[Dict[str, ValueProto]],
        ]
    ]:
        """
        Retrieve documents using vector similarity search or keyword search in Milvus.
        Args:
            config: Feast configuration object
            table: FeatureView object as the table to search
            requested_features: List of requested features to retrieve
            embedding: Query embedding to search for (optional)
            top_k: Number of items to return
            distance_metric: Distance metric to use (optional)
            query_string: The query string to search for using keyword search (optional)
        Returns:
            List of tuples containing the event timestamp, entity key, and feature values
        """
        entity_name_feast_primitive_type_map = {
            k.name: k.dtype for k in table.entity_columns
        }
        self.client = self._connect(config)
        collection_name = _table_id(config.project, table)
        collection = self._get_or_create_collection(config, table)
        if not config.online_store.vector_enabled:
            raise ValueError("Vector search is not enabled in the online store config")

        if embedding is None and query_string is None:
            raise ValueError("Either embedding or query_string must be provided")

        composite_key_name = _get_composite_key_name(table)

        output_fields = (
            [composite_key_name]
            + (requested_features if requested_features else [])
            + ["created_ts", "event_ts"]
        )
        assert all(
            field in [f["name"] for f in collection["fields"]]
            for field in output_fields
        ), (
            f"field(s) [{[field for field in output_fields if field not in [f['name'] for f in collection['fields']]]}] not found in collection schema"
        )

        # Find the vector search field if we need it
        ann_search_field = None
        if embedding is not None:
            for field in collection["fields"]:
                if (
                    field["type"] in [DataType.FLOAT_VECTOR, DataType.BINARY_VECTOR]
                    and field["name"] in output_fields
                ):
                    ann_search_field = field["name"]
                    break

        self.client.load_collection(collection_name)

        if (
            embedding is not None
            and query_string is not None
            and config.online_store.vector_enabled
        ):
            string_field_list = [
                f.name
                for f in table.features
                if isinstance(f.dtype, PrimitiveFeastType)
                and f.dtype.to_value_type() == ValueType.STRING
            ]

            if not string_field_list:
                raise ValueError(
                    "No string fields found in the feature view for text search in hybrid mode"
                )

            # Create a filter expression for text search
            filter_expressions = []
            for field in string_field_list:
                if field in output_fields:
                    filter_expressions.append(f"{field} LIKE '%{query_string}%'")

            # Combine filter expressions with OR
            filter_expr = " OR ".join(filter_expressions) if filter_expressions else ""

            # Vector search with text filter
            search_params = {
                "metric_type": distance_metric or config.online_store.metric_type,
                "params": {"nprobe": 10},
            }

            # For hybrid search, use filter parameter instead of expr
            results = self.client.search(
                collection_name=collection_name,
                data=[embedding],
                anns_field=ann_search_field,
                search_params=search_params,
                limit=top_k,
                output_fields=output_fields,
                filter=filter_expr if filter_expr else None,
            )

        elif embedding is not None and config.online_store.vector_enabled:
            # Vector search only
            search_params = {
                "metric_type": distance_metric or config.online_store.metric_type,
                "params": {"nprobe": 10},
            }

            results = self.client.search(
                collection_name=collection_name,
                data=[embedding],
                anns_field=ann_search_field,
                search_params=search_params,
                limit=top_k,
                output_fields=output_fields,
            )

        elif query_string is not None:
            string_field_list = [
                f.name
                for f in table.features
                if isinstance(f.dtype, PrimitiveFeastType)
                and f.dtype.to_value_type() == ValueType.STRING
            ]

            if not string_field_list:
                raise ValueError(
                    "No string fields found in the feature view for text search"
                )

            filter_expressions = []
            for field in string_field_list:
                if field in output_fields:
                    filter_expressions.append(f"{field} LIKE '%{query_string}%'")

            filter_expr = " OR ".join(filter_expressions)

            if not filter_expr:
                raise ValueError(
                    "No text fields found in requested features for search"
                )

            query_results = self.client.query(
                collection_name=collection_name,
                filter=filter_expr,
                output_fields=output_fields,
                limit=top_k,
            )

            results = [
                [{"entity": entity, "distance": -1.0}] for entity in query_results
            ]
        else:
            raise ValueError(
                "Either vector_enabled must be True for embedding search or query_string must be provided for keyword search"
            )

        result_list = []
        for hits in results:
            for hit in hits:
                res = {}
                res_ts = None
                entity_key_bytes = bytes.fromhex(
                    hit.get("entity", {}).get(composite_key_name, None)
                )
                entity_key_proto = (
                    deserialize_entity_key(entity_key_bytes)
                    if entity_key_bytes
                    else None
                )
                for field in output_fields:
                    val = ValueProto()
                    field_value = hit.get("entity", {}).get(field, None)
                    # entity_key_proto = None
                    if field in ["created_ts", "event_ts"]:
                        res_ts = datetime.fromtimestamp(field_value / 1e6)
                    elif field == ann_search_field and embedding is not None:
                        serialized_embedding = _serialize_vector_to_float_list(
                            embedding
                        )
                        res[ann_search_field] = serialized_embedding
                    elif (
                        entity_name_feast_primitive_type_map.get(
                            field, PrimitiveFeastType.INVALID
                        )
                        == PrimitiveFeastType.STRING
                    ):
                        res[field] = ValueProto(string_val=str(field_value))
                    elif (
                        entity_name_feast_primitive_type_map.get(
                            field, PrimitiveFeastType.INVALID
                        )
                        == PrimitiveFeastType.BYTES
                    ):
                        try:
                            decoded_bytes = base64.b64decode(field_value)
                            res[field] = ValueProto(bytes_val=decoded_bytes)
                        except Exception:
                            res[field] = ValueProto(string_val=str(field_value))
                    elif entity_name_feast_primitive_type_map.get(
                        field, PrimitiveFeastType.INVALID
                    ) in [
                        PrimitiveFeastType.INT64,
                        PrimitiveFeastType.INT32,
                    ]:
                        res[field] = ValueProto(int64_val=int(field_value))
                    elif field == composite_key_name:
                        pass
                    elif isinstance(field_value, bytes):
                        val.ParseFromString(field_value)
                        res[field] = val
                    else:
                        val.string_val = field_value
                        res[field] = val
                distance = hit.get("distance", None)
                res["distance"] = (
                    ValueProto(float_val=distance) if distance else ValueProto()
                )
                result_list.append((res_ts, entity_key_proto, res if res else None))
        return result_list


def _table_id(project: str, table: FeatureView) -> str:
    return f"{project}_{table.name}"


def _get_composite_key_name(table: FeatureView) -> str:
    return "_".join([field.name for field in table.entity_columns]) + "_pk"


def _extract_proto_values_to_dict(
    input_dict: Dict[str, Any],
    vector_cols: List[str],
    serialize_to_string=False,
) -> Dict[str, Any]:
    numeric_vector_list_types = [
        k
        for k in PROTO_VALUE_TO_VALUE_TYPE_MAP.keys()
        if k is not None and "list" in k and "string" not in k
    ]
    numeric_types = [
        "double_val",
        "float_val",
        "int32_val",
        "int64_val",
        "bool_val",
    ]
    output_dict = {}
    for feature_name, feature_values in input_dict.items():
        for proto_val_type in PROTO_VALUE_TO_VALUE_TYPE_MAP:
            if not isinstance(feature_values, (int, float, str)):
                if feature_values.HasField(proto_val_type):
                    if proto_val_type in numeric_vector_list_types:
                        if serialize_to_string and feature_name not in vector_cols:
                            vector_values = getattr(
                                feature_values, proto_val_type
                            ).SerializeToString()
                        else:
                            vector_values = getattr(feature_values, proto_val_type).val
                    else:
                        if (
                            serialize_to_string
                            and proto_val_type
                            not in ["string_val", "bytes_val"] + numeric_types
                        ):
                            vector_values = feature_values.SerializeToString().decode()
                        elif proto_val_type == "bytes_val":
                            byte_data = getattr(feature_values, proto_val_type)
                            vector_values = base64.b64encode(byte_data).decode("utf-8")
                        else:
                            if not isinstance(feature_values, str):
                                vector_values = str(
                                    getattr(feature_values, proto_val_type)
                                )
                            else:
                                vector_values = getattr(feature_values, proto_val_type)
                    output_dict[feature_name] = vector_values
            else:
                if serialize_to_string:
                    if not isinstance(feature_values, str):
                        feature_values = str(feature_values)
                    output_dict[feature_name] = feature_values

    return output_dict
