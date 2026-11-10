import base64
import json
import logging
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, List, Literal, Optional, Sequence, Tuple, Union

from pydantic import StrictStr, field_validator
from pymilvus import (
    CollectionSchema,
    DataType,
    FieldSchema,
    MilvusClient,
)
from pymilvus.client.types import LoadState

from feast import Entity
from feast.feature_view import FeatureView
from feast.field import Field
from feast.filter_models import (
    ComparisonFilter,
    CompoundFilter,
    FilterTranslator,
    FilterType,
    filters_contain_numeric_comparison,
)
from feast.infra.infra_object import InfraObject
from feast.infra.key_encoding_utils import (
    deserialize_entity_key,
    serialize_entity_key,
)
from feast.infra.online_stores.helpers import compute_table_id
from feast.infra.online_stores.online_store import OnlineStore
from feast.infra.online_stores.vector_store import VectorStoreConfig
from feast.protos.feast.core.Registry_pb2 import Registry as RegistryProto
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from feast.repo_config import FeastConfigBaseModel, RepoConfig
from feast.type_map import (
    PROTO_VALUE_TO_VALUE_TYPE_MAP,
    VALUE_TYPE_TO_PROTO_VALUE_MAP,
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

logger = logging.getLogger(__name__)

PROTO_TO_MILVUS_TYPE_MAPPING: Dict[ValueType, DataType] = {
    PROTO_VALUE_TO_VALUE_TYPE_MAP["bytes_val"]: DataType.VARCHAR,
    ValueType.IMAGE_BYTES: DataType.VARCHAR,
    PROTO_VALUE_TO_VALUE_TYPE_MAP["bool_val"]: DataType.BOOL,
    PROTO_VALUE_TO_VALUE_TYPE_MAP["string_val"]: DataType.VARCHAR,
    PROTO_VALUE_TO_VALUE_TYPE_MAP["float_val"]: DataType.FLOAT,
    PROTO_VALUE_TO_VALUE_TYPE_MAP["double_val"]: DataType.DOUBLE,
    PROTO_VALUE_TO_VALUE_TYPE_MAP["int32_val"]: DataType.INT32,
    PROTO_VALUE_TO_VALUE_TYPE_MAP["int64_val"]: DataType.INT64,
    PROTO_VALUE_TO_VALUE_TYPE_MAP["unix_timestamp_val"]: DataType.INT64,
    PROTO_VALUE_TO_VALUE_TYPE_MAP["float_list_val"]: DataType.FLOAT_VECTOR,
    PROTO_VALUE_TO_VALUE_TYPE_MAP["int32_list_val"]: DataType.FLOAT_VECTOR,
    PROTO_VALUE_TO_VALUE_TYPE_MAP["int64_list_val"]: DataType.FLOAT_VECTOR,
    PROTO_VALUE_TO_VALUE_TYPE_MAP["double_list_val"]: DataType.FLOAT_VECTOR,
    PROTO_VALUE_TO_VALUE_TYPE_MAP["bool_list_val"]: DataType.BINARY_VECTOR,
    PROTO_VALUE_TO_VALUE_TYPE_MAP["map_val"]: DataType.VARCHAR,
    PROTO_VALUE_TO_VALUE_TYPE_MAP["map_list_val"]: DataType.VARCHAR,
    PROTO_VALUE_TO_VALUE_TYPE_MAP["json_val"]: DataType.VARCHAR,
    PROTO_VALUE_TO_VALUE_TYPE_MAP["json_list_val"]: DataType.VARCHAR,
    PROTO_VALUE_TO_VALUE_TYPE_MAP["struct_val"]: DataType.VARCHAR,
    PROTO_VALUE_TO_VALUE_TYPE_MAP["struct_list_val"]: DataType.VARCHAR,
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
    elif isinstance(feast_type, ComplexFeastType):
        milvus_type = PROTO_TO_MILVUS_TYPE_MAPPING.get(value_type)
        if milvus_type:
            FEAST_PRIMITIVE_TO_MILVUS_TYPE_MAPPING[feast_type] = milvus_type

logger = logging.getLogger(__name__)

MILVUS_NATIVE_NUMERIC_TYPES = {
    DataType.INT32,
    DataType.INT64,
    DataType.FLOAT,
    DataType.DOUBLE,
    DataType.BOOL,
}
MILVUS_VECTOR_TYPES = {DataType.FLOAT_VECTOR, DataType.BINARY_VECTOR}
# Feast types stored as a JSON column when native types are enabled. Map and
# Struct stay serialized: JSON cannot hold their nested bytes, timestamps or
# empty typed lists.
_JSON_VALUE_TYPES = {ValueType.JSON, ValueType.JSON_LIST}
# Element types of an ARRAY column when native types are enabled.
_ARRAY_ELEMENT_TYPES: Dict[Any, DataType] = {
    PrimitiveFeastType.STRING: DataType.VARCHAR,
    PrimitiveFeastType.BOOL: DataType.BOOL,
    PrimitiveFeastType.INT32: DataType.INT32,
    PrimitiveFeastType.INT64: DataType.INT64,
    PrimitiveFeastType.FLOAT32: DataType.FLOAT,
    PrimitiveFeastType.FLOAT64: DataType.DOUBLE,
    PrimitiveFeastType.UNIX_TIMESTAMP: DataType.INT64,
}
MAX_ARRAY_CAPACITY = 4096
_FILTER_KEY = re.compile(r'^[a-zA-Z_][a-zA-Z0-9_]*(\[("[^"\\]*"|\d+)\])*$')

# Milvus requires every collection to have a vector field, so feature views
# without one get a small placeholder vector. Milvus servers reject vectors
# with fewer than 2 dimensions and non-finite values, and a collection can only
# be loaded once every vector field is indexed.
PLACEHOLDER_VECTOR_FIELD = "_placeholder_vector"
PLACEHOLDER_VECTOR_DIM = 2


def _milvus_escape_string(s: str) -> str:
    """Escape a string for safe use inside a Milvus single-quoted literal.

    Backslashes must be escaped first; otherwise a trailing backslash in the
    input would combine with the escaped quote to break out of the literal.
    Also escapes quotes and common control characters that could disrupt
    Milvus boolean expressions.
    """
    return (
        s.replace("\\", "\\\\")
        .replace("'", "\\'")
        .replace('"', '\\"')
        .replace("\n", "\\n")
        .replace("\r", "\\r")
    )


def _milvus_fmt(value: Any) -> str:
    """Format a Python value for use in a Milvus boolean expression.

    Handles numeric types natively (unquoted) so that Milvus performs
    numeric comparison instead of lexicographic string comparison.
    Bool is checked first because Python ``bool`` is a subclass of ``int``.
    """
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return str(value)
    return f"'{_milvus_escape_string(str(value))}'"


class MilvusFilterTranslator(FilterTranslator):
    """Translates Feast filters into Milvus boolean expression strings."""

    def translate(self, filters: FilterType) -> Optional[str]:
        if filters is None:
            return None
        return self._dispatch(filters)

    def translate_comparison(self, f: ComparisonFilter) -> str:
        key, value, op_type = f.key, f.value, f.type

        if not _FILTER_KEY.match(key):
            raise ValueError(
                f"Invalid filter key: {key!r}. Keys must be field identifiers, with"
                ' optional JSON or array subscripts such as doc["a"] or tags[0].'
            )

        milvus_ops = {"gt": ">", "gte": ">=", "lt": "<", "lte": "<="}

        if op_type == "eq":
            return f"{key} == {_milvus_fmt(value)}"
        elif op_type == "ne":
            return f"{key} != {_milvus_fmt(value)}"
        elif op_type in milvus_ops:
            return f"{key} {milvus_ops[op_type]} {_milvus_fmt(value)}"
        elif op_type in ("in", "nin"):
            if not isinstance(value, list):
                raise ValueError(
                    f"'{op_type}' filter requires a list value, got {type(value)}"
                )
            formatted = [_milvus_fmt(v) for v in value]
            kw = "not in" if op_type == "nin" else "in"
            return f"{key} {kw} [{', '.join(formatted)}]"
        raise ValueError(f"Unsupported comparison operator: {op_type}")

    def translate_compound(self, f: CompoundFilter) -> str:
        if not f.filters:
            return ""
        clauses = []
        for sub_filter in f.filters:
            clause = self._dispatch(sub_filter)
            if clause:
                clauses.append(f"({clause})")
        if not clauses:
            return ""
        operator = " and " if f.type == "and" else " or "
        return operator.join(clauses)


def _has_empty_group(filters: FilterType) -> bool:
    """True when a compound filter has no conditions, at any level."""
    return isinstance(filters, CompoundFilter) and (
        not filters.filters or any(_has_empty_group(f) for f in filters.filters)
    )


class MilvusOnlineStoreConfig(FeastConfigBaseModel, VectorStoreConfig):
    """
    Configuration for the Milvus online store.
    NOTE: The class *must* end with the `OnlineStoreConfig` suffix.
    """

    type: Literal["milvus"] = "milvus"
    path: Optional[StrictStr] = ""
    host: Optional[StrictStr] = "http://localhost"
    port: Optional[int] = 19530
    # Full endpoint, e.g. a Zilliz Cloud https URI. Takes precedence over host/port.
    uri: Optional[StrictStr] = None
    # API key or "username:password". Takes precedence over username/password.
    token: Optional[StrictStr] = None
    # Milvus database to use. The database must already exist.
    db_name: Optional[StrictStr] = None
    index_type: Optional[str] = "FLAT"
    metric_type: Optional[str] = "COSINE"
    embedding_dim: Optional[int] = 128
    vector_enabled: Optional[bool] = True
    text_search_enabled: Optional[bool] = False
    nlist: Optional[int] = 128
    # Index build params for vector fields, e.g. {"M": 16, "efConstruction": 200}.
    # Defaults to {"nlist": nlist}, or {} for AUTOINDEX.
    index_params: Optional[Dict[str, Any]] = None
    # Search params, e.g. {"ef": 64}, or {"level": 2} for AUTOINDEX.
    # Defaults to {"nprobe": 10}, or {} for AUTOINDEX.
    search_params: Optional[Dict[str, Any]] = None
    # Sent with every read and search. When unset, Milvus uses the
    # collection's level.
    consistency_level: Optional[
        Literal["Strong", "Bounded", "Session", "Eventually"]
    ] = None
    # Set when Feast creates a collection. When unset, Milvus uses its
    # default (Bounded).
    collection_consistency_level: Optional[
        Literal["Strong", "Bounded", "Session", "Eventually"]
    ] = None
    # Field to use as the Milvus partition key in feature views that contain it.
    # A feature view's "milvus.partition_key" tag takes precedence.
    partition_key: Optional[StrictStr] = None
    # Number of partitions of a new collection that has a partition key. When
    # unset, Milvus uses its default.
    num_partitions: Optional[int] = None
    # Partition key isolation of a new collection that has a partition key.
    # When unset, Milvus uses its default.
    partition_key_isolation: Optional[bool] = None
    username: Optional[StrictStr] = ""
    password: Optional[StrictStr] = ""
    enable_openai_compatible_store: Optional[bool] = False
    varchar_max_length: Optional[int] = 65535

    @field_validator("num_partitions")
    @classmethod
    def validate_num_partitions(cls, v):
        if v is not None and v < 1:
            raise ValueError(f"num_partitions must be at least 1, got {v}")
        return v

    @field_validator("varchar_max_length")
    @classmethod
    def validate_varchar_max_length(cls, v):
        if v is not None and not (1 <= v <= 65535):
            raise ValueError(f"varchar_max_length must be between 1 and 65535, got {v}")
        return v


class MilvusOnlineStore(OnlineStore):
    """
    Milvus implementation of the online store interface.

    Attributes:
        _collections: Dictionary to cache Milvus collections.
    """

    def __init__(self):
        super().__init__()
        self.client: Optional[MilvusClient] = None
        self._collections: Dict[str, Any] = {}

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
            online_config = config.online_store
            if (
                config.provider == "local"
                and online_config.path
                and not online_config.uri
            ):
                db_path = self._get_db_path(config)
                logger.info("Connecting to Milvus in local mode using %s", db_path)
                self.client = MilvusClient(db_path)
            else:
                uri = online_config.uri or f"{online_config.host}:{online_config.port}"
                logger.info("Connecting to Milvus remotely at %s", uri)
                client_kwargs: Dict[str, Any] = {
                    "uri": uri,
                    "token": _milvus_token(online_config),
                }
                if online_config.db_name:
                    client_kwargs["db_name"] = online_config.db_name
                self.client = MilvusClient(**client_kwargs)
        return self.client

    def _get_or_create_collection(
        self, config: RepoConfig, table: FeatureView
    ) -> Dict[str, Any]:
        self.client = self._connect(config)
        vector_field_dict = {k.name: k for k in table.schema if k.vector_index}
        collection_name = _table_id(
            config.project, table, config.registry.enable_online_feature_view_versioning
        )
        if collection_name not in self._collections:
            # Create a composite key by combining entity fields
            composite_key_name = _get_composite_key_name(table)
            varchar_max_length = int(config.online_store.varchar_max_length or 65535)
            fields = [
                FieldSchema(
                    name=composite_key_name,
                    dtype=DataType.VARCHAR,
                    max_length=varchar_max_length,
                    is_primary=True,
                ),
                FieldSchema(name="event_ts", dtype=DataType.INT64),
                FieldSchema(name="created_ts", dtype=DataType.INT64),
            ]
            fields_to_exclude = [
                "event_ts",
                "created_ts",
                "event_timestamp",
                "created_timestamp",
            ]
            fields_to_add = [f for f in table.schema if f.name not in fields_to_exclude]
            use_typed = config.online_store.enable_openai_compatible_store
            partition_key = _partition_key_name(config.online_store, table)
            entity_columns = {column.name for column in table.entity_columns}
            scalar_indexes: List[Dict[str, Any]] = []
            for field in fields_to_add:
                spec = _column_spec(
                    field,
                    use_typed,
                    varchar_max_length,
                    config.online_store.embedding_dim,
                )
                if spec["dtype"] == DataType.BINARY_VECTOR:
                    raise ValueError(
                        f"Field '{field.name}': Array(Bool) cannot be a vector field,"
                        " Milvus binary vectors are not supported."
                    )
                nullable = _nullable_tag(field)
                if nullable is None:
                    nullable = spec.get("nullable", False)
                if nullable and (
                    field.name in entity_columns
                    or field.name == partition_key
                    or spec["dtype"] in MILVUS_VECTOR_TYPES
                ):
                    raise ValueError(
                        f"Field '{field.name}' cannot be nullable: entity, partition"
                        " key and vector fields must always have a value."
                    )
                spec.pop("nullable", None)
                if nullable:
                    spec["nullable"] = True
                scalar_indexes.extend(_scalar_index_specs(field, spec["dtype"]))
                fields.append(FieldSchema(name=field.name, **spec))
            has_vector_field = any(
                f.dtype in (DataType.FLOAT_VECTOR, DataType.BINARY_VECTOR)
                for f in fields
            )
            if not has_vector_field:
                fields.append(
                    FieldSchema(
                        name=PLACEHOLDER_VECTOR_FIELD,
                        dtype=DataType.FLOAT_VECTOR,
                        dim=PLACEHOLDER_VECTOR_DIM,
                    )
                )
            if partition_key:
                _mark_partition_key(fields, partition_key, table.name)
            schema = CollectionSchema(
                fields=fields, description="Feast feature view data"
            )
            collection_exists = self.client.has_collection(
                collection_name=collection_name
            )
            if not collection_exists:
                index_params = self.client.prepare_index_params()
                for vector_field in schema.fields:
                    if vector_field.dtype not in [
                        DataType.FLOAT_VECTOR,
                        DataType.BINARY_VECTOR,
                    ]:
                        continue
                    if vector_field.name in vector_field_dict:
                        metric = vector_field_dict[
                            vector_field.name
                        ].vector_search_metric
                        index_params.add_index(
                            field_name=vector_field.name,
                            metric_type=metric or config.online_store.metric_type,
                            index_type=config.online_store.index_type,
                            index_name=f"vector_index_{vector_field.name}",
                            params=_index_build_params(config.online_store),
                        )
                    else:
                        # Vector fields that aren't searched (the placeholder,
                        # or arrays without vector_index) still need an index,
                        # otherwise Milvus servers refuse to load the collection.
                        index_params.add_index(
                            field_name=vector_field.name,
                            metric_type="L2"
                            if vector_field.name == PLACEHOLDER_VECTOR_FIELD
                            else config.online_store.metric_type,
                            index_type="FLAT",
                            index_name=f"vector_index_{vector_field.name}",
                        )
                for scalar_index in scalar_indexes:
                    index_params.add_index(**scalar_index)
                # Every collection has at least one vector field, and every
                # vector field is indexed, so passing the index params here
                # makes Milvus create the indexes and load the collection.
                self.client.create_collection(
                    collection_name=collection_name,
                    dimension=config.online_store.embedding_dim,
                    schema=schema,
                    index_params=index_params,
                    **_collection_consistency_kwargs(config.online_store),
                    **_partition_kwargs(config.online_store, partition_key),
                )
            else:
                self._ensure_loaded(collection_name)
                _warn_if_scalar_indexes_missing(
                    self.client,
                    collection_name,
                    [index["index_name"] for index in scalar_indexes],
                )
            # Collections are only cached once loaded, so reads and searches
            # don't need to load them again.
            self._collections[collection_name] = self.client.describe_collection(
                collection_name
            )
            if collection_exists and partition_key:
                _warn_if_partition_key_missing(
                    self._collections[collection_name], partition_key
                )
        return self._collections[collection_name]

    def _ensure_loaded(self, collection_name: str) -> None:
        """Load an existing collection unless Milvus already has it loaded."""
        assert self.client is not None, "Milvus client is not initialized"
        load_state = self.client.get_load_state(collection_name).get("state")
        if load_state != LoadState.Loaded:
            self.client.load_collection(collection_name)

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
        entity_batch_to_insert = []
        unique_entities: dict[str, dict[str, Any]] = {}
        required_fields = {field["name"] for field in collection["fields"]}
        collection_field_types = {
            field["name"]: field["type"] for field in collection["fields"]
        }
        # Collections created by older Feast versions may use a 1-dim placeholder.
        placeholder_dim = next(
            (
                int(field.get("params", {}).get("dim", PLACEHOLDER_VECTOR_DIM))
                for field in collection["fields"]
                if field["name"] == PLACEHOLDER_VECTOR_FIELD
            ),
            PLACEHOLDER_VECTOR_DIM,
        )
        nullable_fields = {
            field["name"] for field in collection["fields"] if field.get("nullable")
        }
        for entity_key, values_dict, timestamp, created_ts in data:
            entity_key_str = serialize_entity_key(
                entity_key,
                entity_key_serialization_version=config.entity_key_serialization_version,
            ).hex()
            composite_key_name = _get_composite_key_name(table)

            timestamp_int = int(to_naive_utc(timestamp).timestamp() * 1e6)
            created_ts_int = (
                int(to_naive_utc(created_ts).timestamp() * 1e6) if created_ts else 0
            )
            values_dict.update(zip(entity_key.join_keys, entity_key.entity_values))
            values_dict = _extract_proto_values_to_dict(
                values_dict, collection_field_types
            )

            # Remove timestamp fields that are handled separately to avoid conflicts
            timestamp_fields = [
                "event_timestamp",
                "created_timestamp",
                "event_ts",
                "created_ts",
            ]
            for field in timestamp_fields:
                values_dict.pop(field, None)

            single_entity_record = {
                composite_key_name: entity_key_str,
                "event_ts": timestamp_int,
                "created_ts": created_ts_int,
            }
            single_entity_record.update(values_dict)
            for field in required_fields:
                if field not in single_entity_record:
                    field_type = collection_field_types.get(field, DataType.VARCHAR)
                    if field == PLACEHOLDER_VECTOR_FIELD:
                        single_entity_record[field] = [0.0] * placeholder_dim
                    elif field in nullable_fields:
                        single_entity_record[field] = None
                    else:
                        single_entity_record[field] = _default_for_milvus_type(
                            field, field_type
                        )
            # Keep one row per entity: the latest event timestamp wins, then the
            # latest created timestamp, then the last row of the batch.
            previous = unique_entities.get(entity_key_str)
            if previous is None or (previous["event_ts"], previous["created_ts"]) <= (
                timestamp_int,
                created_ts_int,
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
    ) -> List[Tuple[Optional[datetime], Optional[Dict[str, ValueProto]]]]:
        self.client = self._connect(config)
        collection_name = _table_id(
            config.project, table, config.registry.enable_online_feature_view_versioning
        )
        collection = self._get_or_create_collection(config, table)
        column_types = {field["name"]: field["type"] for field in collection["fields"]}

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
        results = self.client.query(
            collection_name=collection_name,
            filter=query_filter_for_entities,
            output_fields=output_fields,
            **_consistency_kwargs(config.online_store),
        )
        # Group hits by composite key.
        grouped_hits: Dict[str, Any] = {}
        for hit in results:
            key = hit.get(composite_key_name)
            grouped_hits.setdefault(key, []).append(hit)

        # Map the features to their Feast types.
        feature_name_feast_primitive_type_map = {
            f.name: f.dtype for f in list(table.schema) + list(table.features)
        }
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
                        key_to_use = field.split(":", 1)[-1] if ":" in field else field
                        res[key_to_use] = _stored_value_to_proto(
                            field_value,
                            feature_name_feast_primitive_type_map.get(
                                field, PrimitiveFeastType.INVALID
                            ),
                            column_types.get(key_to_use),
                        )
                results_dict[key] = (res_ts, res if res else None)

        # Map the results back into a list matching the original order of composite_keys.
        result_list = [
            results_dict.get(key, (None, None)) for key in composite_entities
        ]

        return result_list

    def online_delete(
        self,
        config: RepoConfig,
        table: FeatureView,
        entity_keys: Optional[List[EntityKeyProto]] = None,
        *,
        filters: Optional[Union[ComparisonFilter, CompoundFilter]] = None,
    ) -> None:
        """Delete rows of a feature view by entity key or by filter.

        Give exactly one of ``entity_keys`` and ``filters``. An empty key list
        deletes nothing. A filter with an empty group is rejected, so a mistake
        cannot delete more rows than it names. No tombstone is kept: a later
        write creates the row again.
        """
        if (entity_keys is None) == (filters is None):
            raise ValueError("Give exactly one of entity_keys or filters")
        selector: Dict[str, Any]
        if entity_keys is not None:
            if not entity_keys:
                return
            selector = {
                "ids": [
                    serialize_entity_key(
                        entity_key,
                        entity_key_serialization_version=config.entity_key_serialization_version,
                    ).hex()
                    for entity_key in entity_keys
                ]
            }
        else:
            if _has_empty_group(filters):
                raise ValueError(
                    "Each group of the deletion filter must have a condition"
                )
            selector = {"filter": MilvusFilterTranslator().translate(filters)}
        self.client = self._connect(config)
        collection_name = _table_id(
            config.project, table, config.registry.enable_online_feature_view_versioning
        )
        if collection_name not in self._collections:
            if not self.client.has_collection(collection_name):
                return
            self._ensure_loaded(collection_name)
        self.client.delete(collection_name=collection_name, **selector)

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
            self._get_or_create_collection(config, table)

        # Always drop the base collection plus any "_v{N}" siblings, regardless of
        # the current versioning flag. This handles mixed-state repos where
        # versioning was toggled on/off across applies and would otherwise leave
        # orphan collections behind in Milvus.
        for table in tables_to_delete:
            self._drop_all_version_collections(config.project, table)

    def plan(
        self, config: RepoConfig, desired_registry_proto: RegistryProto
    ) -> List[InfraObject]:
        return []

    def teardown(
        self,
        config: RepoConfig,
        tables: Sequence[FeatureView],
        entities: Sequence[Entity],
    ):
        self.client = self._connect(config)
        # See update(): drop base + all "_v{N}" siblings to handle mixed-state repos.
        for table in tables:
            self._drop_all_version_collections(config.project, table)

    def retrieve_online_documents_v2(
        self,
        config: RepoConfig,
        table: FeatureView,
        requested_features: List[str],
        embedding: Optional[List[float]],
        top_k: int,
        distance_metric: Optional[str] = None,
        query_string: Optional[str] = None,
        filters: Optional[Union[ComparisonFilter, CompoundFilter]] = None,
        include_feature_view_version_metadata: bool = False,
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
            k.name: k.dtype for k in list(table.schema) + list(table.features)
        }
        self.client = self._connect(config)
        collection_name = _table_id(
            config.project, table, config.registry.enable_online_feature_view_versioning
        )
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

        if filters and filters_contain_numeric_comparison(filters):
            collection_field_types = {
                f["name"]: f["type"] for f in collection["fields"]
            }
            schema_internal_fields = {"event_ts", "created_ts"}
            has_native = any(
                collection_field_types.get(name) in MILVUS_NATIVE_NUMERIC_TYPES
                for name in collection_field_types
                if name not in schema_internal_fields
            )
            if not has_native:
                logger.warning(
                    "Numeric comparison filters (gt, gte, lt, lte) are being used "
                    "but this collection stores numeric fields as VARCHAR. This "
                    "causes lexicographic comparison instead of numeric comparison "
                    "(e.g. '9' > '100' is True as a string). To fix this, set "
                    "'enable_openai_compatible_store: true' in your online_store "
                    "config, then teardown and re-apply your feature store to "
                    "recreate collections with native numeric types."
                )

        metadata_filter_expr = MilvusFilterTranslator().translate(filters)

        def _combine_exprs(*parts: Optional[str]) -> Optional[str]:
            """Combine non-empty Milvus boolean expressions with AND."""
            active = [p for p in parts if p]
            if not active:
                return None
            if len(active) == 1:
                return active[0]
            return " and ".join(f"({p})" for p in active)

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

            escaped_query = _milvus_escape_string(query_string)
            filter_expressions = []
            for field in string_field_list:
                if field in output_fields:
                    filter_expressions.append(f"{field} LIKE '%{escaped_query}%'")

            text_filter = " OR ".join(filter_expressions) if filter_expressions else ""
            combined_filter = _combine_exprs(text_filter, metadata_filter_expr)

            search_params = {
                "metric_type": distance_metric or config.online_store.metric_type,
                "params": _search_params(config.online_store),
            }

            results = self.client.search(
                collection_name=collection_name,
                data=[embedding],
                anns_field=ann_search_field,
                search_params=search_params,
                limit=top_k,
                output_fields=output_fields,
                filter=combined_filter,
                **_consistency_kwargs(config.online_store),
            )

        elif embedding is not None and config.online_store.vector_enabled:
            # Vector search only
            search_params = {
                "metric_type": distance_metric or config.online_store.metric_type,
                "params": _search_params(config.online_store),
            }

            results = self.client.search(
                collection_name=collection_name,
                data=[embedding],
                anns_field=ann_search_field,
                search_params=search_params,
                limit=top_k,
                output_fields=output_fields,
                filter=metadata_filter_expr,
                **_consistency_kwargs(config.online_store),
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

            escaped_query = _milvus_escape_string(query_string)
            filter_expressions = []
            for field in string_field_list:
                if field in output_fields:
                    filter_expressions.append(f"{field} LIKE '%{escaped_query}%'")

            text_filter = " OR ".join(filter_expressions)

            if not text_filter:
                raise ValueError(
                    "No text fields found in requested features for search"
                )

            combined_filter = _combine_exprs(text_filter, metadata_filter_expr)

            query_results = self.client.query(
                collection_name=collection_name,
                filter=combined_filter or text_filter,
                output_fields=output_fields,
                limit=top_k,
                **_consistency_kwargs(config.online_store),
            )

            results = [
                [{"entity": entity, "distance": -1.0}] for entity in query_results
            ]
        else:
            raise ValueError(
                "Either vector_enabled must be True for embedding search or query_string must be provided for keyword search"
            )

        column_types = {field["name"]: field["type"] for field in collection["fields"]}
        result_list = []
        for hits in results:
            for hit in hits:
                res = {}
                res_ts = None
                raw_key = hit.get("entity", {}).get(composite_key_name)
                entity_key_bytes = bytes.fromhex(raw_key) if raw_key else None
                entity_key_proto = (
                    deserialize_entity_key(entity_key_bytes)
                    if entity_key_bytes
                    else None
                )
                for field in output_fields:
                    field_value = hit.get("entity", {}).get(field, None)
                    # entity_key_proto = None
                    if field in ["created_ts", "event_ts"]:
                        res_ts = datetime.fromtimestamp(field_value / 1e6)
                    elif field == ann_search_field and embedding is not None:
                        serialized_embedding = _serialize_vector_to_float_list(
                            embedding
                        )
                        res[ann_search_field] = serialized_embedding
                    elif field == composite_key_name:
                        pass
                    else:
                        res[field] = _stored_value_to_proto(
                            field_value,
                            entity_name_feast_primitive_type_map.get(
                                field, PrimitiveFeastType.INVALID
                            ),
                            column_types.get(field),
                        )
                raw_distance = hit.get("distance", None)
                res["distance"] = (
                    ValueProto(float_val=raw_distance)
                    if raw_distance is not None
                    else ValueProto()
                )
                result_list.append((res_ts, entity_key_proto, res if res else None))
        return result_list

    def _drop_all_version_collections(self, project: str, table: FeatureView) -> None:
        """Drop the base collection and every ``_v{N}`` versioned sibling.

        Mirrors the ``_drop_all_version_tables`` helpers in the MySQL/PostgreSQL
        online stores. Always called from ``update`` and ``teardown`` so a
        repo that toggles versioning on and off does not leave orphan
        collections behind in Milvus.
        """
        base = f"{project}_{table.name}"
        versioned_prefix = f"{base}_v"
        assert self.client is not None, "Milvus client is not initialized"
        for collection_name in self.client.list_collections():
            if collection_name == base or (
                collection_name.startswith(versioned_prefix)
                and collection_name[len(versioned_prefix) :].isdigit()
            ):
                self.client.drop_collection(collection_name)
                self._collections.pop(collection_name, None)


def _table_id(project: str, table: FeatureView, enable_versioning: bool = False) -> str:
    return compute_table_id(project, table, enable_versioning)


def _is_autoindex(online_config: MilvusOnlineStoreConfig) -> bool:
    return (online_config.index_type or "").upper() == "AUTOINDEX"


def _index_build_params(online_config: MilvusOnlineStoreConfig) -> Dict[str, Any]:
    """Build params for vector indexes. AUTOINDEX accepts none besides the metric."""
    if online_config.index_params is not None:
        return dict(online_config.index_params)
    if _is_autoindex(online_config):
        return {}
    return {"nlist": online_config.nlist}


def _search_params(online_config: MilvusOnlineStoreConfig) -> Dict[str, Any]:
    if online_config.search_params is not None:
        return dict(online_config.search_params)
    if _is_autoindex(online_config):
        return {}
    return {"nprobe": 10}


PARTITION_KEY_TAG = "milvus.partition_key"
PARTITION_KEY_TYPES = {DataType.INT64, DataType.VARCHAR}


def _partition_key_name(
    online_config: MilvusOnlineStoreConfig, table: FeatureView
) -> Optional[str]:
    """Return the partition key field for a feature view, if one is configured.

    The feature view's ``milvus.partition_key`` tag takes precedence over the
    store-level ``partition_key``, which only applies to feature views that
    contain that field.
    """
    tagged = table.tags.get(PARTITION_KEY_TAG)
    if tagged:
        return tagged
    configured = online_config.partition_key
    if configured and any(field.name == configured for field in table.schema):
        return configured
    return None


def _mark_partition_key(
    fields: List[FieldSchema], partition_key: str, table_name: str
) -> None:
    field = next((f for f in fields if f.name == partition_key), None)
    if field is None:
        raise ValueError(
            f"Partition key '{partition_key}' is not a field of feature view "
            f"'{table_name}'."
        )
    if field.dtype not in PARTITION_KEY_TYPES:
        raise ValueError(
            f"Partition key '{partition_key}' of feature view '{table_name}' is "
            f"stored as {field.dtype.name}, but Milvus partition keys must be "
            "INT64 or VARCHAR."
        )
    field.is_partition_key = True


def _warn_if_partition_key_missing(
    collection: Dict[str, Any], partition_key: str
) -> None:
    has_key = any(
        field["name"] == partition_key and field.get("is_partition_key")
        for field in collection["fields"]
    )
    if not has_key:
        logger.warning(
            "Collection '%s' was created without partition key '%s'. The partition "
            "key only applies when a collection is created: run `feast teardown` "
            "and `feast apply`, then materialize again to use it.",
            collection["collection_name"],
            partition_key,
        )


def _consistency_kwargs(online_config: MilvusOnlineStoreConfig) -> Dict[str, Any]:
    """Read and search kwargs; only pass a level when configured."""
    if online_config.consistency_level:
        return {"consistency_level": online_config.consistency_level}
    return {}


def _partition_kwargs(
    online_config: MilvusOnlineStoreConfig, partition_key: Optional[str]
) -> Dict[str, Any]:
    """Partition settings only apply to a collection that has a partition key."""
    kwargs: Dict[str, Any] = {}
    if not partition_key:
        return kwargs
    if online_config.num_partitions is not None:
        kwargs["num_partitions"] = online_config.num_partitions
    if online_config.partition_key_isolation is not None:
        kwargs["properties"] = {
            "partitionkey.isolation": str(online_config.partition_key_isolation).lower()
        }
    return kwargs


def _collection_consistency_kwargs(
    online_config: MilvusOnlineStoreConfig,
) -> Dict[str, Any]:
    """create_collection kwargs; only pass a level when configured."""
    if online_config.collection_consistency_level:
        return {"consistency_level": online_config.collection_consistency_level}
    return {}


def _milvus_token(online_config: MilvusOnlineStoreConfig) -> str:
    """Return the token to authenticate with: ``token``, else ``username:password``."""
    if online_config.token:
        return online_config.token
    if online_config.username and online_config.password:
        return f"{online_config.username}:{online_config.password}"
    return ""


def _get_composite_key_name(table: FeatureView) -> str:
    return "_".join([field.name for field in table.entity_columns]) + "_pk"


_MILVUS_TYPE_DEFAULTS: Dict[DataType, Any] = {
    DataType.INT32: 0,
    DataType.INT64: 0,
    DataType.FLOAT: 0.0,
    DataType.DOUBLE: 0.0,
    DataType.BOOL: False,
    DataType.VARCHAR: "",
}


def _default_for_milvus_type(field_name: str, dtype: DataType) -> Any:
    if dtype in (DataType.JSON, DataType.ARRAY):
        raise ValueError(
            f"Feature '{field_name}' has no value and its column cannot be null:"
            " give it a value or remove the milvus.nullable tag"
        )
    return _MILVUS_TYPE_DEFAULTS.get(dtype, "")


_NUMERIC_SCALAR_PROTO_TYPES = (
    "double_val",
    "float_val",
    "int32_val",
    "int64_val",
    "bool_val",
    "unix_timestamp_val",
)


def _extract_proto_values_to_dict(
    input_dict: Dict[str, ValueProto], column_types: Dict[str, DataType]
) -> Dict[str, Any]:
    """Convert the values of one row to what each column stores.

    A vector or ARRAY column gets a list, a JSON column the Python value and a
    native scalar column the number or bool. A VARCHAR column gets text: the
    string, a number as text, bytes as base64, and every other value as the
    base64 of its serialized ValueProto, so it comes back as written. A null
    value is left out: the caller writes None or the column's default for it.
    """
    output_dict: Dict[str, Any] = {}
    for feature_name, value in input_dict.items():
        column_type = column_types.get(feature_name)
        proto_val_type = value.WhichOneof("val")
        if proto_val_type is None:
            continue
        stored = getattr(value, proto_val_type)
        if column_type in MILVUS_VECTOR_TYPES or column_type == DataType.ARRAY:
            output_dict[feature_name] = list(stored.val)
        elif column_type == DataType.JSON:
            output_dict[feature_name] = _json_column_value(feature_name, value)
        elif column_type in MILVUS_NATIVE_NUMERIC_TYPES:
            output_dict[feature_name] = stored
        elif proto_val_type == "string_val":
            output_dict[feature_name] = stored
        elif proto_val_type == "bytes_val":
            output_dict[feature_name] = base64.b64encode(stored).decode("utf-8")
        elif proto_val_type in _NUMERIC_SCALAR_PROTO_TYPES:
            output_dict[feature_name] = str(stored)
        else:
            output_dict[feature_name] = base64.b64encode(
                value.SerializeToString()
            ).decode("utf-8")
    return output_dict


def _stored_value_to_proto(
    stored: Any, feast_type: Any, column_type: Optional[DataType] = None
) -> ValueProto:
    """Rebuild the ValueProto of a feature from what its column stores.

    The inverse of ``_extract_proto_values_to_dict``. None, and the empty text
    that a collection without nullable columns stores for a missing value, are
    null.
    """
    val = ValueProto()
    if stored is None:
        return val
    value_type = from_feast_type(feast_type)
    proto_attr = VALUE_TYPE_TO_PROTO_VALUE_MAP.get(value_type)
    if proto_attr is None:
        # A column the feature view does not declare, for example an entity key.
        if isinstance(stored, bool):
            val.bool_val = stored
        elif isinstance(stored, int):
            val.int64_val = stored
        elif isinstance(stored, float):
            val.double_val = stored
        else:
            val.string_val = str(stored)
        return val
    if column_type == DataType.JSON:
        if value_type == ValueType.JSON_LIST:
            val.json_list_val.val.extend(json.dumps(item) for item in stored)
        else:
            val.json_val = json.dumps(stored)
        return val
    if isinstance(stored, list):
        getattr(val, proto_attr).SetInParent()
        getattr(val, proto_attr).val.extend(stored)
        return val
    if proto_attr == "string_val":
        val.string_val = str(stored)
        return val
    if proto_attr == "bytes_val":
        val.bytes_val = base64.b64decode(stored)
        return val
    if stored == "":
        return val
    if proto_attr == "bool_val":
        val.bool_val = (
            stored if isinstance(stored, bool) else str(stored).lower() == "true"
        )
    elif proto_attr in ("int32_val", "int64_val", "unix_timestamp_val"):
        setattr(val, proto_attr, int(stored))
    elif proto_attr in ("float_val", "double_val"):
        setattr(val, proto_attr, float(stored))
    else:
        val.ParseFromString(base64.b64decode(stored))
    return val


def _json_column_value(feature_name: str, value: ValueProto) -> Any:
    """The Python value that PyMilvus stores in a JSON column.

    PyMilvus 3.0 stores dicts and lists as they are, turns a top-level JSON
    string into text that Milvus cannot read back and rejects a top-level
    number or bool, so only objects and arrays are accepted. JSON null is
    stored as a NULL column value.
    """
    if value.WhichOneof("val") == "json_list_val":
        return [json.loads(item) for item in value.json_list_val.val]
    try:
        document = json.loads(value.json_val)
    except ValueError as error:
        raise ValueError(f"Feature '{feature_name}' is not valid JSON: {error}")
    if document is None or isinstance(document, (dict, list)):
        return document
    raise ValueError(
        f"Feature '{feature_name}': a Milvus JSON column holds an object or an"
        f" array, not a top-level {type(document).__name__}"
    )


def _milvus_field_type(field: Field) -> DataType:
    """The Milvus type of a feature without native types: a vector only for
    ``vector_index`` fields, a numeric type where the store keeps them native,
    VARCHAR for the rest."""
    if isinstance(field.dtype, Array) and not field.vector_index:
        return DataType.VARCHAR
    return FEAST_PRIMITIVE_TO_MILVUS_TYPE_MAPPING.get(field.dtype, DataType.VARCHAR)


def _column_spec(
    field: Field, native: bool, varchar_max_length: int, embedding_dim: Optional[int]
) -> Dict[str, Any]:
    """The FieldSchema arguments of a feature's column.

    A ``vector_index`` field is a vector. With native types, scalars get their
    own type, arrays of scalars become ARRAY and Json, Map and Struct values
    become JSON. Everything else is VARCHAR: text, base64 bytes or a
    serialized value.
    """
    dtype = _milvus_field_type(field)
    if dtype in MILVUS_VECTOR_TYPES:
        return {"dtype": dtype, "dim": field.vector_length or embedding_dim}
    max_length = _max_length_tag(field, varchar_max_length)
    if native:
        if from_feast_type(field.dtype) in _JSON_VALUE_TYPES:
            return {"dtype": DataType.JSON, "nullable": True}
        if isinstance(field.dtype, Array):
            element = _ARRAY_ELEMENT_TYPES.get(field.dtype.base_type)
            if element is not None:
                spec: Dict[str, Any] = {
                    "dtype": DataType.ARRAY,
                    "element_type": element,
                    "max_capacity": _max_capacity_tag(field),
                    "nullable": True,
                }
                if element == DataType.VARCHAR:
                    spec["max_length"] = max_length
                return spec
        elif dtype in MILVUS_NATIVE_NUMERIC_TYPES:
            return {"dtype": dtype}
    return {"dtype": DataType.VARCHAR, "max_length": max_length}


def _max_length_tag(field: Field, default: int) -> int:
    if "max_length" not in field.tags:
        return default
    try:
        max_length = int(field.tags["max_length"])
    except (ValueError, TypeError):
        raise ValueError(
            f"Field '{field.name}' has invalid max_length tag"
            f" '{field.tags['max_length']}': must be an integer."
        )
    if not (1 <= max_length <= 65535):
        raise ValueError(
            f"Field '{field.name}' max_length tag must be"
            f" between 1 and 65535, got {max_length}"
        )
    return max_length


def _max_capacity_tag(field: Field) -> int:
    value = field.tags.get("milvus.max_capacity", str(MAX_ARRAY_CAPACITY))
    try:
        capacity = int(value)
    except (ValueError, TypeError):
        raise ValueError(
            f"Field '{field.name}' has invalid milvus.max_capacity tag"
            f" '{value}': must be an integer."
        )
    if not (1 <= capacity <= MAX_ARRAY_CAPACITY):
        raise ValueError(
            f"Field '{field.name}' milvus.max_capacity tag must be"
            f" between 1 and {MAX_ARRAY_CAPACITY}, got {capacity}"
        )
    return capacity


def _json_tag(field: Field, tag: str, expected: type) -> Any:
    if tag not in field.tags:
        return None
    try:
        value = json.loads(field.tags[tag])
    except ValueError:
        value = None
    if not isinstance(value, expected):
        raise ValueError(
            f"Field '{field.name}' tag {tag} must be a JSON {expected.__name__},"
            f" got {field.tags[tag]!r}"
        )
    return value


def _scalar_index_specs(field: Field, dtype: DataType) -> List[Dict[str, Any]]:
    """The indexes that the ``milvus.index`` and ``milvus.json_indexes`` tags declare."""
    specs: List[Dict[str, Any]] = []
    index_type = field.tags.get("milvus.index")
    params = _json_tag(field, "milvus.index_params", dict)
    if params is not None and index_type is None:
        raise ValueError(
            f"Field '{field.name}' has a milvus.index_params tag without milvus.index"
        )
    if index_type is not None:
        if dtype in MILVUS_VECTOR_TYPES:
            raise ValueError(
                f"Field '{field.name}' is a vector: set its index with index_type"
                " and index_params in the online store config, not with a tag"
            )
        specs.append(
            {
                "field_name": field.name,
                "index_type": index_type,
                "index_name": f"scalar_index_{field.name}",
                "params": params or {},
            }
        )
    json_indexes = _json_tag(field, "milvus.json_indexes", list)
    if json_indexes:
        if dtype != DataType.JSON:
            raise ValueError(
                f"Field '{field.name}' has a milvus.json_indexes tag but is not"
                " stored as JSON"
            )
        for position, json_index in enumerate(json_indexes):
            if not isinstance(json_index, dict) or "json_path" not in json_index:
                raise ValueError(
                    f"Field '{field.name}' milvus.json_indexes entries must be"
                    " objects with a json_path"
                )
            json_params = dict(json_index)
            specs.append(
                {
                    "field_name": field.name,
                    "index_type": json_params.pop("index_type", "INVERTED"),
                    "index_name": f"json_index_{field.name}_{position}",
                    "params": json_params,
                }
            )
    return specs


def _warn_if_scalar_indexes_missing(
    client: MilvusClient, collection_name: str, index_names: List[str]
) -> None:
    if not index_names:
        return
    missing = sorted(set(index_names) - set(client.list_indexes(collection_name)))
    if missing:
        logger.warning(
            "Collection '%s' does not have the indexes %s that field tags declare. "
            "Indexes only apply when a collection is created: run `feast teardown` "
            "and `feast apply`, then materialize again to use them.",
            collection_name,
            missing,
        )


def _nullable_tag(field: Field) -> Optional[bool]:
    if "milvus.nullable" not in field.tags:
        return None
    value = field.tags["milvus.nullable"].lower()
    if value not in ("true", "false"):
        raise ValueError(
            f"Field '{field.name}' has invalid milvus.nullable tag"
            f" '{field.tags['milvus.nullable']}': must be 'true' or 'false'."
        )
    return value == "true"
