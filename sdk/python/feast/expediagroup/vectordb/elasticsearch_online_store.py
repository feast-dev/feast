import base64
import json
import logging
from datetime import datetime
from typing import Any, Callable, Dict, List, Literal, Optional, Sequence, Tuple

from elasticsearch import Elasticsearch, helpers

from feast import Entity, FeatureView, RepoConfig
from feast.infra.online_stores.online_store import OnlineStore
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import (
    BoolList,
    BytesList,
    DoubleList,
    FloatList,
    Int32List,
    Int64List,
    StringList,
)
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from feast.repo_config import FeastConfigBaseModel
from feast.types import (
    Array,
    Bool,
    Bytes,
    FeastType,
    Float32,
    Float64,
    Int32,
    Int64,
    PrimitiveFeastType,
    String,
    UnixTimestamp,
)

logger = logging.getLogger(__name__)

TYPE_MAPPING = {
    Bytes: "binary",
    Int32: "integer",
    Int64: "long",
    Float32: "float",
    Float64: "double",
    Bool: "boolean",
    String: "text",
    UnixTimestamp: "date_nanos",
    Array(Bytes): "binary",
    Array(Int32): "integer",
    Array(Int64): "long",
    Array(Float32): "float",
    Array(Float64): "double",
    Array(Bool): "boolean",
    Array(String): "text",
    Array(UnixTimestamp): "date_nanos",
}


class ElasticsearchOnlineStoreConfig(FeastConfigBaseModel):
    """Online store config for the Elasticsearch online store"""

    type: Literal["elasticsearch"] = "elasticsearch"
    """Online store type selector"""

    endpoint: str
    """ the http endpoint URL """

    username: str
    """ username to connect to Elasticsearch """

    password: str
    """ password to connect to Elasticsearch """


class ElasticsearchConnectionManager:
    def __init__(self, online_config: RepoConfig):
        self.online_config = online_config

    def __enter__(self):
        # Connecting to Elasticsearch
        logger.info(
            f"Connecting to Elasticsearch with endpoint {self.online_config.endpoint}"
        )
        self.client = Elasticsearch(
            self.online_config.endpoint,
            basic_auth=(self.online_config.username, self.online_config.password),
        )
        return self.client

    def __exit__(self, exc_type, exc_value, traceback):
        # Disconnecting from Elasticsearch
        logger.info("Closing the connection to Elasticsearch")
        self.client.transport.close()


class ElasticsearchOnlineStore(OnlineStore):
    def online_write_batch(
        self,
        config: RepoConfig,
        table: FeatureView,
        data: List[
            Tuple[EntityKeyProto, Dict[str, ValueProto], datetime, Optional[datetime]]
        ],
        progress: Optional[Callable[[int], Any]],
    ) -> None:
        with ElasticsearchConnectionManager(config.online_store) as es:
            resp = es.indices.exists(index=table.name)
            if not resp.body:
                self._create_index(es, table)
            bulk_documents = []
            for entity_key, values, timestamp, created_ts in data:
                id_val = self._get_value_from_value_proto(entity_key.entity_values[0])
                document = {entity_key.join_keys[0]: id_val}
                for feature_name, val in values.items():
                    document[feature_name] = self._get_value_from_value_proto(val)
                bulk_documents.append(
                    {"_index": table.name, "_id": id_val, "_source": document}
                )

            successes, errors = helpers.bulk(client=es, actions=bulk_documents)
            logger.info(f"bulk write completed with {successes} successes")
            if errors:
                logger.error(f"bulk write return errors: {errors}")

    def online_read(
        self,
        config: RepoConfig,
        table: FeatureView,
        entity_keys: List[EntityKeyProto],
        requested_features: Optional[List[str]] = None,
    ) -> List[Tuple[Optional[datetime], Optional[Dict[str, ValueProto]]]]:
        with ElasticsearchConnectionManager(config.online_store) as es:
            id_list = []
            for entity in entity_keys:
                for val in entity.entity_values:
                    id_list.append(self._get_value_from_value_proto(val))

            if requested_features is None:
                requested_features = [f.name for f in table.schema]

            hits = es.search(
                index=table.name,
                source=False,
                fields=requested_features,
                query={"ids": {"values": id_list}},
            )["hits"]
            if len(hits) > 0 and "hits" in hits:
                hits = hits["hits"]
            else:
                return []

            results: List[
                Tuple[Optional[datetime], Optional[Dict[str, ValueProto]]]
            ] = []
            prefix = "valuetype."
            for hit in hits:
                result_row = {}
                doc = hit["fields"]
                for feature in doc:
                    feast_type = next(
                        f.dtype for f in table.schema if f.name == feature
                    )
                    value = (
                        doc[feature][0]
                        if isinstance(feast_type, PrimitiveFeastType)
                        else doc[feature]
                    )
                    value_type_method = f"{feast_type.to_value_type()}_val".lower()
                    if value_type_method.startswith(prefix):
                        value_type_method = value_type_method[len(prefix) :]
                    value_proto = self._create_value_proto(value, value_type_method)
                    result_row[feature] = value_proto
                results.append((None, result_row))
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
        with ElasticsearchConnectionManager(config.online_store) as es:
            for fv in tables_to_delete:
                resp = es.indices.exists(index=fv.name)
                if resp.body:
                    es.indices.delete(index=fv.name)
            for fv in tables_to_keep:
                resp = es.indices.exists(index=fv.name)
                if not resp.body:
                    self._create_index(es, fv)

    def teardown(
        self,
        config: RepoConfig,
        tables: Sequence[FeatureView],
        entities: Sequence[Entity],
    ):
        pass

    def _create_index(self, es, fv):
        index_mapping = {"properties": {}}
        for feature in fv.schema:
            is_primary = True if feature.name in fv.join_keys else False
            if "index_type" in feature.tags:
                dimensions = int(feature.tags.get("dimensions", "0"))
                index_type = feature.tags.get("index_type", "hnsw").lower()
                metric_type = feature.tags.get("metric_type", "l2_norm").lower()
                index_mapping["properties"][feature.name] = {
                    "type": "dense_vector",
                    "dims": dimensions,
                    "index": index_type == "hnsw",
                    "similarity": metric_type,
                }
                index_params = json.loads(feature.tags.get("index_params", "{}"))
                if len(index_params) > 0:
                    index_params["type"] = index_type
                    index_mapping["properties"][feature.name]["index_options"] = (
                        index_params
                    )
            else:
                t = self._get_data_type(feature.dtype)
                t = "keyword" if is_primary and t == "text" else t
                index_mapping["properties"][feature.name] = {"type": t}
                if is_primary:
                    index_mapping["properties"][feature.name]["index"] = True
        es.indices.create(index=fv.name, mappings=index_mapping)
        logger.info(f"Index {fv.name} created")

    def _get_data_type(self, t: FeastType) -> str:
        return TYPE_MAPPING.get(t, "text")

    def _get_value_from_value_proto(self, proto: ValueProto):
        """
        Get the raw value from a value proto.

        Parameters:
        value (ValueProto): the value proto that contains the data.

        Returns:
        value (Any): the extracted value.
        """
        val_type = proto.WhichOneof("val")
        if not val_type:
            return None

        value = getattr(proto, val_type)  # type: ignore
        if val_type == "bytes_val":
            value = base64.b64encode(value).decode()
        if val_type == "bytes_list_val":
            value = [base64.b64encode(v).decode() for v in value.val]
        elif "_list_val" in val_type:
            value = list(value.val)

        return value

    def _create_value_proto(self, feature_val, value_type) -> ValueProto:
        """
        Construct Value Proto so that Feast can interpret Elasticsearch results

        Parameters:
        feature_val (Union[list, int, str, double, float, bool, bytes]): An item in the result that Elasticsearch returns.
        value_type (Str): Feast Value type; example: int64_val, float_val, etc.

        Returns:
        val_proto (ValueProto): Constructed result that Feast can understand.
        """
        if value_type == "bytes_list_val":
            val_proto = ValueProto(
                bytes_list_val=BytesList(val=[base64.b64decode(f) for f in feature_val])
            )
        elif value_type == "bytes_val":
            val_proto = ValueProto(bytes_val=base64.b64decode(feature_val))
        elif value_type == "string_list_val":
            val_proto = ValueProto(string_list_val=StringList(val=feature_val))
        elif value_type == "int32_list_val":
            val_proto = ValueProto(int32_list_val=Int32List(val=feature_val))
        elif value_type == "int64_list_val":
            val_proto = ValueProto(int64_list_val=Int64List(val=feature_val))
        elif value_type == "double_list_val":
            val_proto = ValueProto(double_list_val=DoubleList(val=feature_val))
        elif value_type == "float_list_val":
            val_proto = ValueProto(float_list_val=FloatList(val=feature_val))
        elif value_type == "bool_list_val":
            val_proto = ValueProto(bool_list_val=BoolList(val=feature_val))
        elif value_type == "unix_timestamp_list_val":
            nanos_list = [
                int(datetime.strptime(f, "%Y-%m-%dT%H:%M:%S.%fZ").timestamp() * 1000)
                for f in feature_val
            ]
            val_proto = ValueProto(unix_timestamp_list_val=Int64List(val=nanos_list))
        elif value_type == "unix_timestamp_val":
            nanos = (
                datetime.strptime(feature_val, "%Y-%m-%dT%H:%M:%S.%fZ").timestamp()
                * 1000
            )
            val_proto = ValueProto(unix_timestamp_val=int(nanos))
        else:
            val_proto = ValueProto()
            setattr(val_proto, value_type, feature_val)

        return val_proto
