import json
import logging
from datetime import datetime
from typing import Any, Callable, Dict, List, Literal, Optional, Sequence, Tuple

import numpy as np
import pandas as pd
from bidict import bidict
from pymilvus import (
    Collection,
    CollectionSchema,
    DataType,
    FieldSchema,
    connections,
    utility,
)
from pymilvus.client.types import LoadState

from feast import Entity, FeatureView, RepoConfig
from feast.infra.online_stores.online_store import OnlineStore
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import FloatList
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from feast.repo_config import FeastConfigBaseModel
from feast.types import (
    Array,
    Bytes,
    Float32,
    Float64,
    Int32,
    Int64,
    Invalid,
    String,
    UnixTimestamp,
)

logger = logging.getLogger(__name__)

TYPE_MAPPING = bidict(
    {
        DataType.INT32: Int32,
        DataType.INT64: Int64,
        DataType.FLOAT: Float32,
        DataType.DOUBLE: Float64,
        DataType.VARCHAR: String,
        DataType.UNKNOWN: Invalid,
        DataType.FLOAT_VECTOR: Array(Float32),
        DataType.BINARY_VECTOR: Array(Bytes),
    }
)

MAX_SEARCH_SIZE = 16_384
BATCH_SIZE = 10_000


class EGMilvusOnlineStoreConfig(FeastConfigBaseModel):
    """Online store config for the Milvus online store"""

    type: Literal["eg-milvus"] = "eg-milvus"
    """Online store type selector"""

    alias: str = "default"
    """ alias for milvus connection"""

    host: str
    """ the host URL """

    username: str
    """ username to connect to Milvus """

    password: str
    """ password to connect to Milvus """

    port: int = 19530
    """ the port to connect to a Milvus instance. Should be the one used for GRPC (default: 19530) """


class EGMilvusConnectionManager:
    def __init__(self, online_config: RepoConfig):
        self.online_config = online_config

    def __enter__(self):
        # Connecting to Milvus
        logger.info(
            f"Connecting to Milvus with alias {self.online_config.alias} and host {self.online_config.host} and port {self.online_config.port}."
        )
        print(
            f"Connecting to Milvus with alias {self.online_config.alias} and host {self.online_config.host} and port {self.online_config.port}."
        )
        connections.connect(
            alias=self.online_config.alias,
            host=self.online_config.host,
            port=self.online_config.port,
            user=self.online_config.username,
            password=self.online_config.password,
            use_secure=True,
        )

    def __exit__(self, exc_type, exc_value, traceback):
        # Disconnecting from Milvus
        logger.info("Closing the connection to Milvus")
        print("Closing the connection to Milvus")
        connections.disconnect(self.online_config.alias)
        logger.info("Connection Closed")
        if exc_type is not None:
            logger.error(f"An exception of type {exc_type} occurred: {exc_value}")
            print(f"An exception of type {exc_type} occurred: {exc_value}")


class EGMilvusOnlineStore(OnlineStore):
    def online_write_batch(
        self,
        config: RepoConfig,
        table: FeatureView,
        data: List[
            Tuple[EntityKeyProto, Dict[str, ValueProto], datetime, Optional[datetime]]
        ],
        progress: Optional[Callable[[int], Any]],
    ) -> None:
        with EGMilvusConnectionManager(config.online_store):
            self._create_collection_if_not_exists(table)
            print("Starting the process to batch write data into Milvus.")
            collection_to_load_data = Collection(table.name)
            rows = self._format_data_for_milvus(data, collection_to_load_data)
            collection_to_load_data.insert(rows)
            #  The flush call will seal any remaining segments and send them for indexing
            collection_to_load_data.flush()
            collection_to_load_data.load()
            logger.info("loading data into memory")
            print("loading data into memory")
            utility.wait_for_loading_complete(table.name)
            logger.info("loading data into memory complete")
            print("loading data into memory complete")

    def online_read(
        self,
        config: RepoConfig,
        table: FeatureView,
        entity_keys: List[EntityKeyProto],
        requested_features: Optional[List[str]] = None,
    ) -> List[Tuple[Optional[datetime], Optional[Dict[str, ValueProto]]]]:
        with EGMilvusConnectionManager(config.online_store):
            quer_expr = self._construct_milvus_query(entity_keys)
            use_iter_search = len(entity_keys) > MAX_SEARCH_SIZE
            collection = Collection(table.name)
            if utility.load_state(table.name) is LoadState.NotLoad:
                collection.load()
                utility.wait_for_loading_complete(table.name)

            query_result = []
            if use_iter_search:
                query_iter = collection.query_iterator(
                    batch_size=BATCH_SIZE,
                    expr=quer_expr,
                    output_fields=requested_features,
                )
                res = query_iter.next()
                while len(res) > 0:
                    query_result.extend(res)
                    res = query_iter.next()
            else:
                query_result = collection.query(
                    expr=quer_expr, output_fields=requested_features
                )

            results = self._convert_milvus_result_to_feast_type(
                query_result, collection, requested_features
            )

            # results do not have timestamps
            return [(None, row) for row in results]

    def update(
        self,
        config: RepoConfig,
        tables_to_delete: Sequence[FeatureView],
        tables_to_keep: Sequence[FeatureView],
        entities_to_delete: Sequence[Entity],
        entities_to_keep: Sequence[Entity],
        partial: bool,
    ):
        with EGMilvusConnectionManager(config.online_store):
            for table_to_keep in tables_to_keep:
                self._create_collection_if_not_exists(table_to_keep)

            for table_to_delete in tables_to_delete:
                collection_available = utility.has_collection(table_to_delete.name)
                try:
                    if collection_available:
                        utility.drop_collection(table_to_delete.name)
                        logger.info(
                            f"Collection {table_to_delete.name} has been deleted successfully."
                        )
                    else:
                        logger.warning(
                            f"Collection {table_to_delete.name} does not exist or is already deleted."
                        )
                except Exception as e:
                    logger.error(f"Collection deletion failed due to {e}")

    def teardown(
        self,
        config: RepoConfig,
        tables: Sequence[FeatureView],
        entities: Sequence[Entity],
    ):
        with EGMilvusConnectionManager(config.online_store):
            for table in tables:
                collection_name = table.name
                if utility.has_collection(collection_name):
                    logger.info(f"Dropping collection: {collection_name}")
                    utility.drop_collection(collection_name)

    def _create_collection_if_not_exists(self, feature_view: FeatureView):
        """
        Checks whether the collection already exists and creates it based on the provided feature view.

        Parameters:
        feature_view (FeatureView): the FeatureView that contains the schema.
        """
        collection_available = utility.has_collection(feature_view.name)

        if collection_available:
            logger.info(f"Collection {feature_view.name} already exists.")
            print(f"Collection {feature_view.name} already exists.")
        else:
            if not feature_view.schema:
                raise ValueError(
                    f"a schema must be provided for feature_view: {feature_view.name}"
                )

            (
                schema,
                indexes,
            ) = self._convert_featureview_schema_to_milvus_readable(
                feature_view,
            )

            logger.info(
                f"creating collection {feature_view.name} with schema: {schema}"
            )
            collection = Collection(name=feature_view.name, schema=schema)
            if feature_view.ttl:
                collection.set_properties(
                    properties={
                        "collection.ttl.seconds": feature_view.ttl.total_seconds()
                    }
                )

            for field_name, index_params in indexes.items():
                collection.create_index(field_name, index_params)

            logger.info(
                f"Collection {feature_view.name} has been created successfully."
            )

    def _convert_featureview_schema_to_milvus_readable(
        self, feature_view: FeatureView
    ) -> Tuple[CollectionSchema, Dict]:
        """
        Converting a schema understood by Feast to a schema that is readable by Milvus so that it
        can be used when a collection is created in Milvus.

        Parameters:
        feature_view (FeatureView): the FeatureView that contains the schema.

        Returns:
        (CollectionSchema): Schema readable by Milvus.
        (Dict): A dictionary of indexes to be created with the key as the vector field name and the value as the parameters

        """
        field_list = []
        indexes = {}

        for field in feature_view.schema:
            field_name = field.name
            data_type = self._get_milvus_type(field.dtype)
            max_length = 64
            dimensions = 0
            description = ""
            is_primary = True if field.name in feature_view.join_keys else False

            if field.tags:
                description = field.tags.get("description", "")
                max_length = int(field.tags.get("max_length", "64"))

                if self._data_type_is_supported_vector(data_type) and field.tags.get(
                    "index_type"
                ):
                    dimensions = int(field.tags.get("dimensions", "0"))

                    if dimensions <= 0 or dimensions >= 32768:
                        msg = f"invalid value for dimensions: {dimensions} set for field: {field_name}"
                        logger.error(msg)
                        raise ValueError(msg)

                    else:
                        try:
                            index_params = self._create_index_params(
                                field.tags, data_type
                            )
                            indexes[field_name] = index_params
                        except ValueError as e:
                            logger.error(
                                f"Could not create index for field: {field_name}.", e
                            )
                            raise e

            field = FieldSchema(
                name=field_name,
                dtype=data_type,
                description=description,
                is_primary=is_primary,
                dim=dimensions,
            )

            if data_type == DataType.VARCHAR:
                field = FieldSchema(
                    name=field_name,
                    dtype=data_type,
                    max_length=max_length,
                    description=description,
                    is_primary=is_primary,
                    dim=dimensions,
                )

            # Appending the above converted values to construct a FieldSchema
            field_list.append(field)
        # Returning a CollectionSchema which is a list of type FieldSchema.
        return CollectionSchema(field_list), indexes

    def _data_type_is_supported_vector(self, data_type: DataType) -> bool:
        """
        whether the Milvus data type is a supported vector in this implementation

        Parameters:
        data_type (DataType): data type of field in schema

        Returns:
        bool: True is supported, False if not
        """
        if data_type in [
            DataType.BINARY_VECTOR,
            DataType.FLOAT_VECTOR,
        ]:
            return True

        return False

    def _format_data_for_milvus(self, feast_data, collection: Collection):
        """
        Format Feast input for Milvus: Data stored into Milvus takes the grouped representation approach where each feature value is grouped together:
        [[1,2], [1,3]], [John, Lucy], [3,4]]

        Parameters:
        feast_data: List[
            Tuple[EntityKeyProto, Dict[str, ValueProto], datetime, Optional[datetime]]: Data represented for batch write in Feast
        collection: target collection

        Returns:
        pd.DataFrame: transformed_data: Data that can be directly written into Milvus
        """
        # get the order of columns so that return data frame has the correct order. Milvus does need the correct order
        # and does not use the column names when a data frame is passed.
        schema = collection.schema
        field_names = [field.name for field in schema.fields]

        data = []
        feature_names = None
        for entity_key, values, timestamp, created_ts in feast_data:
            feature_names = [entity_key.join_keys[0]]
            feature = [self._get_value_from_value_proto(entity_key.entity_values[0])]
            for feature_name, val in values.items():
                value = self._get_value_from_value_proto(val)
                feature.append(value)
                feature_names.append(feature_name)
            data.append(feature)

        df = pd.DataFrame(data, columns=feature_names)
        transformed_data = df.reindex(field_names, axis=1)

        return transformed_data

    def _get_value_from_value_proto(self, proto: ValueProto):
        """
        Get the raw value from a value proto.

        Parameters:
        value (ValueProto): the value proto that contains the data.

        Returns:
        value (Any): the extracted value.
        """
        val_type = proto.WhichOneof("val")
        value = getattr(proto, val_type)  # type: ignore
        if val_type == "float_list_val":
            value = np.array(value.val)

        return value

    def _create_index_params(self, tags: Dict[str, str], data_type: DataType):
        """
        Parses the tags to generate the index_params needed to create the specified index

        Parameters:
        index_type (MilvusIndexType): the index type to be created
        tags (Dict): the tags associated with the field
        data_type (DateType): the data type of the field

        Returns:
        (Dict): a dictionary formatted for the create_index params argument
        """
        index_type = tags.get("index_type", "").upper()

        metric_type = "L2"
        if "metric_type" in tags:
            metric_type = tags["metric_type"].upper()

        params = {}
        if "index_params" in tags:
            params = json.loads(tags["index_params"])

        return {
            "metric_type": metric_type,
            "index_type": index_type,
            "params": params,
        }

    def _convert_milvus_result_to_feast_type(
        self, query_result, collection, requested_features
    ):
        """
        Convert Milvus result to Feast types.

        Parameters:
        milvus_result (List[Dict[str, any]]): Milvus query result.
        collection (Collection): Milvus collection schema.
        features_to_request (List[str]): Features to request from Milvus.

        Returns:
        List[Dict[str, ValueProto]]: Processed data with Feast types.
        """

        # constructing the feature list to request from Milvus with their respective types
        features_with_types = list(tuple())
        for field in collection.schema.fields:
            if field.name in requested_features:
                features_with_types.append(
                    (field.name, self._get_feast_type(field.dtype))
                )

        results = []
        prefix = "valuetype."
        for row in query_result:
            result_row = {}
            for feature, feast_type in features_with_types:
                value_proto = ValueProto()
                feature_value = row[feature]
                if feature_value:
                    value_type_method = f"{feast_type.to_value_type()}_val".lower()
                    if value_type_method.startswith(prefix):
                        value_type_method = value_type_method[len(prefix) :]
                    value_proto = self._create_value_proto(
                        value_proto, feature_value, value_type_method
                    )
                result_row[feature] = value_proto
            # Append result after conversion to Feast Type
            results.append(result_row)

        return results

    def _create_value_proto(self, val_proto, feature_val, value_type) -> ValueProto:
        """
        Construct Value Proto so that Feast can interpret Milvus results

        Parameters:
        val_proto (ValueProto): Initialised Value Proto
        feature_val (Union[list, int, str, double, float, bool, bytes]): A row/ an item in the result that Milvus returns.
        value_type (Str): Feast Value type; example: int64_val, float_val, etc.

        Returns:
        val_proto (ValueProto): Constructed result that Feast can understand.
        """
        if value_type == "float_list_val":
            val_proto = ValueProto(float_list_val=FloatList(val=feature_val))
        else:
            setattr(val_proto, value_type, feature_val)

        return val_proto

    def _construct_milvus_query(self, entities) -> str:
        """
        Construct a Milvus query expression based on entity_keys provided.

        Parameters:
        entities (List[Entity]): List of entities with join keys and values.

        Returns:
        str: Constructed Milvus query expression.
        """

        milvus_query_expr = ""
        entity_join_key = []
        values_to_search = []

        for entity in entities:
            for key in entity.join_keys:
                entity_join_key.append(key)
            for value in entity.entity_values:
                value_to_search = self._get_value_from_value_proto(value)
                values_to_search.append(value_to_search)

        # TODO: Enable multiple join key support. Currently only supporting a single primary key/ join key. This is a limitation in Feast.
        milvus_query_expr = f"{entity_join_key[0]} in {values_to_search}"

        return milvus_query_expr

    def _get_milvus_type(self, feast_type) -> DataType:
        """
        Convert Feast type to Milvus type using the TYPE_MAPPING bidict.
        """
        # todo this is just a hacky way to make the event_timestamp work
        if feast_type == UnixTimestamp:
            return DataType.INT64

        return TYPE_MAPPING.inverse.get(feast_type, None)

    def _get_feast_type(self, milvus_type) -> object:
        """
        Convert Milvus type to Feast type using the TYPE_MAPPING bidict.
        """
        return TYPE_MAPPING.get(milvus_type, None)
