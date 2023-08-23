import logging
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

import numpy as np
from pydantic.typing import Literal
from pymilvus import (
    Collection,
    CollectionSchema,
    DataType,
    FieldSchema,
    connections,
    utility,
)
from pymilvus.client.types import IndexType as MilvusIndexType

from feast import Entity, FeatureView, RepoConfig
from feast.expediagroup.vectordb.index_type import IndexType
from feast.expediagroup.vectordb.utils.feast_milvus_type_conversion import TypeConverter
from feast.field import Field
from feast.infra.online_stores.online_store import OnlineStore
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import FloatList
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from feast.repo_config import FeastConfigBaseModel
from feast.usage import log_exceptions_and_usage

logger = logging.getLogger(__name__)


class MilvusOnlineStoreConfig(FeastConfigBaseModel):
    """Online store config for the Milvus online store"""

    type: Literal["milvus"] = "milvus"
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


class MilvusConnectionManager:
    def __init__(self, online_config: RepoConfig):
        self.online_config = online_config

    def __enter__(self):
        # Connecting to Milvus
        logger.info(
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
        connections.disconnect(self.online_config.alias)
        logger.info("Connection Closed")
        if exc_type is not None:
            logger.error(f"An exception of type {exc_type} occurred: {exc_value}")


class MilvusOnlineStore(OnlineStore):
    def online_write_batch(
        self,
        config: RepoConfig,
        table: FeatureView,
        data: List[
            Tuple[EntityKeyProto, Dict[str, ValueProto], datetime, Optional[datetime]]
        ],
        progress: Optional[Callable[[int], Any]],
    ) -> None:
        with MilvusConnectionManager(config.online_store):
            try:
                rows = self._format_data_for_milvus(data)
                collection_to_load_data = Collection(table.name)
                collection_to_load_data.insert(rows)
                #  The flush call will seal any remaining segments and send them for indexing
                collection_to_load_data.flush()
            except Exception as e:
                logger.error(f"Batch writing data failed due to {e}")

    def online_read(
        self,
        config: RepoConfig,
        table: FeatureView,
        entity_keys: List[EntityKeyProto],
        requested_features: Optional[List[str]] = None,
    ) -> List[Tuple[Optional[datetime], Optional[Dict[str, ValueProto]]]]:

        with MilvusConnectionManager(config.online_store):

            quer_expr = self._construct_milvus_query(entity_keys)
            collection = Collection(table.name)
            query_result = collection.query(
                expr=quer_expr, output_fields=requested_features
            )
            results = self._convert_milvus_result_to_feast_type(
                query_result, collection, requested_features
            )

            return results

    @log_exceptions_and_usage(online_store="milvus")
    def update(
        self,
        config: RepoConfig,
        tables_to_delete: Sequence[FeatureView],
        tables_to_keep: Sequence[FeatureView],
        entities_to_delete: Sequence[Entity],
        entities_to_keep: Sequence[Entity],
        partial: bool,
    ):
        with MilvusConnectionManager(config.online_store):
            for table_to_keep in tables_to_keep:
                collection_available = utility.has_collection(table_to_keep.name)
                try:
                    if collection_available:
                        logger.info(f"Collection {table_to_keep.name} already exists.")
                    else:
                        schema = self._convert_featureview_schema_to_milvus_readable(
                            table_to_keep.schema,
                        )

                        collection = Collection(name=table_to_keep.name, schema=schema)
                        logger.info(f"Collection name is {collection.name}")
                        logger.info(
                            f"Collection {table_to_keep.name} has been created successfully."
                        )
                except Exception as e:
                    logger.error(f"Collection update failed due to {e}")

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
        pass

    def _convert_featureview_schema_to_milvus_readable(
        self, feast_schema: List[Field]
    ) -> CollectionSchema:
        """
        Converting a schema understood by Feast to a schema that is readable by Milvus so that it
        can be used when a collection is created in Milvus.

        Parameters:
        feast_schema (List[Field]): Schema stored in FeatureView.

        Returns:
        (CollectionSchema): Schema readable by Milvus.

        """
        boolean_mapping_from_string = {"True": True, "False": False}
        field_list = []
        for field in feast_schema:
            field_name = field.name
            # data_type = self._feast_to_milvus_data_type(field.dtype)
            data_type = TypeConverter.feast_to_milvus_data_type(field.dtype)
            is_vector = False
            dimensions = 0

            if self._data_type_is_supported_vector(data_type):
                is_vector = True

            if field.tags:
                description = field.tags.get("description", " ")
                is_primary = boolean_mapping_from_string.get(
                    field.tags.get("is_primary", "False")
                )

                if is_vector:
                    dimensions = int(field.tags.get("dimensions", "0"))

                    if dimensions <= 0 or dimensions >= 32768:
                        logger.error(
                            f"invalid value for dimensions: {dimensions} set for field: {field_name}"
                        )

                    index_type_from_tag = field.tags.get("index_type")
                    index_type = IndexType(index_type_from_tag).milvus_index_type

                    if index_type == MilvusIndexType.INVALID:
                        logger.error(f"invalid index type: {index_type_from_tag}")

            # Appending the above converted values to construct a FieldSchema
            field_list.append(
                FieldSchema(
                    name=field_name,
                    dtype=data_type,
                    description=description,
                    is_primary=is_primary,
                    dim=dimensions,
                )
            )
        # Returning a CollectionSchema which is a list of type FieldSchema.
        return CollectionSchema(field_list)

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

    def _format_data_for_milvus(self, feast_data):
        """
        Data stored into Milvus takes the grouped representation approach where each feature value is grouped together:
        [[1,2], [1,3]], [John, Lucy], [3,4]]

        Parameters:
        feast_data: List[
            Tuple[EntityKeyProto, Dict[str, ValueProto], datetime, Optional[datetime]]: Data represented for batch write in Feast

        Returns:
        List[List]: transformed_data: Data that can be directly written into Milvus
        """

        milvus_data = []
        for entity_key, values, timestamp, created_ts in feast_data:
            feature = []
            for feature_name, val in values.items():
                val_type = val.WhichOneof("val")
                value = getattr(val, val_type)
                if val_type == "float_list_val":
                    value = np.array(value.val)
                # TODO: Check binary vector conversion
                feature.append(value)
            milvus_data.append(feature)

        transformed_data = [list(item) for item in zip(*milvus_data)]
        return transformed_data

    def _convert_milvus_result_to_feast_type(
        self, milvus_result, collection, features_to_request
    ) -> List[Dict[str, ValueProto]]:
        """
        Convert Milvus result to Feast types.

        Parameters:
        milvus_result (List[Dict[str, any]]): Milvus query result.
        collection (Collection): Milvus collection schema.
        features_to_request (List[str]): Features to request from Milvus.

        Returns:
        List[Dict[str, ValueProto]]: Processed data with Feast types.
        """

        # Here we are constructing the feature list to request from Milvus with their relevant types
        features_with_types = [
            (field.name, TypeConverter.milvus_to_feast_type(field.dtype))
            for field in collection.schema.fields
            if field.name in features_to_request
        ]

        feast_type_result = []
        value_type_actions = {
            "float_list_val": lambda val: ValueProto(float_list_val=FloatList(val=val)),
            "string_val": lambda val: ValueProto(string_val=val),
            "int64_val": lambda val: ValueProto(int64_val=val),
            "double_val": lambda val: ValueProto(double_val=val),
        }
        prefix = "valuetype."

        for row in milvus_result:
            result_row = {}
            for feature, feast_type in features_with_types:

                value_proto = ValueProto()
                feature_value = row[feature]

                if feature_value:
                    # Doing some pre-processing here to remove prefix
                    value_type_method = f"{feast_type.to_value_type()}_val".lower()
                    if value_type_method.startswith(prefix):
                        value_type_method = value_type_method[len(prefix) :]
                    # Construct the appropriate ValueProto instance.
                    value_proto_constructor = value_type_actions.get(value_type_method)
                    if value_proto_constructor:
                        value_proto = value_proto_constructor(feature_value)

                result_row[feature] = value_proto
            # Append result after conversion to Feast Type
            feast_type_result.append(result_row)

        return feast_type_result

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
                val_type = value.WhichOneof("val")
                value_to_search = getattr(value, val_type)
                values_to_search.append(value_to_search)

        # TODO: Enable multiple join key support. Currently only supporting a single primary key/ join key
        milvus_query_expr = f"{entity_join_key[0]} in {values_to_search}"

        return milvus_query_expr
