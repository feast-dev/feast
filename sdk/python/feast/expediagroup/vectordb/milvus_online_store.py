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

from feast import Entity, RepoConfig
from feast.expediagroup.vectordb.vector_feature_view import VectorFeatureView
from feast.expediagroup.vectordb.vector_online_store import VectorOnlineStore
from feast.field import Field
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from feast.repo_config import FeastConfigBaseModel
from feast.types import (
    Array,
    FeastType,
    Float32,
    Float64,
    Int32,
    Int64,
    Invalid,
    String,
)
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
            f"Connecting to Milvus with alias {self.online_config.alias} and host {self.online_config.host} and default port {self.online_config.port}."
        )
        connections.connect(
            host=self.online_config.host,
            username=self.online_config.username,
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


class MilvusOnlineStore(VectorOnlineStore):
    def online_write_batch(
        self,
        config: RepoConfig,
        table: VectorFeatureView,
        data: List[
            Tuple[EntityKeyProto, Dict[str, ValueProto], datetime, Optional[datetime]]
        ],
        progress: Optional[Callable[[int], Any]],
    ) -> None:
        with MilvusConnectionManager(config.online_store):
            try:
                entities = self._format_data_for_milvus(data)
                collection_to_load_data = Collection(table.name)
                collection_to_load_data.insert(entities)
                #  The flush call will seal any remaining segments and send them for indexing
                collection_to_load_data.flush()
            except Exception as e:
                logger.error(f"Batch writing data failed due to {e}")

    def online_read(
        self,
        config: RepoConfig,
        table: VectorFeatureView,
        entity_keys: List[EntityKeyProto],
        requested_features: Optional[List[str]] = None,
    ) -> List[Tuple[Optional[datetime], Optional[Dict[str, ValueProto]]]]:
        raise NotImplementedError(
            "to be implemented in https://jira.expedia.biz/browse/EAPC-7972"
        )

    @log_exceptions_and_usage(online_store="milvus")
    def update(
        self,
        config: RepoConfig,
        tables_to_delete: Sequence[VectorFeatureView],
        tables_to_keep: Sequence[VectorFeatureView],
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
                        # TODO: Enable dynamic schema option
                        schema = self._convert_featureview_schema_to_milvus_readable(
                            table_to_keep.schema,
                            table_to_keep.vector_field,
                            table_to_keep.dimensions,
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
        tables: Sequence[VectorFeatureView],
        entities: Sequence[Entity],
    ):
        raise NotImplementedError(
            "to be implemented in https://jira.expedia.biz/browse/EAPC-7974"
        )

    def _convert_featureview_schema_to_milvus_readable(
        self, feast_schema: List[Field], vector_field, vector_field_dimensions
    ) -> CollectionSchema:
        """
        Converting a schema understood by Feast to a schema that is readable by Milvus so that it
        can be used when a collection is created in Milvus.

        Parameters:
        feast_schema (List[Field]): Schema stored in VectorFeatureView.

        Returns:
        (CollectionSchema): Schema readable by Milvus.

        """
        boolean_mapping_from_string = {"True": True, "False": False}
        field_list = []
        dimension = None

        for field in feast_schema:
            if field.name == vector_field:
                field_name = vector_field
                dimension = vector_field_dimensions
            else:
                field_name = field.name

            data_type = self._feast_to_milvus_data_type(field.dtype)

            if field.tags:
                description = field.tags.get("description", " ")
                is_primary = boolean_mapping_from_string.get(
                    field.tags.get("is_primary", "False")
                )

            # Appending the above converted values to construct a FieldSchema
            field_list.append(
                FieldSchema(
                    name=field_name,
                    dtype=data_type,
                    description=description,
                    is_primary=is_primary,
                    dim=dimension,
                )
            )
        # Returning a CollectionSchema which is a list of type FieldSchema.
        return CollectionSchema(field_list)

    def _feast_to_milvus_data_type(self, feast_type: FeastType) -> DataType:
        """
        Mapping for converting Feast data type to a data type compatible wih Milvus.

        Parameters:
        feast_type (FeastType): This is a type associated with a Feature that is stored in a VectorFeatureView, readable with Feast.

        Returns:
        DataType : DataType associated with what Milvus can understand and associate its Feature types to
        """

        return {
            Int32: DataType.INT32,
            Int64: DataType.INT64,
            Float32: DataType.FLOAT,
            Float64: DataType.DOUBLE,
            String: DataType.STRING,
            Invalid: DataType.UNKNOWN,
            Array(Float32): DataType.FLOAT_VECTOR,
            # TODO: Need to think about list of binaries and list of bytes
            # FeastType.BYTES_LIST: DataType.BINARY_VECTOR
        }.get(feast_type, None)

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
                if val_type == "float_list_val":
                    float_list = val.float_list_val.val
                    final_float_list = np.array(float_list)
                    feature.append(final_float_list)
                # TODO: Test out binary lists
                if val_type == "biinary_list_val":
                    binary_list = val.binary_list_val.val
                    feature.append(binary_list)
                if val_type == "string_val":
                    string_val = val.string_val
                    feature.append(string_val)
                if val_type == "int32_val":
                    int32_val = val.int32_val
                    feature.append(int32_val)
                if val_type == "int64_val":
                    int64_val = val.int64_val
                    feature.append(int64_val)
                if val_type == "bytes_val":
                    bytes_val = val.bytes_val
                    feature.append(bytes_val)
                if val_type == "float_val":
                    float_val = val.float_val
                    feature.append(float_val)
            milvus_data.append(feature)

        transformed_data = [list(item) for item in zip(*milvus_data)]
        return transformed_data
