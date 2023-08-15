import logging
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

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
from feast.field import Field
from feast.infra.online_stores.online_store import OnlineStore
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from feast.repo_config import FeastConfigBaseModel
from feast.types import (
    Array,
    Bytes,
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
        raise NotImplementedError(
            "to be implemented in https://jira.expedia.biz/browse/EAPC-7971"
        )

    def online_read(
        self,
        config: RepoConfig,
        table: FeatureView,
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
            data_type = self._feast_to_milvus_data_type(field.dtype)
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

                    if dimensions <= 0:
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

    def _feast_to_milvus_data_type(self, feast_type: FeastType) -> DataType:
        """
        Mapping for converting Feast data type to a data type compatible wih Milvus.

        Parameters:
        feast_type (FeastType): This is a type associated with a Feature that is stored in a FeatureView, readable with Feast.

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
            Array(Bytes): DataType.BINARY_VECTOR,
        }.get(feast_type, None)
