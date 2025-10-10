import json
from datetime import timedelta
from typing import Any, Callable, Dict, Iterable, Optional, Tuple

from couchbase_columnar.cluster import Cluster
from couchbase_columnar.credential import Credential
from couchbase_columnar.options import ClusterOptions, QueryOptions, TimeoutOptions
from typeguard import typechecked

from feast.data_source import DataSource
from feast.errors import DataSourceNoNameException, ZeroColumnQueryResult
from feast.feature_logging import LoggingDestination
from feast.protos.feast.core.DataSource_pb2 import DataSource as DataSourceProto
from feast.protos.feast.core.FeatureService_pb2 import (
    LoggingConfig as LoggingConfigProto,
)
from feast.protos.feast.core.SavedDataset_pb2 import (
    SavedDatasetStorage as SavedDatasetStorageProto,
)
from feast.repo_config import RepoConfig
from feast.saved_dataset import SavedDatasetStorage
from feast.type_map import ValueType, cb_columnar_type_to_feast_value_type


@typechecked
class CouchbaseColumnarSource(DataSource):
    """A CouchbaseColumnarSource object defines a data source that a CouchbaseColumnarOfflineStore class can use."""

    def source_type(self) -> DataSourceProto.SourceType.ValueType:
        # TODO: Add Couchbase to DataSourceProto.SourceType
        return DataSourceProto.CUSTOM_SOURCE

    def __init__(
        self,
        name: Optional[str] = None,
        query: Optional[str] = None,
        database: Optional[str] = "Default",
        scope: Optional[str] = "Default",
        collection: Optional[str] = None,
        timestamp_field: Optional[str] = "",
        created_timestamp_column: Optional[str] = "",
        field_mapping: Optional[Dict[str, str]] = None,
        description: Optional[str] = "",
        tags: Optional[Dict[str, str]] = None,
        owner: Optional[str] = "",
    ):
        """Creates a CouchbaseColumnarSource object.

        Args:
            name: Name of CouchbaseColumnarSource, which should be unique within a project.
            query: SQL++ query that will be used to fetch the data.
            database: Columnar database name.
            scope: Columnar scope name.
            collection: Columnar collection name.
            timestamp_field (optional): Event timestamp field used for point-in-time joins of
                feature values.
            created_timestamp_column (optional): Timestamp column indicating when the row
                was created, used for deduplicating rows.
            field_mapping (optional): A dictionary mapping of field names in this data
                source to feature names in a feature table or view. Only used for feature
                fields, not entity or timestamp fields.
            description (optional): A human-readable description.
            tags (optional): A dictionary of key-value pairs to store arbitrary metadata.
            owner (optional): The owner of the data source, typically the email of the primary
                maintainer.
        """
        self._couchbase_options = CouchbaseColumnarOptions(
            name=name,
            query=query,
            database=database,
            scope=scope,
            collection=collection,
        )

        # If no name, use the collection as the default name.
        if name is None and collection is None:
            raise DataSourceNoNameException()
        name = name or collection
        assert name

        super().__init__(
            name=name,
            timestamp_field=timestamp_field,
            created_timestamp_column=created_timestamp_column,
            field_mapping=field_mapping,
            description=description,
            tags=tags,
            owner=owner,
        )

    def __hash__(self):
        return super().__hash__()

    def __eq__(self, other):
        if not isinstance(other, CouchbaseColumnarSource):
            raise TypeError(
                "Comparisons should only involve CouchbaseColumnarSource class objects."
            )

        return (
            super().__eq__(other)
            and self._couchbase_options._query == other._couchbase_options._query
            and self.timestamp_field == other.timestamp_field
            and self.created_timestamp_column == other.created_timestamp_column
            and self.field_mapping == other.field_mapping
        )

    @staticmethod
    def from_proto(data_source: DataSourceProto):
        assert data_source.HasField("custom_options")

        couchbase_options = json.loads(data_source.custom_options.configuration)

        return CouchbaseColumnarSource(
            name=couchbase_options["name"],
            query=couchbase_options["query"],
            database=couchbase_options["database"],
            scope=couchbase_options["scope"],
            collection=couchbase_options["collection"],
            field_mapping=dict(data_source.field_mapping),
            timestamp_field=data_source.timestamp_field,
            created_timestamp_column=data_source.created_timestamp_column,
            description=data_source.description,
            tags=dict(data_source.tags),
            owner=data_source.owner,
        )

    def _to_proto_impl(self) -> DataSourceProto:
        data_source_proto = DataSourceProto(
            name=self.name,
            type=DataSourceProto.CUSTOM_SOURCE,
            data_source_class_type="feast.infra.offline_stores.contrib.couchbase_offline_store.couchbase_source.CouchbaseColumnarSource",
            field_mapping=self.field_mapping,
            custom_options=self._couchbase_options.to_proto(),
            description=self.description,
            tags=self.tags,
            owner=self.owner,
        )

        data_source_proto.timestamp_field = self.timestamp_field
        data_source_proto.created_timestamp_column = self.created_timestamp_column

        return data_source_proto

    def validate(self, config: RepoConfig):
        pass

    @staticmethod
    def source_datatype_to_feast_value_type() -> Callable[[str], ValueType]:
        # Define the type conversion for Couchbase fields to Feast ValueType as needed
        return cb_columnar_type_to_feast_value_type

    def _infer_composite_type(self, field: Dict[str, Any]) -> str:
        """
        Infers type signature for a field, rejecting complex nested structures that
        aren't compatible with Feast's type system.

        Args:
            field: Dictionary containing field information including type and nested structures

        Returns:
            String representation of the type, or raises ValueError for incompatible types

        Raises:
            ValueError: If field contains complex nested structures not supported by Feast
        """
        base_type = field.get("field-type", "unknown").lower()

        if base_type == "array":
            if "list" not in field or not field["list"]:
                return "array<unknown>"

            item_type = field["list"][0]
            if item_type.get("field-type") == "object":
                raise ValueError(
                    "Complex object types in arrays are not supported by Feast. "
                    "Arrays must contain homogeneous primitive values."
                )

            # Only allow arrays of primitive types
            inner_type = item_type.get("field-type", "unknown")
            if inner_type in ["array", "multiset", "object"]:
                raise ValueError(
                    "Nested collection types are not supported by Feast. "
                    "Arrays can only be one level deep."
                )

            return f"array<{inner_type}>"

        elif base_type == "object":
            raise ValueError(
                "Complex object types are not supported by Feast. "
                "Only primitive types and homogeneous arrays are allowed."
            )

        elif base_type == "multiset":
            raise ValueError(
                "Multiset types are not supported by Feast. "
                "Only primitive types and homogeneous arrays are allowed."
            )

        return base_type

    def get_table_column_names_and_types(
        self, config: RepoConfig
    ) -> Iterable[Tuple[str, str]]:
        cred = Credential.from_username_and_password(
            config.offline_store.user, config.offline_store.password
        )
        timeout_opts = TimeoutOptions(dispatch_timeout=timedelta(seconds=120))
        cluster = Cluster.create_instance(
            config.offline_store.connection_string,
            cred,
            ClusterOptions(timeout_options=timeout_opts),
        )

        query_context = self.get_table_query_string()
        query = f"""
        SELECT get_object_fields(
            CASE WHEN ARRAY_LENGTH(OBJECT_PAIRS(t)) = 1 AND OBJECT_PAIRS(t)[0].`value` IS NOT MISSING
                 THEN OBJECT_PAIRS(t)[0].`value`
                 ELSE t
            END
        ) AS field_types
        FROM {query_context} AS t
        LIMIT 1;
        """

        result = cluster.execute_query(
            query, QueryOptions(timeout=timedelta(seconds=config.offline_store.timeout))
        )
        if not result:
            raise ZeroColumnQueryResult(query)

        rows = result.get_all_rows()
        field_type_pairs = []
        if rows and rows[0]:
            # Accessing the "field_types" array from the first row
            field_types_list = rows[0].get("field_types", [])
            for field in field_types_list:
                field_name = field.get("field-name", "unknown")
                field_type = field.get("field-type", "unknown")
                # drop uuid fields to ensure schema matches dataframe
                if field_type == "uuid":
                    continue
                field_type = self._infer_composite_type(field)
                field_type_pairs.append((field_name, field_type))
        return field_type_pairs

    def get_table_query_string(self) -> str:
        if (
            self._couchbase_options._database
            and self._couchbase_options._scope
            and self._couchbase_options._collection
        ):
            return f"`{self._couchbase_options._database}`.`{self._couchbase_options._scope}`.`{self._couchbase_options._collection}`"
        else:
            return f"({self._couchbase_options._query})"

    @property
    def database(self) -> str:
        """Returns the database name."""
        return self._couchbase_options._database

    @property
    def scope(self) -> str:
        """Returns the scope name."""
        return self._couchbase_options._scope


class CouchbaseColumnarOptions:
    def __init__(
        self,
        name: Optional[str],
        query: Optional[str],
        database: Optional[str],
        scope: Optional[str],
        collection: Optional[str],
    ):
        self._name = name or ""
        self._query = query or ""
        self._database = database or ""
        self._scope = scope or ""
        self._collection = collection or ""

    @classmethod
    def from_proto(cls, couchbase_options_proto: DataSourceProto.CustomSourceOptions):
        config = json.loads(couchbase_options_proto.configuration.decode("utf8"))
        couchbase_options = cls(
            name=config["name"],
            query=config["query"],
            database=config["database"],
            scope=config["scope"],
            collection=config["collection"],
        )

        return couchbase_options

    def to_proto(self) -> DataSourceProto.CustomSourceOptions:
        couchbase_options_proto = DataSourceProto.CustomSourceOptions(
            configuration=json.dumps(
                {
                    "name": self._name,
                    "query": self._query,
                    "database": self._database,
                    "scope": self._scope,
                    "collection": self._collection,
                }
            ).encode()
        )
        return couchbase_options_proto


class SavedDatasetCouchbaseColumnarStorage(SavedDatasetStorage):
    _proto_attr_name = "custom_storage"

    couchbase_options: CouchbaseColumnarOptions

    def __init__(self, database_ref: str, scope_ref: str, collection_ref: str):
        self.couchbase_options = CouchbaseColumnarOptions(
            database=database_ref,
            scope=scope_ref,
            collection=collection_ref,
            name=None,
            query=None,
        )

    @staticmethod
    def from_proto(storage_proto: SavedDatasetStorageProto) -> SavedDatasetStorage:
        return SavedDatasetCouchbaseColumnarStorage(
            database_ref=CouchbaseColumnarOptions.from_proto(
                storage_proto.custom_storage
            )._database,
            scope_ref=CouchbaseColumnarOptions.from_proto(
                storage_proto.custom_storage
            )._scope,
            collection_ref=CouchbaseColumnarOptions.from_proto(
                storage_proto.custom_storage
            )._collection,
        )

    def to_proto(self) -> SavedDatasetStorageProto:
        return SavedDatasetStorageProto(
            custom_storage=self.couchbase_options.to_proto()
        )

    def to_data_source(self) -> DataSource:
        return CouchbaseColumnarSource(
            database=self.couchbase_options._database,
            scope=self.couchbase_options._scope,
            collection=self.couchbase_options._collection,
        )


class CouchbaseColumnarLoggingDestination(LoggingDestination):
    """
    Couchbase Columnar implementation of a logging destination.
    """

    database: str
    scope: str
    table_name: str

    _proto_kind = "couchbase_columnar_destination"

    def __init__(self, *, database: str, scope: str, table_name: str):
        """
        Args:
            database: The Couchbase database name
            scope: The Couchbase scope name
            table_name: The Couchbase collection name to log features into
        """
        self.database = database
        self.scope = scope
        self.table_name = table_name

    def to_data_source(self) -> DataSource:
        """
        Returns a data source object representing the logging destination.
        """
        return CouchbaseColumnarSource(
            database=self.database,
            scope=self.scope,
            collection=self.table_name,
        )

    def to_proto(self) -> LoggingConfigProto:
        """
        Converts the logging destination to its protobuf representation.
        """
        return LoggingConfigProto(
            couchbase_columnar_destination=LoggingConfigProto.CouchbaseColumnarDestination(
                database=self.database,
                scope=self.scope,
                collection=self.table_name,
            )
        )

    @classmethod
    def from_proto(
        cls, config_proto: LoggingConfigProto
    ) -> "CouchbaseColumnarLoggingDestination":
        """
        Creates a CouchbaseColumnarLoggingDestination from its protobuf representation.
        """
        return CouchbaseColumnarLoggingDestination(
            database=config_proto.CouchbaseColumnarDestination.database,
            scope=config_proto.CouchbaseColumnarDestination.scope,
            table_name=config_proto.CouchbaseColumnarDestination.collection,
        )
