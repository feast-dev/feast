import json
from typing import Callable, Dict, Iterable, Optional, Tuple

from typeguard import typechecked

from feast.data_source import DataSource
from feast.errors import DataSourceNoNameException, ZeroColumnQueryResult
from feast.infra.utils.postgres.connection_utils import _get_conn
from feast.protos.feast.core.DataSource_pb2 import DataSource as DataSourceProto
from feast.protos.feast.core.SavedDataset_pb2 import (
    SavedDatasetStorage as SavedDatasetStorageProto,
)
from feast.repo_config import RepoConfig
from feast.saved_dataset import SavedDatasetStorage
from feast.type_map import pg_type_code_to_pg_type, pg_type_to_feast_value_type
from feast.value_type import ValueType


@typechecked
class PostgreSQLSource(DataSource):
    """A PostgreSQLSource object defines a data source that a PostgreSQLOfflineStore class can use."""

    def source_type(self) -> DataSourceProto.SourceType.ValueType:
        # TODO: Add Postgres to DataSourceProto.SourceType
        return DataSourceProto.CUSTOM_SOURCE

    def __init__(
        self,
        name: Optional[str] = None,
        query: Optional[str] = None,
        table: Optional[str] = None,
        timestamp_field: Optional[str] = "",
        created_timestamp_column: Optional[str] = "",
        field_mapping: Optional[Dict[str, str]] = None,
        description: Optional[str] = "",
        tags: Optional[Dict[str, str]] = None,
        owner: Optional[str] = "",
    ):
        """Creates a PostgreSQLSource object.

        Args:
            name: Name of PostgreSQLSource, which should be unique within a project.
            query: SQL query that will be used to fetch the data.
            table: Table name.
            timestamp_field (optional): Event timestamp field used for point-in-time joins of
                feature values.
            created_timestamp_column (optional): Timestamp column indicating when the row
                was created, used for deduplicating rows.
            field_mapping (optional): A dictionary mapping of column names in this data
                source to feature names in a feature table or view. Only used for feature
                columns, not entity or timestamp columns.
            description (optional): A human-readable description.
            tags (optional): A dictionary of key-value pairs to store arbitrary metadata.
            owner (optional): The owner of the data source, typically the email of the primary
                maintainer.
        """
        self._postgres_options = PostgreSQLOptions(name=name, query=query, table=table)

        # If no name, use the table as the default name.
        if name is None and table is None:
            raise DataSourceNoNameException()
        name = name or table
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
        if not isinstance(other, PostgreSQLSource):
            raise TypeError(
                "Comparisons should only involve PostgreSQLSource class objects."
            )

        return (
            super().__eq__(other)
            and self._postgres_options._query == other._postgres_options._query
            and self.timestamp_field == other.timestamp_field
            and self.created_timestamp_column == other.created_timestamp_column
            and self.field_mapping == other.field_mapping
        )

    @staticmethod
    def from_proto(data_source: DataSourceProto):
        assert data_source.HasField("custom_options")

        postgres_options = json.loads(data_source.custom_options.configuration)

        return PostgreSQLSource(
            name=postgres_options["name"],
            query=postgres_options["query"],
            table=postgres_options["table"],
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
            data_source_class_type="feast.infra.offline_stores.contrib.postgres_offline_store.postgres_source.PostgreSQLSource",
            field_mapping=self.field_mapping,
            custom_options=self._postgres_options.to_proto(),
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
        return pg_type_to_feast_value_type

    def get_table_column_names_and_types(
        self, config: RepoConfig
    ) -> Iterable[Tuple[str, str]]:
        with _get_conn(config.offline_store) as conn, conn.cursor() as cur:
            query = f"SELECT * FROM {self.get_table_query_string()} AS sub LIMIT 0"
            cur.execute(query)
            if not cur.description:
                raise ZeroColumnQueryResult(query)

            return (
                (c.name, pg_type_code_to_pg_type(c.type_code)) for c in cur.description
            )

    def get_table_query_string(self) -> str:
        if self._postgres_options._table:
            return f"{self._postgres_options._table}"
        else:
            return f"({self._postgres_options._query})"


class PostgreSQLOptions:
    def __init__(
        self,
        name: Optional[str],
        query: Optional[str],
        table: Optional[str],
    ):
        self._name = name or ""
        self._query = query or ""
        self._table = table or ""

    @classmethod
    def from_proto(cls, postgres_options_proto: DataSourceProto.CustomSourceOptions):
        config = json.loads(postgres_options_proto.configuration.decode("utf8"))
        postgres_options = cls(
            name=config["name"], query=config["query"], table=config["table"]
        )

        return postgres_options

    def to_proto(self) -> DataSourceProto.CustomSourceOptions:
        postgres_options_proto = DataSourceProto.CustomSourceOptions(
            configuration=json.dumps(
                {"name": self._name, "query": self._query, "table": self._table}
            ).encode()
        )
        return postgres_options_proto


class SavedDatasetPostgreSQLStorage(SavedDatasetStorage):
    _proto_attr_name = "custom_storage"

    postgres_options: PostgreSQLOptions

    def __init__(self, table_ref: str):
        self.postgres_options = PostgreSQLOptions(
            table=table_ref, name=None, query=None
        )

    @staticmethod
    def from_proto(storage_proto: SavedDatasetStorageProto) -> SavedDatasetStorage:
        return SavedDatasetPostgreSQLStorage(
            table_ref=PostgreSQLOptions.from_proto(storage_proto.custom_storage)._table
        )

    def to_proto(self) -> SavedDatasetStorageProto:
        return SavedDatasetStorageProto(custom_storage=self.postgres_options.to_proto())

    def to_data_source(self) -> DataSource:
        return PostgreSQLSource(table=self.postgres_options._table)
