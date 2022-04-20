import json
from typing import Callable, Dict, Iterable, Optional, Tuple

from feast.data_source import DataSource
from feast.infra.utils.postgres.connection_utils import _get_conn
from feast.protos.feast.core.DataSource_pb2 import DataSource as DataSourceProto
from feast.repo_config import RepoConfig
from feast.type_map import pg_type_code_to_pg_type, pg_type_to_feast_value_type
from feast.value_type import ValueType


class PostgreSQLSource(DataSource):
    def __init__(
        self,
        name: str,
        query: str,
        timestamp_field: Optional[str] = "",
        created_timestamp_column: Optional[str] = "",
        field_mapping: Optional[Dict[str, str]] = None,
        date_partition_column: Optional[str] = "",
    ):
        self._postgres_options = PostgreSQLOptions(name=name, query=query)

        super().__init__(
            name=name,
            timestamp_field=timestamp_field,
            created_timestamp_column=created_timestamp_column,
            field_mapping=field_mapping,
            date_partition_column=date_partition_column,
        )

    def __hash__(self):
        return super().__hash__()

    def __eq__(self, other):
        if not isinstance(other, PostgreSQLSource):
            raise TypeError(
                "Comparisons should only involve PostgreSQLSource class objects."
            )

        return (
            self._postgres_options._query == other._postgres_options._query
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
            field_mapping=dict(data_source.field_mapping),
            timestamp_field=data_source.timestamp_field,
            created_timestamp_column=data_source.created_timestamp_column,
            date_partition_column=data_source.date_partition_column,
        )

    def to_proto(self) -> DataSourceProto:
        data_source_proto = DataSourceProto(
            type=DataSourceProto.CUSTOM_SOURCE,
            data_source_class_type="feast.infra.offline_stores.contrib.postgres_offline_store.postgres_source.PostgreSQLSource",
            field_mapping=self.field_mapping,
            custom_options=self._postgres_options.to_proto(),
        )

        data_source_proto.timestamp_field = self.timestamp_field
        data_source_proto.created_timestamp_column = self.created_timestamp_column
        data_source_proto.date_partition_column = self.date_partition_column

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
            cur.execute(
                f"SELECT * FROM ({self.get_table_query_string()}) AS sub LIMIT 0"
            )
            return (
                (c.name, pg_type_code_to_pg_type(c.type_code)) for c in cur.description
            )

    def get_table_query_string(self) -> str:
        return f"({self._postgres_options._query})"


class PostgreSQLOptions:
    def __init__(self, name: str, query: Optional[str]):
        self._name = name
        self._query = query

    @classmethod
    def from_proto(cls, postgres_options_proto: DataSourceProto.CustomSourceOptions):
        config = json.loads(postgres_options_proto.configuration.decode("utf8"))
        postgres_options = cls(name=config["name"], query=config["query"])

        return postgres_options

    def to_proto(self) -> DataSourceProto.CustomSourceOptions:
        postgres_options_proto = DataSourceProto.CustomSourceOptions(
            configuration=json.dumps(
                {"name": self._name, "query": self._query}
            ).encode()
        )

        return postgres_options_proto
