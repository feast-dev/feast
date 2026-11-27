from feast.infra.offline_stores.contrib.trino_offline_store.trino_source import (
    TrinoSource,
)


def test_trino_source_get_table_query_string_with_table():
    source = TrinoSource(
        table="my_catalog.my_schema.my_table",
        timestamp_field="event_timestamp",
    )
    assert source.get_table_query_string() == "my_catalog.my_schema.my_table"


def test_trino_source_get_table_query_string_with_query():
    query = "SELECT user_id, event_timestamp, val FROM catalog.schema.table"
    source = TrinoSource(
        name="user_source",
        query=query,
        timestamp_field="event_timestamp",
    )
    assert source.get_table_query_string() == f"({query})"


def test_trino_source_proto_roundtrip_query():
    query = "SELECT user_id, event_timestamp, val FROM catalog.schema.table"
    source = TrinoSource(
        name="user_source",
        query=query,
        timestamp_field="event_timestamp",
    )
    proto = source.to_proto()
    deserialized = TrinoSource.from_proto(proto)

    assert deserialized.name == source.name
    assert deserialized.query == source.query
    assert deserialized.get_table_query_string() == f"({query})"


def test_trino_source_proto_roundtrip_table():
    source = TrinoSource(
        table="my_catalog.my_schema.my_table",
        timestamp_field="event_timestamp",
    )
    proto = source.to_proto()
    deserialized = TrinoSource.from_proto(proto)

    assert deserialized.name == source.name
    assert deserialized.table == source.table
    assert deserialized.get_table_query_string() == "my_catalog.my_schema.my_table"
