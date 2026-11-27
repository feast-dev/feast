"""The SQL point-in-time join templates identify each entity row by its join key
values and event timestamp. Rows whose values concatenate to the same string,
such as driver_id=1, customer_id=12 and driver_id=11, customer_id=2, must still
get their own feature values."""

import re

import duckdb
import pytest

from feast.infra.offline_stores import offline_utils
from feast.infra.offline_stores.offline_utils import FeatureViewQueryContext

# Templates whose rendered query DuckDB can run.
DUCKDB_RUNNABLE_TEMPLATE_MODULES = [
    "feast.infra.offline_stores.redshift",
    "feast.infra.offline_stores.contrib.athena_offline_store.athena",
    "feast.infra.offline_stores.contrib.postgres_offline_store.postgres",
    "feast.infra.offline_stores.contrib.trino_offline_store.trino",
    "feast.infra.offline_stores.contrib.clickhouse_offline_store.clickhouse",
]

# Templates that use SQL DuckDB does not support (ASOF ... MATCH_CONDITION,
# CREATE TEMP TABLE with backticks, Spark temp views, N1QL).
OTHER_TEMPLATE_MODULES = [
    "feast.infra.offline_stores.bigquery",
    "feast.infra.offline_stores.snowflake",
    "feast.infra.offline_stores.contrib.spark_offline_store.spark",
    "feast.infra.offline_stores.contrib.couchbase_offline_store.couchbase",
]


def _render(query_template: str) -> str:
    context = FeatureViewQueryContext(
        name="trips",
        ttl=0,
        entities=["driver_id", "customer_id"],
        features=["trips"],
        field_mapping={},
        timestamp_field="event_timestamp",
        created_timestamp_column=None,
        table_subquery="(SELECT * FROM feature_table)",
        entity_selections=["driver_id AS driver_id", "customer_id AS customer_id"],
        min_event_timestamp=None,
        max_event_timestamp="2025-01-03T00:00:00",
        date_partition_column=None,
        timestamp_field_type=None,
    )
    return offline_utils.build_point_in_time_query(
        [context],
        left_table_query_string="entity_df",
        entity_df_event_timestamp_col="event_timestamp",
        entity_df_columns={
            "driver_id": None,
            "customer_id": None,
            "event_timestamp": None,
        }.keys(),
        query_template=query_template,
    )


@pytest.mark.parametrize("module_name", DUCKDB_RUNNABLE_TEMPLATE_MODULES)
def test_entity_rows_with_colliding_key_strings_keep_their_own_features(
    module_name,
):
    module = pytest.importorskip(module_name)
    con = duckdb.connect()
    # Athena and Trino parse the event timestamp bounds with this function.
    con.execute("CREATE MACRO from_iso8601_timestamp(s) AS CAST(s AS TIMESTAMP)")
    con.execute(
        "CREATE TABLE entity_df AS SELECT * FROM (VALUES "
        "(1, 12, TIMESTAMP '2025-01-02 00:00:00'), "
        "(11, 2, TIMESTAMP '2025-01-02 00:00:00')"
        ") AS t(driver_id, customer_id, event_timestamp)"
    )
    con.execute(
        "CREATE TABLE feature_table AS SELECT * FROM (VALUES "
        "(1, 12, TIMESTAMP '2025-01-01 00:00:00', 100), "
        "(11, 2, TIMESTAMP '2025-01-01 00:00:00', 200)"
        ") AS t(driver_id, customer_id, event_timestamp, trips)"
    )

    df = con.execute(_render(module.MULTIPLE_FEATURE_VIEW_POINT_IN_TIME_JOIN)).df()

    rows = sorted(zip(df["driver_id"], df["customer_id"], df["trips"]))
    assert rows == [(1, 12, 100), (11, 2, 200)]


@pytest.mark.parametrize("module_name", OTHER_TEMPLATE_MODULES)
def test_entity_row_id_length_prefixes_each_join_key(module_name):
    module = pytest.importorskip(module_name)
    query = _render(module.MULTIPLE_FEATURE_VIEW_POINT_IN_TIME_JOIN)

    for entity in ("driver_id", "customer_id"):
        assert re.search(rf"LENGTH\((CAST|TOSTRING)\([^)]*\b{entity}\b", query)
