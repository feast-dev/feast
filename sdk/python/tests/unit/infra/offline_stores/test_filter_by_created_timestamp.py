from datetime import timedelta
from unittest.mock import MagicMock

import dask.dataframe as dd
import ibis
import pandas as pd
import pytest

from feast.entity import Entity
from feast.feature_view import FeatureView, Field
from feast.infra.offline_stores import dask as dask_mod
from feast.infra.offline_stores import offline_utils
from feast.infra.offline_stores.bigquery import (
    MULTIPLE_FEATURE_VIEW_POINT_IN_TIME_JOIN,
)
from feast.infra.offline_stores.dask import (
    DaskOfflineStore,
    DaskOfflineStoreConfig,
)
from feast.infra.offline_stores.file_source import FileSource
from feast.infra.offline_stores.ibis import point_in_time_join
from feast.infra.offline_stores.offline_utils import FeatureViewQueryContext
from feast.repo_config import RepoConfig
from feast.types import Float32, ValueType

CREATED_TIMESTAMP_PREDICATE = (
    "subquery.created_timestamp <= entity_dataframe.entity_timestamp"
)


def _query_context(created_timestamp_column):
    return FeatureViewQueryContext(
        name="driver_stats",
        ttl=86400,
        entities=["driver_id"],
        features=["conv_rate"],
        field_mapping={},
        timestamp_field="event_timestamp",
        created_timestamp_column=created_timestamp_column,
        table_subquery="`project`.`dataset`.`table`",
        entity_selections=["driver_id AS driver_id"],
        min_event_timestamp="2025-01-01T00:00:00",
        max_event_timestamp="2025-01-02T00:00:00",
        date_partition_column=None,
        timestamp_field_type=None,
    )


def _render(
    created_timestamp_column,
    query_template=MULTIPLE_FEATURE_VIEW_POINT_IN_TIME_JOIN,
    **kwargs,
):
    return offline_utils.build_point_in_time_query(
        [_query_context(created_timestamp_column)],
        left_table_query_string="entity_df_table",
        entity_df_event_timestamp_col="event_timestamp",
        entity_df_columns={"driver_id": None, "event_timestamp": None}.keys(),
        query_template=query_template,
        full_feature_names=False,
        **kwargs,
    )


def test_filter_by_created_timestamp_adds_created_timestamp_cutoff_to_query():
    query = _render("created_ts", filter_by_created_timestamp=True)
    assert CREATED_TIMESTAMP_PREDICATE in query


def test_filter_by_created_timestamp_defaults_to_false_and_leaves_query_unchanged():
    assert CREATED_TIMESTAMP_PREDICATE not in _render("created_ts")
    assert _render("created_ts") == _render(
        "created_ts", filter_by_created_timestamp=False
    )


def test_filter_by_created_timestamp_has_no_effect_without_created_timestamp_column():
    query = _render(None, filter_by_created_timestamp=True)
    assert CREATED_TIMESTAMP_PREDICATE not in query


SQL_TEMPLATE_STORE_MODULES = [
    "feast.infra.offline_stores.redshift",
    "feast.infra.offline_stores.contrib.spark_offline_store.spark",
    "feast.infra.offline_stores.contrib.trino_offline_store.trino",
    "feast.infra.offline_stores.contrib.athena_offline_store.athena",
    "feast.infra.offline_stores.contrib.postgres_offline_store.postgres",
    "feast.infra.offline_stores.contrib.clickhouse_offline_store.clickhouse",
    "feast.infra.offline_stores.contrib.couchbase_offline_store.couchbase",
]


@pytest.mark.parametrize("module_name", SQL_TEMPLATE_STORE_MODULES)
def test_all_sql_templates_gate_the_created_timestamp_cutoff(module_name):
    module = pytest.importorskip(module_name)
    template = module.MULTIPLE_FEATURE_VIEW_POINT_IN_TIME_JOIN

    with_cutoff = _render(
        "created_ts", query_template=template, filter_by_created_timestamp=True
    )
    assert CREATED_TIMESTAMP_PREDICATE in with_cutoff

    assert CREATED_TIMESTAMP_PREDICATE not in _render(
        "created_ts", query_template=template
    )
    assert CREATED_TIMESTAMP_PREDICATE not in _render(
        None, query_template=template, filter_by_created_timestamp=True
    )


class TestDaskFilterByCreatedTimestamp:
    def _run(self, filter_by_created_timestamp, monkeypatch, src=None):
        if src is None:
            src = pd.DataFrame(
                {
                    "driver_id": [1, 1],
                    "event_timestamp": pd.to_datetime(
                        [
                            "2025-01-01T10:00:00Z",
                            "2025-01-01T10:00:00Z",  # same event ts, created after the entity ts
                        ]
                    ),
                    "created_ts": pd.to_datetime(
                        [
                            "2025-01-01T12:00:00Z",  # created before the entity ts
                            "2025-01-03T00:00:00Z",  # created after the entity ts
                        ]
                    ),
                    "conv_rate": [0.4, 0.6],
                }
            )
        ddf = dd.from_pandas(src, npartitions=1)
        monkeypatch.setattr(dask_mod, "_read_datasource", lambda ds, repo_path: ddf)

        repo_config = RepoConfig(
            project="test_project",
            registry="test_registry",
            provider="local",
            offline_store=DaskOfflineStoreConfig(type="dask"),
        )
        fv = FeatureView(
            name="driver_stats",
            entities=[
                Entity(
                    name="driver_id",
                    join_keys=["driver_id"],
                    value_type=ValueType.INT64,
                )
            ],
            schema=[Field(name="conv_rate", dtype=Float32)],
            source=FileSource(
                path="dummy.parquet",  # not read in this test
                timestamp_field="event_timestamp",
                created_timestamp_column="created_ts",
            ),
            ttl=timedelta(days=7),
        )
        registry = MagicMock()
        registry.list_on_demand_feature_views.return_value = []

        entity_df = pd.DataFrame(
            {
                "driver_id": [1],
                "event_timestamp": pd.to_datetime(["2025-01-02T00:00:00Z"]),
            }
        )

        job = DaskOfflineStore.get_historical_features(
            config=repo_config,
            feature_views=[fv],
            feature_refs=["driver_stats:conv_rate"],
            entity_df=entity_df,
            registry=registry,
            project="test_project",
            full_feature_names=False,
            filter_by_created_timestamp=filter_by_created_timestamp,
        )
        return job.to_df()

    def test_default_serves_latest_created_value(self, monkeypatch):
        df = self._run(False, monkeypatch)
        assert df["conv_rate"].tolist() == [0.6]

    def test_filter_by_created_timestamp_excludes_values_created_after_entity_timestamp(
        self, monkeypatch
    ):
        df = self._run(True, monkeypatch)
        assert df["conv_rate"].tolist() == [0.4]

    def test_filter_by_created_timestamp_excludes_rows_with_null_created_timestamp(
        self, monkeypatch
    ):
        src = pd.DataFrame(
            {
                "driver_id": [1, 1],
                "event_timestamp": pd.to_datetime(
                    ["2025-01-01T10:00:00Z", "2025-01-01T10:00:00Z"]
                ),
                "created_ts": pd.to_datetime(["2025-01-01T12:00:00Z", pd.NaT]),
                "conv_rate": [0.4, 0.6],
            }
        )
        df = self._run(True, monkeypatch, src=src)
        assert df["conv_rate"].tolist() == [0.4]


class TestIbisFilterByCreatedTimestamp:
    def _run(self, filter_by_created_timestamp):
        entity_table = ibis.memtable(
            pd.DataFrame(
                {
                    "driver_id": [1],
                    "event_timestamp": pd.to_datetime(["2025-01-02T00:00:00Z"]),
                }
            )
        )
        feature_table = ibis.memtable(
            pd.DataFrame(
                {
                    "driver_id": [1, 1],
                    "event_timestamp": pd.to_datetime(
                        ["2025-01-01T10:00:00Z", "2025-01-01T10:00:00Z"]
                    ),
                    "created_ts": pd.to_datetime(
                        ["2025-01-01T12:00:00Z", "2025-01-03T00:00:00Z"]
                    ),
                    "conv_rate": [0.4, 0.6],
                }
            )
        )
        res = point_in_time_join(
            entity_table=entity_table,
            feature_tables=[
                (
                    feature_table,
                    "event_timestamp",
                    "created_ts",
                    {"driver_id": "driver_id"},
                    ["conv_rate"],
                    None,
                )
            ],
            event_timestamp_col="event_timestamp",
            filter_by_created_timestamp=filter_by_created_timestamp,
        ).execute()
        return res

    def test_default_serves_latest_created_value(self):
        assert self._run(False)["conv_rate"].tolist() == [0.6]

    def test_filter_by_created_timestamp_excludes_values_created_after_entity_timestamp(
        self,
    ):
        assert self._run(True)["conv_rate"].tolist() == [0.4]
