import os
import re
import tempfile
from datetime import datetime, timedelta

import pandas as pd
import pytest

from feast import (
    Entity,
    FeatureStore,
    FeatureView,
    FileSource,
    RepoConfig,
    RequestSource,
)
from feast.driver_test_data import create_driver_hourly_stats_df
from feast.field import Field
from feast.infra.online_stores.sqlite import SqliteOnlineStoreConfig
from feast.on_demand_feature_view import on_demand_feature_view
from feast.types import (
    Array,
    Bool,
    Float32,
    Float64,
    Int64,
    String,
)


def test_pandas_transformation():
    with tempfile.TemporaryDirectory() as data_dir:
        store = FeatureStore(
            config=RepoConfig(
                project="test_on_demand_python_transformation",
                registry=os.path.join(data_dir, "registry.db"),
                provider="local",
                entity_key_serialization_version=3,
                online_store=SqliteOnlineStoreConfig(
                    path=os.path.join(data_dir, "online.db")
                ),
            )
        )

        # Generate test data.
        end_date = datetime.now().replace(microsecond=0, second=0, minute=0)
        start_date = end_date - timedelta(days=15)

        driver_entities = [1001, 1002, 1003, 1004, 1005]
        driver_df = create_driver_hourly_stats_df(driver_entities, start_date, end_date)
        driver_stats_path = os.path.join(data_dir, "driver_stats.parquet")
        driver_df.to_parquet(path=driver_stats_path, allow_truncated_timestamps=True)

        driver = Entity(name="driver", join_keys=["driver_id"])

        driver_stats_source = FileSource(
            name="driver_hourly_stats_source",
            path=driver_stats_path,
            timestamp_field="event_timestamp",
            created_timestamp_column="created",
        )

        driver_stats_fv = FeatureView(
            name="driver_hourly_stats",
            entities=[driver],
            ttl=timedelta(days=0),
            schema=[
                Field(name="conv_rate", dtype=Float32),
                Field(name="acc_rate", dtype=Float32),
                Field(name="avg_daily_trips", dtype=Int64),
            ],
            online=True,
            source=driver_stats_source,
        )

        @on_demand_feature_view(
            sources=[driver_stats_fv],
            schema=[Field(name="conv_rate_plus_acc", dtype=Float64)],
            mode="pandas",
        )
        def pandas_view(inputs: pd.DataFrame) -> pd.DataFrame:
            df = pd.DataFrame()
            df["conv_rate_plus_acc"] = inputs["conv_rate"] + inputs["acc_rate"]
            return df

        store.apply([driver, driver_stats_source, driver_stats_fv, pandas_view])

        entity_rows = [
            {
                "driver_id": 1001,
            }
        ]
        store.write_to_online_store(
            feature_view_name="driver_hourly_stats", df=driver_df
        )

        online_response = store.get_online_features(
            entity_rows=entity_rows,
            features=[
                "driver_hourly_stats:conv_rate",
                "driver_hourly_stats:acc_rate",
                "driver_hourly_stats:avg_daily_trips",
                "pandas_view:conv_rate_plus_acc",
            ],
        ).to_df()

        assert online_response["conv_rate_plus_acc"].equals(
            online_response["conv_rate"] + online_response["acc_rate"]
        )


def test_pandas_transformation_returning_all_data_types():
    with tempfile.TemporaryDirectory() as data_dir:
        store = FeatureStore(
            config=RepoConfig(
                project="test_on_demand_python_transformation",
                registry=os.path.join(data_dir, "registry.db"),
                provider="local",
                entity_key_serialization_version=3,
                online_store=SqliteOnlineStoreConfig(
                    path=os.path.join(data_dir, "online.db")
                ),
            )
        )

        # Generate test data.
        end_date = datetime.now().replace(microsecond=0, second=0, minute=0)
        start_date = end_date - timedelta(days=15)

        driver_entities = [1001, 1002, 1003, 1004, 1005]
        driver_df = create_driver_hourly_stats_df(driver_entities, start_date, end_date)
        driver_stats_path = os.path.join(data_dir, "driver_stats.parquet")
        driver_df.to_parquet(path=driver_stats_path, allow_truncated_timestamps=True)

        driver = Entity(name="driver", join_keys=["driver_id"])

        driver_stats_source = FileSource(
            name="driver_hourly_stats_source",
            path=driver_stats_path,
            timestamp_field="event_timestamp",
            created_timestamp_column="created",
        )

        driver_stats_fv = FeatureView(
            name="driver_hourly_stats",
            entities=[driver],
            ttl=timedelta(days=0),
            schema=[
                Field(name="conv_rate", dtype=Float32),
                Field(name="acc_rate", dtype=Float32),
                Field(name="avg_daily_trips", dtype=Int64),
            ],
            online=True,
            source=driver_stats_source,
        )

        request_source = RequestSource(
            name="request_source",
            schema=[
                Field(name="avg_daily_trip_rank_thresholds", dtype=Array(Int64)),
                Field(name="avg_daily_trip_rank_names", dtype=Array(String)),
            ],
        )

        @on_demand_feature_view(
            sources=[request_source, driver_stats_fv],
            schema=[
                Field(name="highest_achieved_rank", dtype=String),
                Field(name="avg_daily_trips_plus_one", dtype=Int64),
                Field(name="conv_rate_plus_acc", dtype=Float64),
                Field(name="is_highest_rank", dtype=Bool),
                Field(name="achieved_ranks", dtype=Array(String)),
                Field(name="trips_until_next_rank_int", dtype=Array(Int64)),
                Field(name="trips_until_next_rank_float", dtype=Array(Float64)),
                Field(name="achieved_ranks_mask", dtype=Array(Bool)),
            ],
            mode="pandas",
        )
        def pandas_view(inputs: pd.DataFrame) -> pd.DataFrame:
            df = pd.DataFrame()
            df["conv_rate_plus_acc"] = inputs["conv_rate"] + inputs["acc_rate"]
            df["avg_daily_trips_plus_one"] = inputs["avg_daily_trips"] + 1

            df["trips_until_next_rank_int"] = inputs[
                ["avg_daily_trips", "avg_daily_trip_rank_thresholds"]
            ].apply(
                lambda x: [max(threshold - x.iloc[0], 0) for threshold in x.iloc[1]],
                axis=1,
            )
            df["trips_until_next_rank_float"] = df["trips_until_next_rank_int"].map(
                lambda values: [float(value) for value in values]
            )
            df["achieved_ranks_mask"] = df["trips_until_next_rank_int"].map(
                lambda values: [value <= 0 for value in values]
            )

            temp = pd.concat(
                [df[["achieved_ranks_mask"]], inputs[["avg_daily_trip_rank_names"]]],
                axis=1,
            )
            df["achieved_ranks"] = temp.apply(
                lambda x: [
                    rank if achieved else "Locked"
                    for achieved, rank in zip(x.iloc[0], x.iloc[1])
                ],
                axis=1,
            )
            df["highest_achieved_rank"] = (
                df["achieved_ranks"]
                .map(
                    lambda ranks: str(
                        ([rank for rank in ranks if rank != "Locked"][-1:] or ["None"])[
                            0
                        ]
                    )
                )
                .astype("string")
            )
            df["is_highest_rank"] = df["achieved_ranks"].map(
                lambda ranks: ranks[-1] != "Locked"
            )
            return df

        store.apply([driver, driver_stats_source, driver_stats_fv, pandas_view])

        entity_rows = [
            {
                "driver_id": 1001,
                "avg_daily_trip_rank_thresholds": [100, 250, 500, 1000],
                "avg_daily_trip_rank_names": ["Bronze", "Silver", "Gold", "Platinum"],
            }
        ]
        store.write_to_online_store(
            feature_view_name="driver_hourly_stats", df=driver_df
        )

        online_response = store.get_online_features(
            entity_rows=entity_rows,
            features=[
                "driver_hourly_stats:conv_rate",
                "driver_hourly_stats:acc_rate",
                "driver_hourly_stats:avg_daily_trips",
                "pandas_view:avg_daily_trips_plus_one",
                "pandas_view:conv_rate_plus_acc",
                "pandas_view:trips_until_next_rank_int",
                "pandas_view:trips_until_next_rank_float",
                "pandas_view:achieved_ranks_mask",
                "pandas_view:achieved_ranks",
                "pandas_view:highest_achieved_rank",
                "pandas_view:is_highest_rank",
            ],
        ).to_df()
        # We use to_df here to ensure we use the pandas backend, but convert to a dict for comparisons
        result = online_response.to_dict(orient="records")[0]

        # Type assertions
        # Materialized view
        assert type(result["conv_rate"]) == float
        assert type(result["acc_rate"]) == float
        assert type(result["avg_daily_trips"]) == int
        # On-demand view
        assert type(result["avg_daily_trips_plus_one"]) == int
        assert type(result["conv_rate_plus_acc"]) == float
        assert type(result["highest_achieved_rank"]) == str
        assert type(result["is_highest_rank"]) == bool

        assert type(result["trips_until_next_rank_int"]) == list
        assert all([type(e) == int for e in result["trips_until_next_rank_int"]])

        assert type(result["trips_until_next_rank_float"]) == list
        assert all([type(e) == float for e in result["trips_until_next_rank_float"]])

        assert type(result["achieved_ranks"]) == list
        assert all([type(e) == str for e in result["achieved_ranks"]])

        assert type(result["achieved_ranks_mask"]) == list
        assert all([type(e) == bool for e in result["achieved_ranks_mask"]])

        # Value assertions
        expected_trips_until_next_rank = [
            max(threshold - result["avg_daily_trips"], 0)
            for threshold in entity_rows[0]["avg_daily_trip_rank_thresholds"]
        ]
        expected_mask = [value <= 0 for value in expected_trips_until_next_rank]
        expected_ranks = [
            rank if achieved else "Locked"
            for achieved, rank in zip(
                expected_mask, entity_rows[0]["avg_daily_trip_rank_names"]
            )
        ]
        highest_rank = (
            [rank for rank in expected_ranks if rank != "Locked"][-1:] or ["None"]
        )[0]

        assert result["conv_rate_plus_acc"] == result["conv_rate"] + result["acc_rate"]
        assert result["avg_daily_trips_plus_one"] == result["avg_daily_trips"] + 1
        assert result["highest_achieved_rank"] == highest_rank
        assert result["is_highest_rank"] == (expected_ranks[-1] != "Locked")

        assert result["trips_until_next_rank_int"] == expected_trips_until_next_rank
        assert result["trips_until_next_rank_float"] == [
            float(value) for value in expected_trips_until_next_rank
        ]
        assert result["achieved_ranks_mask"] == expected_mask
        assert result["achieved_ranks"] == expected_ranks


def test_invalid_pandas_transformation_raises_type_error_on_apply():
    with tempfile.TemporaryDirectory() as data_dir:
        store = FeatureStore(
            config=RepoConfig(
                project="test_on_demand_python_transformation",
                registry=os.path.join(data_dir, "registry.db"),
                provider="local",
                entity_key_serialization_version=3,
                online_store=SqliteOnlineStoreConfig(
                    path=os.path.join(data_dir, "online.db")
                ),
            )
        )

        request_source = RequestSource(
            name="request_source",
            schema=[
                Field(name="driver_name", dtype=String),
            ],
        )

        @on_demand_feature_view(
            sources=[request_source],
            schema=[Field(name="driver_name_lower", dtype=String)],
            mode="pandas",
        )
        def pandas_view(inputs: pd.DataFrame) -> pd.DataFrame:
            return pd.DataFrame({"driver_name_lower": []})

        with pytest.raises(
            TypeError,
            match=re.escape(
                "Failed to infer type for feature 'driver_name_lower' with value '[]' since no items were returned by the UDF."
            ),
        ):
            store.apply([request_source, pandas_view])


def test_odfv_udf_does_not_receive_undeclared_source_columns():
    """#6158: a pandas ODFV's UDF should only see its own source columns, not
    features from another feature view that was requested too."""
    with tempfile.TemporaryDirectory() as data_dir:
        store = FeatureStore(
            config=RepoConfig(
                project="test_odfv_source_isolation",
                registry=os.path.join(data_dir, "registry.db"),
                provider="local",
                entity_key_serialization_version=3,
                online_store=SqliteOnlineStoreConfig(
                    path=os.path.join(data_dir, "online.db")
                ),
            )
        )

        end_date = datetime.now().replace(microsecond=0, second=0, minute=0)
        start_date = end_date - timedelta(days=15)
        driver_df = create_driver_hourly_stats_df([1001], start_date, end_date)
        driver_stats_path = os.path.join(data_dir, "driver_stats.parquet")
        driver_df.to_parquet(path=driver_stats_path, allow_truncated_timestamps=True)

        driver = Entity(name="driver", join_keys=["driver_id"])
        source = FileSource(
            name="driver_hourly_stats_source",
            path=driver_stats_path,
            timestamp_field="event_timestamp",
            created_timestamp_column="created",
        )

        # Two feature views from the same source. The ODFV declares only fv_declared.
        fv_declared = FeatureView(
            name="fv_declared",
            entities=[driver],
            ttl=timedelta(days=0),
            schema=[Field(name="conv_rate", dtype=Float32)],
            online=True,
            source=source,
        )
        fv_undeclared = FeatureView(
            name="fv_undeclared",
            entities=[driver],
            ttl=timedelta(days=0),
            schema=[Field(name="avg_daily_trips", dtype=Int64)],
            online=True,
            source=source,
        )

        @on_demand_feature_view(
            sources=[fv_declared],
            schema=[
                Field(name="saw_undeclared", dtype=Bool),
                Field(name="saw_declared", dtype=Bool),
                Field(name="saw_join_key", dtype=Bool),
            ],
            mode="pandas",
        )
        def guard_view(inputs: pd.DataFrame) -> pd.DataFrame:
            out = pd.DataFrame()
            n = range(len(inputs))
            out["saw_undeclared"] = ["avg_daily_trips" in inputs.columns for _ in n]
            out["saw_declared"] = ["conv_rate" in inputs.columns for _ in n]
            out["saw_join_key"] = ["driver_id" in inputs.columns for _ in n]
            return out

        store.apply([driver, source, fv_declared, fv_undeclared, guard_view])
        store.write_to_online_store(feature_view_name="fv_declared", df=driver_df)
        store.write_to_online_store(feature_view_name="fv_undeclared", df=driver_df)

        response = store.get_online_features(
            entity_rows=[{"driver_id": 1001}],
            features=[
                "fv_declared:conv_rate",
                "fv_undeclared:avg_daily_trips",
                "guard_view:saw_undeclared",
                "guard_view:saw_declared",
                "guard_view:saw_join_key",
            ],
        ).to_dict()

        # the other FV's column should be hidden; ours and the join key should not
        assert response["saw_undeclared"] == [False]
        assert response["saw_declared"] == [True]
        assert response["saw_join_key"] == [True]


def _two_fv_store(data_dir):
    store = FeatureStore(
        config=RepoConfig(
            project="test_odfv_source_isolation",
            registry=os.path.join(data_dir, "registry.db"),
            provider="local",
            entity_key_serialization_version=3,
            online_store=SqliteOnlineStoreConfig(
                path=os.path.join(data_dir, "online.db")
            ),
        )
    )
    end_date = datetime.now().replace(microsecond=0, second=0, minute=0)
    start_date = end_date - timedelta(days=15)
    driver_df = create_driver_hourly_stats_df([1001], start_date, end_date)
    path = os.path.join(data_dir, "driver_stats.parquet")
    driver_df.to_parquet(path=path, allow_truncated_timestamps=True)
    driver = Entity(name="driver", join_keys=["driver_id"])
    src = FileSource(
        name="driver_hourly_stats_source",
        path=path,
        timestamp_field="event_timestamp",
        created_timestamp_column="created",
    )
    fv1 = FeatureView(
        name="fv1",
        entities=[driver],
        ttl=timedelta(days=0),
        schema=[Field(name="conv_rate", dtype=Float32)],
        online=True,
        source=src,
    )
    fv2 = FeatureView(
        name="fv2",
        entities=[driver],
        ttl=timedelta(days=0),
        schema=[Field(name="avg_daily_trips", dtype=Int64)],
        online=True,
        source=src,
    )
    return store, driver, src, fv1, fv2, driver_df


def test_odfv_python_mode_does_not_receive_undeclared_source_columns():
    """Same as above but for python mode, where the UDF gets a dict (#6158)."""
    with tempfile.TemporaryDirectory() as data_dir:
        store, driver, src, fv1, fv2, driver_df = _two_fv_store(data_dir)

        @on_demand_feature_view(
            sources=[fv1],
            schema=[
                Field(name="saw_undeclared", dtype=Bool),
                Field(name="saw_declared", dtype=Bool),
            ],
            mode="python",
        )
        def guard_py(inputs: dict) -> dict:
            return {
                "saw_undeclared": ["avg_daily_trips" in inputs],
                "saw_declared": ["conv_rate" in inputs],
            }

        store.apply([driver, src, fv1, fv2, guard_py])
        store.write_to_online_store(feature_view_name="fv1", df=driver_df)
        store.write_to_online_store(feature_view_name="fv2", df=driver_df)

        response = store.get_online_features(
            entity_rows=[{"driver_id": 1001}],
            features=[
                "fv1:conv_rate",
                "fv2:avg_daily_trips",
                "guard_py:saw_undeclared",
                "guard_py:saw_declared",
            ],
        ).to_dict()

        assert response["saw_undeclared"] == [False]
        assert response["saw_declared"] == [True]


def test_odfv_does_not_see_other_requested_odfv_source_columns():
    """#6158: one ODFV's UDF should not see another ODFV's source column, even
    when that feature view is only in the response as the other ODFV's input."""
    with tempfile.TemporaryDirectory() as data_dir:
        store, driver, src, fv1, fv2, driver_df = _two_fv_store(data_dir)

        @on_demand_feature_view(
            sources=[fv1],
            schema=[Field(name="a_saw_fv2", dtype=Bool)],
            mode="pandas",
        )
        def odfv_a(inputs: pd.DataFrame) -> pd.DataFrame:
            out = pd.DataFrame()
            out["a_saw_fv2"] = [
                "avg_daily_trips" in inputs.columns for _ in range(len(inputs))
            ]
            return out

        @on_demand_feature_view(
            sources=[fv2],
            schema=[Field(name="b_out", dtype=Int64)],
            mode="pandas",
        )
        def odfv_b(inputs: pd.DataFrame) -> pd.DataFrame:
            out = pd.DataFrame()
            out["b_out"] = list(inputs["avg_daily_trips"])
            return out

        store.apply([driver, src, fv1, fv2, odfv_a, odfv_b])
        store.write_to_online_store(feature_view_name="fv1", df=driver_df)
        store.write_to_online_store(feature_view_name="fv2", df=driver_df)

        # ask for just the two ODFVs - fv2 is only here as odfv_b's input
        response = store.get_online_features(
            entity_rows=[{"driver_id": 1001}],
            features=["odfv_a:a_saw_fv2", "odfv_b:b_out"],
        ).to_dict()

        # odfv_a shouldn't see fv2's column; odfv_b still uses it fine
        assert response["a_saw_fv2"] == [False]
        assert response["b_out"] is not None


def test_odfv_udf_receives_aliased_declared_source_columns():
    """A declared source referenced under an alias (``with_name``) must still reach
    the UDF. The isolation filter keys columns by ``projection.name``, which is what
    the retrieval path emits regardless of the alias, so the aliased source's feature
    is retained (not dropped) while an unrelated feature view stays hidden (#6158)."""
    with tempfile.TemporaryDirectory() as data_dir:
        store, driver, src, fv1, fv2, driver_df = _two_fv_store(data_dir)

        # ODFV declares fv1 *under an alias*; fv2 is requested but undeclared.
        aliased_source = fv1.with_name("aliased_fv1")
        assert aliased_source.projection.name == "fv1"
        assert aliased_source.projection.name_to_use() == "aliased_fv1"

        @on_demand_feature_view(
            sources=[aliased_source],
            schema=[
                Field(name="saw_declared", dtype=Bool),
                Field(name="saw_undeclared", dtype=Bool),
            ],
            mode="pandas",
        )
        def aliased_guard(inputs: pd.DataFrame) -> pd.DataFrame:
            out = pd.DataFrame()
            n = range(len(inputs))
            out["saw_declared"] = ["conv_rate" in inputs.columns for _ in n]
            out["saw_undeclared"] = ["avg_daily_trips" in inputs.columns for _ in n]
            return out

        store.apply([driver, src, fv1, fv2, aliased_guard])
        store.write_to_online_store(feature_view_name="fv1", df=driver_df)
        store.write_to_online_store(feature_view_name="fv2", df=driver_df)

        response = store.get_online_features(
            entity_rows=[{"driver_id": 1001}],
            features=[
                "fv1:conv_rate",
                "fv2:avg_daily_trips",
                "aliased_guard:saw_declared",
                "aliased_guard:saw_undeclared",
            ],
        ).to_dict()

        # the aliased-but-declared source must survive; the unrelated FV stays hidden
        assert response["saw_declared"] == [True]
        assert response["saw_undeclared"] == [False]
