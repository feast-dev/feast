from datetime import datetime, timedelta, timezone
from typing import Dict, List, Tuple
from unittest.mock import MagicMock

import ibis
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pytest

from feast import Entity, FeatureView, Field, FileSource, RequestSource
from feast.errors import (
    FeatureNameCollisionError,
    RequestDataOverriddenByFeatureError,
)
from feast.infra.offline_stores.ibis import (
    get_historical_features_ibis,
    point_in_time_join,
    pull_all_from_table_or_query_ibis,
    pull_latest_from_table_or_query_ibis,
)
from feast.on_demand_feature_view import OnDemandFeatureView, PandasTransformation
from feast.types import Float64, Int64
from feast.value_type import ValueType


def pa_datetime(year, month, day):
    return pa.scalar(datetime(year, month, day), type=pa.timestamp("s", tz="UTC"))


def customer_table():
    return pa.Table.from_arrays(
        arrays=[
            pa.array([1, 1, 2, 3]),
            pa.array(
                [
                    pa_datetime(2024, 1, 1),
                    pa_datetime(2024, 1, 2),
                    pa_datetime(2024, 1, 1),
                    pa_datetime(2024, 1, 3),
                ]
            ),
        ],
        names=["customer_id", "event_timestamp"],
    )


def features_table_1():
    return pa.Table.from_arrays(
        arrays=[
            pa.array([1, 1, 1, 2, 3, 3]),
            pa.array(
                [
                    pa_datetime(2023, 12, 31),
                    pa_datetime(2024, 1, 2),
                    pa_datetime(2024, 1, 3),
                    pa_datetime(2023, 1, 3),
                    pa_datetime(2024, 1, 1),
                    pa_datetime(2024, 1, 1),
                ]
            ),
            pa.array(
                [
                    pa_datetime(2023, 12, 31),
                    pa_datetime(2024, 1, 2),
                    pa_datetime(2024, 1, 3),
                    pa_datetime(2023, 1, 3),
                    pa_datetime(2024, 1, 3),
                    pa_datetime(2024, 1, 2),
                ]
            ),
            pa.array([11, 22, 33, 22, 10, 20]),
        ],
        names=["customer_id", "event_timestamp", "created", "feature1"],
    )


def point_in_time_join_brute(
    entity_table: pa.Table,
    feature_tables: List[
        Tuple[pa.Table, str, str, Dict[str, str], List[str], timedelta]
    ],
    event_timestamp_col="event_timestamp",
):
    ret_fields = [entity_table.schema.field(n) for n in entity_table.schema.names]

    from operator import itemgetter

    ret = entity_table.to_pydict()
    batch_dict = entity_table.to_pydict()

    for i, row_timestmap in enumerate(batch_dict[event_timestamp_col]):
        for (
            feature_table,
            timestamp_key,
            created_timestamp_key,
            join_key_map,
            feature_refs,
            ttl,
        ) in feature_tables:
            if i == 0:
                ret_fields.extend(
                    [
                        feature_table.schema.field(f)
                        for f in feature_table.schema.names
                        if f not in join_key_map.values()
                        and f != timestamp_key
                        and f != created_timestamp_key
                    ]
                )

            def check_equality(ft_dict, batch_dict, x, y):
                return all(
                    [ft_dict[k][x] == batch_dict[v][y] for k, v in join_key_map.items()]
                )

            ft_dict = feature_table.to_pydict()

            found_matches = [
                (j, (ft_dict[timestamp_key][j], ft_dict[created_timestamp_key][j]))
                # (j, ft_dict[timestamp_key][j])
                for j in range(feature_table.num_rows)
                if check_equality(ft_dict, batch_dict, j, i)
                and ft_dict[timestamp_key][j] <= row_timestmap
                and ft_dict[timestamp_key][j] >= row_timestmap - ttl
            ]

            index_found = (
                max(found_matches, key=itemgetter(1))[0] if found_matches else None
            )

            for col in ft_dict.keys():
                if col not in feature_refs:
                    continue

                if col not in ret:
                    ret[col] = []

                if index_found is not None:
                    ret[col].append(ft_dict[col][index_found])
                else:
                    ret[col].append(None)

    return pa.Table.from_pydict(ret, schema=pa.schema(ret_fields))


def tables_equal_ignore_order(actual: pa.Table, expected: pa.Table):
    sort_keys = [(name, "ascending") for name in actual.column_names]
    sort_indices = pc.sort_indices(actual, sort_keys)
    actual = pc.take(actual, sort_indices)

    sort_keys = [(name, "ascending") for name in expected.column_names]
    sort_indices = pc.sort_indices(expected, sort_keys)
    expected = pc.take(expected, sort_indices)

    return actual.equals(expected)


def test_point_in_time_join():
    expected = point_in_time_join_brute(
        customer_table(),
        feature_tables=[
            (
                features_table_1(),
                "event_timestamp",
                "created",
                {"customer_id": "customer_id"},
                ["feature1"],
                timedelta(days=10),
            )
        ],
    )

    actual = point_in_time_join(
        ibis.memtable(customer_table()),
        feature_tables=[
            (
                ibis.memtable(features_table_1()),
                "event_timestamp",
                "created",
                {"customer_id": "customer_id"},
                ["feature1"],
                timedelta(days=10),
            )
        ],
    ).to_pyarrow()

    assert tables_equal_ignore_order(actual, expected)


def test_point_in_time_join_keeps_entity_rows_apart():
    """Entity rows whose key values concatenate to the same string, or that have
    a null key for another feature view, still get their own feature values."""
    entity_table = pa.table(
        {
            "driver_id": ["1", "11", "7"],
            "customer_id": ["11", "1", None],
            "event_timestamp": [pa_datetime(2024, 1, 2)] * 3,
        }
    )
    drivers = pa.table(
        {
            "driver_id": ["1", "11", "7"],
            "event_timestamp": [pa_datetime(2024, 1, 1)] * 3,
            "created": [pa_datetime(2024, 1, 1)] * 3,
            "driver_rating": [1.0, 11.0, 7.0],
        }
    )
    customers = pa.table(
        {
            "customer_id": ["11", "1"],
            "event_timestamp": [pa_datetime(2024, 1, 1)] * 2,
            "created": [pa_datetime(2024, 1, 1)] * 2,
            "customer_score": [110.0, 10.0],
        }
    )

    actual = point_in_time_join(
        ibis.memtable(entity_table),
        feature_tables=[
            (
                ibis.memtable(drivers),
                "event_timestamp",
                "created",
                {"driver_id": "driver_id"},
                ["driver_rating"],
                timedelta(days=10),
            ),
            (
                ibis.memtable(customers),
                "event_timestamp",
                "created",
                {"customer_id": "customer_id"},
                ["customer_score"],
                timedelta(days=10),
            ),
        ],
    ).to_pyarrow()

    rows = {
        row["driver_id"]: (row["driver_rating"], row["customer_score"])
        for row in actual.to_pylist()
    }
    assert rows == {"1": (1.0, 110.0), "11": (11.0, 10.0), "7": (7.0, None)}


def test_point_in_time_join_keeps_features_whose_name_ends_in_y():
    """A feature named like "pos_y" is returned, not dropped with the "_y"
    copies of the entity columns that the join renames."""
    entity_table = pa.table(
        {
            "driver_id": [1, 2],
            "event_timestamp": [pa_datetime(2024, 1, 3)] * 2,
        }
    )
    positions = pa.table(
        {
            "driver_id": [1, 1, 2],
            "event_timestamp": [
                pa_datetime(2024, 1, 1),
                pa_datetime(2024, 1, 2),
                pa_datetime(2024, 1, 1),
            ],
            "created": [
                pa_datetime(2024, 1, 1),
                pa_datetime(2024, 1, 2),
                pa_datetime(2024, 1, 1),
            ],
            "pos_x": [1.0, 2.0, 3.0],
            "pos_y": [10.0, 20.0, 30.0],
        }
    )

    actual = point_in_time_join(
        ibis.memtable(entity_table),
        feature_tables=[
            (
                ibis.memtable(positions),
                "event_timestamp",
                "created",
                {"driver_id": "driver_id"},
                ["pos_x", "pos_y"],
                timedelta(days=10),
            )
        ],
    ).to_pyarrow()

    rows = {
        row["driver_id"]: (row["pos_x"], row["pos_y"]) for row in actual.to_pylist()
    }
    assert rows == {1: (2.0, 20.0), 2: (3.0, 30.0)}


def test_point_in_time_join_with_entity_column_clashing_with_a_feature_name():
    """An entity dataframe column that shares its name with a requested feature
    does not crash the join with a duplicate column; the feature value wins and
    shadows the entity column (issue #8853 family, DuckDB/ibis path)."""
    entity_table = pa.table(
        {
            "driver_id": [1, 2],
            "event_timestamp": [pa_datetime(2024, 1, 3)] * 2,
            "trips": [5, 6],
        }
    )
    positions = pa.table(
        {
            "driver_id": [1, 1, 2],
            "event_timestamp": [
                pa_datetime(2024, 1, 1),
                pa_datetime(2024, 1, 2),
                pa_datetime(2024, 1, 1),
            ],
            "created": [
                pa_datetime(2024, 1, 1),
                pa_datetime(2024, 1, 2),
                pa_datetime(2024, 1, 1),
            ],
            "trips": [10.0, 20.0, 30.0],
        }
    )

    actual = point_in_time_join(
        ibis.memtable(entity_table),
        feature_tables=[
            (
                ibis.memtable(positions),
                "event_timestamp",
                "created",
                {"driver_id": "driver_id"},
                ["trips"],
                timedelta(days=10),
            )
        ],
    ).to_pyarrow()

    rows = {row["driver_id"]: row["trips"] for row in actual.to_pylist()}
    assert rows == {1: 20.0, 2: 30.0}


def test_point_in_time_join_full_feature_names_no_collision_with_entity_columns():
    """With full feature names the generated refs are prefixed with the feature
    view name, so an entity column carrying the bare feature name cannot clash
    and both columns come back."""
    entity_table = pa.table(
        {
            "driver_id": [1, 2],
            "event_timestamp": [pa_datetime(2024, 1, 3)] * 2,
            "trips": [5, 6],
        }
    )
    # read_fv renames the feature columns to "<fv>__<feature>" when
    # full_feature_names is set; simulate that here.
    positions = pa.table(
        {
            "driver_id": [1, 1, 2],
            "event_timestamp": [
                pa_datetime(2024, 1, 1),
                pa_datetime(2024, 1, 2),
                pa_datetime(2024, 1, 1),
            ],
            "created": [
                pa_datetime(2024, 1, 1),
                pa_datetime(2024, 1, 2),
                pa_datetime(2024, 1, 1),
            ],
            "positions__trips": [10.0, 20.0, 30.0],
        }
    )

    actual = point_in_time_join(
        ibis.memtable(entity_table),
        feature_tables=[
            (
                ibis.memtable(positions),
                "event_timestamp",
                "created",
                {"driver_id": "driver_id"},
                ["positions__trips"],
                timedelta(days=10),
            )
        ],
    ).to_pyarrow()

    rows = {
        row["driver_id"]: (row["trips"], row["positions__trips"])
        for row in actual.to_pylist()
    }
    assert rows == {1: (5, 20.0), 2: (6, 30.0)}


def test_point_in_time_join_shadowed_entity_column_does_not_duplicate_later():
    """A feature ref that shadows an entity column must not make a later join
    select the shadowed name twice (once as an entity column and once as the
    earlier feature carried forward), which Ibis rejects with a duplicate
    column error."""
    entity_table = pa.table(
        {
            "driver_id": [1, 2],
            "event_timestamp": [pa_datetime(2024, 1, 3)] * 2,
            "trips": [5, 6],
        }
    )
    positions = pa.table(
        {
            "driver_id": [1, 1, 2],
            "event_timestamp": [
                pa_datetime(2024, 1, 1),
                pa_datetime(2024, 1, 2),
                pa_datetime(2024, 1, 1),
            ],
            "created": [
                pa_datetime(2024, 1, 1),
                pa_datetime(2024, 1, 2),
                pa_datetime(2024, 1, 1),
            ],
            "trips": [10.0, 20.0, 30.0],
        }
    )
    rates = pa.table(
        {
            "driver_id": [1, 2],
            "event_timestamp": [pa_datetime(2024, 1, 1)] * 2,
            "created": [pa_datetime(2024, 1, 1)] * 2,
            "conv_rate": [0.5, 0.75],
        }
    )

    actual = point_in_time_join(
        ibis.memtable(entity_table),
        feature_tables=[
            (
                ibis.memtable(positions),
                "event_timestamp",
                "created",
                {"driver_id": "driver_id"},
                ["trips"],
                timedelta(days=10),
            ),
            (
                ibis.memtable(rates),
                "event_timestamp",
                "created",
                {"driver_id": "driver_id"},
                ["conv_rate"],
                timedelta(days=10),
            ),
        ],
    ).to_pyarrow()

    rows = {
        row["driver_id"]: (row["trips"], row["conv_rate"]) for row in actual.to_pylist()
    }
    # the feature value shadows the entity column on the first join and stays
    # available when the second join contributes a different feature
    assert rows == {1: (20.0, 0.5), 2: (30.0, 0.75)}


def test_point_in_time_join_keeps_first_contribution_of_a_shared_feature_name():
    """When two feature tables contribute the same bare feature name
    (full_feature_names=False, e.g. an on-demand view's source dependency and
    the view that owns it), the first contribution wins instead of both copies
    being dropped, so the requested name stays in the result."""
    entity_table = pa.table(
        {
            "customer_id": [1, 2],
            "event_timestamp": [pa_datetime(2024, 1, 3)] * 2,
        }
    )
    drivers = pa.table(
        {
            "customer_id": [1, 1, 2],
            "event_timestamp": [
                pa_datetime(2024, 1, 1),
                pa_datetime(2024, 1, 2),
                pa_datetime(2024, 1, 1),
            ],
            "created": [
                pa_datetime(2024, 1, 1),
                pa_datetime(2024, 1, 2),
                pa_datetime(2024, 1, 1),
            ],
            "score": [0.5, 0.6, 0.7],
        }
    )
    customers = pa.table(
        {
            "customer_id": [1, 2],
            "event_timestamp": [pa_datetime(2024, 1, 1)] * 2,
            "created": [pa_datetime(2024, 1, 1)] * 2,
            "score": [100.0, 200.0],
        }
    )

    actual = point_in_time_join(
        ibis.memtable(entity_table),
        feature_tables=[
            (
                ibis.memtable(drivers),
                "event_timestamp",
                "created",
                {"customer_id": "customer_id"},
                ["score"],
                timedelta(days=10),
            ),
            (
                ibis.memtable(customers),
                "event_timestamp",
                "created",
                {"customer_id": "customer_id"},
                ["score"],
                timedelta(days=10),
            ),
        ],
    ).to_pyarrow()

    rows = {row["customer_id"]: row["score"] for row in actual.to_pylist()}
    assert rows == {1: 0.6, 2: 0.7}


def test_point_in_time_join_accumulates_features_from_multiple_tables():
    """Feature columns accumulated from earlier feature tables survive the
    left joins of later ones."""
    entity_table = pa.table(
        {
            "driver_id": [1, 2],
            "event_timestamp": [pa_datetime(2024, 1, 3)] * 2,
        }
    )
    positions = pa.table(
        {
            "driver_id": [1, 1, 2],
            "event_timestamp": [
                pa_datetime(2024, 1, 1),
                pa_datetime(2024, 1, 2),
                pa_datetime(2024, 1, 1),
            ],
            "created": [
                pa_datetime(2024, 1, 1),
                pa_datetime(2024, 1, 2),
                pa_datetime(2024, 1, 1),
            ],
            "trips": [10.0, 20.0, 30.0],
        }
    )
    rates = pa.table(
        {
            "driver_id": [1, 2],
            "event_timestamp": [
                pa_datetime(2024, 1, 1),
                pa_datetime(2024, 1, 1),
            ],
            "created": [
                pa_datetime(2024, 1, 1),
                pa_datetime(2024, 1, 1),
            ],
            "conv_rate": [0.5, 0.75],
        }
    )

    actual = point_in_time_join(
        ibis.memtable(entity_table),
        feature_tables=[
            (
                ibis.memtable(positions),
                "event_timestamp",
                "created",
                {"driver_id": "driver_id"},
                ["trips"],
                timedelta(days=10),
            ),
            (
                ibis.memtable(rates),
                "event_timestamp",
                "created",
                {"driver_id": "driver_id"},
                ["conv_rate"],
                timedelta(days=10),
            ),
        ],
    ).to_pyarrow()

    rows = {
        row["driver_id"]: (row["trips"], row["conv_rate"]) for row in actual.to_pylist()
    }
    assert rows == {1: (20.0, 0.5), 2: (30.0, 0.75)}


def test_point_in_time_join_allows_columns_named_like_the_y_copies():
    """Feature-table columns named like the "_y" copies the join makes of
    clashing entity columns ("driver_id_y", "event_timestamp_y") do not clash
    with them, whether they are requested or not."""
    entity_table = pa.table(
        {
            "driver_id": [1, 2],
            "event_timestamp": [pa_datetime(2024, 1, 3)] * 2,
        }
    )
    features = pa.table(
        {
            "driver_id": [1, 2],
            "event_timestamp": [pa_datetime(2024, 1, 1)] * 2,
            "created": [pa_datetime(2024, 1, 1)] * 2,
            "driver_id_y": [10.0, 20.0],
            "event_timestamp_y": [pa_datetime(2023, 1, 1)] * 2,
        }
    )

    actual = point_in_time_join(
        ibis.memtable(entity_table),
        feature_tables=[
            (
                ibis.memtable(features),
                "event_timestamp",
                "created",
                {"driver_id": "driver_id"},
                ["driver_id_y"],
                timedelta(days=10),
            )
        ],
    ).to_pyarrow()

    rows = {row["driver_id"]: row["driver_id_y"] for row in actual.to_pylist()}
    assert rows == {1: 10.0, 2: 20.0}


@pytest.mark.parametrize(
    "dtype",
    ["datetime64[ns, UTC]", "datetime64[us, UTC]", "datetime64[ns]", "datetime64[us]"],
)
@pytest.mark.parametrize(
    "pull", [pull_latest_from_table_or_query_ibis, pull_all_from_table_or_query_ibis]
)
def test_pull_filters_parquet_timestamps_of_any_precision(tmp_path, dtype, pull):
    """Materialize pulls read what pandas writes: nanosecond and zone-aware included."""
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    path = tmp_path / "source.parquet"
    pd.DataFrame(
        {
            "driver_id": [1, 2, 3, 4],
            "event_timestamp": [start + timedelta(hours=h) for h in range(4)],
            "trips": [10, 20, 30, 40],
        }
    ).astype(
        {"event_timestamp": dtype if "UTC" in dtype else "datetime64[ns, UTC]"}
    ).pipe(
        lambda df: (
            df
            if "UTC" in dtype
            else df.assign(
                event_timestamp=df.event_timestamp.dt.tz_localize(None).astype(dtype)
            )
        )
    ).to_parquet(path)

    table = pull(
        config=MagicMock(),
        data_source=MagicMock(),
        join_key_columns=["driver_id"],
        feature_name_columns=["trips"],
        timestamp_field="event_timestamp",
        created_timestamp_column=None,
        start_date=start + timedelta(hours=1),
        end_date=start + timedelta(hours=2),
        data_source_reader=lambda *_: ibis.read_parquet(path),
        data_source_writer=MagicMock(),
    ).to_arrow()

    assert sorted(table["driver_id"].to_pylist()) == [2, 3]


def test_get_historical_features_keeps_join_keys_left_out_of_join_key_map():
    """A join_key_map that renames only some join keys still joins on the others."""
    user = Entity(name="user", join_keys=["user_id"], value_type=ValueType.INT64)
    merchant = Entity(
        name="merchant", join_keys=["merchant_id"], value_type=ValueType.INT64
    )
    user_merchant = FeatureView(
        name="user_merchant",
        entities=[user, merchant],
        schema=[
            Field(name="user_id", dtype=Int64),
            Field(name="merchant_id", dtype=Int64),
            Field(name="spend", dtype=Int64),
        ],
        source=FileSource(path="unused.parquet", timestamp_field="event_timestamp"),
    ).with_join_key_map({"user_id": "sender_id"})
    features = pa.table(
        {
            "user_id": [1, 1],
            "merchant_id": [100, 200],
            "event_timestamp": [pa_datetime(2024, 1, 1), pa_datetime(2024, 1, 2)],
            "spend": [10, 99],
        }
    )
    entity_df = pd.DataFrame(
        {
            "sender_id": [1, 1],
            "merchant_id": [100, 200],
            "event_timestamp": [datetime(2024, 1, 3, tzinfo=timezone.utc)] * 2,
        }
    )

    actual = get_historical_features_ibis(
        config=MagicMock(),
        feature_views=[user_merchant],
        feature_refs=["user_merchant:spend"],
        entity_df=entity_df,
        registry=MagicMock(),
        project="test",
        data_source_reader=lambda *_: ibis.memtable(features),
        data_source_writer=MagicMock(),
    ).to_df()

    spend = dict(zip(actual["merchant_id"], actual["spend"]))
    assert spend == {100: 10, 200: 99}


def test_get_historical_features_rejects_feature_overriding_request_data():
    """A fetched feature that shares a required request-data name would feed the
    on-demand transform the fetched value instead of the entity dataframe value;
    retrieval must fail instead of silently returning a wrong result."""
    driver = Entity(name="driver", join_keys=["driver_id"], value_type=ValueType.INT64)
    driver_stats = FeatureView(
        name="driver_stats",
        entities=[driver],
        schema=[
            Field(name="driver_id", dtype=Int64),
            Field(name="val_to_add", dtype=Int64),
        ],
        source=FileSource(path="unused.parquet", timestamp_field="event_timestamp"),
    )
    request_source = RequestSource(
        name="vals_to_add", schema=[Field(name="val_to_add", dtype=Int64)]
    )
    odfv = OnDemandFeatureView(
        name="val_to_add_plus_100",
        sources=[driver_stats, request_source],
        schema=[Field(name="val_to_add_plus_100", dtype=Int64)],
        feature_transformation=PandasTransformation(
            udf=lambda df: pd.DataFrame(
                {"val_to_add_plus_100": df["val_to_add"] + 100}
            ),
            udf_string="",
        ),
    )
    feature_data = pa.table(
        {
            "driver_id": [1],
            "event_timestamp": [pa_datetime(2024, 1, 1)],
            "val_to_add": [10],
        }
    )
    entity_df = pd.DataFrame(
        {
            "driver_id": [1],
            "event_timestamp": [datetime(2024, 1, 3, tzinfo=timezone.utc)],
            "val_to_add": [5],
        }
    )
    registry = MagicMock()
    registry.list_on_demand_feature_views.return_value = [odfv]

    with pytest.raises(RequestDataOverriddenByFeatureError) as error:
        get_historical_features_ibis(
            config=MagicMock(),
            feature_views=[driver_stats],
            feature_refs=[
                "driver_stats:val_to_add",
                "val_to_add_plus_100:val_to_add_plus_100",
            ],
            entity_df=entity_df,
            registry=registry,
            project="test",
            data_source_reader=lambda *_: ibis.memtable(feature_data),
            data_source_writer=MagicMock(),
        )

    assert "val_to_add" in str(error.value)


def test_get_historical_features_rejects_on_demand_source_name_collision():
    """Another view contributing the bare name an on-demand view reads as its
    source would make the transform compute from the wrong view's column;
    retrieval must reject the implicit collision instead of picking one."""
    driver = Entity(name="driver", join_keys=["driver_id"], value_type=ValueType.INT64)
    drivers = FeatureView(
        name="drivers",
        entities=[driver],
        schema=[
            Field(name="driver_id", dtype=Int64),
            Field(name="score", dtype=Float64),
        ],
        source=FileSource(path="drivers.parquet", timestamp_field="event_timestamp"),
    )
    customers = FeatureView(
        name="customers",
        entities=[driver],
        schema=[
            Field(name="driver_id", dtype=Int64),
            Field(name="score", dtype=Float64),
        ],
        source=FileSource(path="customers.parquet", timestamp_field="event_timestamp"),
    )
    customer_rating = OnDemandFeatureView(
        name="customer_rating",
        sources=[customers[["score"]]],
        schema=[Field(name="adjusted_score", dtype=Float64)],
        feature_transformation=PandasTransformation(
            udf=lambda df: pd.DataFrame({"adjusted_score": df["score"] + 1}),
            udf_string="",
        ),
    )
    feature_data = pa.table(
        {
            "driver_id": [1],
            "event_timestamp": [pa_datetime(2024, 1, 1)],
            "score": [10.0],
        }
    )
    entity_df = pd.DataFrame(
        {
            "driver_id": [1],
            "event_timestamp": [datetime(2024, 1, 3, tzinfo=timezone.utc)],
        }
    )
    registry = MagicMock()
    registry.list_on_demand_feature_views.return_value = [customer_rating]

    with pytest.raises(FeatureNameCollisionError) as error:
        get_historical_features_ibis(
            config=MagicMock(),
            feature_views=[drivers, customers],
            # historical retrieval adds the on-demand view's source dependency
            # ("customers:score") to the refs it hands to the offline store
            feature_refs=[
                "drivers:score",
                "customers:score",
                "customer_rating:adjusted_score",
            ],
            entity_df=entity_df,
            registry=registry,
            project="test",
            data_source_reader=lambda *_: ibis.memtable(feature_data),
            data_source_writer=MagicMock(),
        )

    assert "score" in str(error.value)
