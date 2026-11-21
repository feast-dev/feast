from datetime import datetime, timedelta, timezone
from typing import Dict, List, Tuple
from unittest.mock import MagicMock

import ibis
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pytest

from feast import Entity, FeatureView, Field, FileSource
from feast.infra.offline_stores.ibis import (
    build_entity_df_for_date_range_ibis,
    get_historical_features_ibis,
    point_in_time_join,
    pull_all_from_table_or_query_ibis,
    pull_latest_from_table_or_query_ibis,
)
from feast.types import Float32, Int64
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


def _date_range_feature_view(name, entities, schema, field_mapping=None):
    return FeatureView(
        name=name,
        entities=entities,
        schema=schema,
        source=FileSource(
            name=name,
            path=f"{name}.parquet",
            timestamp_field="event_timestamp",
            field_mapping=field_mapping,
        ),
    )


def _build_entity_df_for_date_range(feature_views, tables, start_date, end_date):
    return build_entity_df_for_date_range_ibis(
        config=MagicMock(repo_path=None),
        feature_views=feature_views,
        start_date=start_date,
        end_date=end_date,
        data_source_reader=lambda source, repo_path: ibis.memtable(tables[source.name]),
    )


def test_build_entity_df_for_date_range_unions_keys_in_the_time_range():
    """Every join key with an event in the range gets one row at end_date, with NULL
    for the keys its feature view does not have."""
    driver = Entity(name="driver", join_keys=["driver_id"], value_type=ValueType.INT64)
    customer = Entity(
        name="customer", join_keys=["customer_id"], value_type=ValueType.INT64
    )
    driver_fv = _date_range_feature_view(
        "driver_stats",
        [driver],
        [Field(name="driver_id", dtype=Int64), Field(name="rating", dtype=Float32)],
    )
    customer_fv = _date_range_feature_view(
        "customer_stats",
        [customer],
        [Field(name="customer_id", dtype=Int64)],
        field_mapping={"cust": "customer_id"},
    )
    tables = {
        "driver_stats": pd.DataFrame(
            {
                "driver_id": [1, 1, 2, 3],
                "event_timestamp": pd.to_datetime(
                    ["2024-01-02", "2024-01-03", "2024-01-04", "2023-12-01"], utc=True
                ),
                "rating": [1.0, 2.0, 3.0, 4.0],
            }
        ),
        "customer_stats": pd.DataFrame(
            {
                "cust": [10, 20],
                "event_timestamp": pd.to_datetime(
                    ["2024-01-02", "2024-02-01"], utc=True
                ),
            }
        ),
    }
    end_date = datetime(2024, 1, 10, tzinfo=timezone.utc)

    entity_df = _build_entity_df_for_date_range(
        [driver_fv, customer_fv],
        tables,
        start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
        end_date=end_date,
    )

    assert list(entity_df.columns) == ["driver_id", "customer_id", "event_timestamp"]
    rows = {
        (
            None if pd.isna(row.driver_id) else int(row.driver_id),
            None if pd.isna(row.customer_id) else int(row.customer_id),
        )
        for row in entity_df.itertuples()
    }
    assert rows == {(1, None), (2, None), (None, 10)}
    assert len(entity_df) == 3
    assert str(entity_df["event_timestamp"].dtype) == "datetime64[ns, UTC]"
    assert (entity_df["event_timestamp"] == pd.Timestamp(end_date)).all()


def test_build_entity_df_for_date_range_uses_projected_join_keys():
    """Join keys renamed by the projection take the new name; the others keep theirs."""
    driver = Entity(name="driver", join_keys=["driver_id"], value_type=ValueType.INT64)
    customer = Entity(
        name="customer", join_keys=["customer_id"], value_type=ValueType.INT64
    )
    trips_fv = _date_range_feature_view(
        "trips",
        [driver, customer],
        [Field(name="driver_id", dtype=Int64), Field(name="customer_id", dtype=Int64)],
    ).with_join_key_map({"driver_id": "origin_id"})
    tables = {
        "trips": pd.DataFrame(
            {
                "driver_id": [1, 1, 2],
                "customer_id": [10, 10, 20],
                "event_timestamp": pd.to_datetime(
                    ["2024-01-02", "2024-01-03", "2024-01-03"], utc=True
                ),
            }
        )
    }

    entity_df = _build_entity_df_for_date_range(
        [trips_fv],
        tables,
        start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
        end_date=datetime(2024, 1, 10, tzinfo=timezone.utc),
    )

    assert list(entity_df.columns) == ["origin_id", "customer_id", "event_timestamp"]
    assert sorted(zip(entity_df["origin_id"], entity_df["customer_id"])) == [
        (1, 10),
        (2, 20),
    ]


def test_build_entity_df_for_date_range_unions_join_keys_of_different_types():
    driver = Entity(name="driver", join_keys=["driver_id"], value_type=ValueType.INT64)
    driver_fv = _date_range_feature_view(
        "driver_stats", [driver], [Field(name="driver_id", dtype=Int64)]
    )
    trips_fv = _date_range_feature_view(
        "driver_trips", [driver], [Field(name="driver_id", dtype=Int64)]
    )
    timestamps = pd.to_datetime(["2024-01-02", "2024-01-03"], utc=True)
    tables = {
        "driver_stats": pd.DataFrame(
            {
                "driver_id": pd.Series([1, 2], dtype="int64"),
                "event_timestamp": timestamps,
            }
        ),
        "driver_trips": pd.DataFrame(
            {
                "driver_id": pd.Series([2, 3], dtype="int32"),
                "event_timestamp": timestamps,
            }
        ),
    }

    entity_df = _build_entity_df_for_date_range(
        [driver_fv, trips_fv],
        tables,
        start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
        end_date=datetime(2024, 1, 10, tzinfo=timezone.utc),
    )

    assert sorted(entity_df["driver_id"]) == [1, 2, 3]


def test_build_entity_df_for_date_range_widens_join_key_types_regardless_of_order():
    driver = Entity(name="driver", join_keys=["driver_id"], value_type=ValueType.INT64)
    driver_fv = _date_range_feature_view(
        "driver_stats", [driver], [Field(name="driver_id", dtype=Int64)]
    )
    trips_fv = _date_range_feature_view(
        "driver_trips", [driver], [Field(name="driver_id", dtype=Int64)]
    )
    timestamps = pd.to_datetime(["2024-01-02", "2024-01-03"], utc=True)
    tables = {
        "driver_stats": pd.DataFrame(
            {
                "driver_id": pd.Series([1, 2], dtype="int32"),
                "event_timestamp": timestamps,
            }
        ),
        "driver_trips": pd.DataFrame(
            {
                "driver_id": pd.Series([2, 3_000_000_000], dtype="int64"),
                "event_timestamp": timestamps,
            }
        ),
    }

    entity_df = _build_entity_df_for_date_range(
        [driver_fv, trips_fv],
        tables,
        start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
        end_date=datetime(2024, 1, 10, tzinfo=timezone.utc),
    )

    assert sorted(entity_df["driver_id"]) == [1, 2, 3_000_000_000]


def test_build_entity_df_for_date_range_without_entities_is_one_row_at_end_date():
    """Feature views without entities join on the timestamp alone and add no rows."""
    global_fv = _date_range_feature_view(
        "global_stats", [], [Field(name="total", dtype=Int64)]
    )

    entity_df = _build_entity_df_for_date_range(
        [global_fv],
        tables={},
        start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
        end_date=datetime(2024, 1, 10),
    )

    assert entity_df.to_dict("list") == {
        "event_timestamp": [pd.Timestamp("2024-01-10", tz="UTC")]
    }


def test_build_entity_df_for_date_range_without_events_in_range_is_empty():
    driver = Entity(name="driver", join_keys=["driver_id"], value_type=ValueType.INT64)
    driver_fv = _date_range_feature_view(
        "driver_stats", [driver], [Field(name="driver_id", dtype=Int64)]
    )
    tables = {
        "driver_stats": pd.DataFrame(
            {
                "driver_id": [1],
                "event_timestamp": pd.to_datetime(["2023-01-01"], utc=True),
            }
        )
    }

    entity_df = _build_entity_df_for_date_range(
        [driver_fv],
        tables,
        start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
        end_date=datetime(2024, 1, 10, tzinfo=timezone.utc),
    )

    assert entity_df.empty
    assert list(entity_df.columns) == ["driver_id", "event_timestamp"]
    assert str(entity_df["event_timestamp"].dtype) == "datetime64[ns, UTC]"


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
