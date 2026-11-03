from datetime import datetime, timedelta, timezone
from typing import Dict, List, Tuple
from unittest.mock import MagicMock

import ibis
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pytest

from feast.infra.offline_stores.ibis import (
    point_in_time_join,
    pull_all_from_table_or_query_ibis,
    pull_latest_from_table_or_query_ibis,
)


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
