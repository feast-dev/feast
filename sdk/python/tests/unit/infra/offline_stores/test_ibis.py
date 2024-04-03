from datetime import datetime, timedelta
from typing import Dict, List, Tuple

import ibis
import pyarrow as pa

from feast.infra.offline_stores.contrib.ibis_offline_store.ibis import (
    point_in_time_join,
)


def pa_datetime(year, month, day):
    return pa.scalar(datetime(year, month, day), type=pa.timestamp("s", tz="UTC"))


def customer_table():
    return pa.Table.from_arrays(
        arrays=[
            pa.array([1, 1, 2]),
            pa.array(
                [
                    pa_datetime(2024, 1, 1),
                    pa_datetime(2024, 1, 2),
                    pa_datetime(2024, 1, 1),
                ]
            ),
        ],
        names=["customer_id", "event_timestamp"],
    )


def features_table_1():
    return pa.Table.from_arrays(
        arrays=[
            pa.array([1, 1, 1, 2]),
            pa.array(
                [
                    pa_datetime(2023, 12, 31),
                    pa_datetime(2024, 1, 2),
                    pa_datetime(2024, 1, 3),
                    pa_datetime(2023, 1, 3),
                ]
            ),
            pa.array([11, 22, 33, 22]),
        ],
        names=["customer_id", "event_timestamp", "feature1"],
    )


def point_in_time_join_brute(
    entity_table: pa.Table,
    feature_tables: List[Tuple[pa.Table, str, Dict[str, str], List[str], timedelta]],
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
            join_key_map,
            feature_refs,
            ttl,
        ) in feature_tables:
            if i == 0:
                ret_fields.extend(
                    [
                        feature_table.schema.field(f)
                        for f in feature_table.schema.names
                        if f not in join_key_map.values() and f != timestamp_key
                    ]
                )

            def check_equality(ft_dict, batch_dict, x, y):
                return all(
                    [ft_dict[k][x] == batch_dict[v][y] for k, v in join_key_map.items()]
                )

            ft_dict = feature_table.to_pydict()
            found_matches = [
                (j, ft_dict[timestamp_key][j])
                for j in range(entity_table.num_rows)
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


def test_point_in_time_join():
    expected = point_in_time_join_brute(
        customer_table(),
        feature_tables=[
            (
                features_table_1(),
                "event_timestamp",
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
                {"customer_id": "customer_id"},
                ["feature1"],
                timedelta(days=10),
            )
        ],
    ).to_pyarrow()

    assert actual.equals(expected)
