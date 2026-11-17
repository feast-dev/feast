import pyarrow as pa

from feast import Entity, FeatureView, Field, FileSource
from feast.on_demand_feature_view import on_demand_feature_view
from feast.types import Float32, Int64
from feast.utils import _convert_arrow_to_proto
from feast.value_type import ValueType

JOIN_KEYS = {"driver_id": ValueType.INT64}


def _chunked_table() -> pa.Table:
    """A table whose rows are spread over three chunks, like a query result
    fetched in batches and concatenated."""
    chunks = [
        pa.table(
            {
                "driver_id": pa.array(ids, pa.int64()),
                "conv_rate": pa.array([i / 10 for i in ids], pa.float32()),
                "event_timestamp": pa.array([1_700_000_000_000_000] * len(ids))
                .cast(pa.int64())
                .cast(pa.timestamp("us", tz="UTC")),
            }
        )
        for ids in ([1, 2], [3, 4, 5], [6])
    ]
    table = pa.concat_tables(chunks)
    assert table.num_rows == 6
    assert len(table.to_batches()) == 3
    return table


def test_convert_arrow_to_proto_keeps_rows_from_every_chunk():
    driver = Entity(
        name="driver_id", join_keys=["driver_id"], value_type=ValueType.INT64
    )
    feature_view = FeatureView(
        name="driver_stats",
        entities=[driver],
        schema=[
            Field(name="driver_id", dtype=Int64),
            Field(name="conv_rate", dtype=Float32),
        ],
        source=FileSource(path="unused.parquet", timestamp_field="event_timestamp"),
    )

    rows = _convert_arrow_to_proto(_chunked_table(), feature_view, JOIN_KEYS)

    assert [r[0].entity_values[0].int64_val for r in rows] == [1, 2, 3, 4, 5, 6]
    assert [round(r[1]["conv_rate"].float_val, 1) for r in rows] == [
        0.1,
        0.2,
        0.3,
        0.4,
        0.5,
        0.6,
    ]


def test_convert_arrow_to_proto_keeps_rows_from_every_chunk_for_odfv():
    driver = Entity(
        name="driver_id", join_keys=["driver_id"], value_type=ValueType.INT64
    )
    source = FeatureView(
        name="driver_stats",
        entities=[driver],
        schema=[Field(name="conv_rate", dtype=Float32)],
        source=FileSource(path="unused.parquet", timestamp_field="event_timestamp"),
    )

    @on_demand_feature_view(
        sources=[source],
        schema=[Field(name="conv_rate", dtype=Float32)],
        mode="pandas",
        write_to_online_store=True,
    )
    def passthrough(inputs):
        return inputs[["conv_rate"]]

    rows = _convert_arrow_to_proto(_chunked_table(), passthrough, JOIN_KEYS)

    assert [r[0].entity_values[0].int64_val for r in rows] == [1, 2, 3, 4, 5, 6]
