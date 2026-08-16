from datetime import datetime

import pyarrow as pa
from pyiceberg.catalog import load_catalog

from feast.infra.data_sources.contrib.iceberg_catalog import IcebergSource


def test_local_iceberg_materialization_is_idempotent(tmp_path):
    catalog_uri = f"sqlite:///{tmp_path / 'catalog.db'}"
    warehouse_uri = (tmp_path / "warehouse").as_uri()
    catalog = load_catalog(
        "test",
        type="sql",
        uri=catalog_uri,
        warehouse=warehouse_uri,
    )
    catalog.create_namespace("features")
    source = IcebergSource(
        catalog_type="sql",
        catalog_name="test",
        catalog_properties={"uri": catalog_uri},
        warehouse=warehouse_uri,
        namespace="features",
        table="driver_stats",
        timestamp_field="event_timestamp",
    )
    first = pa.table(
        {
            "driver_id": [1, 2],
            "event_timestamp": pa.array(
                [datetime(2026, 8, 16, 10), datetime(2026, 8, 16, 11)],
                type=pa.timestamp("us"),
            ),
            "value": [1.0, 2.0],
        }
    )
    join_cols = ["driver_id", "event_timestamp"]

    source.write_materialized_table(first, join_cols=join_cols)
    source.write_materialized_table(first, join_cols=join_cols)

    after_repeat = catalog.load_table("features.driver_stats").scan().to_arrow()
    assert after_repeat.num_rows == 2

    updated = first.set_column(2, "value", pa.array([10.0, 2.0]))
    source.write_materialized_table(updated, join_cols=join_cols)

    iceberg_table = catalog.load_table("features.driver_stats")
    result = iceberg_table.scan().to_arrow().sort_by("driver_id")
    assert result.num_rows == 2
    assert result.column_names == first.column_names
    assert result["driver_id"].to_pylist() == [1, 2]
    assert result["value"].to_pylist() == [10.0, 2.0]
