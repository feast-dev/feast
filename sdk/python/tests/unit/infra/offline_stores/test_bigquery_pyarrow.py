import random
import tempfile
from datetime import datetime

import pandas as pd
import pyarrow as pa


def test_write_to_bigquery():
    now = datetime.utcnow()
    ts = pd.Timestamp(now).round("ms")

    # This dataframe has columns in the wrong order.
    df_to_write = pd.DataFrame.from_dict(
        {
            "avg_daily_trips": [random.randint(0, 10), random.randint(0, 10)],
            "created": [ts, ts],
            "conv_rate": [random.random(), random.random()],
            "event_timestamp": [ts, ts],
            "acc_rate": [random.random(), random.random()],
            "driver_id": [1001, 1001],
        },
    )

    # From line 1527 of feature_store.py
    table = pa.Table.from_pandas(df_to_write)

    with tempfile.TemporaryFile() as parquet_temp_file:
        pa.parquet.write_table(
            table=table, where=parquet_temp_file, coerce_timestamps="ms"
        )

        parquet_temp_file.seek(0)

        # Need to check the timestamp type of this parquet file.
        final_table = pa.parquet.read_table(parquet_temp_file)

        # Google bigquery api only accept "ms"
        assert final_table.schema.field("created").type == pa.timestamp(unit="ms")
