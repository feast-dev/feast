import sqlite3
import tempfile
from datetime import datetime
from typing import Dict, Optional

import pandas as pd

from feast.data_source import DataSource
from feast.infra.offline_stores.contrib.sqlite_offline_store.sqlite_source import (
    SQLiteSource,
)


def create_sqlite_test_data_source(
    table_name: str = "test_table",
    created_timestamp_column: str = "created_ts",
    field_mapping: Optional[Dict[str, str]] = None,
) -> DataSource:
    """Create a SQLite data source for testing."""

    # Create a temporary SQLite database file
    temp_db = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
    temp_db.close()

    # Create test data
    data = {
        "entity_id": [1, 2, 3, 4, 5],
        "feature_1": [10, 20, 30, 40, 50],
        "feature_2": [100.0, 200.0, 300.0, 400.0, 500.0],
        "event_timestamp": [
            datetime(2023, 1, 1, 10, 0, 0),
            datetime(2023, 1, 1, 11, 0, 0),
            datetime(2023, 1, 1, 12, 0, 0),
            datetime(2023, 1, 1, 13, 0, 0),
            datetime(2023, 1, 1, 14, 0, 0),
        ],
        created_timestamp_column: [
            datetime(2023, 1, 1, 9, 0, 0),
            datetime(2023, 1, 1, 10, 0, 0),
            datetime(2023, 1, 1, 11, 0, 0),
            datetime(2023, 1, 1, 12, 0, 0),
            datetime(2023, 1, 1, 13, 0, 0),
        ],
    }

    df = pd.DataFrame(data)

    # Write to SQLite database
    with sqlite3.connect(temp_db.name) as conn:
        df.to_sql(table_name, conn, if_exists="replace", index=False)

    return SQLiteSource(
        database=temp_db.name,
        table=table_name,
        timestamp_field="event_timestamp",
        created_timestamp_column=created_timestamp_column,
        field_mapping=field_mapping or {},
    )
