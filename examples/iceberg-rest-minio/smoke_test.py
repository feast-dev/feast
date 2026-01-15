from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from typing import Dict, List

import pyarrow as pa
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import (
    DoubleType,
    LongType,
    NestedField,
    StringType,
    TimestampType,
)

from feast import Entity, FeatureView, Field, FileSource
from feast.infra.offline_stores.contrib.iceberg_offline_store.iceberg import (
    IcebergOfflineStore,
    IcebergOfflineStoreConfig,
)
from feast.infra.offline_stores.contrib.iceberg_offline_store.iceberg_source import (
    IcebergSource,
)
from feast.infra.online_stores.contrib.iceberg_online_store.iceberg import (
    IcebergOnlineStore,
    IcebergOnlineStoreConfig,
)
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from feast.value_type import ValueType
from feast.types import Float64


@dataclass(frozen=True)
class RestMinioEnv:
    rest_uri: str
    warehouse: str
    catalog_name: str
    namespace_offline: str
    namespace_online: str
    s3_endpoint: str
    s3_access_key: str
    s3_secret_key: str
    s3_region: str


def _env() -> RestMinioEnv:
    # Keep these defaults aligned with docker-compose.yml
    return RestMinioEnv(
        rest_uri="http://localhost:8181",
        warehouse="s3://warehouse/",
        catalog_name="feast_catalog",
        namespace_offline="feast",
        namespace_online="feast_online",
        s3_endpoint="http://localhost:9000",
        s3_access_key="minio",
        s3_secret_key="minio123",
        s3_region="us-east-1",
    )


def _storage_options(env: RestMinioEnv) -> Dict[str, str]:
    # pyiceberg uses dotted keys in its properties mapping.
    return {
        "s3.endpoint": env.s3_endpoint,
        "s3.access-key-id": env.s3_access_key,
        "s3.secret-access-key": env.s3_secret_key,
        "s3.region": env.s3_region,
        # S3-compatible endpoints typically require path-style access.
        "s3.path-style-access": "true",
    }


def _build_offline_table_schema() -> Schema:
    # Minimal offline feature table schema for schema resolution + DuckDB reads.
    return Schema(
        NestedField(field_id=1, name="driver_id", field_type=LongType(), required=False),
        NestedField(
            field_id=2, name="event_timestamp", field_type=TimestampType(), required=False
        ),
        NestedField(
            field_id=3, name="created_ts", field_type=TimestampType(), required=False
        ),
        NestedField(field_id=4, name="conv_rate", field_type=DoubleType(), required=False),
        NestedField(field_id=5, name="acc_rate", field_type=DoubleType(), required=False),
        NestedField(
            field_id=6, name="avg_daily_trips", field_type=LongType(), required=False
        ),
    )


def _build_offline_arrow_table(now: datetime) -> pa.Table:
    # Use microsecond timestamps because Iceberg expects microseconds.
    timestamps = [now - timedelta(hours=2), now - timedelta(hours=1), now]
    created_ts = [t + timedelta(minutes=1) for t in timestamps]

    return pa.Table.from_pydict(
        {
            "driver_id": [1001, 1001, 1002],
            "event_timestamp": pa.array(timestamps, type=pa.timestamp("us")),
            "created_ts": pa.array(created_ts, type=pa.timestamp("us")),
            "conv_rate": [0.1, 0.2, 0.3],
            "acc_rate": [0.9, 0.8, 0.7],
            "avg_daily_trips": [10, 11, 12],
        }
    )


def _ensure_namespace(catalog, namespace: str) -> None:
    try:
        catalog.create_namespace(namespace)
    except Exception:
        pass


def _ensure_table(catalog, identifier: str, schema: Schema) -> None:
    try:
        catalog.load_table(identifier)
        return
    except Exception:
        pass

    catalog.create_table(identifier=identifier, schema=schema)


def _append_arrow(catalog, identifier: str, arrow_table: pa.Table) -> None:
    table = catalog.load_table(identifier)
    table.append(arrow_table)


def _offline_smoke(env: RestMinioEnv) -> None:
    catalog = load_catalog(
        env.catalog_name,
        type="rest",
        uri=env.rest_uri,
        warehouse=env.warehouse,
        **_storage_options(env),
    )

    _ensure_namespace(catalog, env.namespace_offline)

    offline_table_id = f"{env.namespace_offline}.driver_stats"
    _ensure_table(catalog, offline_table_id, _build_offline_table_schema())

    now = datetime.now(timezone.utc).replace(tzinfo=None)
    _append_arrow(catalog, offline_table_id, _build_offline_arrow_table(now))

    offline_store_config = IcebergOfflineStoreConfig(
        type="iceberg",
        catalog_type="rest",
        catalog_name=env.catalog_name,
        uri=env.rest_uri,
        warehouse=env.warehouse,
        namespace=env.namespace_offline,
        storage_options=_storage_options(env),
    )

    repo_config = SimpleNamespace(offline_store=offline_store_config)

    source = IcebergSource(
        name="driver_stats",
        table_identifier=offline_table_id,
        timestamp_field="event_timestamp",
        created_timestamp_column="created_ts",
    )

    source.validate(repo_config)

    job = IcebergOfflineStore.pull_latest_from_table_or_query(
        config=repo_config,
        data_source=source,
        join_key_columns=["driver_id"],
        feature_name_columns=["conv_rate"],
        timestamp_field="event_timestamp",
        created_timestamp_column="created_ts",
        start_date=None,
        end_date=None,
    )

    df = job.to_df()
    assert len(df) >= 1
    assert "driver_id" in df.columns
    assert "event_timestamp" in df.columns
    assert "created_ts" in df.columns
    assert "conv_rate" in df.columns


def _online_smoke(env: RestMinioEnv) -> None:
    online_store = IcebergOnlineStore()

    online_store_config = IcebergOnlineStoreConfig(
        type="iceberg",
        catalog_type="rest",
        catalog_name=env.catalog_name,
        uri=env.rest_uri,
        warehouse=env.warehouse,
        namespace=env.namespace_online,
        partition_strategy="entity_hash",
        partition_count=256,
        read_timeout_ms=1000,
        storage_options=_storage_options(env),
    )

    repo_config = SimpleNamespace(
        online_store=online_store_config,
        project="iceberg_smoke",
        entity_key_serialization_version=3,
    )

    driver = Entity(name="driver", join_keys=["driver_id"], value_type=ValueType.INT64)
    file_source = FileSource(name="dummy", path="/tmp/unused.parquet")

    fv = FeatureView(
        name="driver_stats",
        entities=[driver],
        schema=[
            Field(name="conv_rate", dtype=Float64),
            Field(name="acc_rate", dtype=Float64),
        ],
        source=file_source,
    )

    now = datetime.now(timezone.utc).replace(tzinfo=None)

    ek = EntityKeyProto(join_keys=["driver_id"], entity_values=[ValueProto(int64_val=1001)])

    online_store.online_write_batch(
        config=repo_config,
        table=fv,
        data=[
            (
                ek,
                {
                    "conv_rate": ValueProto(double_val=0.123),
                    "acc_rate": ValueProto(double_val=0.456),
                },
                now,
                now,
            )
        ],
        progress=None,
    )

    results = online_store.online_read(
        config=repo_config,
        table=fv,
        entity_keys=[ek],
        requested_features=["conv_rate"],
    )

    assert len(results) == 1
    ts, features = results[0]
    assert ts is not None
    assert features is not None
    assert "conv_rate" in features


def main() -> None:
    env = _env()
    _offline_smoke(env)
    _online_smoke(env)
    print("✅ Iceberg REST+MinIO smoke test passed")


if __name__ == "__main__":
    main()
