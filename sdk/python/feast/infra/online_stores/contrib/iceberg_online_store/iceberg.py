# Copyright 2025 The Feast Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Iceberg Online Store implementation for Feast.

This module provides a "near-line" serving option using Apache Iceberg tables.
It trades some latency (50-100ms) for operational simplicity and cost efficiency
compared to traditional in-memory stores like Redis.

Design:
- Uses PyIceberg for native Iceberg table operations
- Metadata pruning for efficient partition filtering
- Entity hash partitioning for single-partition lookups
- Direct Parquet reading for low latency
"""

import hashlib
import logging
from datetime import datetime
from typing import (
    Any,
    Callable,
    Dict,
    List,
    Literal,
    Optional,
    Sequence,
    Tuple,
    Union,
)

import pyarrow as pa
from pydantic import StrictInt, StrictStr
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.table import Table
from pyiceberg.types import NestedField, StringType, TimestampType

from feast import Entity
from feast.batch_feature_view import BatchFeatureView
from feast.feature_view import FeatureView
from feast.infra.key_encoding_utils import serialize_entity_key
from feast.infra.online_stores.online_store import OnlineStore
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from feast.repo_config import FeastConfigBaseModel, RepoConfig
from feast.stream_feature_view import StreamFeatureView
from feast.type_map import feast_value_type_to_pa
from feast.utils import to_naive_utc
from feast.value_type import ValueType

logger = logging.getLogger(__name__)


class IcebergOnlineStoreConfig(FeastConfigBaseModel):
    """
    Configuration for Iceberg Online Store.

    Attributes:
        type: Online store type selector
        catalog_type: Type of Iceberg catalog (rest, glue, hive, sql)
        catalog_name: Name of the Iceberg catalog
        uri: Catalog URI (for REST catalog)
        warehouse: Warehouse path (S3/GCS/local path)
        namespace: Iceberg namespace (default: "feast")
        partition_strategy: Partitioning strategy for entity lookups
        partition_count: Number of partitions for hash-based partitioning (default: 256)
        read_timeout_ms: Timeout for online reads in milliseconds (default: 100)
        storage_options: Additional storage configuration (e.g., S3 credentials)
    """

    type: Literal[
        "iceberg",
        "feast.infra.online_stores.contrib.iceberg_online_store.iceberg.IcebergOnlineStore",
    ] = "iceberg"
    """Online store type selector"""

    catalog_type: StrictStr = "rest"
    """Type of Iceberg catalog: rest, glue, hive, sql"""

    catalog_name: StrictStr = "feast_catalog"
    """Name of the Iceberg catalog"""

    uri: Optional[StrictStr] = None
    """Catalog URI (required for REST catalog)"""

    warehouse: StrictStr = "warehouse"
    """Warehouse path (S3/GCS/local path)"""

    namespace: StrictStr = "feast_online"
    """Iceberg namespace for online tables"""

    partition_strategy: Literal["entity_hash", "timestamp", "hybrid"] = "entity_hash"
    """Partitioning strategy for entity lookups"""

    partition_count: StrictInt = 256
    """Number of partitions for hash-based partitioning"""

    read_timeout_ms: StrictInt = 100
    """Timeout for online reads in milliseconds"""

    storage_options: Dict[str, str] = {}
    """Additional storage configuration (e.g., S3 credentials)"""


class IcebergOnlineStore(OnlineStore):
    """
    Iceberg-based online store for Feast.

    This online store uses Apache Iceberg tables to serve features with:
    - Metadata-based partition pruning for efficient lookups
    - Entity hash partitioning for single-partition reads
    - Direct Parquet reading for low latency (50-100ms)
    - Operational simplicity (no separate infrastructure)

    Trade-offs vs Redis:
    - Latency: 50-100ms vs <10ms (acceptable for near-line serving)
    - Cost: Object storage vs in-memory (significantly cheaper)
    - Complexity: Reuse Iceberg catalog vs manage separate cluster
    """

    def online_write_batch(
        self,
        config: RepoConfig,
        table: FeatureView,
        data: List[
            Tuple[EntityKeyProto, Dict[str, ValueProto], datetime, Optional[datetime]]
        ],
        progress: Optional[Callable[[int], Any]],
    ) -> None:
        """
        Write a batch of feature rows to the Iceberg online store.

        Args:
            config: Feast repo configuration
            table: Feature view to write to
            data: List of (entity_key, feature_values, event_ts, created_ts) tuples
            progress: Optional progress callback
        """
        if not data:
            return

        online_config = config.online_store
        assert isinstance(online_config, IcebergOnlineStoreConfig)

        # Load catalog and table
        catalog = self._load_catalog(online_config)
        iceberg_table = self._get_or_create_online_table(
            catalog, online_config, config.project, table
        )

        # Convert Feast data to Arrow table
        arrow_table = self._convert_feast_to_arrow(data, table, online_config, config)

        # Append to Iceberg table
        iceberg_table.append(arrow_table)

        if progress:
            progress(len(data))

    def online_read(
        self,
        config: RepoConfig,
        table: FeatureView,
        entity_keys: List[EntityKeyProto],
        requested_features: Optional[List[str]] = None,
    ) -> List[Tuple[Optional[datetime], Optional[Dict[str, ValueProto]]]]:
        """
        Read feature values for the given entity keys from the Iceberg online store.

        Uses metadata pruning to filter partitions before reading data files.

        Args:
            config: Feast repo configuration
            table: Feature view to read from
            entity_keys: List of entity keys to read
            requested_features: Optional list of features to read (defaults to all)

        Returns:
            List of (event_timestamp, feature_dict) tuples, one per entity key
        """
        if not entity_keys:
            return []

        online_config = config.online_store
        assert isinstance(online_config, IcebergOnlineStoreConfig)

        # Load catalog and table
        catalog = self._load_catalog(online_config)
        table_identifier = self._get_table_identifier(
            online_config, config.project, table
        )

        try:
            iceberg_table = catalog.load_table(table_identifier)
        except Exception:
            # Table doesn't exist yet
            return [(None, None) for _ in entity_keys]

        # Build entity hash filter for partition pruning
        entity_hashes = [
            self._hash_entity_key(
                ek,
                online_config.partition_count,
                config.entity_key_serialization_version,
            )
            for ek in entity_keys
        ]

        # Scan with partition filter
        scan = iceberg_table.scan(
            row_filter=f"entity_hash IN ({','.join(map(str, entity_hashes))})"
        )

        # Project only requested columns
        columns = ["entity_key", "event_ts", "created_ts"]
        if requested_features:
            columns.extend(requested_features)
        else:
            columns.extend([f.name for f in table.features])

        arrow_table = scan.to_arrow()

        # Convert to result format
        return self._convert_arrow_to_feast(
            arrow_table,
            entity_keys,
            requested_features or [f.name for f in table.features],
            config,
        )

    def update(
        self,
        config: RepoConfig,
        tables_to_delete: Sequence[FeatureView],
        tables_to_keep: Sequence[
            Union[BatchFeatureView, StreamFeatureView, FeatureView]
        ],
        entities_to_delete: Sequence[Entity],
        entities_to_keep: Sequence[Entity],
        partial: bool,
    ) -> None:
        """
        Update online store tables (create/delete).

        Args:
            config: Feast repo configuration
            tables_to_delete: Feature views to delete
            tables_to_keep: Feature views to keep/create
            entities_to_delete: Entities to delete (not used)
            entities_to_keep: Entities to keep (not used)
            partial: Whether to do partial update
        """
        online_config = config.online_store
        assert isinstance(online_config, IcebergOnlineStoreConfig)

        catalog = self._load_catalog(online_config)

        # Delete tables
        for table in tables_to_delete:
            table_identifier = self._get_table_identifier(
                online_config, config.project, table
            )
            try:
                catalog.drop_table(table_identifier)
                logger.info(f"Deleted online table: {table_identifier}")
            except Exception as e:
                logger.warning(f"Failed to delete table {table_identifier}: {e}")

        # Create tables
        for table in tables_to_keep:
            if isinstance(table, FeatureView):
                self._get_or_create_online_table(
                    catalog, online_config, config.project, table
                )

    def teardown(
        self,
        config: RepoConfig,
        tables: Sequence[FeatureView],
        entities: Sequence[Entity],
    ) -> None:
        """
        Tear down online store tables.

        Args:
            config: Feast repo configuration
            tables: Feature views to delete
            entities: Entities to delete (not used)
        """
        online_config = config.online_store
        assert isinstance(online_config, IcebergOnlineStoreConfig)

        catalog = self._load_catalog(online_config)

        for table in tables:
            table_identifier = self._get_table_identifier(
                online_config, config.project, table
            )
            try:
                catalog.drop_table(table_identifier)
                logger.info(f"Deleted online table: {table_identifier}")
            except Exception as e:
                logger.warning(f"Failed to delete table {table_identifier}: {e}")

    # Helper methods

    def _load_catalog(self, config: IcebergOnlineStoreConfig):
        """Load Iceberg catalog from configuration."""
        catalog_config = {
            "type": config.catalog_type,
            "warehouse": config.warehouse,
            **config.storage_options,
        }

        if config.uri:
            catalog_config["uri"] = config.uri

        return load_catalog(config.catalog_name, **catalog_config)

    def _get_table_identifier(
        self, config: IcebergOnlineStoreConfig, project: str, table: FeatureView
    ) -> str:
        """Get fully qualified Iceberg table identifier."""
        return f"{config.namespace}.{project}_{table.name}_online"

    def _get_or_create_online_table(
        self,
        catalog,
        config: IcebergOnlineStoreConfig,
        project: str,
        table: FeatureView,
    ) -> Table:
        """Get or create an Iceberg table for online features."""
        table_identifier = self._get_table_identifier(config, project, table)

        try:
            return catalog.load_table(table_identifier)
        except Exception:
            # Create table with schema
            schema = self._build_online_schema(table, config)
            partition_spec = self._build_partition_spec(config)

            # Create namespace if it doesn't exist
            try:
                catalog.create_namespace(config.namespace)
            except Exception:
                pass  # Namespace already exists

            iceberg_table = catalog.create_table(
                identifier=table_identifier,
                schema=schema,
                partition_spec=partition_spec,
            )
            logger.info(f"Created online table: {table_identifier}")
            return iceberg_table

    def _build_online_schema(
        self, table: FeatureView, config: IcebergOnlineStoreConfig
    ) -> Schema:
        """Build Iceberg schema for online table."""
        from pyiceberg.types import (
            BinaryType,
            BooleanType,
            DoubleType,
            FloatType,
            IntegerType,
            LongType,
            StringType,
            TimestampType,
        )

        def _pa_to_iceberg(pa_type: pa.DataType):
            if pa_type == pa.bool_():
                return BooleanType()
            if pa_type == pa.int32():
                return IntegerType()
            if pa_type == pa.int64():
                return LongType()
            if pa_type == pa.float32():
                return FloatType()
            if pa_type == pa.float64():
                return DoubleType()
            if pa_type == pa.string():
                return StringType()
            if pa_type == pa.binary():
                return BinaryType()
            if isinstance(pa_type, pa.TimestampType):
                return TimestampType()
            raise TypeError(f"Unsupported Arrow type for Iceberg: {pa_type}")

        fields = [
            NestedField(
                field_id=1, name="entity_key", type=StringType(), required=False
            ),
            NestedField(
                field_id=2, name="entity_hash", type=IntegerType(), required=False
            ),
            NestedField(
                field_id=3, name="event_ts", type=TimestampType(), required=False
            ),
            NestedField(
                field_id=4, name="created_ts", type=TimestampType(), required=False
            ),
        ]

        # Add feature columns
        field_id = 5
        for feature in table.features:
            pa_type = feast_value_type_to_pa(feature.dtype.to_value_type())
            fields.append(
                NestedField(
                    field_id=field_id,
                    name=feature.name,
                    type=_pa_to_iceberg(pa_type),
                    required=False,
                )
            )
            field_id += 1

        return Schema(*fields)

    def _build_partition_spec(self, config: IcebergOnlineStoreConfig):
        """Build partition specification based on strategy."""
        from pyiceberg.partitioning import PartitionField, PartitionSpec
        from pyiceberg.transforms import BucketTransform, DayTransform, HourTransform

        if config.partition_strategy == "entity_hash":
            # Partition by entity_hash modulo partition_count
            return PartitionSpec(
                PartitionField(
                    source_id=2,  # entity_hash field
                    field_id=1000,
                    transform=BucketTransform(config.partition_count),
                    name="entity_hash_bucket",
                )
            )
        elif config.partition_strategy == "timestamp":
            # Partition by hour of event_ts
            return PartitionSpec(
                PartitionField(
                    source_id=3,  # event_ts field
                    field_id=1000,
                    transform=HourTransform(),
                    name="event_hour",
                )
            )
        elif config.partition_strategy == "hybrid":
            # Partition by both entity_hash and day
            return PartitionSpec(
                PartitionField(
                    source_id=2,
                    field_id=1000,
                    transform=BucketTransform(config.partition_count),
                    name="entity_hash_bucket",
                ),
                PartitionField(
                    source_id=3,
                    field_id=1001,
                    transform=DayTransform(),
                    name="event_day",
                ),
            )
        else:
            raise ValueError(f"Unknown partition strategy: {config.partition_strategy}")

    def _hash_entity_key(
        self,
        entity_key: EntityKeyProto,
        partition_count: int,
        serialization_version: int,
    ) -> int:
        """Hash entity key to partition ID."""
        entity_key_bin = serialize_entity_key(
            entity_key, entity_key_serialization_version=serialization_version
        )
        hash_val = int(hashlib.md5(entity_key_bin).hexdigest(), 16)
        return hash_val % partition_count

    def _convert_feast_to_arrow(
        self,
        data: List[
            Tuple[EntityKeyProto, Dict[str, ValueProto], datetime, Optional[datetime]]
        ],
        table: FeatureView,
        config: IcebergOnlineStoreConfig,
        repo_config: RepoConfig,
    ) -> pa.Table:
        """Convert Feast data format to Arrow table."""
        # Build column arrays
        entity_keys = []
        entity_hashes = []
        event_timestamps = []
        created_timestamps = []
        feature_data = {f.name: [] for f in table.features}

        for entity_key, values, event_ts, created_ts in data:
            # Serialize entity key
            entity_key_bin = serialize_entity_key(
                entity_key,
                entity_key_serialization_version=repo_config.entity_key_serialization_version,
            )
            entity_keys.append(entity_key_bin.hex())

            # Compute entity hash
            entity_hash = self._hash_entity_key(
                entity_key,
                config.partition_count,
                repo_config.entity_key_serialization_version,
            )
            entity_hashes.append(entity_hash)

            # Convert timestamps to naive UTC
            event_timestamps.append(to_naive_utc(event_ts))
            created_timestamps.append(to_naive_utc(created_ts) if created_ts else None)

            # Extract feature values
            for feature in table.features:
                if feature.name in values:
                    value_proto = values[feature.name]
                    # Convert ValueProto to Python value
                    py_value = self._value_proto_to_python(value_proto, feature.dtype)
                    feature_data[feature.name].append(py_value)
                else:
                    feature_data[feature.name].append(None)

        # Build Arrow arrays
        arrays = [
            pa.array(entity_keys, type=pa.string()),
            pa.array(entity_hashes, type=pa.int32()),
            pa.array(event_timestamps, type=pa.timestamp("us")),
            pa.array(created_timestamps, type=pa.timestamp("us")),
        ]

        schema_fields = [
            pa.field("entity_key", pa.string()),
            pa.field("entity_hash", pa.int32()),
            pa.field("event_ts", pa.timestamp("us")),
            pa.field("created_ts", pa.timestamp("us")),
        ]

        # Add feature arrays
        for feature in table.features:
            pa_type = feast_value_type_to_pa(feature.dtype.to_value_type())
            arrays.append(pa.array(feature_data[feature.name], type=pa_type))
            schema_fields.append(pa.field(feature.name, pa_type))

        return pa.Table.from_arrays(arrays, schema=pa.schema(schema_fields))

    def _convert_arrow_to_feast(
        self,
        arrow_table: pa.Table,
        entity_keys: List[EntityKeyProto],
        requested_features: List[str],
        config: RepoConfig,
    ) -> List[Tuple[Optional[datetime], Optional[Dict[str, ValueProto]]]]:
        """Convert Arrow table to Feast format, matching entity_keys order."""
        # Serialize entity keys for matching
        entity_key_bins = {
            serialize_entity_key(
                ek,
                entity_key_serialization_version=config.entity_key_serialization_version,
            ).hex(): ek
            for ek in entity_keys
        }

        # Group by entity_key and get latest record per entity
        results: Dict[
            str, Tuple[Optional[datetime], Optional[Dict[str, ValueProto]]]
        ] = {key: (None, None) for key in entity_key_bins.keys()}

        if len(arrow_table) == 0:
            return [(None, None) for _ in entity_keys]

        # Process rows
        for i in range(len(arrow_table)):
            entity_key_hex = arrow_table["entity_key"][i].as_py()
            event_ts = arrow_table["event_ts"][i].as_py()

            # Check if this is the latest record for this entity
            if entity_key_hex in results:
                current_ts, _ = results[entity_key_hex]
                if current_ts is None or event_ts > current_ts:
                    # Extract feature values
                    feature_dict = {}
                    for feature_name in requested_features:
                        value = arrow_table[feature_name][i].as_py()
                        if value is not None:
                            # Convert to ValueProto
                            value_proto = self._python_to_value_proto(value)
                            feature_dict[feature_name] = value_proto

                    results[entity_key_hex] = (
                        event_ts,
                        feature_dict if feature_dict else None,
                    )

        # Return in original entity_keys order
        return [results[ek_hex] for ek_hex in entity_key_bins.keys()]

    def _value_proto_to_python(self, value_proto: ValueProto, dtype) -> Any:
        """Convert Feast ValueProto to Python value."""
        from feast.type_map import feast_value_type_to_python_type

        return feast_value_type_to_python_type(value_proto)

    def _python_to_value_proto(self, value: Any) -> ValueProto:
        """Convert Python value to Feast ValueProto."""
        from feast.type_map import python_values_to_proto_values

        return python_values_to_proto_values([value], ValueType.UNKNOWN)[0]
