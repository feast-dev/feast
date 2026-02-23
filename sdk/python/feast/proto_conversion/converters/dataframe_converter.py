# Copyright 2024 The Feast Authors
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
DataFrameProtoConverter for Arrow/DataFrame to proto conversions.

This module consolidates the _convert_arrow_* functions from utils.py
into a single converter class.
"""

from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple, Union

import pandas as pd
import pyarrow

from feast.proto_conversion.converters.value_converter import (
    python_values_to_proto_values,
)
from feast.proto_conversion.errors import ArrowConversionError
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from feast.value_type import ValueType

if TYPE_CHECKING:
    from feast.base_feature_view import BaseFeatureView
    from feast.feature_view import FeatureView
    from feast.on_demand_feature_view import OnDemandFeatureView


# Type alias for the conversion result
ConversionResult = List[
    Tuple[EntityKeyProto, Dict[str, ValueProto], datetime, Optional[datetime]]
]


def _coerce_datetime(ts: Any) -> datetime:
    """Coerce a timestamp to datetime, handling pandas.Timestamp."""
    if isinstance(ts, pd.Timestamp):
        return ts.to_pydatetime()
    return ts


def _is_on_demand_feature_view(feature_view: Any) -> bool:
    """Check if a feature view is an OnDemandFeatureView."""
    return (
        getattr(feature_view, "source_request_sources", None) is not None
        or getattr(feature_view, "source_feature_view_projections", None) is not None
    )


class DataFrameProtoConverter:
    """
    Converter for Arrow tables/DataFrames to proto representation.

    Handles both FeatureView and OnDemandFeatureView conversions with
    straightforward conditional logic.

    Example:
        >>> converter = DataFrameProtoConverter()
        >>> results = converter.convert(table, feature_view, join_keys)
        >>> for entity_key, features, event_ts, created_ts in results:
        ...     # Process each row
        ...     pass
    """

    def convert(
        self,
        table: Union[pyarrow.Table, pyarrow.RecordBatch],
        feature_view: Union["FeatureView", "BaseFeatureView", "OnDemandFeatureView"],
        join_keys: Dict[str, ValueType],
    ) -> ConversionResult:
        """
        Convert an Arrow table to proto representation.

        Args:
            table: The Arrow table or record batch to convert.
            feature_view: The feature view defining the schema.
            join_keys: Dictionary mapping join key names to their value types.

        Returns:
            List of tuples containing (entity_key, features, event_ts, created_ts)
            for each row in the table.
        """
        try:
            batch = self._ensure_record_batch(table)
            is_odfv = _is_on_demand_feature_view(feature_view)

            # Get columns to convert
            columns = [
                (field.name, field.dtype.to_value_type())
                for field in feature_view.features
            ] + list(join_keys.items())

            # Convert columns to proto values
            proto_values = self._convert_columns(
                batch, columns, join_keys, feature_view, is_odfv
            )

            # Build entity keys
            entity_keys = self._build_entity_keys(batch, join_keys, proto_values)

            # Build feature dicts
            features = self._build_feature_dicts(
                batch, feature_view, proto_values, is_odfv
            )

            # Extract timestamps
            event_ts, created_ts = self._extract_timestamps(
                batch, feature_view, is_odfv
            )

            return list(zip(entity_keys, features, event_ts, created_ts))

        except ArrowConversionError:
            raise
        except Exception as e:
            raise ArrowConversionError(
                f"Failed to convert Arrow table: {e}",
                cause=e,
            ) from e

    def _ensure_record_batch(
        self, table: Union[pyarrow.Table, pyarrow.RecordBatch]
    ) -> pyarrow.RecordBatch:
        """Convert Table to RecordBatch if needed."""
        if isinstance(table, pyarrow.Table):
            batches = table.to_batches()
            if not batches:
                raise ArrowConversionError("Empty table provided")
            return batches[0]
        return table

    def _convert_columns(
        self,
        table: pyarrow.RecordBatch,
        columns: List[Tuple[str, ValueType]],
        join_keys: Dict[str, ValueType],
        feature_view: Any,
        is_odfv: bool,
    ) -> Dict[str, List[ValueProto]]:
        """Convert columns to proto values."""
        if not is_odfv:
            # Standard FeatureView: simple direct mapping
            return {
                column: python_values_to_proto_values(
                    table.column(column).to_numpy(zero_copy_only=False), value_type
                )
                for column, value_type in columns
            }

        # OnDemandFeatureView: handle missing columns
        from feast.types import from_feast_to_pyarrow_type

        proto_values: Dict[str, List[ValueProto]] = {}

        # Convert existing columns
        for column, value_type in columns:
            if column in table.column_names:
                proto_values[column] = python_values_to_proto_values(
                    table.column(column).to_numpy(zero_copy_only=False), value_type
                )

        # Handle missing join keys
        for join_key, value_type in join_keys.items():
            if join_key not in proto_values:
                if join_key in table.column_names:
                    proto_values[join_key] = python_values_to_proto_values(
                        table.column(join_key).to_numpy(zero_copy_only=False),
                        value_type,
                    )
                else:
                    proto_values[join_key] = python_values_to_proto_values(
                        [None] * table.num_rows, value_type
                    )

        # Handle missing feature columns (initialize as null)
        for feature in feature_view.features:
            if (
                feature.name in [c[0] for c in columns]
                and feature.name not in proto_values
            ):
                pa_type = from_feast_to_pyarrow_type(feature.dtype)
                null_array = pyarrow.array([None] * table.num_rows, type=pa_type)
                proto_values[feature.name] = python_values_to_proto_values(
                    null_array.to_numpy(zero_copy_only=False),
                    feature.dtype.to_value_type(),
                )

        return proto_values

    def _build_entity_keys(
        self,
        table: pyarrow.RecordBatch,
        join_keys: Dict[str, ValueType],
        proto_values: Dict[str, List[ValueProto]],
    ) -> List[EntityKeyProto]:
        """Build entity key protos for each row."""
        entity_keys = []
        for idx in range(table.num_rows):
            entity_values = [
                proto_values[k][idx] for k in join_keys if k in proto_values
            ]
            entity_keys.append(
                EntityKeyProto(join_keys=join_keys, entity_values=entity_values)
            )
        return entity_keys

    def _build_feature_dicts(
        self,
        table: pyarrow.RecordBatch,
        feature_view: Any,
        proto_values: Dict[str, List[ValueProto]],
        is_odfv: bool,
    ) -> List[Dict[str, ValueProto]]:
        """Build feature dictionaries for each row."""
        feature_dict = {
            feature.name: proto_values[feature.name]
            for feature in feature_view.features
            if feature.name in proto_values
        }

        # Handle write_to_online_store mode for ODFV
        if is_odfv and getattr(feature_view, "write_to_online_store", False):
            table_columns = [col.name for col in table.schema]
            for feature in feature_view.schema:
                if feature.name not in feature_dict and feature.name in table_columns:
                    if feature.name in proto_values:
                        feature_dict[feature.name] = proto_values[feature.name]

        if not feature_dict:
            return [{}] * table.num_rows

        return [
            dict(zip(feature_dict.keys(), values))
            for values in zip(*feature_dict.values())
        ]

    def _extract_timestamps(
        self,
        table: pyarrow.RecordBatch,
        feature_view: Any,
        is_odfv: bool,
    ) -> Tuple[List[datetime], List[Optional[datetime]]]:
        """Extract event and created timestamps."""
        if is_odfv:
            # OnDemandFeatureView: use current time
            now = datetime.now(tz=timezone.utc)
            event_ts = [now] * table.num_rows
            created_ts: List[Optional[datetime]] = [now] * table.num_rows
            return event_ts, created_ts

        # Standard FeatureView: read from batch_source
        event_ts = [
            _coerce_datetime(val)
            for val in pd.to_datetime(
                table.column(feature_view.batch_source.timestamp_field).to_numpy(
                    zero_copy_only=False
                )
            )
        ]

        if feature_view.batch_source.created_timestamp_column:
            created_ts = [
                _coerce_datetime(val)
                for val in pd.to_datetime(
                    table.column(
                        feature_view.batch_source.created_timestamp_column
                    ).to_numpy(zero_copy_only=False)
                )
            ]
        else:
            created_ts = [None] * table.num_rows

        return event_ts, created_ts


def convert_arrow_to_proto(
    table: Union[pyarrow.Table, pyarrow.RecordBatch],
    feature_view: Union["FeatureView", "BaseFeatureView", "OnDemandFeatureView"],
    join_keys: Dict[str, ValueType],
) -> ConversionResult:
    """
    Convert an Arrow table to proto representation.

    This is a convenience function for backward compatibility.
    """
    converter = DataFrameProtoConverter()
    return converter.convert(table, feature_view, join_keys)


# Backward compatibility alias
_convert_arrow_to_proto = convert_arrow_to_proto
