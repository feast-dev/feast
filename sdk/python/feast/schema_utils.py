"""
Schema matching utilities for automatic transformation detection.

This module provides utilities to automatically determine whether transformations
should be applied based on whether incoming data matches input schemas (raw data)
or output schemas (pre-transformed data).
"""

import logging
from typing import Any, Dict, List, Optional, Set, Union

import pandas as pd
import pyarrow as pa

from feast.feature_view import FeatureView
from feast.on_demand_feature_view import OnDemandFeatureView

logger = logging.getLogger(__name__)


def get_input_schema_columns(
    feature_view: Union[FeatureView, OnDemandFeatureView],
) -> Set[str]:
    """
    Extract expected input column names from a feature view.

    For FeatureViews with transformations, this returns the source schema columns.
    For OnDemandFeatureViews, this returns the input request schema columns.

    Args:
        feature_view: The feature view to analyze

    Returns:
        Set of expected input column names
    """
    if isinstance(feature_view, FeatureView):
        if feature_view.source and hasattr(feature_view.source, "schema"):
            # Use source schema for FeatureViews
            schema_columns = set()
            for field in feature_view.source.schema:
                schema_columns.add(field.name)
            return schema_columns
        elif feature_view.source:
            # For sources without explicit schema, use entity columns + timestamp
            schema_columns = set()
            for entity in feature_view.entities:
                if hasattr(entity, "join_keys"):
                    # Entity object
                    schema_columns.update(entity.join_keys)
                elif isinstance(entity, str):
                    # Entity name string
                    schema_columns.add(entity)
            if (
                hasattr(feature_view.source, "timestamp_field")
                and feature_view.source.timestamp_field
            ):
                schema_columns.add(feature_view.source.timestamp_field)
            return schema_columns

    elif isinstance(feature_view, OnDemandFeatureView):
        # Use input request schema for ODFVs
        if feature_view.source_request_sources:
            schema_columns = set()
            for (
                source_name,
                request_source,
            ) in feature_view.source_request_sources.items():
                for field in request_source.schema:
                    schema_columns.add(field.name)
            return schema_columns

    return set()


def get_output_schema_columns(
    feature_view: Union[FeatureView, OnDemandFeatureView],
) -> Set[str]:
    """
    Extract expected output column names from a feature view.

    This returns the feature schema columns that result after transformation.

    Args:
        feature_view: The feature view to analyze

    Returns:
        Set of expected output column names
    """
    schema_columns = set()

    # Add feature columns
    for field in feature_view.schema:
        schema_columns.add(field.name)

    # Add entity columns (present in both input and output)
    # For OnDemandFeatureViews, we skip adding entity columns since they're not meaningful
    if not isinstance(feature_view, OnDemandFeatureView):
        for entity in feature_view.entities:
            if hasattr(entity, "join_keys"):
                # Entity object
                schema_columns.update(entity.join_keys)
            elif isinstance(entity, str):
                # Entity name string - filter out dummy entities
                if entity != "__dummy":
                    schema_columns.add(entity)

    return schema_columns


def extract_column_names(
    data: Union[pd.DataFrame, pa.Table, Dict[str, Any], List[Dict[str, Any]]],
) -> Set[str]:
    """
    Extract column names from various data formats.

    Args:
        data: Input data in various formats

    Returns:
        Set of column names found in the data
    """
    if isinstance(data, pd.DataFrame):
        return set(data.columns)

    elif isinstance(data, pa.Table):
        return set(data.column_names)

    elif isinstance(data, dict):
        return set(data.keys())

    elif isinstance(data, list) and len(data) > 0 and isinstance(data[0], dict):
        # List of dictionaries - use keys from first dict
        return set(data[0].keys())

    else:
        logger.warning(f"Unsupported data type for column extraction: {type(data)}")
        return set()


def should_apply_transformation(
    feature_view: Union[FeatureView, OnDemandFeatureView],
    data: Union[pd.DataFrame, pa.Table, Dict[str, Any], List[Dict[str, Any]]],
    require_exact_match: bool = False,
) -> Optional[bool]:
    """
    Automatically determine if transformation should be applied based on data schema.

    Logic:
    - If data matches input schema: return True (apply transformation)
    - If data matches output schema: return False (skip transformation)
    - If ambiguous or no transformation: return None (fallback to default behavior)

    Args:
        feature_view: The feature view with potential transformation
        data: Input data to analyze
        require_exact_match: If True, requires exact column match. If False, allows subset matching.

    Returns:
        True if transformation should be applied, False if it should be skipped,
        None if auto-detection is inconclusive
    """
    # Only apply auto-detection if feature view has a transformation
    transformation = getattr(feature_view, "feature_transformation", None)
    if not transformation:
        return None

    data_columns = extract_column_names(data)
    if not data_columns:
        logger.warning("Could not extract column names from input data")
        return None

    input_columns = get_input_schema_columns(feature_view)
    output_columns = get_output_schema_columns(feature_view)

    if not input_columns and not output_columns:
        logger.warning(
            f"Could not determine input/output schemas for {feature_view.name}"
        )
        return None

    # Check input schema match
    input_match = _check_schema_match(data_columns, input_columns, require_exact_match)

    # Check output schema match
    output_match = _check_schema_match(
        data_columns, output_columns, require_exact_match
    )

    # Decision logic
    if input_match and not output_match:
        # Data matches input schema but not output - needs transformation
        logger.info(
            f"Auto-detected: applying transformation for {feature_view.name} (input schema match)"
        )
        return True

    elif output_match and not input_match:
        # Data matches output schema but not input - already transformed
        logger.info(
            f"Auto-detected: skipping transformation for {feature_view.name} (output schema match)"
        )
        return False

    elif input_match and output_match:
        # Ambiguous case - data matches both schemas
        logger.warning(
            f"Ambiguous schema match for {feature_view.name} - data matches both input and output schemas"
        )
        return None

    else:
        # Data doesn't clearly match either schema
        logger.warning(
            f"Schema mismatch for {feature_view.name} - data doesn't match input or output schemas clearly"
        )
        return None


def _check_schema_match(
    data_columns: Set[str], schema_columns: Set[str], require_exact_match: bool
) -> bool:
    """
    Check if data columns match a schema.

    Args:
        data_columns: Columns present in the data
        schema_columns: Expected schema columns
        require_exact_match: Whether to require exact match or allow subset

    Returns:
        True if schemas match according to the matching criteria
    """
    if not schema_columns:
        return False

    if require_exact_match:
        return data_columns == schema_columns
    else:
        # Allow data to be a superset of schema (extra columns ok)
        # But all schema columns must be present in data
        return schema_columns.issubset(data_columns)


def validate_transformation_compatibility(
    feature_view: Union[FeatureView, OnDemandFeatureView],
    input_data: Union[pd.DataFrame, pa.Table, Dict[str, Any], List[Dict[str, Any]]],
    transformed_data: Union[
        pd.DataFrame, pa.Table, Dict[str, Any], List[Dict[str, Any]]
    ] = None,
) -> List[str]:
    """
    Validate that transformation input/output data is compatible with feature view schemas.

    Args:
        feature_view: The feature view to validate against
        input_data: Input data before transformation
        transformed_data: Output data after transformation (optional)

    Returns:
        List of validation error messages (empty if valid)
    """
    errors = []

    input_columns = extract_column_names(input_data)
    expected_input_columns = get_input_schema_columns(feature_view)

    # Validate input data
    if expected_input_columns:
        missing_input_columns = expected_input_columns - input_columns
        if missing_input_columns:
            errors.append(
                f"Input data missing required columns: {sorted(missing_input_columns)}"
            )

    # Validate transformed data if provided
    if transformed_data is not None:
        output_columns = extract_column_names(transformed_data)
        expected_output_columns = get_output_schema_columns(feature_view)

        if expected_output_columns:
            missing_output_columns = expected_output_columns - output_columns
            if missing_output_columns:
                errors.append(
                    f"Transformed data missing required columns: {sorted(missing_output_columns)}"
                )

    return errors
