"""Utility functions for Feast MCP integration."""

import json
import pandas as pd
from typing import Any, Dict, List


def format_feature_view_info(feature_view) -> str:
    """Format feature view information as a string.

    Args:
        feature_view: A Feast feature view

    Returns:
        Formatted string with feature view information
    """
    features = [f.name for f in feature_view.features]
    entities = feature_view.entities

    info = [
        f"Feature View: {feature_view.name}",
        f"Description: {feature_view.description or 'No description'}",
        f"Entities: {', '.join(entities)}",
        f"Features: {', '.join(features)}",
    ]

    if feature_view.ttl:
        info.append(f"TTL: {feature_view.ttl}")

    if feature_view.tags:
        info.append(f"Tags: {json.dumps(feature_view.tags)}")

    return "\n".join(info)

def format_entity_info(entity) -> str:
    """Format entity information as a string.

    Args:
        entity: A Feast entity

    Returns:
        Formatted string with entity information
    """
    info = [
        f"Entity: {entity.name}",
        f"Description: {entity.description or 'No description'}",
        f"Value Type: {entity.value_type}",
        f"Join Key: {entity.join_key}",
    ]

    if entity.tags:
        info.append(f"Tags: {json.dumps(entity.tags)}")

    return "\n".join(info)

def format_feature_service_info(feature_service) -> str:
    """Format feature service information as a string.

    Args:
        feature_service: A Feast feature service

    Returns:
        Formatted string with feature service information
    """
    info = [
        f"Feature Service: {feature_service.name}",
        f"Description: {feature_service.description or 'No description'}",
    ]

    if feature_service.feature_view_projections:
        info.append("Feature Views:")
        for projection in feature_service.feature_view_projections:
            features = [f.name for f in projection.features] if projection.features else []
            info.append(f"  - {projection.name}: {', '.join(features)}")

    if feature_service.tags:
        info.append(f"Tags: {json.dumps(feature_service.tags)}")

    return "\n".join(info)

def dict_to_dataframe(data: Dict[str, List[Any]]) -> pd.DataFrame:
    """Convert a dictionary to a pandas DataFrame.

    Args:
        data: Dictionary with column names as keys and lists of values

    Returns:
        Pandas DataFrame
    """
    return pd.DataFrame(data)
