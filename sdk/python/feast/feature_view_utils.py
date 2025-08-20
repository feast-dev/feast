"""
Utility functions for feature view operations including source resolution.
"""

import logging
import typing
from dataclasses import dataclass
from typing import Callable, Optional

if typing.TYPE_CHECKING:
    from feast.data_source import DataSource
    from feast.feature_view import FeatureView
    from feast.repo_config import RepoConfig

logger = logging.getLogger(__name__)


@dataclass
class FeatureViewSourceInfo:
    """Information about a feature view's data source resolution."""

    data_source: "DataSource"
    source_type: str
    has_transformation: bool
    transformation_func: Optional[Callable] = None
    source_description: str = ""


def has_transformation(feature_view: "FeatureView") -> bool:
    """Check if a feature view has transformations (UDF or feature_transformation)."""
    return (
        getattr(feature_view, "udf", None) is not None
        or getattr(feature_view, "feature_transformation", None) is not None
    )


def get_transformation_function(feature_view: "FeatureView") -> Optional[Callable]:
    """Extract the transformation function from a feature view."""
    feature_transformation = getattr(feature_view, "feature_transformation", None)
    if feature_transformation:
        # Use feature_transformation if available (preferred)
        if hasattr(feature_transformation, "udf") and callable(
            feature_transformation.udf
        ):
            return feature_transformation.udf

    # Fallback to direct UDF
    udf = getattr(feature_view, "udf", None)
    if udf and callable(udf):
        return udf

    return None


def find_original_source_view(feature_view: "FeatureView") -> "FeatureView":
    """
    Recursively find the original source feature view that has a batch_source.
    For derived feature views, this follows the source_views chain until it finds
    a feature view with an actual DataSource (batch_source).
    """
    current_view = feature_view
    while hasattr(current_view, "source_views") and current_view.source_views:
        if not current_view.source_views:
            break
        current_view = current_view.source_views[0]  # Assuming single source for now
    return current_view


def check_sink_source_exists(data_source: "DataSource") -> bool:
    """
    Check if a sink_source file actually exists.
    Args:
        data_source: The DataSource to check
    Returns:
        bool: True if the source exists, False otherwise
    """
    try:
        import fsspec

        # Get the source path
        if hasattr(data_source, "path"):
            source_path = data_source.path
        else:
            source_path = str(data_source)

        fs, path_in_fs = fsspec.core.url_to_fs(source_path)
        return fs.exists(path_in_fs)
    except Exception as e:
        logger.warning(f"Failed to check if source exists: {e}")
        return False


def resolve_feature_view_source(
    feature_view: "FeatureView",
    config: Optional["RepoConfig"] = None,
    is_materialization: bool = False,
) -> FeatureViewSourceInfo:
    """
    Resolve the appropriate data source for a feature view.

    This handles the complex logic of determining whether to read from:
    1. sink_source (materialized data from parent views)
    2. batch_source (original data source)
    3. Recursive resolution for derived views

    Args:
        feature_view: The feature view to resolve
        config: Repository configuration (optional)
        is_materialization: Whether this is during materialization (affects derived view handling)

    Returns:
        FeatureViewSourceInfo: Information about the resolved source
    """
    view_has_transformation = has_transformation(feature_view)
    transformation_func = (
        get_transformation_function(feature_view) if view_has_transformation else None
    )

    # Check if this is a derived feature view (has source_views)
    is_derived_view = (
        hasattr(feature_view, "source_views") and feature_view.source_views
    )

    if not is_derived_view:
        # Regular feature view - use its batch_source directly
        return FeatureViewSourceInfo(
            data_source=feature_view.batch_source,
            source_type="batch_source",
            has_transformation=view_has_transformation,
            transformation_func=transformation_func,
            source_description=f"Direct batch_source for {feature_view.name}",
        )

    # This is a derived feature view - need to resolve parent source
    if not feature_view.source_views:
        raise ValueError(
            f"Derived feature view {feature_view.name} has no source_views"
        )
    parent_view = feature_view.source_views[0]  # Assuming single source for now

    # For derived views: distinguish between materialization and historical retrieval
    if (
        hasattr(parent_view, "sink_source")
        and parent_view.sink_source
        and is_materialization
    ):
        # During materialization, try to use sink_source if it exists
        if check_sink_source_exists(parent_view.sink_source):
            logger.debug(
                f"Materialization: Using parent {parent_view.name} sink_source"
            )
            return FeatureViewSourceInfo(
                data_source=parent_view.sink_source,
                source_type="sink_source",
                has_transformation=view_has_transformation,
                transformation_func=transformation_func,
                source_description=f"Parent {parent_view.name} sink_source for derived view {feature_view.name}",
            )
        else:
            logger.info(
                f"Parent {parent_view.name} sink_source doesn't exist during materialization"
            )

    # Check if parent is also a derived view first - if so, recursively resolve to original source
    if hasattr(parent_view, "source_views") and parent_view.source_views:
        # Parent is also a derived view - recursively find original source
        original_source_view = find_original_source_view(parent_view)
        return FeatureViewSourceInfo(
            data_source=original_source_view.batch_source,
            source_type="original_source",
            has_transformation=view_has_transformation,
            transformation_func=transformation_func,
            source_description=f"Original source {original_source_view.name} batch_source for derived view {feature_view.name} (via {parent_view.name})",
        )
    elif hasattr(parent_view, "batch_source") and parent_view.batch_source:
        # Parent has a direct batch_source, use it
        return FeatureViewSourceInfo(
            data_source=parent_view.batch_source,
            source_type="batch_source",
            has_transformation=view_has_transformation,
            transformation_func=transformation_func,
            source_description=f"Parent {parent_view.name} batch_source for derived view {feature_view.name}",
        )
    else:
        # No valid source found
        raise ValueError(
            f"Unable to resolve data source for derived feature view {feature_view.name} via parent {parent_view.name}"
        )


def resolve_feature_view_source_with_fallback(
    feature_view: "FeatureView",
    config: Optional["RepoConfig"] = None,
    is_materialization: bool = False,
) -> FeatureViewSourceInfo:
    """
    Resolve feature view source with fallback error handling.

    This version includes additional error handling and fallback logic
    for cases where the primary resolution fails.
    """
    try:
        return resolve_feature_view_source(feature_view, config, is_materialization)
    except Exception as e:
        logger.warning(f"Primary source resolution failed for {feature_view.name}: {e}")

        # Fallback: try to find any available source
        if hasattr(feature_view, "batch_source") and feature_view.batch_source:
            return FeatureViewSourceInfo(
                data_source=feature_view.batch_source,
                source_type="fallback_batch_source",
                has_transformation=has_transformation(feature_view),
                transformation_func=get_transformation_function(feature_view),
                source_description=f"Fallback batch_source for {feature_view.name}",
            )
        elif hasattr(feature_view, "source_views") and feature_view.source_views:
            # Try the original source view as last resort
            original_view = find_original_source_view(feature_view)
            return FeatureViewSourceInfo(
                data_source=original_view.batch_source,
                source_type="fallback_original_source",
                has_transformation=has_transformation(feature_view),
                transformation_func=get_transformation_function(feature_view),
                source_description=f"Fallback original source {original_view.name} for {feature_view.name}",
            )
        else:
            raise ValueError(
                f"Unable to resolve any data source for feature view {feature_view.name}"
            )
