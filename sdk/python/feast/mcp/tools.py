"""MCP tools for Feast feature store."""

from datetime import datetime
import pandas as pd
from typing import Any, Dict, List, Optional

from mcp.server.fastmcp import FastMCP
from feast import FeatureStore
from feast.data_source import PushMode


def register_feature_tools(mcp: FastMCP, feature_store: FeatureStore):
    """Register Feast feature tools with the MCP server.

    Args:
        mcp: The MCP server instance
        feature_store: The Feast feature store instance
    """

    @mcp.tool()
    def get_online_features(
        entity_rows: List[Dict[str, Any]],
        features: List[str],
        full_feature_names: bool = False
    ) -> Dict[str, Any]:
        """Retrieve online feature values.

        Args:
            entity_rows: List of entity rows to retrieve features for
            features: List of feature references to retrieve
            full_feature_names: Whether to include the feature view name as a prefix

        Returns:
            Dictionary containing the retrieved features
        """
        try:
            response = feature_store.get_online_features(
                entity_rows=entity_rows,
                features=features,
                full_feature_names=full_feature_names
            )
            return response.to_dict()
        except Exception as e:
            return {"error": str(e)}

    @mcp.tool()
    def materialize_features(
        feature_view_name: str,
        start_date: str,
        end_date: str
    ) -> str:
        """Materialize features for a feature view.

        Args:
            feature_view_name: Name of the feature view to materialize
            start_date: Start date for materialization (YYYY-MM-DD)
            end_date: End date for materialization (YYYY-MM-DD)

        Returns:
            Status message
        """
        try:
            start = datetime.strptime(start_date, "%Y-%m-%d")
            end = datetime.strptime(end_date, "%Y-%m-%d")

            feature_store.materialize(
                feature_views=[feature_view_name],
                start_date=start,
                end_date=end
            )
            return f"Successfully materialized {feature_view_name} from {start_date} to {end_date}"
        except Exception as e:
            return f"Error materializing features: {str(e)}"

    @mcp.tool()
    def materialize_incremental(
        end_date: str,
        feature_views: Optional[List[str]] = None
    ) -> str:
        """Materialize features incrementally.

        Args:
            end_date: End date for materialization (YYYY-MM-DD)
            feature_views: Optional list of feature views to materialize

        Returns:
            Status message
        """
        try:
            end = datetime.strptime(end_date, "%Y-%m-%d")

            feature_store.materialize_incremental(
                end_date=end,
                feature_views=feature_views
            )

            fv_str = ", ".join(feature_views) if feature_views else "all feature views"
            return f"Successfully materialized {fv_str} incrementally up to {end_date}"
        except Exception as e:
            return f"Error materializing features incrementally: {str(e)}"

    @mcp.tool()
    def push_features(
        push_source_name: str,
        df_dict: Dict[str, List[Any]],
        to: str = "online"
    ) -> str:
        """Push features to the feature store.

        Args:
            push_source_name: Name of the push source
            df_dict: Dictionary representation of a DataFrame with features to push
            to: Destination for the pushed features ("online", "offline", or "online_and_offline")

        Returns:
            Status message
        """
        try:
            df = pd.DataFrame(df_dict)

            if to == "offline":
                push_mode = PushMode.OFFLINE
            elif to == "online":
                push_mode = PushMode.ONLINE
            elif to == "online_and_offline":
                push_mode = PushMode.ONLINE_AND_OFFLINE
            else:
                return f"Invalid push mode: {to}. Must be one of 'online', 'offline', or 'online_and_offline'."

            feature_store.push(
                push_source_name=push_source_name,
                df=df,
                to=push_mode
            )

            return f"Successfully pushed {len(df)} rows to {push_source_name} ({to})"
        except Exception as e:
            return f"Error pushing features: {str(e)}"

    @mcp.tool()
    def get_feature_view_schema(feature_view_name: str) -> Dict[str, Any]:
        """Get the schema of a feature view.

        Args:
            feature_view_name: Name of the feature view

        Returns:
            Dictionary containing the feature view schema
        """
        try:
            fv = feature_store.get_feature_view(feature_view_name)

            schema = {
                "name": fv.name,
                "entities": fv.entities,
                "features": [
                    {
                        "name": feature.name,
                        "dtype": str(feature.dtype)
                    }
                    for feature in fv.features
                ],
                "ttl": str(fv.ttl) if fv.ttl else None,
                "online": fv.online,
                "description": fv.description
            }

            return schema
        except Exception as e:
            return {"error": str(e)}
