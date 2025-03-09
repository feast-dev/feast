"""Example MCP client for Feast feature store."""

import json
import sys
from typing import Dict, Any

from mcp.client import MCPClient

def main():
    """Run the MCP client example."""
    if len(sys.argv) < 2:
        print("Usage: python mcp_client.py <mcp_server_url>")
        sys.exit(1)
    
    mcp_server_url = sys.argv[1]
    client = MCPClient(url=mcp_server_url)
    
    # Get an overview of the feature store
    print("Getting feature store overview...")
    overview = client.get_prompt("feast_feature_store_overview")
    print(overview)
    print("\n" + "-" * 80 + "\n")
    
    # List all feature views
    print("Listing feature views...")
    feature_views = client.get_resource("feast://feature-views")
    print(feature_views)
    print("\n" + "-" * 80 + "\n")
    
    # Get feature retrieval guide
    print("Getting feature retrieval guide...")
    guide = client.get_prompt("feast_feature_retrieval_guide")
    print(guide)
    print("\n" + "-" * 80 + "\n")
    
    # Try to retrieve online features if entity rows are provided
    if len(sys.argv) > 2:
        entity_rows_file = sys.argv[2]
        with open(entity_rows_file, 'r') as f:
            entity_rows = json.load(f)
        
        features_arg = sys.argv[3] if len(sys.argv) > 3 else None
        features = features_arg.split(',') if features_arg else []
        
        print(f"Retrieving online features for {len(entity_rows)} entity rows...")
        response = client.invoke_tool("get_online_features", {
            "entity_rows": entity_rows,
            "features": features,
            "full_feature_names": False
        })
        print(json.dumps(response, indent=2))

if __name__ == "__main__":
    main()
