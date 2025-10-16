#!/usr/bin/env python3
"""
Script to create feature store YAMLs and feature store objects from client config YAML contents.
Especially made for Workbenches to create feature store YAMLs and objects from client config contents from dashboard

This script:
1. Takes multiple Feast client config YAML contents as input
2. Creates feature_store.yaml files in the current directory for each config
3. Creates FeatureStore objects for each configuration
4. Prints success messages and feature store object names
"""

import os
import yaml
from pathlib import Path
from typing import Dict, List, Any, Optional
from feast import FeatureStore
from feast.repo_config import RepoConfig


def create_feature_store_yaml(config_content: str, config_name: str) -> str:
    """
    Create a feature_store.yaml file from config content.
    
    Args:
        config_content: YAML content as string
        config_name: Name identifier for the config (used for filename)
    
    Returns:
        Path to the created YAML file
    """
    # Parse the YAML content to validate it
    try:
        config_dict = yaml.safe_load(config_content)
    except yaml.YAMLError as e:
        raise ValueError(f"Failed to parse YAML content for {config_name}: {e}")
    
    # Ensure required fields are present
    required_fields = ['project', 'registry', 'provider']
    for field in required_fields:
        if field not in config_dict:
            raise ValueError(f"Failed to create config {config_name}: missing required field '{field}'")
    
    # Create filename
    filename = f"feature_store_{config_name}.yaml"
    filepath = Path(filename)
    
    # Write the YAML file
    with open(filepath, 'w') as f:
        yaml.dump(config_dict, f, default_flow_style=False, sort_keys=False)
    
    return str(filepath)


def create_feature_store_object(yaml_file_path: str) -> FeatureStore:
    """
    Create a FeatureStore object from a YAML file.
    
    Args:
        yaml_file_path: Path to the feature_store.yaml file
    
    Returns:
        FeatureStore object
    """
    try:
        # Create FeatureStore from the YAML file
        fs = FeatureStore(fs_yaml_file=Path(yaml_file_path))
        return fs
    except Exception as e:
        raise RuntimeError(f"Failed to create FeatureStore object from {yaml_file_path}: {e}")


def process_client_configs(client_configs: Dict[str, str]) -> Dict[str, Dict[str, Any]]:
    """
    Process multiple client config YAML contents and create feature stores.
    
    Args:
        client_configs: Dictionary mapping config names to YAML content strings
    
    Returns:
        Dictionary with results for each config
    """
    results = {}
    created_yamls = []
    feature_stores = {}
    
    print("Creating feature store YAMLs and objects...")
    print("=" * 50)
    
    for config_name, config_content in client_configs.items():
        try:
            print(f"\nProcessing config: {config_name}")

            # Create YAML file
            yaml_path = create_feature_store_yaml(config_content, config_name)
            created_yamls.append(yaml_path)
            print(f"✓ Created YAML file: {yaml_path}")
            
            # Create FeatureStore object
            fs = create_feature_store_object(yaml_path)
            fs_var_name = f"fs_{fs.project}"
            globals()[fs_var_name] = fs
            feature_stores[config_name] = fs_var_name
            print(f"✓ Created FeatureStore object: {fs_var_name}")

            results[config_name] = {
                'yaml_path': yaml_path,
                'feature_store': fs_var_name,
                'project_name': fs.project,
                'success': True,
                'error': None
            }
            
        except Exception as e:
            print(f"✗ Failed to process config {config_name}: {e}")
            results[config_name] = {
                'yaml_path': None,
                'feature_store': None,
                'project_name': None,
                'success': False,
                'error': str(e)
            }
    
    return results


def print_summary(results: Dict[str, Dict[str, Any]]) -> None:
    """
    Print summary of all operations.
    
    Args:
        results: Results dictionary from process_client_configs
    """
    print("\n" + "=" * 50)
    print("SUMMARY")
    print("=" * 50)

    successful_configs = [name for name, result in results.items() if result['success']]
    failed_configs = [name for name, result in results.items() if not result['success']]
    print(f"\n\n✓✓Feature Store YAML files have been created in: {os.getcwd()}")
    print(f"\n✓ Successfully processed {len(successful_configs)} config(s):")
    for config_name in successful_configs:
        result = results[config_name]
        print(f"  - {config_name}: {result['yaml_path']} (Project: {result['project_name']})")

    if failed_configs:
        print(f"\n✗ Failed to process {len(failed_configs)} config(s):")
        for config_name in failed_configs:
            result = results[config_name]
            print(f"  - {config_name}: {result['error']}")

    print(f"\n\n✓✓ Feature Store Object(s) details:")
    for config_name in successful_configs:
        result = results[config_name]
        print(f"> Object Name - {result['feature_store']} ; project name - {result['project_name']} ; yaml path - {result['yaml_path']}")

    print("\n")
    print("=" * 25, "Usage:", "=" * 25)
    print("You can now use feature store object(s) to access the feature store resources and functions!")
    print("\n// Note: Replace object_name with the actual object name from the list above.")
    print("object_name.list_features()\nobject_name.get_historical_features()")
    print("=" * 58)


def main():
    """
    Main function to demonstrate usage with example configs.
    """
    # Example client config YAML contents
    example_configs = {
        "local_sqlite": """
project: local_feature_store
registry: data/registry.db
provider: local
online_store:
    type: sqlite
    path: data/online_store.db
offline_store:
    type: file
entity_key_serialization_version: 3
""",
        
        "aws_redshift": """
project: aws_feature_store
registry: data/registry.db
provider: aws
online_store:
    type: sqlite
    path: data/online_store.db
offline_store:
    type: redshift
    cluster_id: my-cluster
    region: us-west-2
    database: my_database
    user: my_user
    s3_staging_location: s3://my-bucket/staging
    iam_role: arn:aws:iam::123456789012:role/RedshiftRole
entity_key_serialization_version: 3
""",
        
        "gcp_bigquery": """
project: gcp_feature_store
registry: data/registry.db
provider: gcp
online_store:
    type: sqlite
    path: data/online_store.db
offline_store:
    type: bigquery
    project_id: my-gcp-project
    dataset_id: my_dataset
entity_key_serialization_version: 3
"""
    }
    print("=" * 50)
    print("This script will create feature store YAMLs and objects from client configs.")
    print(f"Processing {len(example_configs)} example configurations...")
    
    # Process the configs
    results = process_client_configs(example_configs)
    
    # Print summary
    print_summary(results)


if __name__ == "__main__":
    main()