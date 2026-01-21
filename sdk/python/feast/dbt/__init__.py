"""
dbt integration for Feast.

This module provides functionality to import dbt models as Feast FeatureViews,
enabling automatic generation of Feast objects from dbt manifest.json files.

Example usage::

    from feast.dbt import DbtManifestParser, DbtToFeastMapper
    parser = DbtManifestParser("target/manifest.json")
    parser.parse()
    models = parser.get_models(tag_filter="feast")
    mapper = DbtToFeastMapper(data_source_type="bigquery")
    for model in models:
        data_source = mapper.create_data_source(model)
        feature_view = mapper.create_feature_view(model, data_source, "driver_id")
"""

from feast.dbt.codegen import DbtCodeGenerator, generate_feast_code
from feast.dbt.mapper import DbtToFeastMapper
from feast.dbt.parser import DbtColumn, DbtManifestParser, DbtModel

__all__ = [
    "DbtManifestParser",
    "DbtModel",
    "DbtColumn",
    "DbtToFeastMapper",
    "DbtCodeGenerator",
    "generate_feast_code",
]
