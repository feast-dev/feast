"""
feast.yaml_feature_definition
==============================
Translate a YAML feature-definition file into Feast objects or generated Python code.

Quickstart
----------
**Load live objects** (use directly in Python):

    from feast.yaml_feature_definition import YamlToFeastMapper, YamlDefinitionParser

    spec    = YamlDefinitionParser().parse_file("features.yaml")
    objects = YamlToFeastMapper().map(spec)
    # objects["entities"], objects["feature_views"], …

**Generate a Python module** (for use with ``feast apply``):

    from feast.yaml_feature_definition import generate_feast_code

    generate_feast_code("features.yaml", output_path="feature_repo/features.py")

**CLI** (from the shell):

    python -m feast.yaml_feature_definition features.yaml -o feature_repo/features.py
"""

from .codegen import YamlCodeGenerator, generate_feast_code
from .mapper import YamlToFeastMapper, parse_duration, parse_feast_type
from .parser import (
    AggregationSpec,
    DataSourceSpec,
    EntitySpec,
    FeastDefinitionSpec,
    FeatureViewSpec,
    FieldSpec,
    OnDemandFeatureViewSpec,
    YamlDefinitionParser,
)

__all__ = [
    # Parser
    "YamlDefinitionParser",
    "FeastDefinitionSpec",
    "EntitySpec",
    "DataSourceSpec",
    "FeatureViewSpec",
    "OnDemandFeatureViewSpec",
    "AggregationSpec",
    "FieldSpec",
    # Mapper
    "YamlToFeastMapper",
    "parse_feast_type",
    "parse_duration",
    # Codegen
    "YamlCodeGenerator",
    "generate_feast_code",
]
