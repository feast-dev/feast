"""
Parses a YAML feature-definition file into structured dataclasses.

YAML schema overview
--------------------
entities:
  - name: user_id
    dtype: Int64          # Feast type string; defaults to String
    join_key: user_id     # optional – defaults to name
    description: "..."
    tags: {team: payments}
    owner: owner@example.com

data_sources:
  - name: user_events
    type: bigquery        # bigquery | snowflake | file | redshift | push
    table: project.dataset.user_events   # type-specific key
    timestamp_field: event_timestamp
    created_timestamp_column: created_at
    field_mapping: {source_col: feast_col}
    description: "..."
    tags: {}
    owner: ""

feature_views:
  - name: user_features
    entities: [user_id]
    source: user_events   # optional – references a data_source by name
    ttl_days: 7           # optional – defaults to 0
    online: true
    offline: false
    description: "..."
    tags: {}
    owner: ""
    schema:
      - name: user_id
        dtype: Int64
      - name: purchase_count
        dtype: Int64
        description: "..."
        tags: {sensitive: "false"}

on_demand_feature_views:
  - name: user_risk_features
    entities: [user_id]
    sources: [user_features]   # list of feature_view names
    mode: pandas               # pandas | python
    write_to_online_store: false
    description: "..."
    tags: {}
    owner: ""
    aggregations:
      - column: purchase_count
        function: sum            # sum | max | min | count | mean | count_distinct
        time_window: 30d         # Nd | Nh | Nm | Ns
        slide_interval: 1d       # optional – defaults to time_window
        name: purchase_sum_30d   # optional output-name override
    schema:
      - name: purchase_sum_30d
        dtype: Float64
"""

from __future__ import annotations

import yaml
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


# Keys consumed at the DataSourceSpec level; anything else is forwarded as
# type-specific config (e.g. table=, path=, database=, schema=).
_KNOWN_DS_KEYS = frozenset({
    "name", "type", "timestamp_field", "created_timestamp_column",
    "field_mapping", "description", "tags", "owner",
})


@dataclass
class FieldSpec:
    name: str
    dtype: str
    description: str = ""
    tags: Dict[str, str] = field(default_factory=dict)


@dataclass
class EntitySpec:
    name: str
    dtype: str = "String"
    join_key: Optional[str] = None
    description: str = ""
    tags: Dict[str, str] = field(default_factory=dict)
    owner: str = ""


@dataclass
class DataSourceSpec:
    name: str
    type: str
    timestamp_field: str = "event_timestamp"
    created_timestamp_column: Optional[str] = None
    field_mapping: Dict[str, str] = field(default_factory=dict)
    description: str = ""
    tags: Dict[str, str] = field(default_factory=dict)
    owner: str = ""
    # Type-specific keys (table=, path=, database=, …) collected here.
    config: Dict[str, Any] = field(default_factory=dict)


@dataclass
class FeatureViewSpec:
    name: str
    schema: List[FieldSpec]
    entities: List[str] = field(default_factory=list)
    source: Optional[str] = None   # references a DataSourceSpec by name
    ttl_days: int = 0
    online: bool = True
    offline: bool = False
    description: str = ""
    tags: Dict[str, str] = field(default_factory=dict)
    owner: str = ""


@dataclass
class AggregationSpec:
    column: str
    function: str             # sum | max | min | count | mean | count_distinct
    time_window: str          # e.g. "30d", "12h"
    slide_interval: Optional[str] = None
    name: Optional[str] = None   # output feature name override


@dataclass
class OnDemandFeatureViewSpec:
    name: str
    sources: List[str]         # FeatureView names
    schema: List[FieldSpec]
    entities: List[str] = field(default_factory=list)
    aggregations: List[AggregationSpec] = field(default_factory=list)
    mode: str = "pandas"
    write_to_online_store: bool = False
    description: str = ""
    tags: Dict[str, str] = field(default_factory=dict)
    owner: str = ""


@dataclass
class FeastDefinitionSpec:
    entities: List[EntitySpec] = field(default_factory=list)
    data_sources: List[DataSourceSpec] = field(default_factory=list)
    feature_views: List[FeatureViewSpec] = field(default_factory=list)
    on_demand_feature_views: List[OnDemandFeatureViewSpec] = field(default_factory=list)


class YamlDefinitionParser:
    """Loads a YAML feature-definition file into :class:`FeastDefinitionSpec`."""

    def parse_file(self, path: str) -> FeastDefinitionSpec:
        with open(path) as fh:
            return self.parse_dict(yaml.safe_load(fh) or {})

    def parse_string(self, content: str) -> FeastDefinitionSpec:
        return self.parse_dict(yaml.safe_load(content) or {})

    def parse_dict(self, data: Dict[str, Any]) -> FeastDefinitionSpec:
        return FeastDefinitionSpec(
            entities=[self._entity(e) for e in data.get("entities", [])],
            data_sources=[self._data_source(ds) for ds in data.get("data_sources", [])],
            feature_views=[self._feature_view(fv) for fv in data.get("feature_views", [])],
            on_demand_feature_views=[
                self._odfv(o) for o in data.get("on_demand_feature_views", [])
            ],
        )

    # ── helpers ──────────────────────────────────────────────────────────────

    def _field(self, raw: Dict[str, Any]) -> FieldSpec:
        return FieldSpec(
            name=raw["name"],
            dtype=raw["dtype"],
            description=raw.get("description", ""),
            tags=raw.get("tags", {}),
        )

    def _entity(self, raw: Dict[str, Any]) -> EntitySpec:
        return EntitySpec(
            name=raw["name"],
            dtype=raw.get("dtype", "String"),
            join_key=raw.get("join_key"),
            description=raw.get("description", ""),
            tags=raw.get("tags", {}),
            owner=raw.get("owner", ""),
        )

    def _data_source(self, raw: Dict[str, Any]) -> DataSourceSpec:
        return DataSourceSpec(
            name=raw["name"],
            type=raw["type"],
            timestamp_field=raw.get("timestamp_field", "event_timestamp"),
            created_timestamp_column=raw.get("created_timestamp_column"),
            field_mapping=raw.get("field_mapping", {}),
            description=raw.get("description", ""),
            tags=raw.get("tags", {}),
            owner=raw.get("owner", ""),
            config={k: v for k, v in raw.items() if k not in _KNOWN_DS_KEYS},
        )

    def _feature_view(self, raw: Dict[str, Any]) -> FeatureViewSpec:
        return FeatureViewSpec(
            name=raw["name"],
            schema=[self._field(f) for f in raw.get("schema", [])],
            entities=raw.get("entities", []),
            source=raw.get("source"),
            ttl_days=raw.get("ttl_days", 0),
            online=raw.get("online", True),
            offline=raw.get("offline", False),
            description=raw.get("description", ""),
            tags=raw.get("tags", {}),
            owner=raw.get("owner", ""),
        )

    def _odfv(self, raw: Dict[str, Any]) -> OnDemandFeatureViewSpec:
        return OnDemandFeatureViewSpec(
            name=raw["name"],
            sources=raw.get("sources", []),
            schema=[self._field(f) for f in raw.get("schema", [])],
            entities=raw.get("entities", []),
            aggregations=[self._aggregation(a) for a in raw.get("aggregations", [])],
            mode=raw.get("mode", "pandas"),
            write_to_online_store=raw.get("write_to_online_store", False),
            description=raw.get("description", ""),
            tags=raw.get("tags", {}),
            owner=raw.get("owner", ""),
        )

    def _aggregation(self, raw: Dict[str, Any]) -> AggregationSpec:
        return AggregationSpec(
            column=raw["column"],
            function=raw["function"],
            time_window=raw["time_window"],
            slide_interval=raw.get("slide_interval"),
            name=raw.get("name"),
        )
