"""
Maps parsed YAML specs into live Feast objects.

OnDemandFeatureViews with aggregations always receive an auto-generated
passthrough UDF (identity function) so that the Feast runtime only runs the
declared aggregations with no additional transformation logic.
"""

from __future__ import annotations

import importlib
import re
from datetime import timedelta
from typing import Any, Dict, List, Optional

from feast import Entity, FeatureView, Field, OnDemandFeatureView
from feast.aggregation import Aggregation
from feast.types import (
    Array,
    Bool,
    Bytes,
    Decimal,
    Float32,
    Float64,
    Int32,
    Int64,
    Json,
    String,
    UnixTimestamp,
)

from .parser import (
    AggregationSpec,
    DataSourceSpec,
    EntitySpec,
    FeastDefinitionSpec,
    FeatureViewSpec,
    FieldSpec,
    OnDemandFeatureViewSpec,
)

# ── type resolution ──────────────────────────────────────────────────────────

_PRIMITIVE_TYPES: Dict[str, Any] = {
    "string": String,
    "str": String,
    "int32": Int32,
    "int64": Int64,
    "int": Int64,
    "integer": Int64,
    "float32": Float32,
    "float64": Float64,
    "float": Float64,
    "double": Float64,
    "bool": Bool,
    "boolean": Bool,
    "bytes": Bytes,
    "unixtimestamp": UnixTimestamp,
    "unix_timestamp": UnixTimestamp,
    "json": Json,
    "decimal": Decimal,
}

_ARRAY_RE = re.compile(r"^array\((.+)\)$", re.IGNORECASE)
_DURATION_RE = re.compile(r"^(\d+)([dhms])$")
_DURATION_UNITS = {"d": "days", "h": "hours", "m": "minutes", "s": "seconds"}

# Dotted-path to the (module, ClassName) for each supported source type.
_DS_CLASS_PATH: Dict[str, tuple[str, str]] = {
    "bigquery": ("feast.infra.offline_stores.bigquery", "BigQuerySource"),
    "snowflake": ("feast.infra.offline_stores.snowflake", "SnowflakeSource"),
    "redshift": ("feast.infra.offline_stores.redshift", "RedshiftSource"),
    "file": ("feast.infra.offline_stores.file", "FileSource"),
    "push": ("feast.data_source", "PushSource"),
}


def parse_feast_type(dtype: str) -> Any:
    """
    Resolve a type-string to a Feast type object.

    Supported forms:
      * Primitives (case-insensitive): ``Int64``, ``float32``, ``String`` …
      * Nested arrays:                 ``Array(Int64)``, ``Array(Array(Float64))``
    """
    key = dtype.strip()
    primitive = _PRIMITIVE_TYPES.get(key.lower())
    if primitive is not None:
        return primitive
    m = _ARRAY_RE.match(key)
    if m:
        return Array(parse_feast_type(m.group(1)))
    raise ValueError(
        f"Unknown Feast type {dtype!r}. "
        f"Supported primitives: {sorted(_PRIMITIVE_TYPES)} "
        f"and Array(<type>)."
    )


def parse_duration(s: str) -> timedelta:
    """Parse duration strings such as ``30d``, ``12h``, ``60m``, ``30s``."""
    m = _DURATION_RE.match(s.strip())
    if not m:
        raise ValueError(
            f"Invalid duration {s!r}. Expected <N><unit> where unit is d/h/m/s."
        )
    value, unit = int(m.group(1)), m.group(2)
    return timedelta(**{_DURATION_UNITS[unit]: value})


# ── mapper ───────────────────────────────────────────────────────────────────


class YamlToFeastMapper:
    """Converts a :class:`~parser.FeastDefinitionSpec` into live Feast objects."""

    def map(self, spec: FeastDefinitionSpec) -> Dict[str, Any]:
        """
        Returns a dict with four keys:

        * ``entities``                – ``{name: Entity}``
        * ``data_sources``            – ``{name: DataSource}``
        * ``feature_views``           – ``{name: FeatureView}``
        * ``on_demand_feature_views`` – ``{name: OnDemandFeatureView}``
        """
        entities = {e.name: self._entity(e) for e in spec.entities}
        data_sources = {ds.name: self._data_source(ds) for ds in spec.data_sources}
        feature_views = {
            fv.name: self._feature_view(fv, entities, data_sources)
            for fv in spec.feature_views
        }
        on_demand_feature_views = {
            o.name: self._odfv(o, entities, feature_views)
            for o in spec.on_demand_feature_views
        }
        return {
            "entities": entities,
            "data_sources": data_sources,
            "feature_views": feature_views,
            "on_demand_feature_views": on_demand_feature_views,
        }

    # ── object builders ──────────────────────────────────────────────────────

    def _entity(self, spec: EntitySpec) -> Entity:
        return Entity(
            name=spec.name,
            join_keys=[spec.join_key or spec.name],
            description=spec.description,
            tags=spec.tags or None,
            owner=spec.owner,
        )

    def _field(self, spec: FieldSpec) -> Field:
        return Field(
            name=spec.name,
            dtype=parse_feast_type(spec.dtype),
            description=spec.description,
            tags=spec.tags or None,
        )

    def _data_source(self, spec: DataSourceSpec) -> Any:
        ds_type = spec.type.lower()
        if ds_type not in _DS_CLASS_PATH:
            raise ValueError(
                f"Unsupported data source type {spec.type!r}. "
                f"Supported: {sorted(_DS_CLASS_PATH)}."
            )
        module_name, class_name = _DS_CLASS_PATH[ds_type]
        cls = getattr(importlib.import_module(module_name), class_name)

        kwargs: Dict[str, Any] = {
            "name": spec.name,
            "timestamp_field": spec.timestamp_field,
            "description": spec.description,
            "tags": spec.tags or None,
            "owner": spec.owner,
            **spec.config,
        }
        if spec.created_timestamp_column:
            kwargs["created_timestamp_column"] = spec.created_timestamp_column
        if spec.field_mapping:
            kwargs["field_mapping"] = spec.field_mapping
        return cls(**kwargs)

    def _feature_view(
        self,
        spec: FeatureViewSpec,
        entities: Dict[str, Entity],
        data_sources: Dict[str, Any],
    ) -> FeatureView:
        return FeatureView(
            name=spec.name,
            entities=[entities[n] for n in spec.entities if n in entities] or None,
            source=data_sources.get(spec.source) if spec.source else None,
            schema=[self._field(f) for f in spec.schema],
            ttl=timedelta(days=spec.ttl_days),
            online=spec.online,
            offline=spec.offline,
            description=spec.description,
            tags=spec.tags or None,
            owner=spec.owner,
        )

    def _odfv(
        self,
        spec: OnDemandFeatureViewSpec,
        entities: Dict[str, Entity],
        feature_views: Dict[str, FeatureView],
    ) -> OnDemandFeatureView:
        # Passthrough UDF: the runtime applies only the declared aggregations;
        # the UDF itself is an identity function.
        udf_name = f"_{spec.name}_passthrough"
        udf_src = f"def {udf_name}(df):\n    return df\n"
        _ns: Dict[str, Any] = {}
        exec(udf_src, _ns)  # noqa: S102 – deterministic, no user input

        return OnDemandFeatureView(
            name=spec.name,
            entities=[entities[n] for n in spec.entities if n in entities] or None,
            sources=[feature_views[n] for n in spec.sources if n in feature_views],
            schema=[self._field(f) for f in spec.schema],
            aggregations=[self._aggregation(a) for a in spec.aggregations],
            udf=_ns[udf_name],
            udf_string=udf_src,
            mode=spec.mode,
            write_to_online_store=spec.write_to_online_store,
            description=spec.description,
            tags=spec.tags or None,
            owner=spec.owner,
        )

    def _aggregation(self, spec: AggregationSpec) -> Aggregation:
        kwargs: Dict[str, Any] = {
            "column": spec.column,
            "function": spec.function,
            "time_window": parse_duration(spec.time_window),
        }
        if spec.slide_interval:
            kwargs["slide_interval"] = parse_duration(spec.slide_interval)
        if spec.name:
            kwargs["name"] = spec.name
        return Aggregation(**kwargs)
