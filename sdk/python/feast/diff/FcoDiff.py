from dataclasses import dataclass
from enum import Enum
from typing import Any, List, Set, Tuple, Union

from feast.entity import Entity
from feast.feature_service import FeatureService
from feast.feature_table import FeatureTable
from feast.feature_view import FeatureView
from feast.on_demand_feature_view import OnDemandFeatureView
from feast.request_feature_view import RequestFeatureView


@dataclass
class PropertyDiff:
    property_name: str
    val_existing: str
    val_declared: str


class TransitionType(Enum):
    UNKNOWN = 0
    CREATE = 1
    DELETE = 2
    UPDATE = 3
    UNCHANGED = 4


@dataclass
class FcoDiff:
    current_fco: Any
    new_fco: Any
    fco_property_diffs: List[PropertyDiff]
    transition_type: TransitionType


class RegistryDiff:
    fco_diffs: List[FcoDiff]

    def __init__(self):
        self.fco_diffs = []

    def add_fco_diff(self, fco_diff: FcoDiff):
        self.fco_diffs.append(fco_diff)


def _tag_registry_entities_for_keep_delete(
    existing_entities: Set[Entity], desired_entities: Set[Entity]
) -> Tuple[Set[Entity], Set[Entity], Set[Entity]]:
    existing_entity_names = {e.name for e in existing_entities}
    desired_entity_names = {e.name for e in desired_entities}

    entities_to_add = {
        e for e in desired_entities if e.name not in existing_entity_names
    }
    entities_to_keep = {e for e in desired_entities if e.name in existing_entity_names}
    entities_to_delete = {
        e for e in existing_entities if e.name not in desired_entity_names
    }

    return entities_to_keep, entities_to_delete, entities_to_add


def _tag_registry_views_for_keep_delete(
    existing_views: Union[
        Set[FeatureView], Set[RequestFeatureView], Set[OnDemandFeatureView]
    ],
    desired_views: Union[
        Set[FeatureView], Set[RequestFeatureView], Set[OnDemandFeatureView]
    ],
) -> Tuple[
    Set[Union[FeatureView, RequestFeatureView, OnDemandFeatureView]],
    Set[Union[FeatureView, RequestFeatureView, OnDemandFeatureView]],
    Set[Union[FeatureView, RequestFeatureView, OnDemandFeatureView]],
]:
    existing_view_names = {v.name for v in existing_views}
    desired_view_names = {v.name for v in desired_views}

    views_to_add = {v for v in desired_views if v.name not in existing_view_names}
    views_to_keep = {v for v in desired_views if v.name in existing_view_names}
    views_to_delete = {v for v in existing_views if v.name not in desired_view_names}
    return views_to_keep, views_to_delete, views_to_add


def _tag_registry_tables_for_keep_delete(
    existing_tables: Set[FeatureTable], desired_tables: Set[FeatureTable]
) -> Tuple[Set[FeatureTable], Set[FeatureTable], Set[FeatureTable]]:
    existing_table_names = {v.name for v in existing_tables}
    desired_table_names = {v.name for v in desired_tables}

    tables_to_add = {t for t in desired_tables if t.name not in existing_table_names}
    tables_to_keep = {t for t in desired_tables if t.name in existing_table_names}
    tables_to_delete = {t for t in existing_tables if t.name not in desired_table_names}
    return tables_to_keep, tables_to_delete, tables_to_add


def _tag_registry_services_for_keep_delete(
    existing_service: Set[FeatureService], desired_service: Set[FeatureService]
) -> Tuple[Set[FeatureService], Set[FeatureService], Set[FeatureService]]:
    existing_service_names = {v.name for v in existing_service}
    desired_service_names = {v.name for v in desired_service}

    services_to_add = {
        s for s in desired_service if s.name not in existing_service_names
    }
    services_to_delete = {
        s for s in existing_service if s.name not in desired_service_names
    }
    services_to_keep = {s for s in desired_service if s.name in existing_service_names}
    return services_to_keep, services_to_delete, services_to_add
