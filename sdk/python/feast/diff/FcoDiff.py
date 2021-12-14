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
    desired_entity_names = {e.name: e for e in desired_entities}
    existing_entity_names = {e.name: e for e in existing_entities}

    entities_to_add = set(
        [
            desired_entity_names[name]
            for name in desired_entity_names.keys() - existing_entity_names.keys()
        ]
    )
    entities_to_delete = set(
        [
            existing_entity_names[name]
            for name in existing_entity_names.keys() - desired_entity_names.keys()
        ]
    )
    entities_to_keep = set(
        [
            desired_entity_names[name]
            for name in desired_entity_names.keys() & existing_entity_names.keys()
        ]
    )

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

    existing_views_by_name = {v.name: v for v in existing_views}
    desired_views_by_name = {v.name: v for v in desired_views}

    views_to_add = set(
        [
            desired_views_by_name[name]
            for name in desired_views_by_name.keys() - existing_views_by_name.keys()
        ]
    )
    views_to_delete = set(
        [
            existing_views_by_name[name]
            for name in existing_views_by_name.keys() - desired_views_by_name.keys()
        ]
    )
    views_to_keep = set(
        [
            desired_views_by_name[name]
            for name in desired_views_by_name.keys() & existing_views_by_name.keys()
        ]
    )
    return views_to_keep, views_to_delete, views_to_add


def _tag_registry_tables_for_keep_delete(
    existing_tables: Set[FeatureTable], desired_tables: Set[FeatureTable]
) -> Tuple[Set[FeatureTable], Set[FeatureTable], Set[FeatureTable]]:
    existing_tables_by_name = {v.name: v for v in existing_tables}
    desired_tables_by_name = {v.name: v for v in desired_tables}

    tables_to_add = set(
        [
            desired_tables_by_name[name]
            for name in desired_tables_by_name.keys() - existing_tables_by_name.keys()
        ]
    )
    tables_to_delete = set(
        [
            existing_tables_by_name[name]
            for name in existing_tables_by_name.keys() - desired_tables_by_name.keys()
        ]
    )
    tables_to_keep = set(
        [
            desired_tables_by_name[name]
            for name in desired_tables_by_name.keys() & existing_tables_by_name.keys()
        ]
    )
    return tables_to_keep, tables_to_delete, tables_to_add


def _tag_registry_services_for_keep_delete(
    existing_service: Set[FeatureService], desired_service: Set[FeatureService]
) -> Tuple[Set[FeatureService], Set[FeatureService], Set[FeatureService]]:
    existing_services_by_name = {v.name: v for v in existing_service}
    desired_services_by_name = {v.name: v for v in desired_service}

    services_to_add = set(
        [
            desired_services_by_name[name]
            for name in desired_services_by_name.keys()
            - existing_services_by_name.keys()
        ]
    )
    services_to_delete = set(
        [
            existing_services_by_name[name]
            for name in existing_services_by_name.keys()
            - desired_services_by_name.keys()
        ]
    )
    services_to_keep = set(
        [
            desired_services_by_name[name]
            for name in desired_services_by_name.keys()
            & existing_services_by_name.keys()
        ]
    )
    return services_to_keep, services_to_delete, services_to_add
