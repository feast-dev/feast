import logging
from pathlib import Path
from typing import Callable, List, Optional, cast

from pydantic import StrictStr

from feast import (
    Entity,
    FeatureService,
    FeatureView,
    OnDemandFeatureView,
    SortedFeatureView,
    StreamFeatureView,
)
from feast.base_feature_view import BaseFeatureView
from feast.data_source import DataSource
from feast.infra.registry import proto_registry_utils
from feast.infra.registry.sql import SqlRegistry, SqlRegistryConfig
from feast.permissions.permission import Permission
from feast.saved_dataset import SavedDataset, ValidationReference

logger = logging.getLogger(__name__)


class SqlFallbackRegistryConfig(SqlRegistryConfig):
    registry_type: StrictStr = "sql-fallback"
    """ str: Provider name or a class name that implements Registry."""


def _obj_to_proto_with_project_name(obj, project: str):
    proto = obj.to_proto()
    if "spec" in proto.DESCRIPTOR.fields_by_name:
        proto.spec.project = project
    else:
        proto.project = project
    return proto


class SqlFallbackRegistry(SqlRegistry):
    def __init__(
        self,
        registry_config,
        project: str,
        repo_path: Optional[Path],
    ):
        assert registry_config is not None and isinstance(
            registry_config, SqlFallbackRegistryConfig
        ), "SqlFallbackRegistry needs a valid registry_config"

        super().__init__(registry_config, project, repo_path)

    def _get_and_cache_objects(
        self,
        registry_proto_field_name: str,
        get_fn: Callable,
        project: str,
        tags: Optional[dict[str, str]],
    ):
        objects = get_fn(project, tags)
        if len(objects) == 0:
            return []
        # Do not add to cache if the list was filtered by tags or the project is exempt from caching
        elif tags is not None or project in self.cache_exempt_projects:
            return objects
        # Clear and reset the cache with the new list of objects
        protos = [_obj_to_proto_with_project_name(obj, project) for obj in objects]
        with self._refresh_lock:
            self.cached_registry_proto.ClearField(registry_proto_field_name)  # type: ignore[arg-type]
            self.cached_registry_proto.__getattribute__(
                registry_proto_field_name
            ).extend(protos)
        return objects

    def list_data_sources(
        self,
        project: str,
        allow_cache: bool = False,
        tags: Optional[dict[str, str]] = None,
    ) -> List[DataSource]:
        if allow_cache:
            result = proto_registry_utils.list_data_sources(
                self.cached_registry_proto, project, tags
            )
            if len(result) > 0:
                return result
        return self._get_and_cache_objects(
            "data_sources", self._list_data_sources, project, tags
        )

    def list_entities(
        self,
        project: str,
        allow_cache: bool = False,
        tags: Optional[dict[str, str]] = None,
    ) -> List[Entity]:
        if allow_cache:
            result = proto_registry_utils.list_entities(
                self.cached_registry_proto, project, tags
            )
            if len(result) > 0:
                return result
        return self._get_and_cache_objects(
            "entities", self._list_entities, project, tags
        )

    def list_all_feature_views(
        self,
        project: str,
        allow_cache: bool = False,
        tags: Optional[dict[str, str]] = None,
    ) -> List[BaseFeatureView]:
        if allow_cache:
            result = proto_registry_utils.list_all_feature_views(
                self.cached_registry_proto, project, tags
            )
            if len(result) > 0:
                return result
        # If allow_cache=False or failed to find any feature views in the cache, fetch from the registry
        feature_views = self._get_and_cache_objects(
            "feature_views", self._list_feature_views, project, tags
        )
        on_demand_feature_views = self._get_and_cache_objects(
            "on_demand_feature_views", self._list_on_demand_feature_views, project, tags
        )
        stream_feature_views = self._get_and_cache_objects(
            "stream_feature_views", self._list_stream_feature_views, project, tags
        )
        sorted_feature_views = self._get_and_cache_objects(
            "sorted_feature_views", self._list_sorted_feature_views, project, tags
        )
        return (
            cast(list[BaseFeatureView], feature_views)
            + cast(list[BaseFeatureView], on_demand_feature_views)
            + cast(list[BaseFeatureView], stream_feature_views)
            + cast(list[BaseFeatureView], sorted_feature_views)
        )

    def list_feature_views(
        self,
        project: str,
        allow_cache: bool = False,
        tags: Optional[dict[str, str]] = None,
    ) -> List[FeatureView]:
        if allow_cache:
            result = proto_registry_utils.list_feature_views(
                self.cached_registry_proto, project, tags
            )
            if len(result) > 0:
                return result
        return self._get_and_cache_objects(
            "feature_views", self._list_feature_views, project, tags
        )

    def list_on_demand_feature_views(
        self,
        project: str,
        allow_cache: bool = False,
        tags: Optional[dict[str, str]] = None,
    ) -> List[OnDemandFeatureView]:
        if allow_cache:
            result = proto_registry_utils.list_on_demand_feature_views(
                self.cached_registry_proto, project, tags
            )
            if len(result) > 0:
                return result
        return self._get_and_cache_objects(
            "on_demand_feature_views", self._list_on_demand_feature_views, project, tags
        )

    def list_stream_feature_views(
        self,
        project: str,
        allow_cache: bool = False,
        tags: Optional[dict[str, str]] = None,
    ) -> List[StreamFeatureView]:
        if allow_cache:
            result = proto_registry_utils.list_stream_feature_views(
                self.cached_registry_proto, project, tags
            )
            if len(result) > 0:
                return result
        return self._get_and_cache_objects(
            "stream_feature_views", self._list_stream_feature_views, project, tags
        )

    def list_sorted_feature_views(
        self,
        project: str,
        allow_cache: bool = False,
        tags: Optional[dict[str, str]] = None,
    ) -> List[SortedFeatureView]:
        if allow_cache:
            result = proto_registry_utils.list_sorted_feature_views(
                self.cached_registry_proto, project, tags
            )
            if len(result) > 0:
                return result
        return self._get_and_cache_objects(
            "sorted_feature_views", self._list_sorted_feature_views, project, tags
        )

    def list_feature_services(
        self,
        project: str,
        allow_cache: bool = False,
        tags: Optional[dict[str, str]] = None,
    ) -> List[FeatureService]:
        if allow_cache:
            result = proto_registry_utils.list_feature_services(
                self.cached_registry_proto, project, tags
            )
            if len(result) > 0:
                return result
        return self._get_and_cache_objects(
            "feature_services", self._list_feature_services, project, tags
        )

    def list_saved_datasets(
        self,
        project: str,
        allow_cache: bool = False,
        tags: Optional[dict[str, str]] = None,
    ) -> List[SavedDataset]:
        if allow_cache:
            result = proto_registry_utils.list_saved_datasets(
                self.cached_registry_proto, project, tags
            )
            if len(result) > 0:
                return result
        return self._get_and_cache_objects(
            "saved_datasets", self._list_saved_datasets, project, tags
        )

    def list_validation_references(
        self,
        project: str,
        allow_cache: bool = False,
        tags: Optional[dict[str, str]] = None,
    ) -> List[ValidationReference]:
        if allow_cache:
            result = proto_registry_utils.list_validation_references(
                self.cached_registry_proto, project, tags
            )
            if len(result) > 0:
                return result
        return self._get_and_cache_objects(
            "validation_references", self._list_validation_references, project, tags
        )

    def list_permissions(
        self,
        project: str,
        allow_cache: bool = False,
        tags: Optional[dict[str, str]] = None,
    ) -> List[Permission]:
        if allow_cache:
            result = proto_registry_utils.list_permissions(
                self.cached_registry_proto, project, tags
            )
            if len(result) > 0:
                return result
        return self._get_and_cache_objects(
            "permissions", self._list_permissions, project, tags
        )
