import atexit
import logging
import threading
import warnings
from abc import abstractmethod
from datetime import timedelta
from threading import Lock
from typing import List, Optional

from feast.base_feature_view import BaseFeatureView
from feast.data_source import DataSource
from feast.entity import Entity
from feast.feature_service import FeatureService
from feast.feature_view import FeatureView
from feast.infra.infra_object import Infra
from feast.infra.registry import proto_registry_utils
from feast.infra.registry.base_registry import BaseRegistry
from feast.on_demand_feature_view import OnDemandFeatureView
from feast.permissions.permission import Permission
from feast.project import Project
from feast.project_metadata import ProjectMetadata
from feast.protos.feast.core.Registry_pb2 import Registry as RegistryProto
from feast.saved_dataset import SavedDataset, ValidationReference
from feast.stream_feature_view import StreamFeatureView
from feast.utils import _utc_now

logger = logging.getLogger(__name__)


class CachingRegistry(BaseRegistry):
    def __init__(self, project: str, cache_ttl_seconds: int, cache_mode: str):
        self.cache_mode = cache_mode
        self.cached_registry_proto = RegistryProto()
        self._refresh_lock = Lock()
        self.cached_registry_proto_ttl = timedelta(
            seconds=cache_ttl_seconds if cache_ttl_seconds is not None else 0
        )
        self.cached_registry_proto = self.proto()
        self.cached_registry_proto_created = _utc_now()
        if cache_mode == "thread":
            self._start_thread_async_refresh(cache_ttl_seconds)
            atexit.register(self._exit_handler)

    @abstractmethod
    def _get_data_source(self, name: str, project: str) -> DataSource:
        pass

    def get_data_source(
        self, name: str, project: str, allow_cache: bool = False
    ) -> DataSource:
        if allow_cache:
            self._refresh_cached_registry_if_necessary()
            return proto_registry_utils.get_data_source(
                self.cached_registry_proto, name, project
            )
        return self._get_data_source(name, project)

    @abstractmethod
    def _list_data_sources(
        self, project: str, tags: Optional[dict[str, str]]
    ) -> List[DataSource]:
        pass

    def list_data_sources(
        self,
        project: str,
        allow_cache: bool = False,
        tags: Optional[dict[str, str]] = None,
    ) -> List[DataSource]:
        if allow_cache:
            self._refresh_cached_registry_if_necessary()
            return proto_registry_utils.list_data_sources(
                self.cached_registry_proto, project, tags
            )
        return self._list_data_sources(project, tags)

    @abstractmethod
    def _get_entity(self, name: str, project: str) -> Entity:
        pass

    def get_entity(self, name: str, project: str, allow_cache: bool = False) -> Entity:
        if allow_cache:
            self._refresh_cached_registry_if_necessary()
            return proto_registry_utils.get_entity(
                self.cached_registry_proto, name, project
            )
        return self._get_entity(name, project)

    @abstractmethod
    def _list_entities(
        self, project: str, tags: Optional[dict[str, str]]
    ) -> List[Entity]:
        pass

    def list_entities(
        self,
        project: str,
        allow_cache: bool = False,
        tags: Optional[dict[str, str]] = None,
    ) -> List[Entity]:
        if allow_cache:
            self._refresh_cached_registry_if_necessary()
            return proto_registry_utils.list_entities(
                self.cached_registry_proto, project, tags
            )
        return self._list_entities(project, tags)

    @abstractmethod
    def _get_any_feature_view(self, name: str, project: str) -> BaseFeatureView:
        pass

    def get_any_feature_view(
        self, name: str, project: str, allow_cache: bool = False
    ) -> BaseFeatureView:
        if allow_cache:
            self._refresh_cached_registry_if_necessary()
            return proto_registry_utils.get_any_feature_view(
                self.cached_registry_proto, name, project
            )
        return self._get_any_feature_view(name, project)

    @abstractmethod
    def _list_all_feature_views(
        self, project: str, tags: Optional[dict[str, str]]
    ) -> List[BaseFeatureView]:
        pass

    def list_all_feature_views(
        self,
        project: str,
        allow_cache: bool = False,
        tags: Optional[dict[str, str]] = None,
    ) -> List[BaseFeatureView]:
        if allow_cache:
            self._refresh_cached_registry_if_necessary()
            return proto_registry_utils.list_all_feature_views(
                self.cached_registry_proto, project, tags
            )
        return self._list_all_feature_views(project, tags)

    @abstractmethod
    def _get_feature_view(self, name: str, project: str) -> FeatureView:
        pass

    def get_feature_view(
        self, name: str, project: str, allow_cache: bool = False
    ) -> FeatureView:
        if allow_cache:
            self._refresh_cached_registry_if_necessary()
            return proto_registry_utils.get_feature_view(
                self.cached_registry_proto, name, project
            )
        return self._get_feature_view(name, project)

    @abstractmethod
    def _list_feature_views(
        self, project: str, tags: Optional[dict[str, str]]
    ) -> List[FeatureView]:
        pass

    def list_feature_views(
        self,
        project: str,
        allow_cache: bool = False,
        tags: Optional[dict[str, str]] = None,
    ) -> List[FeatureView]:
        if allow_cache:
            self._refresh_cached_registry_if_necessary()
            return proto_registry_utils.list_feature_views(
                self.cached_registry_proto, project, tags
            )
        return self._list_feature_views(project, tags)

    @abstractmethod
    def _get_on_demand_feature_view(
        self, name: str, project: str
    ) -> OnDemandFeatureView:
        pass

    def get_on_demand_feature_view(
        self, name: str, project: str, allow_cache: bool = False
    ) -> OnDemandFeatureView:
        if allow_cache:
            self._refresh_cached_registry_if_necessary()
            return proto_registry_utils.get_on_demand_feature_view(
                self.cached_registry_proto, name, project
            )
        return self._get_on_demand_feature_view(name, project)

    @abstractmethod
    def _list_on_demand_feature_views(
        self, project: str, tags: Optional[dict[str, str]]
    ) -> List[OnDemandFeatureView]:
        pass

    def list_on_demand_feature_views(
        self,
        project: str,
        allow_cache: bool = False,
        tags: Optional[dict[str, str]] = None,
    ) -> List[OnDemandFeatureView]:
        if allow_cache:
            self._refresh_cached_registry_if_necessary()
            return proto_registry_utils.list_on_demand_feature_views(
                self.cached_registry_proto, project, tags
            )
        return self._list_on_demand_feature_views(project, tags)

    @abstractmethod
    def _get_stream_feature_view(self, name: str, project: str) -> StreamFeatureView:
        pass

    def get_stream_feature_view(
        self, name: str, project: str, allow_cache: bool = False
    ) -> StreamFeatureView:
        if allow_cache:
            self._refresh_cached_registry_if_necessary()
            return proto_registry_utils.get_stream_feature_view(
                self.cached_registry_proto, name, project
            )
        return self._get_stream_feature_view(name, project)

    @abstractmethod
    def _list_stream_feature_views(
        self, project: str, tags: Optional[dict[str, str]]
    ) -> List[StreamFeatureView]:
        pass

    def list_stream_feature_views(
        self,
        project: str,
        allow_cache: bool = False,
        tags: Optional[dict[str, str]] = None,
    ) -> List[StreamFeatureView]:
        if allow_cache:
            self._refresh_cached_registry_if_necessary()
            return proto_registry_utils.list_stream_feature_views(
                self.cached_registry_proto, project, tags
            )
        return self._list_stream_feature_views(project, tags)

    @abstractmethod
    def _get_feature_service(self, name: str, project: str) -> FeatureService:
        pass

    def get_feature_service(
        self, name: str, project: str, allow_cache: bool = False
    ) -> FeatureService:
        if allow_cache:
            self._refresh_cached_registry_if_necessary()
            return proto_registry_utils.get_feature_service(
                self.cached_registry_proto, name, project
            )
        return self._get_feature_service(name, project)

    @abstractmethod
    def _list_feature_services(
        self, project: str, tags: Optional[dict[str, str]]
    ) -> List[FeatureService]:
        pass

    def list_feature_services(
        self,
        project: str,
        allow_cache: bool = False,
        tags: Optional[dict[str, str]] = None,
    ) -> List[FeatureService]:
        if allow_cache:
            self._refresh_cached_registry_if_necessary()
            return proto_registry_utils.list_feature_services(
                self.cached_registry_proto, project, tags
            )
        return self._list_feature_services(project, tags)

    @abstractmethod
    def _get_saved_dataset(self, name: str, project: str) -> SavedDataset:
        pass

    def get_saved_dataset(
        self, name: str, project: str, allow_cache: bool = False
    ) -> SavedDataset:
        if allow_cache:
            self._refresh_cached_registry_if_necessary()
            return proto_registry_utils.get_saved_dataset(
                self.cached_registry_proto, name, project
            )
        return self._get_saved_dataset(name, project)

    @abstractmethod
    def _list_saved_datasets(
        self, project: str, tags: Optional[dict[str, str]] = None
    ) -> List[SavedDataset]:
        pass

    def list_saved_datasets(
        self,
        project: str,
        allow_cache: bool = False,
        tags: Optional[dict[str, str]] = None,
    ) -> List[SavedDataset]:
        if allow_cache:
            self._refresh_cached_registry_if_necessary()
            return proto_registry_utils.list_saved_datasets(
                self.cached_registry_proto, project, tags
            )
        return self._list_saved_datasets(project, tags)

    @abstractmethod
    def _get_validation_reference(self, name: str, project: str) -> ValidationReference:
        pass

    def get_validation_reference(
        self, name: str, project: str, allow_cache: bool = False
    ) -> ValidationReference:
        if allow_cache:
            self._refresh_cached_registry_if_necessary()
            return proto_registry_utils.get_validation_reference(
                self.cached_registry_proto, name, project
            )
        return self._get_validation_reference(name, project)

    @abstractmethod
    def _list_validation_references(
        self, project: str, tags: Optional[dict[str, str]] = None
    ) -> List[ValidationReference]:
        pass

    def list_validation_references(
        self,
        project: str,
        allow_cache: bool = False,
        tags: Optional[dict[str, str]] = None,
    ) -> List[ValidationReference]:
        if allow_cache:
            self._refresh_cached_registry_if_necessary()
            return proto_registry_utils.list_validation_references(
                self.cached_registry_proto, project, tags
            )
        return self._list_validation_references(project, tags)

    @abstractmethod
    def _list_project_metadata(self, project: str) -> List[ProjectMetadata]:
        pass

    def list_project_metadata(
        self, project: str, allow_cache: bool = False
    ) -> List[ProjectMetadata]:
        warnings.warn(
            "list_project_metadata is deprecated and will be removed in a future version. Use list_projects() and get_project() methods instead.",
            DeprecationWarning,
        )
        if allow_cache:
            self._refresh_cached_registry_if_necessary()
            return proto_registry_utils.list_project_metadata(
                self.cached_registry_proto, project
            )
        return self._list_project_metadata(project)

    @abstractmethod
    def _get_infra(self, project: str) -> Infra:
        pass

    def get_infra(self, project: str, allow_cache: bool = False) -> Infra:
        return self._get_infra(project)

    @abstractmethod
    def _get_permission(self, name: str, project: str) -> Permission:
        pass

    def get_permission(
        self, name: str, project: str, allow_cache: bool = False
    ) -> Permission:
        if allow_cache:
            self._refresh_cached_registry_if_necessary()
            return proto_registry_utils.get_permission(
                self.cached_registry_proto, name, project
            )
        return self._get_permission(name, project)

    @abstractmethod
    def _list_permissions(
        self, project: str, tags: Optional[dict[str, str]]
    ) -> List[Permission]:
        pass

    def list_permissions(
        self,
        project: str,
        allow_cache: bool = False,
        tags: Optional[dict[str, str]] = None,
    ) -> List[Permission]:
        if allow_cache:
            self._refresh_cached_registry_if_necessary()
            return proto_registry_utils.list_permissions(
                self.cached_registry_proto, project, tags
            )
        return self._list_permissions(project, tags)

    @abstractmethod
    def _get_project(self, name: str) -> Project:
        pass

    def get_project(
        self,
        name: str,
        allow_cache: bool = False,
    ) -> Project:
        if allow_cache:
            self._refresh_cached_registry_if_necessary()
            return proto_registry_utils.get_project(self.cached_registry_proto, name)
        return self._get_project(name)

    @abstractmethod
    def _list_projects(self, tags: Optional[dict[str, str]]) -> List[Project]:
        pass

    def list_projects(
        self,
        allow_cache: bool = False,
        tags: Optional[dict[str, str]] = None,
    ) -> List[Project]:
        if allow_cache:
            self._refresh_cached_registry_if_necessary()
            return proto_registry_utils.list_projects(self.cached_registry_proto, tags)
        return self._list_projects(tags)

    def refresh(self, project: Optional[str] = None):
        try:
            self.cached_registry_proto = self.proto()
            self.cached_registry_proto_created = _utc_now()
        except Exception as e:
            logger.debug(f"Error while refreshing registry: {e}", exc_info=True)

    def _refresh_cached_registry_if_necessary(self):
        if self.cache_mode == "sync":

            def is_cache_expired():
                if (
                    self.cached_registry_proto is None
                    or self.cached_registry_proto == RegistryProto()
                ):
                    return True

                # Cache is expired if creation time is None
                if (
                    not hasattr(self, "cached_registry_proto_created")
                    or self.cached_registry_proto_created is None
                ):
                    return True

                # Cache is expired if TTL > 0 and current time exceeds creation + TTL
                if self.cached_registry_proto_ttl.total_seconds() > 0 and _utc_now() > (
                    self.cached_registry_proto_created + self.cached_registry_proto_ttl
                ):
                    return True

                return False

            if is_cache_expired():
                if not self._refresh_lock.acquire(blocking=False):
                    logger.debug(
                        "Skipping refresh if lock is already held by another thread"
                    )
                    return
                try:
                    logger.info(
                        f"Registry cache expired(ttl: {self.cached_registry_proto_ttl.total_seconds()} seconds), so refreshing"
                    )
                    self.refresh()
                except Exception as e:
                    logger.debug(
                        f"Error in _refresh_cached_registry_if_necessary: {e}",
                        exc_info=True,
                    )
                finally:
                    self._refresh_lock.release()

    def _start_thread_async_refresh(self, cache_ttl_seconds):
        self.refresh()
        if cache_ttl_seconds <= 0:
            return
        self.registry_refresh_thread = threading.Timer(
            cache_ttl_seconds, self._start_thread_async_refresh, [cache_ttl_seconds]
        )
        self.registry_refresh_thread.daemon = True
        self.registry_refresh_thread.start()

    def _exit_handler(self):
        self.registry_refresh_thread.cancel()
