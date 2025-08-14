import logging
import threading
import time
from datetime import datetime, timedelta
from pathlib import Path
from threading import Lock
from typing import Any, List, Optional, Set, Union

import httpx
from httpx import HTTPStatusError
from pydantic import StrictStr

from feast.base_feature_view import BaseFeatureView
from feast.data_source import DataSource, KafkaSource, PushSource, RequestSource
from feast.entity import Entity
from feast.errors import (
    DataSourceObjectNotFoundException,
    EntityNotFoundException,
    FeatureServiceNotFoundException,
    FeatureViewNotFoundException,
    ProjectMetadataNotFoundException,
    SortedFeatureViewNotFoundException,
)
from feast.expediagroup.pydantic_models.data_source_model import (
    KafkaSourceModel,
    PushSourceModel,
    RequestSourceModel,
    SparkSourceModel,
)
from feast.expediagroup.pydantic_models.entity_model import EntityModel
from feast.expediagroup.pydantic_models.feature_service import FeatureServiceModel
from feast.expediagroup.pydantic_models.feature_view_model import (
    FeatureViewModel,
    OnDemandFeatureViewModel,
    SortedFeatureViewModel,
)
from feast.expediagroup.pydantic_models.project_metadata_model import (
    ProjectMetadataModel,
)
from feast.feature_service import FeatureService
from feast.feature_view import FeatureView
from feast.infra.infra_object import Infra
from feast.infra.offline_stores.contrib.spark_offline_store.spark_source import (
    SparkSource,
)
from feast.infra.registry import proto_registry_utils
from feast.infra.registry.base_registry import BaseRegistry
from feast.on_demand_feature_view import OnDemandFeatureView
from feast.permissions.permission import Permission
from feast.project import Project
from feast.project_metadata import ProjectMetadata
from feast.protos.feast.core.Registry_pb2 import Registry as RegistryProto
from feast.repo_config import RegistryConfig
from feast.saved_dataset import SavedDataset, ValidationReference
from feast.sorted_feature_view import SortedFeatureView
from feast.stream_feature_view import StreamFeatureView

logger = logging.getLogger(__name__)


class HttpRegistryConfig(RegistryConfig):
    registry_type: StrictStr = "http"
    """ str: Provider name or a class name that implements Registry."""

    path: StrictStr = ""
    """ str: Endpoint of Feature registry.
    If registry_type is 'http', then this is a endpoint of Feature Registry """

    client_id: Optional[StrictStr] = "Unknown"


CACHE_REFRESH_THRESHOLD_SECONDS = 300


class HttpRegistry(BaseRegistry):
    def __init__(
        self,
        registry_config: Optional[Union[RegistryConfig, HttpRegistryConfig]],
        project: str,
        repo_path: Optional[Path],
    ):
        assert registry_config is not None, "HTTPRegistry needs a valid registry_config"
        # Timeouts in seconds
        timeout = httpx.Timeout(5.0, connect=60.0)
        transport = httpx.HTTPTransport(retries=3, verify=False)
        self.base_url = registry_config.path
        headers_dict = {
            "Content-Type": "application/json",
            "Client-Id": registry_config.client_id,
        }
        headers = httpx.Headers(
            {k: str(v) for k, v in headers_dict.items() if v is not None}
        )

        self.http_client = httpx.Client(
            timeout=timeout, transport=transport, headers=headers
        )
        self.project = project
        self.apply_project(Project(name=project))
        self.cached_registry_proto_created = datetime.utcnow()
        self._refresh_lock = Lock()
        self.cached_registry_proto_ttl = timedelta(
            seconds=(
                registry_config.cache_ttl_seconds
                if registry_config.cache_ttl_seconds is not None
                else 0
            )
        )
        self.cached_registry_proto = self.proto()
        self.stop_thread = False
        self.refresh_cache_thread = threading.Thread(target=self._refresh_cache)
        self.refresh_cache_thread.daemon = True
        self.refresh_cache_thread.start()

    def _refresh_cache(self):
        while not self.stop_thread:
            try:
                self.refresh()
            except Exception as e:
                logger.error(f"Registry refresh failed with exception: {e}")
            # Sleep for cached_registry_proto_ttl - 10 seconds
            # TODO: This process needs an update. There is not way to send kill signal to
            # the thread. When the cached_registry_proto_ttl is large, it will wait longer
            # before it terminates the thread.
            time.sleep(self.cached_registry_proto_ttl.total_seconds() - 10)

    def close(self):
        self.stop_thread = True
        self.refresh_cache_thread.join(10)

    def teardown(self):
        self.close()

    def _handle_exception(self, exception: Exception):
        logger.exception("Request failed with exception: %s", repr(exception))
        # If it's already an HTTPStatusError, re-raise it as is
        if isinstance(exception, HTTPStatusError):
            raise HTTPStatusError(
                "Request failed with exception: " + repr(exception),
                request=exception.request,
                response=exception.response,
            ) from exception
        raise httpx.HTTPError(
            "Request failed with exception: " + repr(exception)
        ) from exception

    def _send_request(self, method: str, url: str, params=None, data=None):
        try:
            request = httpx.Request(method=method, url=url, params=params, data=data)
            response = self.http_client.send(request)
            response.raise_for_status()
            return response.json()
        except httpx.HTTPError as http_exception:
            self._handle_exception(http_exception)
        except Exception as exception:
            self._handle_exception(exception)

    def apply_project_metadata(  # type: ignore[return]
        self,
        project: str,
        commit: bool = True,
    ) -> ProjectMetadataModel:
        try:
            url = f"{self.base_url}/projects"
            params = {"project": project, "commit": commit}
            response_data = self._send_request("PUT", url, params=params)
            return ProjectMetadataModel.model_validate(response_data)
        except Exception as exception:
            self._handle_exception(exception)

    def apply_entity(self, entity: Entity, project: str, commit: bool = True):
        try:
            url = f"{self.base_url}/projects/{project}/entities"
            data = EntityModel.from_entity(entity).model_dump_json()
            params = {"commit": commit}

            response_data = self._send_request("PUT", url, params=params, data=data)
            return EntityModel.model_validate(response_data).to_entity()
        except Exception as exception:
            self._handle_exception(exception)

    def delete_entity(self, name: str, project: str, commit: bool = True):
        try:
            url = f"{self.base_url}/projects/{project}/entities/{name}"
            params = {"commit": commit}
            self._send_request("DELETE", url, params=params)
            logger.info(f"Deleted Entity {name} from project {project}")
        except EntityNotFoundException as exception:
            logger.error(
                f"Entity {name} requested does not exist for deletion: {str(exception)}",
            )
            raise httpx.HTTPError(message=f"Entity: {name} not found")
        except Exception as exception:
            self._handle_exception(exception)

    def get_entity(  # type: ignore[return]
        self,
        name: str,
        project: str,
        allow_cache: bool = False,
    ) -> Entity:
        if allow_cache:
            self._check_if_registry_refreshed()
            return proto_registry_utils.get_entity(
                self.cached_registry_proto, name, project
            )
        try:
            url = f"{self.base_url}/projects/{project}/entities/{name}"
            response_data = self._send_request("GET", url)
            return EntityModel.model_validate(response_data).to_entity()
        except EntityNotFoundException as exception:
            logger.error(
                f"Entity {name} requested does not exist: {str(exception)}",
            )
            raise httpx.HTTPError(message=f"Entity: {name} not found")
        except Exception as exception:
            self._handle_exception(exception)

    def list_entities(  # type: ignore[return]
        self,
        project: str,
        allow_cache: bool = False,
        tags: Optional[dict[str, str]] = None,
    ) -> List[Entity]:
        if allow_cache:
            self._check_if_registry_refreshed()
            return proto_registry_utils.list_entities(
                self.cached_registry_proto, project, tags
            )
        try:
            url = f"{self.base_url}/projects/{project}/entities"
            response_data = self._send_request("GET", url)
            response_list = response_data if isinstance(response_data, list) else []
            return [
                EntityModel.model_validate(entity).to_entity()
                for entity in response_list
            ]
        except Exception as exception:
            self._handle_exception(exception)

    def apply_data_source(
        self, data_source: DataSource, project: str, commit: bool = True
    ):
        try:
            url = f"{self.base_url}/projects/{project}/data_sources"
            params = {"commit": commit}
            if isinstance(data_source, SparkSource):
                data = SparkSourceModel.from_data_source(data_source).model_dump_json()
                response_data = self._send_request("PUT", url, params=params, data=data)
                return SparkSourceModel.model_validate(response_data).to_data_source()
            elif isinstance(data_source, RequestSource):
                data = RequestSourceModel.from_data_source(
                    data_source
                ).model_dump_json()
                response_data = self._send_request("PUT", url, params=params, data=data)
                return RequestSourceModel.model_validate(response_data).to_data_source()
            elif isinstance(data_source, PushSource):
                data = PushSourceModel.from_data_source(data_source).model_dump_json()
                response_data = self._send_request("PUT", url, params=params, data=data)
                return PushSourceModel.model_validate(response_data).to_data_source()
            elif isinstance(data_source, KafkaSource):
                data = KafkaSourceModel.from_data_source(data_source).model_dump_json()
                response_data = self._send_request("PUT", url, params=params, data=data)
                return KafkaSourceModel.model_validate(response_data).to_data_source()
            else:
                raise TypeError(
                    "Unsupported DataSource type. Please use either SparkSource, RequestSource, PushSource or KafkaSource only"
                )
        except Exception as exception:
            self._handle_exception(exception)

    def delete_data_source(self, name: str, project: str, commit: bool = True):
        try:
            url = f"{self.base_url}/projects/{project}/data_sources/{name}"
            params = {"commit": commit}
            self._send_request("DELETE", url, params=params)
            logger.info(f"Deleted Datasource {name} from project {project}")
        except DataSourceObjectNotFoundException as exception:
            logger.error(
                f"Requested DataSource {name} does not exist for deletion: {str(exception)}",
            )
            raise httpx.HTTPError(message=f"DataSource: {name} not found")
        except Exception as exception:
            self._handle_exception(exception)

    def get_data_source(  # type: ignore[return]
        self,
        name: str,
        project: str,
        allow_cache: bool = False,
    ) -> DataSource:
        if allow_cache:
            self._check_if_registry_refreshed()
            return proto_registry_utils.get_data_source(
                self.cached_registry_proto, name, project
            )
        try:
            url = f"{self.base_url}/projects/{project}/data_sources/{name}"
            response_data = self._send_request("GET", url)
            if "model_type" in response_data:
                if response_data["model_type"] == "RequestSourceModel":
                    return RequestSourceModel.model_validate(
                        response_data
                    ).to_data_source()
                elif response_data["model_type"] == "SparkSourceModel":
                    return SparkSourceModel.model_validate(
                        response_data
                    ).to_data_source()
                elif response_data["model_type"] == "KafkaSourceModel":
                    return KafkaSourceModel.model_validate(
                        response_data
                    ).to_data_source()
            logger.error(f"Unable to parse object with response: {response_data}")
            raise ValueError("Unable to parse object")

        except DataSourceObjectNotFoundException as exception:
            logger.error(
                f"DataSource {name} requested does not exist: {str(exception)}",
            )
            raise httpx.HTTPError(message=f"DataSource: {name} not found")
        except Exception as exception:
            self._handle_exception(exception)

    def list_data_sources(  # type: ignore[return]
        self,
        project: str,
        allow_cache: bool = False,
        tags: Optional[dict[str, str]] = None,
    ) -> List[DataSource]:
        if allow_cache:
            self._check_if_registry_refreshed()
            return proto_registry_utils.list_data_sources(
                self.cached_registry_proto, project, tags
            )
        try:
            url = f"{self.base_url}/projects/{project}/data_sources"
            response_data = self._send_request("GET", url)
            response_list = response_data if isinstance(response_data, list) else []
            data_source_list: List[DataSource] = []
            for data_source in response_list:
                if "model_type" in data_source:
                    if data_source["model_type"] == "RequestSourceModel":
                        data_source_list.append(
                            RequestSourceModel.model_validate(
                                data_source
                            ).to_data_source()
                        )
                    elif data_source["model_type"] == "SparkSourceModel":
                        data_source_list.append(
                            SparkSourceModel.model_validate(
                                data_source
                            ).to_data_source()
                        )
                    elif data_source["model_type"] == "KafkaSourceModel":
                        data_source_list.append(
                            KafkaSourceModel.model_validate(
                                data_source
                            ).to_data_source()
                        )
                    else:
                        logger.error(
                            f"Unable to parse model_type for data_source response: {data_source}"
                        )
                        raise ValueError(
                            "Unable to parse object with data_source name: {data_source.name}"
                        )

            return data_source_list
        except Exception as exception:
            self._handle_exception(exception)

    def apply_feature_service(
        self, feature_service: FeatureService, project: str, commit: bool = True
    ):
        try:
            url = f"{self.base_url}/projects/{project}/feature_services"
            data = FeatureServiceModel.from_feature_service(
                feature_service
            ).model_dump_json()
            params = {"commit": commit}
            response_data = self._send_request("PUT", url, params=params, data=data)
            return FeatureServiceModel.model_validate(
                response_data
            ).to_feature_service()
        except Exception as exception:
            self._handle_exception(exception)

    def delete_feature_service(self, name: str, project: str, commit: bool = True):
        try:
            url = f"{self.base_url}/projects/{project}/feature_services/{name}"
            params = {"commit": commit}
            self._send_request("DELETE", url, params=params)
            logger.info(f"Deleted FeatureService {name} from project {project}")
        except FeatureServiceNotFoundException as exception:
            logger.error(
                f"FeatureService {name} requested does not exist for deletion: %s",
                str(exception),
            )
            raise httpx.HTTPError(message=f"FeatureService: {name} not found")
        except Exception as exception:
            self._handle_exception(exception)

    def get_feature_service(  # type: ignore[return]
        self,
        name: str,
        project: str,
        allow_cache: bool = False,
    ) -> FeatureService:
        if allow_cache:
            self._check_if_registry_refreshed()
            return proto_registry_utils.get_feature_service(
                self.cached_registry_proto, name, project
            )
        try:
            url = f"{self.base_url}/projects/{project}/feature_services/{name}"
            response_data = self._send_request("GET", url)
            return FeatureServiceModel.model_validate(
                response_data
            ).to_feature_service()
        except FeatureServiceNotFoundException as exception:
            logger.error(
                f"FeatureService {name} requested does not exist: %s", str(exception)
            )
            raise httpx.HTTPError(message=f"FeatureService: {name} not found")
        except Exception as exception:
            self._handle_exception(exception)

    def list_feature_services(  # type: ignore[return]
        self,
        project: str,
        allow_cache: bool = False,
        tags: Optional[dict[str, str]] = None,
    ) -> List[FeatureService]:
        if allow_cache:
            self._check_if_registry_refreshed()
            return proto_registry_utils.list_feature_services(
                self.cached_registry_proto, project, tags
            )
        try:
            url = f"{self.base_url}/projects/{project}/feature_services"
            response_data = self._send_request("GET", url)
            response_list = response_data if isinstance(response_data, list) else []
            return [
                FeatureServiceModel.model_validate(feature_service).to_feature_service()
                for feature_service in response_list
            ]
        except Exception as exception:
            self._handle_exception(exception)

    def apply_feature_view(
        self, feature_view: BaseFeatureView, project: str, commit: bool = True
    ):
        try:
            params = {"commit": commit}
            if isinstance(feature_view, SortedFeatureView):
                url = f"{self.base_url}/projects/{project}/feature_views"
                data = SortedFeatureViewModel.from_feature_view(
                    feature_view
                ).model_dump_json()
                response_data = self._send_request("PUT", url, params=params, data=data)
                return SortedFeatureViewModel.model_validate(
                    response_data
                ).to_feature_view()
            elif isinstance(feature_view, FeatureView):
                url = f"{self.base_url}/projects/{project}/feature_views"
                data = FeatureViewModel.from_feature_view(
                    feature_view
                ).model_dump_json()
                response_data = self._send_request("PUT", url, params=params, data=data)
                return FeatureViewModel.model_validate(response_data).to_feature_view()
            elif isinstance(feature_view, OnDemandFeatureView):
                url = f"{self.base_url}/projects/{project}/on_demand_feature_views"
                data = OnDemandFeatureViewModel.from_feature_view(
                    feature_view
                ).model_dump_json()
                response_data = self._send_request("PUT", url, params=params, data=data)
                return OnDemandFeatureViewModel.model_validate(
                    response_data
                ).to_feature_view()
            else:
                raise TypeError(
                    "Unsupported FeatureView type. Please use either FeatureView or OnDemandFeatureView only"
                )
        except Exception as exception:
            self._handle_exception(exception)

    def delete_feature_view(self, name: str, project: str, commit: bool = True):
        try:
            url = f"{self.base_url}/projects/{project}/feature_views/{name}"
            params = {"commit": commit}
            self._send_request("DELETE", url, params=params)
            logger.info(f"Deleted FeatureView {name} from project {project}")
        except FeatureViewNotFoundException as exception:
            logger.error(
                f"Requested FeatureView {name} does not exist for deletion: %s",
                str(exception),
            )
            raise httpx.HTTPError(message=f"FeatureView: {name} not found")
        except Exception as exception:
            self._handle_exception(exception)

    def get_feature_view(  # type: ignore[return]
        self,
        name: str,
        project: str,
        allow_cache: bool = False,
    ) -> FeatureView:
        if allow_cache:
            self._check_if_registry_refreshed()
            return proto_registry_utils.get_feature_view(
                self.cached_registry_proto, name, project
            )
        try:
            url = f"{self.base_url}/projects/{project}/feature_views/{name}"
            response_data = self._send_request("GET", url)
            return FeatureViewModel.model_validate(response_data).to_feature_view()
        except HTTPStatusError as http_exc:
            if http_exc.response.status_code == 404:
                logger.error("FeatureView %s not found", name)
                raise FeatureViewNotFoundException(name, project)
            self._handle_exception(http_exc)
        except Exception as exception:
            self._handle_exception(exception)

    def list_feature_views(  # type: ignore[return]
        self,
        project: str,
        allow_cache: bool = False,
        tags: Optional[dict[str, str]] = None,
    ) -> List[FeatureView]:
        if allow_cache:
            self._check_if_registry_refreshed()
            return proto_registry_utils.list_feature_views(
                self.cached_registry_proto, project, tags
            )
        try:
            url = f"{self.base_url}/projects/{project}/feature_views"
            response_data = self._send_request("GET", url)
            response_list = response_data if isinstance(response_data, list) else []
            return [
                FeatureViewModel.model_validate(feature_view).to_feature_view()
                for feature_view in response_list
            ]
        except Exception as exception:
            self._handle_exception(exception)

    def get_sorted_feature_view(  # type: ignore[return]
        self, name: str, project: str, allow_cache: bool = False
    ) -> SortedFeatureView:
        if allow_cache:
            self._check_if_registry_refreshed()
            return proto_registry_utils.get_sorted_feature_view(
                self.cached_registry_proto, name, project
            )
        try:
            url = f"{self.base_url}/projects/{project}/sorted_feature_views/{name}"
            response_data = self._send_request("GET", url)
            return SortedFeatureViewModel.model_validate(
                response_data
            ).to_feature_view()
        except HTTPStatusError as http_exc:
            if http_exc.response.status_code == 404:
                logger.error(
                    "SortedFeatureView %s not found.",
                    name,
                )
                raise SortedFeatureViewNotFoundException(name, project)
            self._handle_exception(http_exc)

        except Exception as exception:
            self._handle_exception(exception)

    def list_sorted_feature_views(  # type: ignore[return]
        self,
        project: str,
        allow_cache: bool = False,
        tags: Optional[dict[str, str]] = None,
    ) -> List[SortedFeatureView]:
        if allow_cache:
            self._check_if_registry_refreshed()
            return proto_registry_utils.list_sorted_feature_views(
                self.cached_registry_proto, project, tags
            )
        try:
            url = f"{self.base_url}/projects/{project}/sorted_feature_views"
            response_data = self._send_request("GET", url)
            response_list = response_data if isinstance(response_data, list) else []
            return [
                SortedFeatureViewModel.model_validate(sfv).to_feature_view()
                for sfv in response_list
            ]
        except Exception as exception:
            self._handle_exception(exception)

    def get_on_demand_feature_view(  # type: ignore[return]
        self, name: str, project: str, allow_cache: bool = False
    ) -> OnDemandFeatureView:
        if allow_cache:
            self._check_if_registry_refreshed()
            return proto_registry_utils.get_on_demand_feature_view(
                self.cached_registry_proto, name, project
            )
        try:
            url = f"{self.base_url}/projects/{project}/on_demand_feature_views/{name}"
            response_data = self._send_request("GET", url)
            return OnDemandFeatureViewModel.model_validate(
                response_data
            ).to_feature_view()
        except FeatureViewNotFoundException as exception:
            logger.error(
                f"FeatureView {name} requested does not exist: %s", str(exception)
            )
            raise httpx.HTTPError(message=f"FeatureView: {name} not found")
        except Exception as exception:
            self._handle_exception(exception)

    def list_on_demand_feature_views(  # type: ignore[return]
        self,
        project: str,
        allow_cache: bool = False,
        tags: Optional[dict[str, str]] = None,
    ) -> List[OnDemandFeatureView]:
        if allow_cache:
            self._check_if_registry_refreshed()
            return proto_registry_utils.list_on_demand_feature_views(
                self.cached_registry_proto, project, tags
            )
        try:
            url = f"{self.base_url}/projects/{project}/on_demand_feature_views"
            response_data = self._send_request("GET", url)
            response_list = response_data if isinstance(response_data, list) else []
            return [
                OnDemandFeatureViewModel.model_validate(feature_view).to_feature_view()
                for feature_view in response_list
            ]
        except Exception as exception:
            self._handle_exception(exception)

    def get_stream_feature_view(
        self,
        name: str,
        project: str,
        allow_cache: bool = False,
    ):
        raise NotImplementedError("Method not implemented")

    def list_stream_feature_views(
        self,
        project: str,
        allow_cache: bool = False,
        tags: Optional[dict[str, str]] = None,
    ) -> List[StreamFeatureView]:
        # TODO: Implement listing Stream Feature Views
        return []

    def apply_materialization(
        self,
        feature_view: FeatureView,
        project: str,
        start_date: datetime,
        end_date: datetime,
        commit: bool = True,
    ):
        try:
            feature_view.materialization_intervals.append((start_date, end_date))
            params = {"commit": commit}
            url = f"{self.base_url}/projects/{project}/feature_views"
            if isinstance(feature_view, SortedFeatureView):
                data = SortedFeatureViewModel.from_feature_view(
                    feature_view
                ).model_dump_json()
                response_data = self._send_request("PUT", url, params=params, data=data)
                return SortedFeatureViewModel.model_validate(
                    response_data
                ).to_feature_view()
            elif isinstance(feature_view, FeatureView):
                data = FeatureViewModel.from_feature_view(
                    feature_view
                ).model_dump_json()
                response_data = self._send_request("PUT", url, params=params, data=data)
                return FeatureViewModel.model_validate(response_data).to_feature_view()
            else:
                raise TypeError(
                    "Unsupported FeatureView type. Please use either FeatureView, SortedFeatureView or OnDemandFeatureView only"
                )
        except Exception as exception:
            self._handle_exception(exception)

    def apply_saved_dataset(
        self, saved_dataset: SavedDataset, project: str, commit: bool = True
    ):
        raise NotImplementedError("Method not implemented")

    def get_saved_dataset(
        self,
        name: str,
        project: str,
        allow_cache: bool = False,
    ) -> SavedDataset:
        raise NotImplementedError("Method not implemented")

    def list_saved_datasets(
        self,
        project: str,
        allow_cache: bool = False,
        tags: Optional[dict[str, str]] = None,
    ) -> List[SavedDataset]:
        return []

    def apply_validation_reference(
        self,
        validation_reference: ValidationReference,
        project: str,
        commit: bool = True,
    ):
        raise NotImplementedError("Method not implemented")

    def delete_validation_reference(self, name: str, project: str, commit: bool = True):
        raise NotImplementedError("Method not implemented")

    def get_validation_reference(
        self,
        name: str,
        project: str,
        allow_cache: bool = False,
    ) -> ValidationReference:
        raise NotImplementedError("Method not implemented")

    def update_infra(self, infra: Infra, project: str, commit: bool = True):
        raise NotImplementedError("Method not implemented")

    def get_infra(
        self,
        project: str,
        allow_cache: bool = False,
    ) -> Infra:
        # TODO: Need to implement this when necessary
        return Infra()

    def apply_user_metadata(
        self,
        project: str,
        feature_view: BaseFeatureView,
        metadata_bytes: Optional[bytes],
    ):
        raise NotImplementedError("Method not implemented")

    def get_user_metadata(
        self, project: str, feature_view: BaseFeatureView
    ) -> Optional[bytes]:
        raise NotImplementedError("Method not implemented")

    def list_validation_references(
        self,
        project: str,
        allow_cache: bool = False,
        tags: Optional[dict[str, str]] = None,
    ) -> List[ValidationReference]:
        return []

    def get_permission(
        self, name: str, project: str, allow_cache: bool = False
    ) -> Permission:
        # TODO: Need to implement this when necessary
        raise NotImplementedError("Method not implemented")

    def list_permissions(
        self,
        project: str,
        allow_cache: bool = False,
        tags: Optional[dict[str, str]] = None,
    ) -> List[Permission]:
        # TODO: Need to implement this when necessary
        return []

    def apply_permission(
        self, permission: Permission, project: str, commit: bool = True
    ):
        # TODO: Need to implement this when necessary
        pass

    def delete_permission(self, name: str, project: str, commit: bool = True):
        # TODO: Need to implement this when necessary
        pass

    def proto(self) -> RegistryProto:
        r = RegistryProto()
        # last_updated_timestamps = []
        if self.project is None:
            projects = self._get_all_projects()
        else:
            projects = [self.project]

        for project in projects:
            for lister, registry_proto_field in [
                (self.list_entities, r.entities),
                (self.list_feature_views, r.feature_views),
                (self.list_data_sources, r.data_sources),
                (self.list_on_demand_feature_views, r.on_demand_feature_views),
                (self.list_sorted_feature_views, r.sorted_feature_views),
                (self.list_stream_feature_views, r.stream_feature_views),
                (self.list_feature_services, r.feature_services),
                (self.list_saved_datasets, r.saved_datasets),
                (self.list_validation_references, r.validation_references),
                (self.list_project_metadata, r.project_metadata),
            ]:
                objs: List[Any] = lister(project)  # type: ignore
                if objs:
                    obj_protos = [obj.to_proto() for obj in objs]
                    for obj_proto in obj_protos:
                        if "spec" in obj_proto.DESCRIPTOR.fields_by_name:
                            obj_proto.spec.project = project
                        else:
                            obj_proto.project = project
                    registry_proto_field.extend(obj_protos)

            # This is suuuper jank. Because of https://github.com/feast-dev/feast/issues/2783,
            # the registry proto only has a single infra field, which we're currently setting as the "last" project.
            r.infra.CopyFrom(self.get_infra(project).to_proto())
            # last_updated_timestamps.append(self._get_last_updated_metadata(project))

        # if last_updated_timestamps:
        #     r.last_updated.FromDatetime(max(last_updated_timestamps))
        r.last_updated.FromDatetime(datetime.utcnow())

        return r

    def commit(self):
        # This method is a no-op since we're always writing values eagerly to the db.
        pass

    def apply_project(
        self,
        project: Project,
        commit: bool = True,
    ):  # type: ignore[return]
        """
        We were not applying projects before the addition of the line below this comment.
        When either validating features or trying to register them, the feature-store-feast-sdk would fail.
        cli.py --> validate.py --> instantiating FeatureStore obj --> instantiating HttpRegistry obj --> apply_project()
        No project was applied.
        We then query the feature store registry and get back a 404 for the project.
        Using apply_project_metadata(project.name) restores functionality.


        Update this with correct implimentation for Project Objects later.
        """
        self.apply_project_metadata(project.name)
        return None

    def delete_project(
        self,
        name: str,
        commit: bool = True,
    ):
        pass

    def get_any_feature_view(  # type: ignore
        self, name: str, project: str, allow_cache: bool = False
    ) -> BaseFeatureView:
        raise NotImplementedError("Method not implemented")

    def get_project(  # type: ignore
        self,
        name: str,
        allow_cache: bool = False,
    ) -> Project:
        raise NotImplementedError("Method not implemented")

    def list_all_feature_views(
        self,
        project: str,
        allow_cache: bool = False,
        tags: Optional[dict[str, str]] = None,
    ) -> List[BaseFeatureView]:
        return []

    def list_projects(
        self,
        allow_cache: bool = False,
        tags: Optional[dict[str, str]] = None,
    ) -> List[Project]:
        return []

    def refresh(self, project: Optional[str] = None):
        refreshed_cache_registry_proto = self.proto()
        with self._refresh_lock:
            self.cached_registry_proto = refreshed_cache_registry_proto
        self.cached_registry_proto_created = datetime.utcnow()

    def _refresh_cached_registry_if_necessary(self):
        with self._refresh_lock:
            expired = (
                self.cached_registry_proto is None
                or self.cached_registry_proto_created is None
            ) or (
                self.cached_registry_proto_ttl.total_seconds()
                > 0  # 0 ttl means infinity
                and (
                    datetime.utcnow()
                    > (
                        self.cached_registry_proto_created
                        + self.cached_registry_proto_ttl
                    )
                )
            )

            if expired:
                logger.info("Registry cache expired, so refreshing")
                self.refresh()

    def _check_if_registry_refreshed(self):
        if (
            self.cached_registry_proto is None
            or self.cached_registry_proto_created is None
        ) or (
            self.cached_registry_proto_ttl.total_seconds() > 0  # 0 ttl means infinity
            and (
                datetime.utcnow()
                > (self.cached_registry_proto_created + self.cached_registry_proto_ttl)
            )
        ):
            seconds_since_last_refresh = (
                datetime.utcnow() - self.cached_registry_proto_created
            ).total_seconds()
            if seconds_since_last_refresh > CACHE_REFRESH_THRESHOLD_SECONDS:
                logger.warning(
                    f"Cache is stale: {seconds_since_last_refresh} seconds since last refresh"
                )

    def _get_all_projects(self) -> Set[str]:  # type: ignore[return]
        try:
            url = f"{self.base_url}/projects"
            projects = self._send_request("GET", url)
            return {project["project_name"] for project in projects}
        except Exception as exception:
            self._handle_exception(exception)

    def _get_last_updated_metadata(self, project: str):
        try:
            url = f"{self.base_url}/projects/{project}"
            response_data = self._send_request("GET", url)
            return datetime.strptime(
                response_data["last_updated_timestamp"], "%Y-%m-%dT%H:%M:%S"
            )
        except Exception as exception:
            self._handle_exception(exception)

    def list_project_metadata(  # type: ignore[return]
        self, project: str, allow_cache: bool = False
    ) -> List[ProjectMetadata]:
        if allow_cache:
            self._check_if_registry_refreshed()
            return proto_registry_utils.list_project_metadata(
                self.cached_registry_proto, project
            )
        try:
            url = f"{self.base_url}/projects/{project}"
            response_data = self._send_request("GET", url)
            return [
                ProjectMetadataModel.model_validate(response_data).to_project_metadata()
            ]
        except ProjectMetadataNotFoundException as exception:
            logger.error(
                f"Project {project} requested does not exist: {str(exception)}"
            )
            raise httpx.HTTPError(message=f"ProjectMetadata: {project} not found")
        except Exception as exception:
            self._handle_exception(exception)
