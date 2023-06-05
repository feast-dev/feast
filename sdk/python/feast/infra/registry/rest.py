import uuid
import logging
from datetime import datetime, timedelta
from pathlib import Path
from threading import Lock
from typing import Any, Callable, List, Optional, Set, Union

from pydantic import StrictStr

import requests
import json
import base64

from feast.base_feature_view import BaseFeatureView
from feast.data_source import DataSource
from feast.entity import Entity
from feast.errors import (
    DataSourceObjectNotFoundException,
    EntityNotFoundException,
    FeatureServiceNotFoundException,
    FeatureViewNotFoundException,
    SavedDatasetNotFound,
    ValidationReferenceNotFound,
)
from feast.feature_service import FeatureService
from feast.feature_view import FeatureView
from feast.infra.infra_object import Infra

from feast.infra.registry.base_registry import BaseRegistry
from feast.on_demand_feature_view import OnDemandFeatureView
from feast.project_metadata import ProjectMetadata
from feast.protos.feast.core.DataSource_pb2 import DataSource as DataSourceProto
from feast.protos.feast.core.Entity_pb2 import Entity as EntityProto
from feast.protos.feast.core.FeatureService_pb2 import (
    FeatureService as FeatureServiceProto,
)
from feast.protos.feast.core.FeatureView_pb2 import FeatureView as FeatureViewProto
from feast.protos.feast.core.InfraObject_pb2 import Infra as InfraProto
from feast.protos.feast.core.OnDemandFeatureView_pb2 import (
    OnDemandFeatureView as OnDemandFeatureViewProto,
)
from feast.protos.feast.core.Registry_pb2 import Registry as RegistryProto
from feast.protos.feast.core.RequestFeatureView_pb2 import (
    RequestFeatureView as RequestFeatureViewProto,
)
from feast.protos.feast.core.SavedDataset_pb2 import SavedDataset as SavedDatasetProto
from feast.protos.feast.core.StreamFeatureView_pb2 import (
    StreamFeatureView as StreamFeatureViewProto,
)
from feast.protos.feast.core.ValidationProfile_pb2 import (
    ValidationReference as ValidationReferenceProto,
)
from feast.protos.feast.core.Registry_pb2 import ProjectMetadata as ProjectMetadataProto
from feast.repo_config import RegistryConfig
from feast.request_feature_view import RequestFeatureView
from feast.saved_dataset import SavedDataset, ValidationReference
from feast.stream_feature_view import StreamFeatureView


logger = logging.getLogger(__name__)


class CustomJsonEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        elif isinstance(obj, bytes):
            return base64.b64encode(
                obj
            ).decode('ascii')

        return super().default(obj)


class CustomJsonDecoder(json.JSONDecoder):
    def decode(self, obj):
        rv = super().decode(obj)
        if isinstance(rv, dict):
            if 'datetime' in rv:
                rv["datetime"] = datetime.fromisoformat(rv["datetime"])
            if 'metadata_bytes' in rv:
                rv["user_metadata"] = base64.b64decode(
                    rv["user_metadata"].encode('ascii')
                )
            if 'protostring' in rv:
                rv["protostring"] = base64.b64decode(
                    rv["protostring"].encode('ascii')
                )
            if 'protostrings' in rv:
                rv["protostrings"] = [
                    base64.b64decode(
                        protostring.encode('ascii')
                    )
                    for protostring in rv["protostrings"]
                ]

        return rv


class RestRegistryConfig(RegistryConfig):
    registry_type: StrictStr = "rest"
    """ str: Provider name or a class name that implements Registry."""

    path: StrictStr = ""
    """ str: Path to REST served metadata store.
    If registry_type is 'rest', then this is a RESTful URL that serves the Registry """


class RestRegistry(BaseRegistry):

    def __init__(
        self,
        registry_config: Optional[Union[RegistryConfig, RestRegistryConfig]],
        project: str,
        repo_path: Optional[Path],
    ):
        assert registry_config is not None, "RestRegistry needs a valid registry_config"
        self.uri = registry_config.path
        self.json_encoder = CustomJsonEncoder
        self.json_decoder = CustomJsonDecoder

        self.project = project

    # CRUD operations
    @staticmethod
    def _content_type(
        data, jsonEncoder=json.JSONEncoder
    ):  # returns converted data, {"Content-Type": }
        if isinstance(data, dict):
            return json.dumps(data, cls=jsonEncoder), {
                "Content-Type": "application/json"
            }
        bytes_data = bytes(data) if not isinstance(data, bytes) else data
        bytes_data_len = len(bytes_data)
        return bytes_data, {
            "Content-Type": "application/octet-stream",
            "Content-Length": str(bytes_data_len),
        }

    def _manage_CRUD_request(
        self, request_func, endpoint, data=None, params={}, files=None
    ):
        uri = f"{self.uri}{endpoint}"

        if data is None and files is None:
            response = request_func(url=uri, params=params)
        elif data is not None and (files is None or len(files) == 0):
            reqdata, header = self._content_type(data, self.json_encoder)
            response = request_func(
                url=uri, params=params, data=reqdata, headers=header
            )
        else:  # data and files
            response = request_func(url=uri, params=params, data=data, files=files)
        return response

    def _delete(self, endpoint, data=None, params={}):
        return self._manage_CRUD_request(requests.delete, endpoint, data, params)

    def _get(self, endpoint, data=None, params={}):
        return self._manage_CRUD_request(requests.get, endpoint, data, params)

    def _patch(self, endpoint, data=None, params={}, files=None):
        return self._manage_CRUD_request(requests.patch, endpoint, data, params, files)

    def _post(self, endpoint, data=None, params={}, files=None):
        return self._manage_CRUD_request(requests.post, endpoint, data, params, files)

    def _put(self, endpoint, data=None, params={}, files=None):
        return self._manage_CRUD_request(requests.put, endpoint, data, params, files)

    def _apply_object(
        self,
        resource: str,
        project: str,
        id_field_name: str,
        obj: Any,
        proto_field_name: str,
        name: Optional[str] = None,
    ):
        name = name or (obj.name if hasattr(obj, "name") else None)
        response = self._post(
            f"/{project}",
            data={
                "proto": obj.to_proto().SerializeToString(),
                "last_updated_timestamp": datetime.utcnow(),
            },
            params={
                "resource": resource,
                # "id_field_name": id_field_name,
                "name": name,
            }
        )
        response_json = json.loads(
            response.content, cls=self.json_decoder
        )
        if response.status_code != 200:
            raise RuntimeError(f"Failed to apply object: Project={project} {id_field_name}={name} (E{response.status_code}: {response_json})")

    def _delete_object(
        self,
        resource: str,
        name: str,
        project: str,
        id_field_name: str,
        not_found_exception: Optional[Callable],
    ):
        response = self._delete(
            f"/{project}",
            params={
                "resource": resource,
                # "id_field_name": id_field_name,
                "name": name,
            }
        )
        response_json = json.loads(
            response.content, cls=self.json_decoder
        )
        if response.status_code == 404:
            if not_found_exception:
                raise not_found_exception(name, project)
            return 0
        return response_json["count"]

    def _get_object(
        self,
        resource: str,
        name: str,
        project: str,
        proto_class: Any,
        python_class: Any,
        id_field_name: str,
        proto_field_name: str,
        not_found_exception: Optional[Callable],
    ):
        response = self._get(
            f"/{project}",
            params={
                "resource": resource,
                # "id_field_name": id_field_name,
                "name": name,
            }
        )
        response_json = json.loads(
            response.content, cls=self.json_decoder
        )
        if response.status_code == 404:
            if not_found_exception:
                raise not_found_exception(name, project)
            else:
                return None
        _proto = proto_class.FromString(response_json["protostring"])
        return python_class.from_proto(_proto)

    def _list_objects(
        self,
        resource: str,
        project: str,
        proto_class: Any,
        python_class: Any,
        proto_field_name: str,
    ):
        response = self._get(
            f"/{project}/list",
            params={
                "resource": resource
            }
        )
        response_json = json.loads(
            response.content, cls=self.json_decoder
        )
        if response.status_code == 200:
            return [
                python_class.from_proto(
                    proto_class.FromString(
                        proto_field
                    )
                )
                for proto_field in response_json["protostrings"]
            ]
        return []

    def _get_all_projects(self) -> Set[str]:
        response = self._get(
            "/projects",
        )
        if response.status_code != 200:
            return set()

        response_json = json.loads(
            response.content, cls=self.json_decoder
        )
        return set(map(
            lambda protostring: protostring.decode('ascii'),
            response_json["protostrings"]
        ))

    def _get_last_updated_metadata(self, project: str) -> datetime:
        response = self._get(
            f"/{project}/last_updated",
        )
        response_json = json.loads(
            response.content, cls=self.json_decoder
        )
        return response_json["datetime"]

    # Entity operations
    def apply_entity(self, entity: Entity, project: str, commit: bool = True):
        self._apply_object(
            resource="entity",
            project=project,
            id_field_name="entity_name",
            obj=entity,
            proto_field_name="entity_proto",
        )

    def delete_entity(self, name: str, project: str, commit: bool = True):
        return self._delete_object(
            resource="entity",
            name=name,
            project=project,
            id_field_name="entity_name",
            not_found_exception=EntityNotFoundException
        )

    def get_entity(self, name: str, project: str, allow_cache: bool = False) -> Entity:
        return self._get_object(
            resource="entity",
            name=name,
            project=project,
            proto_class=EntityProto,
            python_class=Entity,
            id_field_name="entity_name",
            proto_field_name="entity_proto",
            not_found_exception=EntityNotFoundException,
        )

    def list_entities(self, project: str, allow_cache: bool = False) -> List[Entity]:
        return self._list_objects(
            resource="entity",
            project=project,
            proto_class=EntityProto,
            python_class=Entity,
            proto_field_name="entity_proto"
        )

    # Data source operations
    def apply_data_source(
        self, data_source: DataSource, project: str, commit: bool = True
    ):
        self._apply_object(
            resource="data_source",
            project=project,
            id_field_name="data_source_name",
            obj=data_source,
            proto_field_name="data_source_proto",
        )

    def delete_data_source(self, name: str, project: str, commit: bool = True):
        return self._delete_object(
            resource="data_source",
            name=name,
            project=project,
            id_field_name="data_source_name",
            not_found_exception=DataSourceObjectNotFoundException
        )

    def get_data_source(
        self, name: str, project: str, allow_cache: bool = False
    ) -> DataSource:
        return self._get_object(
            resource="data_source",
            name=name,
            project=project,
            proto_class=DataSourceProto,
            python_class=DataSource,
            id_field_name="data_source_name",
            proto_field_name="data_source_proto",
            not_found_exception=DataSourceObjectNotFoundException,
        )

    def list_data_sources(
        self, project: str, allow_cache: bool = False
    ) -> List[DataSource]:
        return self._list_objects(
            resource="data_source",
            project=project,
            proto_class=DataSourceProto,
            python_class=DataSource,
            proto_field_name="data_source_proto"
        )

    # Feature service operations
    def apply_feature_service(
        self, feature_service: FeatureService, project: str, commit: bool = True
    ):
        self._apply_object(
            resource="feature_service",
            project=project,
            id_field_name="feature_service_name",
            obj=feature_service,
            proto_field_name="feature_service_proto",
        )

    def delete_feature_service(self, name: str, project: str, commit: bool = True):
        return self._delete_object(
            resource="feature_service",
            name=name,
            project=project,
            id_field_name="feature_service_name",
            not_found_exception=FeatureServiceNotFoundException
        )

    def get_feature_service(
        self, name: str, project: str, allow_cache: bool = False
    ) -> FeatureService:
        return self._get_object(
            resource="feature_service",
            name=name,
            project=project,
            proto_class=FeatureServiceProto,
            python_class=FeatureService,
            id_field_name="feature_service_name",
            proto_field_name="feature_service_proto",
            not_found_exception=FeatureServiceNotFoundException,
        )

    def list_feature_services(
        self, project: str, allow_cache: bool = False
    ) -> List[FeatureService]:
        return self._list_objects(
            resource="feature_service",
            project=project,
            proto_class=FeatureServiceProto,
            python_class=FeatureService,
            proto_field_name="feature_service_proto"
        )

    # Feature view operations
    def apply_feature_view(
        self, feature_view: BaseFeatureView, project: str, commit: bool = True
    ):
        self._apply_object(
            resource=self._infer_fv_resource(feature_view),
            project=project,
            id_field_name="feature_view_name",
            obj=feature_view,
            proto_field_name="feature_view_proto",
        )

    def delete_feature_view(self, name: str, project: str, commit: bool = True):
        return self._delete_object(
            resource="feature_view",
            name=name,
            project=project,
            id_field_name="feature_view_name",
            not_found_exception=FeatureViewNotFoundException
        )

    # stream feature view operations
    def get_stream_feature_view(
        self, name: str, project: str, allow_cache: bool = False
    ):
        return self._get_object(
            resource="stream_feature_view",
            name=name,
            project=project,
            proto_class=StreamFeatureViewProto,
            python_class=StreamFeatureView,
            id_field_name="feature_view_name",
            proto_field_name="feature_view_proto",
            not_found_exception=FeatureViewNotFoundException,
        )

    def list_stream_feature_views(
        self, project: str, allow_cache: bool = False
    ) -> List[StreamFeatureView]:
        return self._list_objects(
            resource="stream_feature_view",
            project=project,
            proto_class=FeatureViewProto,
            python_class=FeatureView,
            proto_field_name="feature_view_proto"
        )

    # on demand feature view operations
    def get_on_demand_feature_view(
        self, name: str, project: str, allow_cache: bool = False
    ) -> OnDemandFeatureView:
        return self._get_object(
            resource="on_demand_feature_view",
            name=name,
            project=project,
            proto_class=OnDemandFeatureViewProto,
            python_class=OnDemandFeatureView,
            id_field_name="feature_view_name",
            proto_field_name="feature_view_proto",
            not_found_exception=FeatureViewNotFoundException,
        )

    def list_on_demand_feature_views(
        self, project: str, allow_cache: bool = False
    ) -> List[OnDemandFeatureView]:
        return self._list_objects(
            resource="on_demand_feature_view",
            project=project,
            proto_class=OnDemandFeatureViewProto,
            python_class=OnDemandFeatureView,
            proto_field_name="feature_view_proto"
        )

    # regular feature view operations
    def get_feature_view(
        self, name: str, project: str, allow_cache: bool = False
    ) -> FeatureView:
        return self._get_object(
            resource="feature_view",
            name=name,
            project=project,
            proto_class=FeatureViewProto,
            python_class=FeatureView,
            id_field_name="feature_view_name",
            proto_field_name="feature_view_proto",
            not_found_exception=FeatureViewNotFoundException,
        )

    def list_feature_views(
        self, project: str, allow_cache: bool = False
    ) -> List[FeatureView]:
        return self._list_objects(
            resource="feature_view",
            project=project,
            proto_class=FeatureViewProto,
            python_class=FeatureView,
            proto_field_name="feature_view_proto"
        )

    # request feature view operations
    def get_request_feature_view(self, name: str, project: str) -> RequestFeatureView:
        return self._get_object(
            resource="request_feature_view",
            name=name,
            project=project,
            proto_class=RequestFeatureViewProto,
            python_class=RequestFeatureView,
            id_field_name="feature_view_name",
            proto_field_name="feature_view_proto",
            not_found_exception=FeatureViewNotFoundException,
        )

    def list_request_feature_views(
        self, project: str, allow_cache: bool = False
    ) -> List[RequestFeatureView]:
        return self._list_objects(
            resource="request_feature_view",
            project=project,
            proto_class=RequestFeatureViewProto,
            python_class=RequestFeatureView,
            proto_field_name="feature_view_proto"
        )

    def apply_materialization(
        self,
        feature_view: FeatureView,
        project: str,
        start_date: datetime,
        end_date: datetime,
        commit: bool = True,
    ):
        resource = self._infer_fv_resource(feature_view)
        python_class, proto_class = self._infer_fv_classes(feature_view)

        if python_class in {RequestFeatureView, OnDemandFeatureView}:
            raise ValueError(
                f"Cannot apply materialization for feature {feature_view.name} of type {python_class}"
            )
        fv: Union[FeatureView, StreamFeatureView] = self._get_object(
            resource=resource,
            name=feature_view.name,
            project=project,
            proto_class=proto_class,
            python_class=python_class,
            id_field_name="feature_view_name",
            proto_field_name="feature_view_proto",
            not_found_exception=FeatureViewNotFoundException,
        )
        fv.materialization_intervals.append((start_date, end_date))
        self._apply_object(
            resource=resource,
            project=project,
            id_field_name="feature_view_name",
            obj=fv,
            proto_field_name="feature_view_proto",
        )

    # Saved dataset operations
    def apply_saved_dataset(
        self,
        saved_dataset: SavedDataset,
        project: str,
        commit: bool = True,
    ):
        return self._apply_object(
            resource="saved_dataset",
            project=project,
            id_field_name="saved_dataset_name",
            obj=saved_dataset,
            proto_field_name="saved_dataset_proto",
        )

    def delete_saved_dataset(self, name: str, project: str, allow_cache: bool = False):
        return self._delete_object(
            resource="saved_dataset",
            name=name,
            project=project,
            id_field_name="saved_dataset_name",
            not_found_exception=SavedDatasetNotFound
        )

    def get_saved_dataset(
        self, name: str, project: str, allow_cache: bool = False
    ) -> SavedDataset:
        return self._get_object(
            resource="saved_dataset",
            name=name,
            project=project,
            proto_class=SavedDatasetProto,
            python_class=SavedDataset,
            id_field_name="saved_dataset_name",
            proto_field_name="saved_dataset_proto",
            not_found_exception=SavedDatasetNotFound,
        )

    def list_saved_datasets(
        self, project: str, allow_cache: bool = False
    ) -> List[SavedDataset]:
        return self._list_objects(
            resource="saved_dataset",
            project=project,
            proto_class=SavedDatasetProto,
            python_class=SavedDataset,
            proto_field_name="saved_dataset_proto"
        )

    # Validation reference operations
    def apply_validation_reference(
        self,
        validation_reference: ValidationReference,
        project: str,
        commit: bool = True,
    ):
        return self._apply_object(
            resource="validation_reference",
            project=project,
            id_field_name="validation_reference_name",
            obj=validation_reference,
            proto_field_name="validation_reference_proto",
        )

    def delete_validation_reference(self, name: str, project: str, commit: bool = True):
        return self._delete_object(
            resource="validation_reference",
            name=name,
            project=project,
            id_field_name="validation_reference_name",
            not_found_exception=ValidationReferenceNotFound
        )

    def get_validation_reference(
        self, name: str, project: str, allow_cache: bool = False
    ) -> ValidationReference:
        return self._get_object(
            resource="validation_reference",
            name=name,
            project=project,
            proto_class=ValidationReferenceProto,
            python_class=ValidationReference,
            id_field_name="validation_reference_name",
            proto_field_name="validation_reference_proto",
            not_found_exception=ValidationReferenceNotFound,
        )

    def list_validation_references(
        self, project: str, allow_cache: bool = False
    ) -> List[ValidationReference]:
        return self._list_objects(
            resource="validation_reference",
            project=project,
            proto_class=ValidationReferenceProto,
            python_class=ValidationReference,
            proto_field_name="validation_reference_proto"
        )

    def list_project_metadata(
        self, project: str, allow_cache: bool = False
    ) -> List[ProjectMetadata]:
        response = self._get(
            f"/{project}/feast_metadata"
        )
        response_json = json.loads(
            response.content, cls=self.json_decoder
        )
        if response.status_code == 200:
            return [
                ProjectMetadata.from_proto(
                    ProjectMetadataProto.FromString(
                        proto_field
                    )
                )
                for proto_field in response_json["protostrings"]
            ]
        return []

    def update_infra(self, infra: Infra, project: str, commit: bool = True):
        return self._apply_object(
            resource="managed_infra",
            project=project,
            id_field_name="infra_name",
            obj=infra,
            proto_field_name="infra_proto",
            name="infra_obj"
        )

    def get_infra(self, project: str, allow_cache: bool = False) -> Infra:
        return self._get_object(
            resource="managed_infra",
            name="infra_obj",
            project=project,
            proto_class=InfraProto,
            python_class=Infra,
            id_field_name="infra_name",
            proto_field_name="infra_proto",
            not_found_exception=RuntimeError,
        )

    def apply_user_metadata(
        self,
        project: str,
        feature_view: BaseFeatureView,
        metadata_bytes: Optional[bytes],
    ):
        resource = self._infer_fv_resource(feature_view)

        update_datetime = datetime.utcnow()
        update_time = int(update_datetime.timestamp())

        response = self._post(
            endpoint=f"{project}/user_metadata",
            params={
                "resource": resource,
                "name": feature_view.name,
            },
            data={
                    "proto": metadata_bytes,
                    "last_updated_timestamp": update_time,
            }
        )

        response_json = json.loads(
            response.content, cls=self.json_decoder
        )
        if response.status_code == 404:
            raise FeatureViewNotFoundException(feature_view.name, project=project)
        if response.status_code != 200:
            raise RuntimeError(f"Failed to apply user metadata: Project={project} feature_view_name={feature_view.name} (E{response.status_code}: {response_json['error']})")

    def get_user_metadata(
        self, project: str, feature_view: BaseFeatureView
    ) -> Optional[bytes]:
        resource = self._infer_fv_resource(feature_view)

        response = self._get(
            f"/{project}/user_metadata",
            params={
                "feature_view_name": feature_view.name,
                "resource": resource
            }
        )
        response_json = json.loads(
            response.content, cls=self.json_decoder
        )
        if response.status_code == 404:
            raise FeatureViewNotFoundException(feature_view.name, project=project)

        return response_json["user_metadata"]

    def proto(self) -> RegistryProto:
        r = RegistryProto()
        last_updated_timestamps = []
        projects = self._get_all_projects()
        for project in projects:
            for lister, registry_proto_field in [
                (self.list_entities, r.entities),
                (self.list_feature_views, r.feature_views),
                (self.list_data_sources, r.data_sources),
                (self.list_on_demand_feature_views, r.on_demand_feature_views),
                (self.list_request_feature_views, r.request_feature_views),
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
            last_updated_timestamps.append(self._get_last_updated_metadata(project))

        if last_updated_timestamps:
            r.last_updated.FromDatetime(max(last_updated_timestamps))

        return r

    def commit(self):
        # This method is a no-op since we're always writing values eagerly to the db.
        pass

    def refresh(self, project: Optional[str] = None):
        # This method is a no-op since we're always reading values from the db.
        pass

    def teardown(self):
        response = self._delete(
            "/teardown"
        )
        response_json = json.loads(
            response.content, cls=self.json_decoder
        )
        if response.status_code != 200:
            raise RuntimeError(f"Failed to tear down registry: {response_json['detail']}")

    def _infer_fv_resource(self, feature_view):
        if isinstance(feature_view, FeatureView):
            resource = "feature_view"
        elif isinstance(feature_view, StreamFeatureView):
            resource = "stream_feature_view"
        elif isinstance(feature_view, OnDemandFeatureView):
            resource = "on_demand_feature_view"
        elif isinstance(feature_view, RequestFeatureView):
            resource = "request_feature_view"
        else:
            raise ValueError(f"Unexpected feature view type: {type(feature_view)}")
        return resource

    def _infer_fv_classes(self, feature_view):
        if isinstance(feature_view, FeatureView):
            python_class, proto_class = FeatureView, FeatureViewProto
        elif isinstance(feature_view, StreamFeatureView):
            python_class, proto_class = StreamFeatureView, StreamFeatureViewProto
        elif isinstance(feature_view, OnDemandFeatureView):
            python_class, proto_class = OnDemandFeatureView, OnDemandFeatureViewProto
        elif isinstance(feature_view, RequestFeatureView):
            python_class, proto_class = RequestFeatureView, RequestFeatureViewProto
        else:
            raise ValueError(f"Unexpected feature view type: {type(feature_view)}")
        return python_class, proto_class
