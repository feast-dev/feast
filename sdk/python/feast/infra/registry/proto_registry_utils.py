import uuid
from functools import wraps
from typing import List, Optional

from feast import utils
from feast.data_source import DataSource
from feast.entity import Entity
from feast.errors import (
    DataSourceObjectNotFoundException,
    EntityNotFoundException,
    FeatureServiceNotFoundException,
    FeatureViewNotFoundException,
    PermissionObjectNotFoundException,
    SavedDatasetNotFound,
    ValidationReferenceNotFound,
)
from feast.feature_service import FeatureService
from feast.feature_view import FeatureView
from feast.on_demand_feature_view import OnDemandFeatureView
from feast.permissions.permission import Permission
from feast.project_metadata import ProjectMetadata
from feast.protos.feast.core.Registry_pb2 import ProjectMetadata as ProjectMetadataProto
from feast.protos.feast.core.Registry_pb2 import Registry as RegistryProto
from feast.saved_dataset import SavedDataset, ValidationReference
from feast.stream_feature_view import StreamFeatureView


def registry_proto_cache(func):
    cache_key = None
    cache_value = None

    @wraps(func)
    def wrapper(registry_proto: RegistryProto, project: str):
        nonlocal cache_key, cache_value

        key = tuple([id(registry_proto), registry_proto.version_id, project])

        if key == cache_key:
            return cache_value
        else:
            cache_value = func(registry_proto, project)
            cache_key = key
            return cache_value

    return wrapper


def registry_proto_cache_with_tags(func):
    cache_key = None
    cache_value = None

    @wraps(func)
    def wrapper(
        registry_proto: RegistryProto,
        project: str,
        tags: Optional[dict[str, str]],
    ):
        nonlocal cache_key, cache_value

        key = tuple([id(registry_proto), registry_proto.version_id, project, tags])

        if key == cache_key:
            return cache_value
        else:
            cache_value = func(registry_proto, project, tags)
            cache_key = key
            return cache_value

    return wrapper


def init_project_metadata(cached_registry_proto: RegistryProto, project: str):
    new_project_uuid = f"{uuid.uuid4()}"
    cached_registry_proto.project_metadata.append(
        ProjectMetadata(project_name=project, project_uuid=new_project_uuid).to_proto()
    )


def get_project_metadata(
    registry_proto: Optional[RegistryProto], project: str
) -> Optional[ProjectMetadataProto]:
    if not registry_proto:
        return None
    for pm in registry_proto.project_metadata:
        if pm.project == project:
            return pm
    return None


def get_feature_service(
    registry_proto: RegistryProto, name: str, project: str
) -> FeatureService:
    for feature_service_proto in registry_proto.feature_services:
        if (
            feature_service_proto.spec.project == project
            and feature_service_proto.spec.name == name
        ):
            return FeatureService.from_proto(feature_service_proto)
    raise FeatureServiceNotFoundException(name, project=project)


def get_feature_view(
    registry_proto: RegistryProto, name: str, project: str
) -> FeatureView:
    for feature_view_proto in registry_proto.feature_views:
        if (
            feature_view_proto.spec.name == name
            and feature_view_proto.spec.project == project
        ):
            return FeatureView.from_proto(feature_view_proto)
    raise FeatureViewNotFoundException(name, project)


def get_stream_feature_view(
    registry_proto: RegistryProto, name: str, project: str
) -> StreamFeatureView:
    for feature_view_proto in registry_proto.stream_feature_views:
        if (
            feature_view_proto.spec.name == name
            and feature_view_proto.spec.project == project
        ):
            return StreamFeatureView.from_proto(feature_view_proto)
    raise FeatureViewNotFoundException(name, project)


def get_on_demand_feature_view(
    registry_proto: RegistryProto, name: str, project: str
) -> OnDemandFeatureView:
    for on_demand_feature_view in registry_proto.on_demand_feature_views:
        if (
            on_demand_feature_view.spec.project == project
            and on_demand_feature_view.spec.name == name
        ):
            return OnDemandFeatureView.from_proto(on_demand_feature_view)
    raise FeatureViewNotFoundException(name, project=project)


def get_data_source(
    registry_proto: RegistryProto, name: str, project: str
) -> DataSource:
    for data_source in registry_proto.data_sources:
        if data_source.project == project and data_source.name == name:
            return DataSource.from_proto(data_source)
    raise DataSourceObjectNotFoundException(name, project=project)


def get_entity(registry_proto: RegistryProto, name: str, project: str) -> Entity:
    for entity_proto in registry_proto.entities:
        if entity_proto.spec.name == name and entity_proto.spec.project == project:
            return Entity.from_proto(entity_proto)
    raise EntityNotFoundException(name, project=project)


def get_saved_dataset(
    registry_proto: RegistryProto, name: str, project: str
) -> SavedDataset:
    for saved_dataset in registry_proto.saved_datasets:
        if saved_dataset.spec.name == name and saved_dataset.spec.project == project:
            return SavedDataset.from_proto(saved_dataset)
    raise SavedDatasetNotFound(name, project=project)


def get_validation_reference(
    registry_proto: RegistryProto, name: str, project: str
) -> ValidationReference:
    for validation_reference in registry_proto.validation_references:
        if (
            validation_reference.name == name
            and validation_reference.project == project
        ):
            return ValidationReference.from_proto(validation_reference)
    raise ValidationReferenceNotFound(name, project=project)


@registry_proto_cache_with_tags
def list_feature_services(
    registry_proto: RegistryProto, project: str, tags: Optional[dict[str, str]]
) -> List[FeatureService]:
    feature_services = []
    for feature_service_proto in registry_proto.feature_services:
        if feature_service_proto.spec.project == project and utils.has_all_tags(
            feature_service_proto.spec.tags, tags
        ):
            feature_services.append(FeatureService.from_proto(feature_service_proto))
    return feature_services


@registry_proto_cache_with_tags
def list_feature_views(
    registry_proto: RegistryProto, project: str, tags: Optional[dict[str, str]]
) -> List[FeatureView]:
    feature_views: List[FeatureView] = []
    for feature_view_proto in registry_proto.feature_views:
        if feature_view_proto.spec.project == project and utils.has_all_tags(
            feature_view_proto.spec.tags, tags
        ):
            feature_views.append(FeatureView.from_proto(feature_view_proto))
    return feature_views


@registry_proto_cache_with_tags
def list_stream_feature_views(
    registry_proto: RegistryProto, project: str, tags: Optional[dict[str, str]]
) -> List[StreamFeatureView]:
    stream_feature_views = []
    for stream_feature_view in registry_proto.stream_feature_views:
        if stream_feature_view.spec.project == project and utils.has_all_tags(
            stream_feature_view.spec.tags, tags
        ):
            stream_feature_views.append(
                StreamFeatureView.from_proto(stream_feature_view)
            )
    return stream_feature_views


@registry_proto_cache_with_tags
def list_on_demand_feature_views(
    registry_proto: RegistryProto, project: str, tags: Optional[dict[str, str]]
) -> List[OnDemandFeatureView]:
    on_demand_feature_views = []
    for on_demand_feature_view in registry_proto.on_demand_feature_views:
        if on_demand_feature_view.spec.project == project and utils.has_all_tags(
            on_demand_feature_view.spec.tags, tags
        ):
            on_demand_feature_views.append(
                OnDemandFeatureView.from_proto(on_demand_feature_view)
            )
    return on_demand_feature_views


@registry_proto_cache_with_tags
def list_entities(
    registry_proto: RegistryProto, project: str, tags: Optional[dict[str, str]]
) -> List[Entity]:
    entities = []
    for entity_proto in registry_proto.entities:
        if entity_proto.spec.project == project and utils.has_all_tags(
            entity_proto.spec.tags, tags
        ):
            entities.append(Entity.from_proto(entity_proto))
    return entities


@registry_proto_cache_with_tags
def list_data_sources(
    registry_proto: RegistryProto, project: str, tags: Optional[dict[str, str]]
) -> List[DataSource]:
    data_sources = []
    for data_source_proto in registry_proto.data_sources:
        if data_source_proto.project == project and utils.has_all_tags(
            data_source_proto.tags, tags
        ):
            data_sources.append(DataSource.from_proto(data_source_proto))
    return data_sources


@registry_proto_cache_with_tags
def list_saved_datasets(
    registry_proto: RegistryProto, project: str, tags: Optional[dict[str, str]]
) -> List[SavedDataset]:
    saved_datasets = []
    for saved_dataset in registry_proto.saved_datasets:
        if saved_dataset.spec.project == project and utils.has_all_tags(
            saved_dataset.tags, tags
        ):
            saved_datasets.append(SavedDataset.from_proto(saved_dataset))
    return saved_datasets


@registry_proto_cache_with_tags
def list_validation_references(
    registry_proto: RegistryProto, project: str, tags: Optional[dict[str, str]]
) -> List[ValidationReference]:
    validation_references = []
    for validation_reference in registry_proto.validation_references:
        if validation_reference.project == project and utils.has_all_tags(
            validation_reference.tags, tags
        ):
            validation_references.append(
                ValidationReference.from_proto(validation_reference)
            )
    return validation_references


@registry_proto_cache
def list_project_metadata(
    registry_proto: RegistryProto, project: str
) -> List[ProjectMetadata]:
    return [
        ProjectMetadata.from_proto(project_metadata)
        for project_metadata in registry_proto.project_metadata
        if project_metadata.project == project
    ]


@registry_proto_cache_with_tags
def list_permissions(
    registry_proto: RegistryProto, project: str, tags: Optional[dict[str, str]]
) -> List[Permission]:
    permissions = []
    for permission_proto in registry_proto.permissions:
        if permission_proto.spec.project == project and utils.has_all_tags(
            permission_proto.spec.tags, tags
        ):
            permissions.append(Permission.from_proto(permission_proto))
    return permissions


def get_permission(
    registry_proto: RegistryProto, name: str, project: str
) -> Permission:
    for permission_proto in registry_proto.permissions:
        if (
            permission_proto.spec.project == project
            and permission_proto.spec.name == name
        ):
            return Permission.from_proto(permission_proto)
    raise PermissionObjectNotFoundException(name=name, project=project)
