import logging
import uuid
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Set, Union

from pydantic import (
    StrictInt,
    StrictStr,
)
from neo4j import GraphDatabase

from feast import utils
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
from feast.infra.registry.caching_registry import CachingRegistry
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
from feast.protos.feast.core.SavedDataset_pb2 import SavedDataset as SavedDatasetProto
from feast.protos.feast.core.StreamFeatureView_pb2 import (
    StreamFeatureView as StreamFeatureViewProto,
)
from feast.protos.feast.core.ValidationProfile_pb2 import (
    ValidationReference as ValidationReferenceProto,
)
from feast.repo_config import RegistryConfig
from feast.saved_dataset import SavedDataset, ValidationReference
from feast.stream_feature_view import StreamFeatureView

entities = "Entity"
data_sources = "DataSource"
feature_views = "FeatureView"
stream_feature_views = "StreamFeatureView"
on_demand_feature_views = "OnDemandFeatureView"
feature_services = "FeatureService"
saved_datasets = "SavedDataset"
validation_references = "ValidationReference"
managed_infra = "Infra"

class FeastMetadataKeys(Enum):
    LAST_UPDATED_TIMESTAMP = "last_updated_timestamp"
    PROJECT_UUID = "project_uuid"


logger = logging.getLogger(__name__)


class GraphRegistryConfig(RegistryConfig):
    registry_type: StrictStr = "graph"
    """ str: Provider name or a class name that implements Registry."""

    uri: StrictStr
    """ str: URI for the Neo4j database, e.g., bolt://localhost:7687 """

    user: StrictStr
    """ str: Username for the Neo4j database """

    password: StrictStr
    """ str: Password for the Neo4j database """

    cache_ttl_seconds: StrictInt = 600
    """ int: The cache TTL is the amount of time registry state will be cached in memory. If this TTL is exceeded then
     the registry will be refreshed when any feature store method asks for access to registry state. The TTL can be
     set to infinity by setting TTL to 0 seconds, which means the cache will only be loaded once and will never
     expire. Users can manually refresh the cache by calling feature_store.refresh_registry() """


class GraphRegistry(CachingRegistry):
    def __init__(
        self,
        registry_config: GraphRegistryConfig,
        project: str,
        repo_path: Optional[Path]
    ):  
        self._registry_config = registry_config
        self._project = project
        self._repo_path = repo_path
        
        # Initialize Neo4j driver using configuration
        self.driver = GraphDatabase.driver(
            registry_config.uri,
            auth=(registry_config.user, registry_config.password)
        )
        
        # Initialize CachingRegistry with cache TTL
        super().__init__(
            project=project,
            cache_ttl_seconds=registry_config.cache_ttl_seconds
        )


    def teardown(self):
        for label in {
            entities,
            data_sources,
            feature_views,
            feature_services,
            on_demand_feature_views,
            saved_datasets,
            validation_references
        }:
            with self.driver.session() as session:
                session.run(
                    f"""
                    MATCH (n:{label})
                    DETACH DELETE n
                    """
                )
        self.driver.close()

    def commit(self):
        # This method is a no-op since we're always writing values eagerly to the db.
        pass

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
        '''
        if last_updated_timestamps:
            r.last_updated.FromDatetime(max(last_updated_timestamps))
        else:
            r.last_updated.FromDatetime(datetime.utcnow())
        '''
        return r

    def _maybe_init_project_metadata(self, project):
        # Initialize project metadata if needed    
        update_datetime = datetime.utcnow()
        update_time = int(update_datetime.timestamp())

        with self.driver.session() as session:
            result = session.run(
                f"""
                MATCH (p:Project {{ project_id: $project }})-[:CONTAINS]->(n:ProjectMetadata {{ metadata_key: $key }})
                RETURN n
                """, 
                key=FeastMetadataKeys.PROJECT_UUID.value, 
                project=project
            )
            node = result.single()
            print(f"Searching for project: {project}, metadata {FeastMetadataKeys.PROJECT_UUID.value}")
            print(f"Result {node}")

            if not node:
                new_project_uuid = f"{uuid.uuid4()}"
                session.run(
                    f"""
                    CREATE (p:Project {{ 
                        metadata_key: $project_key, 
                        metadata_value: $project_metadata_value, 
                        last_updated_timestamp: $update_time,
                        project_id: $project
                    }})
                    CREATE (n:ProjectMetadata {{ 
                        metadata_key: $project_key, 
                        metadata_value: $project_metadata_value, 
                        last_updated_timestamp: $update_time
                    }})
                    CREATE (p)-[:CONTAINS]->(n)
                    """, 
                    project_key=FeastMetadataKeys.PROJECT_UUID.value, 
                    key=FeastMetadataKeys.LAST_UPDATED_TIMESTAMP.value, 
                    project_metadata_value=new_project_uuid, 
                    metadata_value=f"{update_time}",
                    update_time=update_time,
                    project=project,
                )

    def get_user_metadata(self, project: str, feature_view: BaseFeatureView) -> Optional[bytes]:
        label = self._infer_fv_label(feature_view)
        name = feature_view.name

        with self.driver.session() as session:
            result = session.run(
                f"""
                MATCH (p:Project {{ project_id: $project }})-[:CONTAINS]->(n:{label} {{ feature_view_name: $name }})
                RETURN n.user_metadata AS user_metadata
                """, 
                name=name, 
                project=project
            )
            node = result.single()

            if node:
                return node["user_metadata"]
            else:
                raise FeatureViewNotFoundException(feature_view.name, project=project)

    def _infer_fv_label(self, feature_view):
        if isinstance(feature_view, StreamFeatureView):
            label = stream_feature_views
        elif isinstance(feature_view, FeatureView):
            label = feature_views
        elif isinstance(feature_view, OnDemandFeatureView):
            label = on_demand_feature_views
        else:
            raise ValueError(f"Unexpected feature view type: {type(feature_view)}")
        return label

    def _infer_fv_classes(self, feature_view):
        if isinstance(feature_view, StreamFeatureView):
            python_class, proto_class = StreamFeatureView, StreamFeatureViewProto
        elif isinstance(feature_view, FeatureView):
            python_class, proto_class = FeatureView, FeatureViewProto
        elif isinstance(feature_view, OnDemandFeatureView):
            python_class, proto_class = OnDemandFeatureView, OnDemandFeatureViewProto
        else:
            raise ValueError(f"Unexpected feature view type: {type(feature_view)}")
        return python_class, proto_class

    def _apply_object(
        self,
        label: str,
        project: str,
        id_field_name: str,
        obj: Any,
        proto_field_name: str,
        name: Optional[str] = None
    ):
        self._maybe_init_project_metadata(project)

        name = name or (obj.name if hasattr(obj, "name") else None)
        assert name, f"name needs to be provided for {obj}"

        update_datetime = datetime.utcnow()
        update_time = int(update_datetime.timestamp())

        with self.driver.session() as session:
            if hasattr(obj, "last_updated_timestamp"):
                obj.last_updated_timestamp = update_datetime

            result = session.run(
                f"""
                MATCH (p:Project {{ project_id: $project }})-[:CONTAINS]->(n:{label} {{ {id_field_name}: $name }})
                RETURN n
                """, 
                name=name, 
                project=project
            )
            node = result.single()
            
            if node:
                if proto_field_name in [
                    "entity_proto",
                    "saved_dataset_proto",
                    "feature_view_proto",
                    "feature_service_proto"
                ]:
                    deserialized_proto = self.deserialize_registry_values(
                        node[proto_field_name], type(obj) 
                    )
                    obj.created_timestamp = (
                        deserialized_proto.meta.created_timestamp.ToDatetime()
                    )
                    if isinstance(obj, (FeatureView, StreamFeatureView)):
                        obj.update_materialization_intervals(
                            type(obj)
                            .from_proto(deserialized_proto)
                            .materialization_intervals
                        )

                session.run(
                    f"""
                    MATCH (p:Project {{ project_id: $project }})-[:CONTAINS]->(n:{label} {{ {id_field_name}: $name }})
                    SET n.{proto_field_name} = $proto_data,
                        n.last_updated_timestamp = $update_time
                    """, 
                    name=name, 
                    project=project, 
                    proto_data=obj.to_proto().SerializeToString(), 
                    update_time=update_time
                )
            else:
                obj_proto = obj.to_proto()

                if hasattr(obj_proto, "meta") and hasattr(
                    obj_proto.meta, "created_timestamp"
                ):
                    obj_proto.meta.created_timestamp.FromDatetime(update_datetime)

                session.run(
                    f"""
                    MATCH (p:Project {{ project_id: $project }})
                    CREATE (n:{label} {{ 
                        {id_field_name}: $name, 
                        {proto_field_name}: $proto_data, 
                        last_updated_timestamp: $update_time
                    }})
                    CREATE (p)-[:CONTAINS]->(n)
                    """, 
                    name=name, 
                    project=project,
                    proto_data=obj_proto.SerializeToString(), 
                    update_time=update_time
                )

            self._set_last_updated_metadata(update_datetime, project)

    def update_infra(self, infra: Infra, project: str, commit: bool = True):
        self._apply_object(
            label=managed_infra,
            project=project,
            id_field_name="infra_name",
            obj=infra,
            proto_field_name="infra_proto",
            name="infra_obj"
        )

    def apply_entity(self, entity: Entity, project: str, commit: bool = True):
        return self._apply_object(
            label=entities,
            project=project,
            id_field_name="entity_name",
            obj=entity,
            proto_field_name="entity_proto"
        )

    def apply_data_source(self, data_source: DataSource, project: str, commit: bool = True):
        return self._apply_object(
            label=data_sources, 
            project=project, 
            id_field_name="data_source_name", 
            obj=data_source, 
            proto_field_name="data_source_proto"
        )

    def apply_feature_view(self, feature_view: BaseFeatureView, project: str, commit: bool = True):
        fv_label = self._infer_fv_label(feature_view)

        return self._apply_object(
            label=fv_label, 
            project=project, 
            id_field_name="feature_view_name", 
            obj=feature_view, 
            proto_field_name="feature_view_proto"
        )

    def apply_feature_service(self, feature_service: FeatureService, project: str, commit: bool = True):
        return self._apply_object(
            label=feature_services,
            project=project,
            id_field_name="feature_service_name",
            obj=feature_service,
            proto_field_name="feature_service_proto"
        )       

    def apply_saved_dataset(self, saved_dataset: SavedDataset, project: str, commit: bool = True):
        return self._apply_object(
            label=saved_datasets,
            project=project,
            id_field_name="saved_dataset_name",
            obj=saved_dataset,
            proto_field_name="saved_dataset_proto"
        ) 

    def apply_validation_reference(self, validation_reference: ValidationReference, project: str, commit: bool = True):
        return self._apply_object(
            label=validation_references,
            project=project,
            id_field_name="validation_reference_name",
            obj=validation_reference,
            proto_field_name="validation_reference_proto"
        )

    def apply_materialization(
        self,
        feature_view: FeatureView,
        project: str,
        start_date: datetime,
        end_date: datetime,
        commit: bool = True
    ):
        label = self._infer_fv_label(feature_view)
        python_class, proto_class = self._infer_fv_classes(feature_view)

        if python_class in {OnDemandFeatureView}:
            raise ValueError(
                f"Cannot apply materialization for feature {feature_view.name} of type {python_class}"
            )
        fv: Union[FeatureView, StreamFeatureView] = self._get_object(
            label=label,
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
            label=label,
            project=project,
            id_field_name="feature_view_name",
            obj=fv,
            proto_field_name="feature_view_proto"
        )

    def apply_user_metadata(
        self,
        project: str,
        feature_view: BaseFeatureView,
        metadata_bytes: Optional[bytes],
    ):
        label = self._infer_fv_label(feature_view)
        name = feature_view.name

        with self.driver.session() as session:
            result = session.run(
                f"""
                MATCH (p:Project {{ project_id: $project }})-[:CONTAINS]->(n:{label} {{ feature_view_name: $name }})
                RETURN n
                """, 
                name=name, 
                project=project
            )
            node = result.single()

            update_datetime = datetime.utcnow()
            update_time = int(update_datetime.timestamp())
            if node:
                session.run(
                    f"""
                    MATCH (p:Project {{ project_id: $project }})-[:CONTAINS]->(n:{label} {{ feature_view_name: $name }})
                    SET n.user_metadata = $user_metadata,
                        n.last_updated_timestamp = $update_time
                    """, 
                    name=name, 
                    project=project, 
                    user_metadata=metadata_bytes, 
                    update_time=update_time
                )
            else:
                raise FeatureViewNotFoundException(feature_view.name, project=project)

    def _set_last_updated_metadata(self, last_updated: datetime, project: str):
        with self.driver.session() as session:
            result = session.run(
                f"""
                MATCH (p:Project {{ project_id: $project }})-[:CONTAINS]->(n:ProjectMetadata {{ metadata_key: $key  }})
                RETURN n
                """, 
                key=FeastMetadataKeys.LAST_UPDATED_TIMESTAMP.value, 
                project=project
            )
            node = result.single()

            update_time = int(last_updated.timestamp())

            if node:
                session.run(
                    f"""
                    MATCH (p:Project {{ project_id: $project }})-[:CONTAINS]->(n:ProjectMetadata {{ metadata_key: $key  }})
                    SET n.metadata_key = $key,
                        n.metadata_value = $metadata_value,
                        n.last_updated_timestamp = $update_time
                    """, 
                    key=FeastMetadataKeys.LAST_UPDATED_TIMESTAMP.value, 
                    metadata_value=f"{update_time}", 
                    update_time=update_time,
                    project=project
                )
            else:
                session.run(
                    f"""
                    MATCH (p:Project {{ project_id: $project }})
                    CREATE (n:ProjectMetadata {{ 
                        metadata_key: $key, 
                        metadata_value: $metadata_value, 
                        last_updated_timestamp: $update_time
                    }})
                    CREATE (p)-[:CONTAINS]->(n)
                    """, 
                    key=FeastMetadataKeys.LAST_UPDATED_TIMESTAMP.value, 
                    metadata_value=f"{update_time}", 
                    update_time=update_time,
                    project=project 
                )

    def _get_object(
        self,
        label: str,
        name: str,
        project: str,
        proto_class: Any,
        python_class: Any,
        id_field_name: str,
        proto_field_name: str,
        not_found_exception: Optional[Callable] = None
    ):
        self._maybe_init_project_metadata(project)

        with self.driver.session() as session:
            result = session.run(
                f"""
                MATCH (p:Project {{ project_id: $project }})-[:CONTAINS]->(n:{label} {{ {id_field_name}: $name }})
                RETURN n.{proto_field_name} AS proto_data
                """, 
                name=name, 
                project=project
            )
            node = result.single()

            if node:
                _proto = proto_class.FromString(node["proto_data"]) 
                return python_class.from_proto(_proto)

            
        if not_found_exception:
            raise not_found_exception(name, project)
        else:
            return None

    def _get_infra(self, project: str) -> Infra:
        infra_object = self._get_object(
            label=managed_infra,
            name="infra_obj",
            project=project,
            proto_class=InfraProto,
            python_class=Infra,
            id_field_name="infra_name",
            proto_field_name="infra_proto"
        )
        if infra_object:
            return infra_object
        return Infra()

    def _get_entity(self, name: str, project: str) -> Entity:
        return self._get_object(
            label=entities,
            name=name,
            project=project,
            proto_class=EntityProto,
            python_class=Entity,
            id_field_name="entity_name",
            proto_field_name="entity_proto",
            not_found_exception=EntityNotFoundException
        )

    def _get_data_source(self, name: str, project: str) -> DataSource:
        return self._get_object(
            label=data_sources,
            name=name,
            project=project,
            proto_class=DataSourceProto,
            python_class=DataSource,
            id_field_name="data_source_name",
            proto_field_name="data_source_proto",
            not_found_exception=DataSourceObjectNotFoundException
        )

    def _get_feature_view(self, name: str, project: str) -> FeatureView:
        return self._get_object(
            label=feature_views,
            name=name,
            project=project,
            proto_class=FeatureViewProto,
            python_class=FeatureView,
            id_field_name="feature_view_name",
            proto_field_name="feature_view_proto",
            not_found_exception=FeatureViewNotFoundException
        )

    def _get_stream_feature_view(self, name: str, project: str) -> StreamFeatureView:
        return self._get_object(
            label=stream_feature_views,
            name=name,
            project=project,
            proto_class=StreamFeatureViewProto,
            python_class=StreamFeatureView,
            id_field_name="feature_view_name",
            proto_field_name="feature_view_proto",
            not_found_exception=FeatureViewNotFoundException
        )

    def _get_on_demand_feature_view(self, name: str, project: str) -> OnDemandFeatureView:
        return self._get_object(
            label=on_demand_feature_views,
            name=name,
            project=project,
            proto_class=OnDemandFeatureViewProto,
            python_class=OnDemandFeatureView,
            id_field_name="feature_view_name",
            proto_field_name="feature_view_proto",
            not_found_exception=FeatureViewNotFoundException
        )

    def _get_feature_service(self, name: str, project: str) -> FeatureService:
        return self._get_object(
            label=feature_services,
            name=name,
            project=project,
            proto_class=FeatureServiceProto,
            python_class=FeatureService,
            id_field_name="feature_service_name",
            proto_field_name="feature_service_proto",
            not_found_exception=FeatureServiceNotFoundException
        )

    def _get_saved_dataset(self, name: str, project: str) -> SavedDataset:
        return self._get_object(
            label=saved_datasets,
            name=name,
            project=project,
            proto_class=SavedDatasetProto,
            python_class=SavedDataset,
            id_field_name="saved_dataset_name",
            proto_field_name="saved_dataset_proto",
            not_found_exception=SavedDatasetNotFound
        )

    def _get_validation_reference(self, name: str, project: str) -> ValidationReference:
        return self._get_object(
            label=validation_references,
            name=name,
            project=project,
            proto_class=ValidationReferenceProto,
            python_class=ValidationReference,
            id_field_name="validation_reference_name",
            proto_field_name="validation_reference_proto",
            not_found_exception=ValidationReferenceNotFound
        )

    def _get_last_updated_metadata(self, project: str):
        with self.driver.session() as session:
            result = session.run(
                f"""
                MATCH (p:Project {{ project_id: $project  }})-[:CONTAINS]->(n:ProjectMetadata {{ metadata_key: $key }})
                RETURN n.last_updated_timestamp AS last_updated_timestamp
                """, 
                key=FeastMetadataKeys.LAST_UPDATED_TIMESTAMP.value, 
                project=project
            )
            node = result.single()

            if not node:
                return None
            update_time = int(node["last_updated_timestamp"])

            return datetime.fromtimestamp(update_time)

    def _get_all_projects(self) -> Set[str]:
        projects = set()
        with self.driver.session() as session:
            '''
            for label in {
                entities,
                data_sources,
                feature_views,
                on_demand_feature_views,
                stream_feature_views
            }:
                result = session.run(
                    f"""
                    MATCH (n:{label})
                    RETURN n.project_id AS project_id
                    """ 
                )
                nodes = result.data()
            '''
            result = session.run(
                f"""
                MATCH (p:Project)
                RETURN p.project_id AS project_id
                """ 
            )
            nodes = result.data()
            
            for node in nodes:
                projects.add(node["project_id"])

        return projects

    def _list_objects(
        self,
        label: str,
        project: str,
        proto_class: Any,
        python_class: Any,
        proto_field_name: str,
        tags: Optional[dict[str, str]] = None
    ):
        self._maybe_init_project_metadata(project)

        with self.driver.session() as session:
            result = session.run(
                f"""
                MATCH (p:Project {{ project_id: $project }})-[:CONTAINS]->(n:{label})
                RETURN n.{proto_field_name} AS proto_data, n.tags AS tags
                """, 
                project=project
            )
            nodes = result.data()

            if nodes:
                objects = []
                for node in nodes:
                    obj = python_class.from_proto(
                        proto_class.FromString(node["proto_data"])
                    )
                    if utils.has_all_tags(obj.tags, tags):
                        objects.append(obj)
                return objects

        return []

    def _list_entities(self, project: str, tags: Optional[dict[str, str]] = None) -> List[Entity]:
        return self._list_objects(
            label=entities,
            project=project,
            proto_class=EntityProto,
            python_class=Entity,
            proto_field_name="entity_proto",
            tags=tags
        )

    def _list_data_sources(self, project: str, tags: Optional[dict[str, str]] = None) -> List[DataSource]:
        return self._list_objects(
            label=data_sources,
            project=project,
            proto_class=DataSourceProto,
            python_class=DataSource,
            proto_field_name="data_source_proto",
            tags=tags
        )

    def _list_feature_views(self, project: str, tags: Optional[dict[str, str]] = None) -> List[FeatureView]:
        return self._list_objects(
            label=feature_views,
            project=project,
            proto_class=FeatureViewProto,
            python_class=FeatureView,
            proto_field_name="feature_view_proto",
            tags=tags
        )

    def _list_stream_feature_views(self, project: str, tags: Optional[dict[str, str]] = None) -> List[StreamFeatureView]:
        return self._list_objects(
            label=stream_feature_views,
            project=project,
            proto_class=StreamFeatureViewProto,
            python_class=StreamFeatureView,
            proto_field_name="feature_view_proto",
            tags=tags
        )

    def _list_on_demand_feature_views(self, project: str, tags: Optional[dict[str, str]] = None) -> List[OnDemandFeatureView]:
        return self._list_objects(
            label=on_demand_feature_views,
            project=project,
            proto_class=OnDemandFeatureViewProto,
            python_class=OnDemandFeatureView,
            proto_field_name="feature_view_proto",
            tags=tags
        )

    def _list_feature_services(self, project: str, tags: Optional[dict[str, str]] = None) -> List[FeatureService]:
        return self._list_objects(
            label=feature_services,
            project=project,
            proto_class=FeatureServiceProto,
            python_class=FeatureService,
            proto_field_name="feature_service_proto",
            tags=tags
        )

    def _list_saved_datasets(self, project: str) -> List[SavedDataset]:
        return self._list_objects(
            label=saved_datasets,
            project=project,
            proto_class=SavedDatasetProto,
            python_class=SavedDataset,
            proto_field_name="saved_dataset_proto"
        )

    def _list_validation_references(self, project: str) -> List[ValidationReference]:
        return self._list_objects(
            label=validation_references,
            project=project,
            proto_class=ValidationReferenceProto,
            python_class=ValidationReference,
            proto_field_name="validation_reference_proto"
        )

    def _list_project_metadata(self, project: str) -> List[ProjectMetadata]:
        with self.driver.session() as session:
            result = session.run(
                f"""
                MATCH (p:Project {{ project_id: $project }})
                RETURN p.metadata_key AS metadata_key, p.metadata_value AS metadata_value
                """, 
                project=project
            )
            nodes = result.data()

            if nodes:
                project_metadata = ProjectMetadata(project_name=project)
                for node in nodes:
                    if (
                        node["metadata_key"]== FeastMetadataKeys.PROJECT_UUID.value
                    ):
                        project_metadata.project_uuid = node["metadata_value"]
                        break
                    # TODO(adchia): Add other project metadata in a structured way
                return [project_metadata]
        return []

    def _delete_object(
        self,
        label: str,
        name: str,
        project: str,
        id_field_name: str,
        not_found_exception: Optional[Callable] = None
    ):
        with self.driver.session() as session:
            result = session.run(
                f"""
                MATCH (p:Project {{ project_id: $project }})-[:CONTAINS]->(n:{label} {{ {id_field_name}: $name }})
                DETACH DELETE n
                RETURN COUNT(n) AS deleted_count
                """, 
                name=name, 
                project=project
            )
            nodes = result.data()

            if nodes["deleted_count"] < 1 and not_found_exception:
                raise not_found_exception(name, project)
            self._set_last_updated_metadata(datetime.utcnow(), project)

            return nodes["deleted_count"]

    def delete_entity(self, name: str, project: str, commit: bool = True):
        return self._delete_object(
            label=entities,
            name=name,
            project=project,
            id_field_name="entity_name",
            not_found_exception=EntityNotFoundException
        )

    def delete_data_source(self, name: str, project: str, commit: bool = True):
        return self._delete_object(
            label=data_sources,
            name=name,
            project=project,
            id_field_name="data_source_name",
            not_found_exception=DataSourceObjectNotFoundException
        )

    def delete_feature_view(self, name: str, project: str, commit: bool = True):
        deleted_count = 0
        for label in {
            feature_views,
            on_demand_feature_views,
            stream_feature_views
        }:
            deleted_count += self._delete_object(
                label=data_sources,
                name=name,
                project=project,
                id_field_name="feature_view_name"
            )
        if deleted_count == 0:
            raise FeatureViewNotFoundException(name, project)

    def delete_feature_service(self, name: str, project: str, commit: bool = True):
        return self._delete_object(
            label=feature_services,
            name=name,
            project=project,
            id_field_name="feature_service_name",
            not_found_exception=FeatureServiceNotFoundException
        )

    def delete_validation_reference(self, name: str, project: str, commit: bool = True):
        self._delete_object(
            label=validation_references,
            name=name,
            project=project,
            id_field_name="validation_reference_name",
            not_found_exception=ValidationReferenceNotFound
        )

