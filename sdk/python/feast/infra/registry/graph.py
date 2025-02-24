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
from feast.data_source import DataSource, RequestSource, PushSource
from feast.entity import Entity
from feast.errors import (
    DataSourceObjectNotFoundException,
    EntityNotFoundException,
    FeatureServiceNotFoundException,
    FeatureViewNotFoundException,
    SavedDatasetNotFound,
    ValidationReferenceNotFound,
    FieldNotFoundException
)
from feast.feature_service import FeatureService
from feast.feature_view import FeatureView
from feast.feature_view_projection import FeatureViewProjection
from feast.infra.infra_object import Infra
from feast.infra.registry.caching_registry import CachingRegistry
from feast.on_demand_feature_view import OnDemandFeatureView
from feast.project_metadata import ProjectMetadata
from feast.field import Field
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
from feast.protos.feast.core.Feature_pb2 import FeatureSpecV2 as FieldProto
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


logger = logging.getLogger("neo4j").setLevel(logging.ERROR)


class GraphRegistryConfig(RegistryConfig):
    registry_type: StrictStr = "graph"
    """ str: Provider name or a class name that implements Registry."""

    uri: StrictStr
    """ str: URI for the Neo4j database, e.g., bolt://localhost:7687 """

    user: StrictStr
    """ str: Username for the Neo4j database """

    password: StrictStr
    """ str: Password for the Neo4j database """

    database: StrictStr
    """ str: Name for the Neo4j database """


class GraphRegistry(CachingRegistry):
    def __init__(
        self,
        registry_config: Optional[Union[RegistryConfig,GraphRegistryConfig]],
        project: str,
        repo_path: Optional[Path]
    ):  
        self._registry_config = registry_config
        self._project = project
        self._repo_path = repo_path
        self.database = registry_config.database
        
        # Initialize Neo4j driver using configuration
        self.driver = GraphDatabase.driver(
            registry_config.uri,
            auth=(registry_config.user, registry_config.password)
        )

        # Create indexes
        self.create_indexes()
        
        # Initialize CachingRegistry with cache TTL
        super().__init__(
            project=project,
            cache_ttl_seconds=registry_config.cache_ttl_seconds
        )

    def create_indexes(self):
        with self.driver.session(database=self.database) as session:
            session.run("CREATE INDEX project_id_index IF NOT EXISTS FOR (n:Project) ON (n.project_id);")
            session.run("CREATE INDEX field_name_index IF NOT EXISTS FOR (n:Field) ON (n.field_name);")
            session.run("CREATE INDEX entity_name_index IF NOT EXISTS FOR (n:Entity) ON (n.entity_name);")
            session.run("CREATE INDEX data_source_name_index IF NOT EXISTS FOR (n:DataSource) ON (n.data_source_name);")
            session.run("CREATE INDEX feature_view_name_index IF NOT EXISTS FOR (n:FeatureView) ON (n.feature_view_name);")
            session.run("CREATE INDEX on_demand_feature_view_name_index IF NOT EXISTS FOR (n:OnDemandFeatureView) ON (n.feature_view_name);")
            session.run("CREATE INDEX feature_service_name_index IF NOT EXISTS FOR (n:FeatureService) ON (n.feature_service_name);")

    def teardown(self):
        for label in [
            "Field",
            "Entity",
            "DataSource",
            "FeatureView",
            "StreamFeatureView",
            "OnDemandFeatureView",
            "FeatureService",
            "SavedDataset",
            "ValidationReference",
            "Infra",
            "ProjectMetadata",
            "Project"
        ]:
            with self.driver.session(database=self.database) as session:
                with session.begin_transaction() as tx:
                    tx.run(
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
            if self._get_last_updated_metadata(project) is not None:
                last_updated_timestamps.append(self._get_last_updated_metadata(project))
        
        # print(f"Last updated timestamps: {last_updated_timestamps}")
        if last_updated_timestamps:
            r.last_updated.FromDatetime(max(last_updated_timestamps))
        
        return r

    def _maybe_init_project_metadata(self, project):
        # Initialize project metadata if needed    
        update_datetime = datetime.now()
        update_time = int(update_datetime.timestamp())

        with self.driver.session(database=self.database) as session:
            with session.begin_transaction() as tx:
                result = tx.run(
                    f"""
                    MATCH (p:Project {{ project_id: $project }})-[:CONTAINS]->(n:ProjectMetadata {{ metadata_key: $key }})
                    RETURN n
                    """, 
                    key=FeastMetadataKeys.PROJECT_UUID.value, 
                    project=project
                )
                node = result.single()

                if not node:
                    new_project_uuid = f"{uuid.uuid4()}"
                    tx.run(
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
                        CREATE (m:ProjectMetadata {{ 
                            metadata_key: $key, 
                            metadata_value: $metadata_value, 
                            last_updated_timestamp: $update_time
                        }})
                        CREATE (p)-[:CONTAINS]->(n)
                        CREATE (p)-[:CONTAINS]->(m)
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

        with self.driver.session(database=self.database) as session:
            with session.begin_transaction() as tx:
                result = tx.run(
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
        name: Optional[str] = None,
        parents: Optional[dict] = None, # Only used for Field, refers to DataSource and OnDemandFeatureView names
        just_create: Optional[bool] = False # Used when creating node as prerequisite
    ):
        self._maybe_init_project_metadata(project)

        name = name or (obj.name if hasattr(obj, "name") else None)
        assert name, f"name needs to be provided for {obj}"

        update_datetime = datetime.now()
        update_time = int(datetime.timestamp(update_datetime))

        with self.driver.session(database=self.database) as session:
            with session.begin_transaction() as tx:
                if hasattr(obj, "last_updated_timestamp"):
                    obj.last_updated_timestamp = update_datetime

                if parents and "ds" in parents:
                    result = tx.run(
                        f"""
                        MATCH (p:Project {{ project_id: $project }})-[:CONTAINS]->(n:{label} {{ {id_field_name}: $name }})
                        MATCH (p)-[:CONTAINS]->(s:DataSource)
                        WHERE ALL(parent_name IN $parents WHERE
                            EXISTS(
                                (p)-[:CONTAINS]->(s {{ data_source_name: parent_name }})-[:HAS]->(n)
                            )
                        )
                        RETURN n
                        """, 
                        name=name, 
                        parents=[parents["ds"]],
                        project=project
                    )
                    node = result.single()
                elif parents and "odfv" in parents:
                    # print(f"Applying field {name} with parent feature view {parents['odfv']}")
                    result = tx.run(
                        f"""
                        MATCH (p:Project {{ project_id: $project }})-[:CONTAINS]->(n:{label} {{ {id_field_name}: $name }})
                        MATCH (p)-[:CONTAINS]->(v:OnDemandFeatureView)
                        WHERE ALL(parent_name IN $parents WHERE
                            EXISTS(
                                (p)-[:CONTAINS]->(v {{ feature_view_name: parent_name }})-[:PRODUCES]->(n)
                            )
                        )
                        RETURN n
                        """, 
                        name=name, 
                        parents=[parents["odfv"]],
                        project=project
                    )
                    node = result.single()
                else:
                    result = tx.run(
                        f"""
                        MATCH (p:Project {{ project_id: $project }})-[:CONTAINS]->(n:{label} {{ {id_field_name}: $name }})
                        RETURN n
                        """, 
                        name=name, 
                        project=project
                    )
                    node = result.single()
                
                if node:
                    # print(f"Node: {node}")
                    obj_proto = obj.to_proto()
                    
                    if parents and "ds" in parents:
                        tx.run(
                            f"""
                            MATCH (p:Project {{ project_id: $project }})-[:CONTAINS]->(n:{label} {{ {id_field_name}: $name }})
                            MATCH (p)-[:CONTAINS]->(s:DataSource)
                            WHERE ALL(parent_name IN $parents WHERE
                                EXISTS(
                                    (p)-[:CONTAINS]->(s {{ data_source_name: parent_name }})-[:HAS]->(n)
                                )
                            )
                            SET n.{proto_field_name} = $proto_data,
                                n.last_updated_timestamp = $update_time,
                                n.data_type = $dtype,
                                n.description = $description
                            """, 
                            name=name, 
                            parents=[parents["ds"]],
                            project=project,
                            proto_data=obj_proto.SerializeToString(), 
                            update_time=update_time,
                            dtype=f"{obj.dtype}",
                            description=obj.description
                        )
                    elif parents and "odfv" in parents:
                        tx.run(
                            f"""
                            MATCH (p:Project {{ project_id: $project }})-[:CONTAINS]->(n:{label} {{ {id_field_name}: $name }})
                            MATCH (p)-[:CONTAINS]->(v:OnDemandFeatureView)
                            WHERE ALL(parent_name IN $parents WHERE
                                EXISTS(
                                    (p)-[:CONTAINS]->(v {{ feature_view_name: parent_name }})-[:PRODUCES]->(n)
                                )
                            )
                            SET n.{proto_field_name} = $proto_data,
                                n.last_updated_timestamp = $update_time,
                                n.data_type = $dtype,
                                n.description = $description
                            """, 
                            name=name, 
                            parents=[parents["odfv"]],
                            project=project,
                            proto_data=obj_proto.SerializeToString(), 
                            update_time=update_time,
                            dtype=f"{obj.dtype}",
                            description=obj.description
                        )
                    else:
                        tx.run(
                            f"""
                            MATCH (p:Project {{ project_id: $project }})-[:CONTAINS]->(n:{label} {{ {id_field_name}: $name }})
                            SET n.{proto_field_name} = $proto_data,
                                n.last_updated_timestamp = $update_time
                            """, 
                            name=name, 
                            project=project, 
                            proto_data=obj_proto.SerializeToString(), 
                            update_time=update_time
                        )
                else:
                    obj_proto = obj.to_proto()

                    if hasattr(obj_proto, "meta") and hasattr(
                        obj_proto.meta, "created_timestamp"
                    ):
                        obj_proto.meta.created_timestamp.FromDatetime(update_datetime)

                    if parents and "ds" in parents:
                        tx.run(
                            f"""
                            MATCH (p:Project {{ project_id: $project }})
                            CREATE (n:{label} {{ 
                                {id_field_name}: $name, 
                                {proto_field_name}: $proto_data, 
                                last_updated_timestamp: $created_time,
                                data_type: $dtype,
                                description: $description
                            }})
                            CREATE (p)-[:CONTAINS]->(n)
                            WITH n,p
                            UNWIND $parents AS parent_name
                            MATCH (p)-[:CONTAINS]->(s:DataSource {{ data_source_name: parent_name }})
                            CREATE (s)-[r:HAS]->(n)
                            SET r.created_at = $created_time
                            """, 
                            name=name, 
                            parents=[parents["ds"]],
                            project=project,
                            proto_data=obj_proto.SerializeToString(), 
                            created_time=datetime.now(),
                            dtype=f"{obj.dtype}",
                            description=obj.description
                        )
                    elif parents and "odfv" in parents:
                        tx.run(
                            f"""
                            MATCH (p:Project {{ project_id: $project }})
                            CREATE (n:{label} {{ 
                                {id_field_name}: $name, 
                                {proto_field_name}: $proto_data, 
                                last_updated_timestamp: $created_time,
                                data_type: $dtype,
                                description: $description
                            }})
                            CREATE (p)-[:CONTAINS]->(n)
                            WITH n,p
                            UNWIND $parents AS parent_name
                            MATCH (p)-[:CONTAINS]->(v:OnDemandFeatureView {{ feature_view_name: parent_name }})
                            CREATE (v)-[r:PRODUCES]->(n)
                            SET r.created_at = $created_time
                            """, 
                            name=name, 
                            parents=[parents["odfv"]],
                            project=project,
                            proto_data=obj_proto.SerializeToString(), 
                            created_time=datetime.now(),
                            dtype=f"{obj.dtype}",
                            description=obj.description
                        )
                    else:
                        tx.run(
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

                if just_create:
                    return
                
                # Add description to nodes other than Fields
                if hasattr(obj, "description") and not isinstance(obj, Field):
                    tx.run(
                        f"""
                        MATCH (p:Project {{ project_id: $project }})-[:CONTAINS]->(n:{label} {{ {id_field_name}: $name }})
                        SET n.description = $description
                        """, 
                        name=name, 
                        project=project, 
                        description=obj.description
                    )

                # Add owner relationship to nodes other than Fields
                if hasattr(obj, "owner") and obj.owner != "":
                    # Get existing owner
                    tx.run(
                        f"""
                        MATCH (p:Project {{ project_id: $project }})-[:CONTAINS]->(n:{label} {{ {id_field_name}: $name }})
                        MERGE (co:Owner {{ name: $owner}})
                        MERGE (co)-[:OWNS]->(n)
                        """, 
                        name=name, 
                        project=project,
                        owner=obj.owner
                    )          

                # Add relationship to tags if applicable
                if hasattr(obj, "tags"):
                    if parents and "ds" in parents:
                        tags = [{'label': key.capitalize(), 'value': value} for key,value in obj.tags.items()]
                        tags = [frozenset(t.items()) for t in tags]

                        if tags:
                            tags = [dict(item) for item in tags]
                            for t in tags:
                                tx.run(
                                    f"""
                                    MATCH (p:Project {{ project_id: $project }})-[:CONTAINS]->(n:{label} {{ {id_field_name}: $name }})
                                    MATCH (p)-[:CONTAINS]->(s:DataSource)
                                    WHERE ALL(parent_name IN $parents WHERE
                                        EXISTS(
                                            (p)-[:CONTAINS]->(s {{ data_source_name: parent_name }})-[:HAS]->(n)
                                        )
                                    )
                                    MERGE (t:{t["label"]} {{ value: $value }})
                                    MERGE (n)-[r:TAG]->(t)
                                    """,
                                    name=name,
                                    parents=[parents["ds"]],
                                    project=project,
                                    value=t["value"]
                                )
                    
                    elif parents and "odfv" in parents:
                        tags = [{'label': key.capitalize(), 'value': value} for key,value in obj.tags.items()]
                        tags = [frozenset(t.items()) for t in tags]

                        if tags:
                            tags = [dict(item) for item in tags]
                            for t in tags:
                                tx.run(
                                    f"""
                                    MATCH (p:Project {{ project_id: $project }})-[:CONTAINS]->(n:{label} {{ {id_field_name}: $name }})
                                    MATCH (p)-[:CONTAINS]->(v:OnDemandFeatureView)
                                    WHERE ALL(parent_name IN $parents WHERE
                                        EXISTS(
                                            (p)-[:CONTAINS]->(v {{ feature_view_name: parent_name }})-[:PRODUCES]->(n)
                                        )
                                    )
                                    MERGE (t:{t["label"]} {{ value: $value }})
                                    MERGE (n)-[r:TAG]->(t)
                                    """,
                                    name=name,
                                    parents=[parents["odfv"]],
                                    project=project,
                                    value=t["value"]
                                )
                        
                    else:
                        tags = [{'label': key.capitalize(), 'value': value} for key,value in obj.tags.items()]
                        tags = [frozenset(t.items()) for t in tags]
                       
                        if tags:
                            tags = [dict(item) for item in tags]
                            for t in tags:
                                tx.run(
                                    f"""
                                    MATCH (p:Project {{ project_id: $project }})-[:CONTAINS]->(n:{label} {{ {id_field_name}: $name }})
                                    MERGE (t:{t["label"]} {{ value: $value }})
                                    MERGE (n)-[r:TAG]->(t)
                                    """,
                                    name=name,
                                    project=project,
                                    value=t["value"]
                                )

                            
                # Add relationship from RequestSource to Fields
                if isinstance(obj, (RequestSource)):
                    # print(f"Features to connect: {obj.schema}")
                    for field in obj.schema: 
                        # print(f"Here for field: {field}")
                        # Check if the relationship already exists before creating it
                        tx.run(
                            f"""
                            MATCH (p:Project {{ project_id: $project }})-[:CONTAINS]->(v:{label} {{ {id_field_name}: $name }})
                            MATCH (p:Project {{ project_id: $project }})-[:CONTAINS]->(f:Field {{ field_name: $field_name }})
                            MERGE (v)-[r:HAS]->(f)
                            ON CREATE SET r.created_at = $created_time
                            """, 
                            name=name, 
                            field_name=field.name,
                            project=project,
                            created_time=datetime.now() 
                        )
                    
                # Add relationship from PushSource to batch source
                if isinstance(obj, (PushSource)):
                    # print(f"Batch source to connect: {obj.batch_source.name}")
                    # Check if the relationship already exists before creating it  
                    tx.run(
                        f"""
                        MATCH (p:Project {{ project_id: $project }})-[:CONTAINS]->(ps:{label} {{ {id_field_name}: $name }})
                        MATCH (p:Project {{ project_id: $project }})-[:CONTAINS]->(bs:DataSource {{ data_source_name: $batch_name }})
                        MERGE (ps)-[r:RETRIEVES_FROM]->(bs)
                        ON CREATE SET r.created_at = $created_time
                        """, 
                        name=name, 
                        batch_name=obj.batch_source.name,
                        project=project,
                        created_time=datetime.now() 
                    )
                    
                # Add relationship from FeatureView to Entities, Fields, Data Sources
                if isinstance(obj, (FeatureView)):
                    # Add materilization intervals to registry
                    # print(f"Materialization intervals: {obj.materialization_intervals}")
                    materialization_timestamps = []
                    for el1,el2 in obj.materialization_intervals:
                        interval = f"{el1.strftime('%d/%m/%y, %H:%M:%S')}-{el2.strftime('%d/%m/%y, %H:%M:%S')}"
                        materialization_timestamps.append(interval)
                    # print(f"Materialization timestamps: {materialization_timestamps}")
                    tx.run(
                        f"""
                        MATCH (p:Project {{ project_id: $project }})-[:CONTAINS]->(v:{label} {{ {id_field_name}: $name }})
                        SET v.materialization_intervals = $materialization_intervals
                        """, 
                        name=name, 
                        project=project,
                        materialization_intervals=materialization_timestamps 
                    )

                    if obj.entities:
                        tx.run(
                            f"""
                            MATCH (p:Project {{ project_id: $project }})-[:CONTAINS]->(n:{label} {{ {id_field_name}: $name }})
                            UNWIND $entities_to_add AS e_name
                            MATCH (p)-[:CONTAINS]->(e:Entity {{ entity_name: e_name }})
                            MERGE (n)-[r:USES]->(e)
                            ON CREATE SET r.created_at = $created_time
                            """,
                            name=name,
                            project=project,
                            entities_to_add=list(obj.entities),
                            created_time=datetime.now()
                        )


                    # If a stream source exists, connect to it and its children-fields
                    # Else connect to the batch source and its children-fields
                    source = obj.batch_source
                    if obj.stream_source:
                        source = obj.stream_source

                    tx.run(
                        f"""
                        MATCH (p:Project {{ project_id: $project }})-[:CONTAINS]->(n:{label} {{ {id_field_name}: $name }})
                        MATCH (p)-[:CONTAINS]->(s:DataSource {{ data_source_name: $source }})
                        MERGE (n)-[r:POPULATED_FROM]->(s)
                        ON CREATE SET r.created_at = $created_time
                        """,
                        name=name,
                        project=project,
                        source=source.name,
                        created_time=datetime.now()
                    )

                    # print(f"Features to connect: {obj.features}")
                    fields = [{'field': f.name, 'source': source.name} for f in obj.features]
                    fields = [frozenset(f.items()) for f in fields]
                    
                    if fields:
                        fields = [dict(item) for item in fields]
                        for f in fields:
                            tx.run(
                                f"""
                                MATCH (p:Project {{ project_id: $project }})-[:CONTAINS]->(n:{label} {{ {id_field_name}: $name }})
                                MATCH (s:DataSource {{ data_source_name: $source_name}})-[:HAS]->(f:Field {{ field_name: $field_name}})
                                MERGE (n)-[r:HAS]->(f)
                                ON CREATE SET r.created_at = $created_time
                                """,
                                name=name,
                                project=project,
                                field_name=f["field"],
                                source_name=f["source"],
                                created_time=datetime.now()
                            )
                        
                
                # Add relationship from OnDemandFeatureView to Fields, Data Sources, Feature Views
                if isinstance(obj, (OnDemandFeatureView)):                    
                    sources = []
                    for source in obj.source_request_sources.keys():
                        sources.append(source)
                    for source in obj.source_feature_view_projections.keys():
                        sources.append(source)
                    # print(f"Sources to connect: {sources}")
                    for source in sources: 
                        # Check if the relationship already exists before creating it  
                        tx.run(
                            f"""
                            MATCH (p:Project {{ project_id: $project }})-[:CONTAINS]->(v:{label} {{ {id_field_name}: $name }})
                            MATCH (p:Project {{ project_id: $project }})-[:CONTAINS]->(s)
                            WHERE (s:DataSource AND s.data_source_name = $source_name) OR (s:FeatureView AND s.feature_view_name = $source_name)
                            MERGE (v)-[r:BASED_ON]->(s)
                            ON CREATE SET r.created_at = $created_time
                            """, 
                            name=name, 
                            source_name=source,
                            project=project,
                            created_time=datetime.now() 
                        )

                    # Map dependencies with Field nodes
                    if obj.feature_dependencies:
                        for feature, dep in obj.feature_dependencies.items():
                            for dep_name in dep:
                                self._create_field_relationship(feature, dep_name, project, obj.name)  

                # Add relationship from FeatureService to FeatureView/OnDemandFeatureView and Fields
                if isinstance(obj, (FeatureService)):
                    fvs = [f.name for f in obj._features]
 
                    if fvs:
                        tx.run(
                            f"""
                            MATCH (p:Project {{ project_id: $project }})-[:CONTAINS]->(n:{label} {{ {id_field_name}: $name }})
                            UNWIND $fvs_to_add AS fv_name
                            MATCH (p)-[:CONTAINS]->(fv {{ feature_view_name: fv_name }})
                            MERGE (n)-[r:CONSUMES]->(fv)
                            ON CREATE SET r.created_at = $created_time
                            """,
                            name=name,
                            project=project,
                            fvs_to_add=list(fvs),
                            created_time=datetime.now()
                        )
                    
                    features = []
                    for fv in obj._features:
                        if isinstance(fv, (FeatureView)):
                            source = fv.stream_source or fv.batch_source
                            for f in fv.projection.features:
                                features.append({'field': f.name, 'source': source.name, 'odfv': None})
                        elif isinstance(fv, (OnDemandFeatureView)):
                            # print(f"ODFV Projection: {fv.projection.name} features: {fv.projection.features}")
                            for f in fv.projection.features:
                                features.append({'field': f.name, 'odfv': fv.projection.name, 'source': None})
                    fields = [frozenset(f.items()) for f in features]
                    
                    if fields:
                        fields = [dict(item) for item in fields]
                        for f in fields:
                            if f["source"] is not None:
                                tx.run(
                                    f"""
                                    MATCH (p:Project {{ project_id: $project }})-[:CONTAINS]->(n:{label} {{ {id_field_name}: $name }})
                                    MATCH (s:DataSource {{ data_source_name: $source_name}})-[:HAS]->(f:Field {{ field_name: $field_name}})
                                    MERGE (n)-[r:SERVES]->(f)
                                    ON CREATE SET r.created_at = $created_time
                                    """,
                                    name=name,
                                    project=project,
                                    field_name=f["field"],
                                    source_name=f["source"],
                                    created_time=datetime.now()
                                )
                            elif f["odfv"] is not None:
                                tx.run(
                                    f"""
                                    MATCH (p:Project {{ project_id: $project }})-[:CONTAINS]->(n:{label} {{ {id_field_name}: $name }})
                                    MATCH (fv:OnDemandFeatureView {{ feature_view_name: $odfv_name}})-[:PRODUCES]->(f:Field {{ field_name: $field_name}})
                                    MERGE (n)-[r:SERVES]->(f)
                                    ON CREATE SET r.created_at = $created_time
                                    """,
                                    name=name,
                                    project=project,
                                    field_name=f["field"],
                                    odfv_name=f["odfv"],
                                    created_time=datetime.now()
                                )


        self._set_last_updated_metadata(update_datetime, project)
    
    def trouble_apply_object(
        self,
        label: str,
        project: str,
        id_field_name: str,
        obj: Any,
        proto_field_name: str,
        name: Optional[str] = None,
        parents: Optional[dict] = None, # Only used for Field, refers to DataSource and OnDemandFeatureView names
        just_create: Optional[bool] = False # Used when creating node as prerequisite
    ):
        self._maybe_init_project_metadata(project)

        name = name or (obj.name if hasattr(obj, "name") else None)
        assert name, f"name needs to be provided for {obj}"

        update_datetime = datetime.now()
        update_time = int(datetime.timestamp(update_datetime))

        with self.driver.session(database=self.database) as session:
            with session.begin_transaction() as tx:
                if hasattr(obj, "last_updated_timestamp"):
                    obj.last_updated_timestamp = update_datetime

                if parents and "ds" in parents:
                    # print(f"Applying field {name} with parent data source {parents['ds']}")
                    result = tx.run(
                        f"""
                        MATCH (p:Project {{ project_id: $project }})-[:CONTAINS]->(n:{label} {{ {id_field_name}: $name }})
                        MATCH (p)-[:CONTAINS]->(s:DataSource)
                        WHERE ALL(parent_name IN $parents WHERE
                            EXISTS(
                                (p)-[:CONTAINS]->(s {{ data_source_name: parent_name }})-[:HAS]->(n)
                            )
                        )
                        RETURN n
                        """, 
                        name=name, 
                        parents=[parents["ds"]],
                        project=project
                    )
                    node = result.single()
                elif parents and "odfv" in parents:
                    # print(f"Applying field {name} with parent feature view {parents['odfv']}")
                    result = tx.run(
                        f"""
                        MATCH (p:Project {{ project_id: $project }})-[:CONTAINS]->(n:{label} {{ {id_field_name}: $name }})
                        MATCH (p)-[:CONTAINS]->(v:OnDemandFeatureView)
                        WHERE ALL(parent_name IN $parents WHERE
                            EXISTS(
                                (p)-[:CONTAINS]->(v {{ feature_view_name: parent_name }})-[:PRODUCES]->(n)
                            )
                        )
                        RETURN n
                        """, 
                        name=name, 
                        parents=[parents["odfv"]],
                        project=project
                    )
                    node = result.single()
                else:
                    result = tx.run(
                        f"""
                        MATCH (p:Project {{ project_id: $project }})-[:CONTAINS]->(n:{label} {{ {id_field_name}: $name }})
                        RETURN n
                        """, 
                        name=name, 
                        project=project
                    )
                    node = result.single()
                
                if node:
                    # print(f"Node: {node}")
                    obj_proto = obj.to_proto()
                    
                    if parents and "ds" in parents:
                        tx.run(
                            f"""
                            MATCH (p:Project {{ project_id: $project }})-[:CONTAINS]->(n:{label} {{ {id_field_name}: $name }})
                            MATCH (p)-[:CONTAINS]->(s:DataSource)
                            WHERE ALL(parent_name IN $parents WHERE
                                EXISTS(
                                    (p)-[:CONTAINS]->(s {{ data_source_name: parent_name }})-[:HAS]->(n)
                                )
                            )
                            SET n.{proto_field_name} = $proto_data,
                                n.last_updated_timestamp = $update_time,
                                n.data_type = $dtype,
                                n.description = $description
                            """, 
                            name=name, 
                            parents=[parents["ds"]],
                            project=project,
                            proto_data=obj_proto.SerializeToString(), 
                            update_time=update_time,
                            dtype=f"{obj.dtype}",
                            description=obj.description
                        )
                    elif parents and "odfv" in parents:
                        tx.run(
                            f"""
                            MATCH (p:Project {{ project_id: $project }})-[:CONTAINS]->(n:{label} {{ {id_field_name}: $name }})
                            MATCH (p)-[:CONTAINS]->(v:OnDemandFeatureView)
                            WHERE ALL(parent_name IN $parents WHERE
                                EXISTS(
                                    (p)-[:CONTAINS]->(v {{ feature_view_name: parent_name }})-[:PRODUCES]->(n)
                                )
                            )
                            SET n.{proto_field_name} = $proto_data,
                                n.last_updated_timestamp = $update_time,
                                n.data_type = $dtype,
                                n.description = $description
                            """, 
                            name=name, 
                            parents=[parents["odfv"]],
                            project=project,
                            proto_data=obj_proto.SerializeToString(), 
                            update_time=update_time,
                            dtype=f"{obj.dtype}",
                            description=obj.description
                        )
                    else:
                        tx.run(
                            f"""
                            MATCH (p:Project {{ project_id: $project }})-[:CONTAINS]->(n:{label} {{ {id_field_name}: $name }})
                            SET n.{proto_field_name} = $proto_data,
                                n.last_updated_timestamp = $update_time
                            """, 
                            name=name, 
                            project=project, 
                            proto_data=obj_proto.SerializeToString(), 
                            update_time=update_time
                        )
                else:
                    obj_proto = obj.to_proto()

                    if hasattr(obj_proto, "meta") and hasattr(
                        obj_proto.meta, "created_timestamp"
                    ):
                        obj_proto.meta.created_timestamp.FromDatetime(update_datetime)

                    if parents and "ds" in parents:
                        tx.run(
                            f"""
                            MATCH (p:Project {{ project_id: $project }})
                            CREATE (n:{label} {{ 
                                {id_field_name}: $name, 
                                {proto_field_name}: $proto_data, 
                                last_updated_timestamp: $created_time,
                                data_type: $dtype,
                                description: $description
                            }})
                            CREATE (p)-[:CONTAINS]->(n)
                            WITH n,p
                            UNWIND $parents AS parent_name
                            MATCH (p)-[:CONTAINS]->(s:DataSource {{ data_source_name: parent_name }})
                            CREATE (s)-[r:HAS]->(n)
                            SET r.created_at = $created_time
                            """, 
                            name=name, 
                            parents=[parents["ds"]],
                            project=project,
                            proto_data=obj_proto.SerializeToString(), 
                            created_time=datetime.now(),
                            dtype=f"{obj.dtype}",
                            description=obj.description
                        )
                    elif parents and "odfv" in parents:
                        tx.run(
                            f"""
                            MATCH (p:Project {{ project_id: $project }})
                            CREATE (n:{label} {{ 
                                {id_field_name}: $name, 
                                {proto_field_name}: $proto_data, 
                                last_updated_timestamp: $created_time,
                                data_type: $dtype,
                                description: $description
                            }})
                            CREATE (p)-[:CONTAINS]->(n)
                            WITH n,p
                            UNWIND $parents AS parent_name
                            MATCH (p)-[:CONTAINS]->(v:OnDemandFeatureView {{ feature_view_name: parent_name }})
                            CREATE (v)-[r:PRODUCES]->(n)
                            SET r.created_at = $created_time
                            """, 
                            name=name, 
                            parents=[parents["odfv"]],
                            project=project,
                            proto_data=obj_proto.SerializeToString(), 
                            created_time=datetime.now(),
                            dtype=f"{obj.dtype}",
                            description=obj.description
                        )
                    else:
                        tx.run(
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

                if just_create:
                    # tx.commit()
                    return
                
                # Add description to nodes other than Fields
                if hasattr(obj, "description") and not isinstance(obj, Field):
                    tx.run(
                        f"""
                        MATCH (p:Project {{ project_id: $project }})-[:CONTAINS]->(n:{label} {{ {id_field_name}: $name }})
                        SET n.description = $description
                        """, 
                        name=name, 
                        project=project, 
                        description=obj.description
                    )

                # Add owner relationship to nodes other than Fields
                if hasattr(obj, "owner") and obj.owner != "":
                    # Get existing owner
                    result = tx.run(
                        f"""
                        MATCH (p:Project {{ project_id: $project }})-[:CONTAINS]->(n:{label} {{ {id_field_name}: $name }})
                        MATCH (o:Owner)-[:OWNS]->(n)
                        RETURN o.name AS name
                        """,
                        name=name,
                        project=project
                    )
                    owner = result.data()
                    # print(f"Current owner: {owner}")
                    if owner != obj.owner:
                        # Delete relationship to previous owner (if needed)
                        # Create relationship to current owner (and node if needed)
                        tx.run(
                            f"""
                            MATCH (p:Project {{ project_id: $project }})-[:CONTAINS]->(n:{label} {{ {id_field_name}: $name }})
                            OPTIONAL MATCH (o:Owner)-[r:OWNS]->(n)
                            DELETE r
                            MERGE (co:Owner {{ name: $owner}})
                            MERGE (co)-[:OWNS]->(n)
                            """, 
                            name=name, 
                            project=project,
                            owner=obj.owner
                        )
                else:
                    # Delete relationship to previous owner (if needed)
                    tx.run(
                            f"""
                            MATCH (p:Project {{ project_id: $project }})-[:CONTAINS]->(n:{label} {{ {id_field_name}: $name }})
                            OPTIONAL MATCH (o:Owner)-[r:OWNS]->(n)
                            DELETE r
                            """, 
                            name=name, 
                            project=project
                        )            

                # Add relationship to tags if applicable
                if hasattr(obj, "tags"):
                    if parents and "ds" in parents:
                        # Get existing relationships
                        result = tx.run(
                            f"""
                            MATCH (p:Project {{ project_id: $project }})-[:CONTAINS]->(n:{label} {{ {id_field_name}: $name }})
                            MATCH (p)-[:CONTAINS]->(s:DataSource)
                            WHERE ALL(parent_name IN $parents WHERE
                                EXISTS(
                                    (p)-[:CONTAINS]->(s {{ data_source_name: parent_name }})-[:HAS]->(n)
                                )
                            )
                            MATCH (n)-[:TAG]->(t)
                            RETURN t.value AS value, labels(t) AS label
                            """,
                            name=name,
                            parents=[parents["ds"]],
                            project=project
                        )
                        relationships = result.data()
                        relationships = [{'label': r["label"][0], 'value': r["value"]} for r in relationships]
                        relationships = [frozenset(r.items()) for r in relationships]
                        # print(f"Existing TAG relationships: {relationships}")
                        tags = [{'label': key.capitalize(), 'value': value} for key,value in obj.tags.items()]
                        tags = [frozenset(t.items()) for t in tags]
                        # print(f"New TAG relationships: {tags}")
                        tags_to_remove = set(relationships) - set(tags)
                        tags_to_add = set(tags) - set(relationships)

                        # Remove old relationships
                        if tags_to_remove:
                            tags_to_remove = [dict(item) for item in tags_to_remove]
                            for t in tags_to_remove:
                                # print(f"Remove tag: {t}")
                                tx.run(
                                    f"""
                                    MATCH (p:Project {{ project_id: $project }})-[:CONTAINS]->(n:{label} {{ {id_field_name}: $name }})
                                    MATCH (p)-[:CONTAINS]->(s:DataSource)
                                    WHERE ALL(parent_name IN $parents WHERE
                                        EXISTS(
                                            (p)-[:CONTAINS]->(s {{ data_source_name: parent_name }})-[:HAS]->(n)
                                        )
                                    )
                                    MATCH (n)-[r:TAG]->(t:{t["label"]} {{ value: $value }})
                                    DELETE r
                                    """,
                                    name=name,
                                    parents=[parents["ds"]],
                                    project=project,
                                    value=t["value"]
                                )
                        
                        # Add new relationships
                        if tags_to_add:
                            tags_to_add = [dict(item) for item in tags_to_add]
                            for t in tags_to_add:
                                # print(f"Add tag: {t}")
                                tx.run(
                                    f"""
                                    MATCH (p:Project {{ project_id: $project }})-[:CONTAINS]->(n:{label} {{ {id_field_name}: $name }})
                                    MATCH (p)-[:CONTAINS]->(s:DataSource)
                                    WHERE ALL(parent_name IN $parents WHERE
                                        EXISTS(
                                            (p)-[:CONTAINS]->(s {{ data_source_name: parent_name }})-[:HAS]->(n)
                                        )
                                    )
                                    MERGE (t:{t["label"]} {{ value: $value }})
                                    MERGE (n)-[r:TAG]->(t)
                                    """,
                                    name=name,
                                    parents=[parents["ds"]],
                                    project=project,
                                    value=t["value"]
                                )
                    
                    elif parents and "odfv" in parents:
                        # Get existing relationships
                        result = tx.run(
                            f"""
                            MATCH (p:Project {{ project_id: $project }})-[:CONTAINS]->(n:{label} {{ {id_field_name}: $name }})
                            MATCH (p)-[:CONTAINS]->(v:OnDemandFeatureView)
                            WHERE ALL(parent_name IN $parents WHERE
                                EXISTS(
                                    (p)-[:CONTAINS]->(v {{ feature_view_name: parent_name }})-[:PRODUCES]->(n)
                                )
                            )
                            MATCH (n)-[:TAG]->(t)
                            RETURN t.value AS value, labels(t) AS label
                            """,
                            name=name,
                            parents=[parents["odfv"]],
                            project=project
                        )
                        relationships = result.data()
                        relationships = [{'label': r["label"][0], 'value': r["value"]} for r in relationships]
                        relationships = [frozenset(r.items()) for r in relationships]
                        # print(f"Existing TAG relationships: {relationships}")
                        tags = [{'label': key.capitalize(), 'value': value} for key,value in obj.tags.items()]
                        tags = [frozenset(t.items()) for t in tags]
                        # print(f"New TAG relationships: {tags}")
                        tags_to_remove = set(relationships) - set(tags)
                        tags_to_add = set(tags) - set(relationships)

                        # Remove old relationships
                        if tags_to_remove:
                            tags_to_remove = [dict(item) for item in tags_to_remove]
                            for t in tags_to_remove:
                                # print(f"Remove tag: {t}")
                                tx.run(
                                    f"""
                                    MATCH (p:Project {{ project_id: $project }})-[:CONTAINS]->(n:{label} {{ {id_field_name}: $name }})
                                    MATCH (p)-[:CONTAINS]->(v:OnDemandFeatureView)
                                    WHERE ALL(parent_name IN $parents WHERE
                                        EXISTS(
                                            (p)-[:CONTAINS]->(v {{ feature_view_name: parent_name }})-[:PRODUCES]->(n)
                                        )
                                    )
                                    MATCH (n)-[r:TAG]->(t:{t["label"]} {{ value: $value }})
                                    DELETE r
                                    """,
                                    name=name,
                                    parents=[parents["odfv"]],
                                    project=project,
                                    value=t["value"]
                                )
                        
                        # Add new relationships
                        if tags_to_add:
                            tags_to_add = [dict(item) for item in tags_to_add]
                            for t in tags_to_add:
                                # print(f"Add tag: {t}")
                                tx.run(
                                    f"""
                                    MATCH (p:Project {{ project_id: $project }})-[:CONTAINS]->(n:{label} {{ {id_field_name}: $name }})
                                    MATCH (p)-[:CONTAINS]->(v:OnDemandFeatureView)
                                    WHERE ALL(parent_name IN $parents WHERE
                                        EXISTS(
                                            (p)-[:CONTAINS]->(v {{ feature_view_name: parent_name }})-[:PRODUCES]->(n)
                                        )
                                    )
                                    MERGE (t:{t["label"]} {{ value: $value }})
                                    MERGE (n)-[r:TAG]->(t)
                                    """,
                                    name=name,
                                    parents=[parents["odfv"]],
                                    project=project,
                                    value=t["value"]
                                )
                        
                    else:
                        # Get existing relationships
                        result = tx.run(
                            f"""
                            MATCH (p:Project {{ project_id: $project }})-[:CONTAINS]->(n:{label} {{ {id_field_name}: $name }})
                            MATCH (n)-[:TAG]->(t)
                            RETURN t.value AS value, labels(t) AS label
                            """,
                            name=name,
                            project=project
                        )
                        relationships = result.data()
                        relationships = [{'label': r["label"][0], 'value': r["value"]} for r in relationships]
                        relationships = [frozenset(r.items()) for r in relationships]
                        # print(f"Existing TAG relationships: {relationships}")
                        tags = [{'label': key.capitalize(), 'value': value} for key,value in obj.tags.items()]
                        tags = [frozenset(t.items()) for t in tags]
                        # print(f"New TAG relationships: {tags}")
                        tags_to_remove = set(relationships) - set(tags)
                        tags_to_add = set(tags) - set(relationships)

                        # Remove old relationships
                        if tags_to_remove:
                            tags_to_remove = [dict(item) for item in tags_to_remove]
                            for t in tags_to_remove:
                                # print(f"Remove tag: {t}")
                                tx.run(
                                    f"""
                                    MATCH (p:Project {{ project_id: $project }})-[:CONTAINS]->(n:{label} {{ {id_field_name}: $name }})
                                    MATCH (n)-[r:TAG]->(t:{t["label"]} {{ value: $value }})
                                    DELETE r
                                    """,
                                    name=name,
                                    project=project,
                                    value=t["value"]
                                )
                        
                        # Add new relationships
                        if tags_to_add:
                            tags_to_add = [dict(item) for item in tags_to_add]
                            for t in tags_to_add:
                                # print(f"Add tag: {t}")
                                tx.run(
                                    f"""
                                    MATCH (p:Project {{ project_id: $project }})-[:CONTAINS]->(n:{label} {{ {id_field_name}: $name }})
                                    MERGE (t:{t["label"]} {{ value: $value }})
                                    MERGE (n)-[r:TAG]->(t)
                                    """,
                                    name=name,
                                    project=project,
                                    value=t["value"]
                                )

                            
                # Add relationship from RequestSource to Fields
                if isinstance(obj, (RequestSource)):
                    # print(f"Features to connect: {obj.schema}")
                    for field in obj.schema: 
                        # print(f"Here for field: {field}")
                        # Check if the relationship already exists before creating it
                        tx.run(
                            f"""
                            MATCH (p:Project {{ project_id: $project }})-[:CONTAINS]->(v:{label} {{ {id_field_name}: $name }})
                            MATCH (p:Project {{ project_id: $project }})-[:CONTAINS]->(f:Field {{ field_name: $field_name }})
                            MERGE (v)-[r:HAS]->(f)
                            ON CREATE SET r.created_at = $created_time
                            """, 
                            name=name, 
                            field_name=field.name,
                            project=project,
                            created_time=datetime.now() 
                        )
                    
                # Add relationship from PushSource to batch source
                if isinstance(obj, (PushSource)):
                    # print(f"Batch source to connect: {obj.batch_source.name}")
                    # Check if the relationship already exists before creating it  
                    tx.run(
                        f"""
                        MATCH (p:Project {{ project_id: $project }})-[:CONTAINS]->(ps:{label} {{ {id_field_name}: $name }})
                        MATCH (p:Project {{ project_id: $project }})-[:CONTAINS]->(bs:DataSource {{ data_source_name: $batch_name }})
                        MERGE (ps)-[r:RETRIEVES_FROM]->(bs)
                        ON CREATE SET r.created_at = $created_time
                        """, 
                        name=name, 
                        batch_name=obj.batch_source.name,
                        project=project,
                        created_time=datetime.now() 
                    )
                    
                # Add relationship from FeatureView to Entities, Fields, Data Sources
                if isinstance(obj, (FeatureView)):
                    # Add materilization intervals to registry
                    # print(f"Materialization intervals: {obj.materialization_intervals}")
                    materialization_timestamps = []
                    for el1,el2 in obj.materialization_intervals:
                        interval = f"{el1.strftime('%d/%m/%y, %H:%M:%S')}-{el2.strftime('%d/%m/%y, %H:%M:%S')}"
                        materialization_timestamps.append(interval)
                    # print(f"Materialization timestamps: {materialization_timestamps}")
                    # Search for active nodes and relationships only
                    tx.run(
                        f"""
                        MATCH (p:Project {{ project_id: $project }})-[:CONTAINS]->(v:{label} {{ {id_field_name}: $name }})
                        SET v.materialization_intervals = $materialization_intervals
                        """, 
                        name=name, 
                        project=project,
                        materialization_intervals=materialization_timestamps 
                    )


                    # print(f"Entities to connect: {obj.entities}")
                    # Get existing relationships
                    result = tx.run(
                        f"""
                        MATCH (p:Project {{ project_id: $project }})-[:CONTAINS]->(n:{label} {{ {id_field_name}: $name }})
                        MATCH (n)-[r:USES]->(e:Entity)
                        RETURN e.entity_name AS entity_name
                        """,
                        name=name,
                        project=project
                    )
                    relationships = result.data()
                    current_entities = [r["entity_name"] for r in relationships]
                    entities_to_remove = set(current_entities) - set(obj.entities)
                    entities_to_add = set(obj.entities) - set(current_entities)

                    # Remove old relationships
                    if entities_to_remove:
                        tx.run(
                            f"""
                            MATCH (p:Project {{ project_id: $project }})-[:CONTAINS]->(n:{label} {{ {id_field_name}: $name }})
                            MATCH (n)-[r:USES]->(e)
                            WHERE e.entity_name IN $entities_to_remove
                            DELETE r
                            """,
                            name=name,
                            project=project,
                            entities_to_remove=list(entities_to_remove)
                        )

                    # Add new relationships
                    if entities_to_add:
                        tx.run(
                            f"""
                            MATCH (p:Project {{ project_id: $project }})-[:CONTAINS]->(n:{label} {{ {id_field_name}: $name }})
                            UNWIND $entities_to_add AS e_name
                            MATCH (p)-[:CONTAINS]->(e:Entity {{ entity_name: e_name }})
                            MERGE (n)-[r:USES]->(e)
                            ON CREATE SET r.created_at = $created_time
                            """,
                            name=name,
                            project=project,
                            entities_to_add=list(entities_to_add),
                            created_time=datetime.now()
                        )


                    # If a stream source exists, connect to it and its children-fields
                    # Else connect to the batch source and its children-fields
                    source = obj.batch_source
                    if obj.stream_source:
                        source = obj.stream_source

                    # Get existing relationship
                    result = tx.run(
                        f"""
                        MATCH (p:Project {{ project_id: $project }})-[:CONTAINS]->(n:{label} {{ {id_field_name}: $name }})
                        MATCH (n)-[r:POPULATED_FROM]->(s)
                        RETURN s.data_source_name AS data_source
                        """,
                        name=name,
                        project=project
                    )
                    relationship = result.data()
                    if relationship:
                        current_ds = relationship[0]["data_source"]
                    else:
                        current_ds = None
                    
                    if current_ds != None and current_ds != source.name:
                        # Remove old relationship
                        tx.run(
                            f"""
                            MATCH (p:Project {{ project_id: $project }})-[:CONTAINS]->(n:{label} {{ {id_field_name}: $name }})
                            MATCH (n)-[r:POPULATED_FROM]->(s:DataSource {{ data_source_name: $source}})
                            DELETE r
                            """,
                            name=name,
                            project=project,
                            source=current_ds
                        )
                    if current_ds != source.name:
                        # Add new relationship
                        tx.run(
                            f"""
                            MATCH (p:Project {{ project_id: $project }})-[:CONTAINS]->(n:{label} {{ {id_field_name}: $name }})
                            MATCH (p)-[:CONTAINS]->(s:DataSource {{ data_source_name: $source }})
                            MERGE (n)-[r:POPULATED_FROM]->(s)
                            ON CREATE SET r.created_at = $created_time
                            """,
                            name=name,
                            project=project,
                            source=source.name,
                            created_time=datetime.now()
                        )

                    # print(f"Features to connect: {obj.features}")
                    # Get existing relationships
                    result = tx.run(
                        f"""
                        MATCH (p:Project {{ project_id: $project }})-[:CONTAINS]->(n:{label} {{ {id_field_name}: $name }})
                        MATCH (n)-[:HAS]->(f:Field)
                        MATCH (s:DataSource)-[:HAS]->(f)
                        RETURN f.field_name AS field, s.data_source_name AS source
                        """,
                        name=name,
                        project=project
                    )
                    relationships = result.data()
                    relationships = [frozenset(r.items()) for r in relationships]
                    # print(f"Existing relationships: {relationships}")
                    fields = [{'field': f.name, 'source': source.name} for f in obj.features]
                    fields = [frozenset(f.items()) for f in fields]
                    # print(f"New relationships: {fields}")
                    fields_to_remove = set(relationships) - set(fields)
                    fields_to_add = set(fields) - set(relationships)

                    # Remove old relationships
                    if fields_to_remove:
                        fields_to_remove = [dict(item) for item in fields_to_remove]
                        for f in fields_to_remove:
                            # print(f"Remove field: {f}")
                            tx.run(
                                f"""
                                MATCH (p:Project {{ project_id: $project }})-[:CONTAINS]->(n:{label} {{ {id_field_name}: $name }})
                                MATCH (n)-[r:HAS]->(f:Field {{ field_name: $field_name}})
                                MATCH (s:DataSource {{ data_source_name: $source_name}})-[:HAS]->(f)
                                DELETE r
                                """,
                                name=name,
                                project=project,
                                field_name=f["field"],
                                source_name=f["source"]
                            )

                    # Add new relationships
                    if fields_to_add:
                        fields_to_add = [dict(item) for item in fields_to_add]
                        for f in fields_to_add:
                            # print(f"Add field: {f}")
                            tx.run(
                                f"""
                                MATCH (p:Project {{ project_id: $project }})-[:CONTAINS]->(n:{label} {{ {id_field_name}: $name }})
                                MATCH (s:DataSource {{ data_source_name: $source_name}})-[:HAS]->(f:Field {{ field_name: $field_name}})
                                MERGE (n)-[r:HAS]->(f)
                                ON CREATE SET r.created_at = $created_time
                                """,
                                name=name,
                                project=project,
                                field_name=f["field"],
                                source_name=f["source"],
                                created_time=datetime.now()
                            )
                        
                
                # Add relationship from OnDemandFeatureView to Fields, Data Sources, Feature Views
                if isinstance(obj, (OnDemandFeatureView)):
                    # Remove old relationships
                    result = tx.run(
                            f"""
                            MATCH (p:Project {{ project_id: $project }})-[:CONTAINS]->(v:{label} {{ {id_field_name}: $name }})
                            MATCH (v)-[r:PRODUCES]->(f: Field)
                            RETURN f.field_name AS field
                            """, 
                            name=name, 
                            project=project
                        )
                    relationships = result.data()
                    features = [f.name for f in obj.features]
                    for rel in relationships:
                        if rel["field"] not in features:
                            # print(f"Relationship to {rel['field']} no longer applies")
                            tx.run(
                                f"""
                                MATCH (p:Project {{ project_id: $project }})-[:CONTAINS]->(v:{label} {{ {id_field_name}: $name }})
                                MATCH (v)-[r:PRODUCES]->(f: Field {{ field_name: $field }})
                                DETACH DELETE f
                                """, 
                                name=name, 
                                project=project,
                                field=rel["field"]
                            )
                    
                    # Get existing relationships
                    result = tx.run(
                        f"""
                        MATCH (p:Project {{ project_id: $project }})-[:CONTAINS]->(n:{label} {{ {id_field_name}: $name }})
                        MATCH (n)-[:BASED_ON]->(s)
                        WHERE (s:DataSource OR s:FeatureView)
                        RETURN s AS source
                        """,
                        name=name,
                        project=project
                    )
                    relationships = result.data()
                    # print(f"ODFV based on relationships: {relationships}")
                    
                    sources = []
                    for source in obj.source_request_sources.keys():
                        sources.append(source)
                    for source in obj.source_feature_view_projections.keys():
                        sources.append(source)
                    # print(f"Sources to connect: {sources}")
                    for source in sources: 
                        # Check if the relationship already exists before creating it  
                        tx.run(
                            f"""
                            MATCH (p:Project {{ project_id: $project }})-[:CONTAINS]->(v:{label} {{ {id_field_name}: $name }})
                            MATCH (p:Project {{ project_id: $project }})-[:CONTAINS]->(s)
                            WHERE (s:DataSource AND s.data_source_name = $source_name) OR (s:FeatureView AND s.feature_view_name = $source_name)
                            MERGE (v)-[r:BASED_ON]->(s)
                            ON CREATE SET r.created_at = $created_time
                            """, 
                            name=name, 
                            source_name=source,
                            project=project,
                            created_time=datetime.now() 
                        )

                    # Map dependencies with Field nodes
                    if obj.feature_dependencies:
                        for feature, dep in obj.feature_dependencies.items():
                            for dep_name in dep:
                                self._create_field_relationship(feature, dep_name, project, obj.name)  

                # Add relationship from FeatureService to FeatureView/OnDemandFeatureView and Fields
                if isinstance(obj, (FeatureService)):
                    # Get existing relationships
                    result = tx.run(
                        f"""
                        MATCH (p:Project {{ project_id: $project }})-[:CONTAINS]->(n:{label} {{ {id_field_name}: $name }})
                        MATCH (n)-[r:CONSUMES]->(fv)
                        RETURN fv.feature_view_name AS feature_view
                        """,
                        name=name,
                        project=project
                    )
                    relationships = result.data()
                    current_fvs = [r["feature_view"] for r in relationships]
                    fvs = [f.name for f in obj._features]
                    fvs_to_remove = set(current_fvs) - set(fvs)
                    fvs_to_add = set(fvs) - set(current_fvs)

                    # Remove old relationships
                    if fvs_to_remove:
                        tx.run(
                            f"""
                            MATCH (p:Project {{ project_id: $project }})-[:CONTAINS]->(n:{label} {{ {id_field_name}: $name }})
                            MATCH (n)-[r:CONSUMES]->(fv)
                            WHERE fv.feature_view_name IN $fvs_to_remove
                            DELETE r
                            """,
                            name=name,
                            project=project,
                            fvs_to_remove=list(fvs_to_remove)
                        )

                    # Add new relationships
                    if fvs_to_add:
                        tx.run(
                            f"""
                            MATCH (p:Project {{ project_id: $project }})-[:CONTAINS]->(n:{label} {{ {id_field_name}: $name }})
                            UNWIND $fvs_to_add AS fv_name
                            MATCH (p)-[:CONTAINS]->(fv {{ feature_view_name: fv_name }})
                            MERGE (n)-[r:CONSUMES]->(fv)
                            ON CREATE SET r.created_at = $created_time
                            """,
                            name=name,
                            project=project,
                            fvs_to_add=list(fvs_to_add),
                            created_time=datetime.now()
                        )
                    
                    # Get existing relationships
                    result = tx.run(
                        f"""
                        MATCH (p:Project {{ project_id: $project }})-[:CONTAINS]->(n:{label} {{ {id_field_name}: $name }})
                        MATCH (n)-[:SERVES]->(f:Field)
                        OPTIONAL MATCH (s:DataSource)-[:HAS]->(f)
                        OPTIONAL MATCH (fv:OnDemandFeatureView)-[:PRODUCES]->(f)
                        RETURN f.field_name AS field, s.data_source_name AS source, fv.feature_view_name AS odfv
                        """,
                        name=name,
                        project=project
                    )
                    relationships = result.data()
                    relationships = [frozenset(r.items()) for r in relationships]
                    # print(f"Existing relationships: {relationships}")
                    features = []
                    for fv in obj._features:
                        if isinstance(fv, (FeatureView)):
                            # print(f"FV Projection: {fv.projection.name} features: {fv.projection.features}")
                            source = fv.stream_source or fv.batch_source
                            # print(f"FV Projection: {fv.projection.name} source: {source.name}")
                            for f in fv.projection.features:
                                features.append({'field': f.name, 'source': source.name, 'odfv': None})
                        elif isinstance(fv, (OnDemandFeatureView)):
                            # print(f"ODFV Projection: {fv.projection.name} features: {fv.projection.features}")
                            for f in fv.projection.features:
                                features.append({'field': f.name, 'odfv': fv.projection.name, 'source': None})
                    fields = [frozenset(f.items()) for f in features]
                    # print(f"New relationships: {features}")
                    fields_to_remove = set(relationships) - set(fields)
                    fields_to_add = set(fields) - set(relationships)

                    # Remove old relationships
                    if fields_to_remove:
                        fields_to_remove = [dict(item) for item in fields_to_remove]
                        for f in fields_to_remove:
                            # print(f"Remove field: {f}")
                            if f["source"] is not None:
                                tx.run(
                                    f"""
                                    MATCH (p:Project {{ project_id: $project }})-[:CONTAINS]->(n:{label} {{ {id_field_name}: $name }})
                                    MATCH (n)-[r:SERVES]->(f:Field {{ field_name: $field_name}})
                                    MATCH (s:DataSource {{ data_source_name: $source_name}})-[:HAS]->(f)
                                    DELETE r
                                    """,
                                    name=name,
                                    project=project,
                                    field_name=f["field"],
                                    source_name=f["source"]
                                )
                            elif f["odfv"] is not None:
                                tx.run(
                                    f"""
                                    MATCH (p:Project {{ project_id: $project }})-[:CONTAINS]->(n:{label} {{ {id_field_name}: $name }})
                                    MATCH (n)-[r:SERVES]->(f:Field {{ field_name: $field_name}})
                                    MATCH (fv:OnDemandFeatureView {{ feature_view_name: $odfv_name}})-[:PRODUCES]->(f:Field {{ field_name: $field_name}})
                                    DELETE r
                                    """,
                                    name=name,
                                    project=project,
                                    field_name=f["field"],
                                    odfv_name=f["odfv"],
                                    created_time=datetime.now()
                                )

                    # Add new relationships
                    if fields_to_add:
                        fields_to_add = [dict(item) for item in fields_to_add]
                        for f in fields_to_add:
                            # print(f"Add field: {f}")
                            if f["source"] is not None:
                                tx.run(
                                    f"""
                                    MATCH (p:Project {{ project_id: $project }})-[:CONTAINS]->(n:{label} {{ {id_field_name}: $name }})
                                    MATCH (s:DataSource {{ data_source_name: $source_name}})-[:HAS]->(f:Field {{ field_name: $field_name}})
                                    MERGE (n)-[r:SERVES]->(f)
                                    ON CREATE SET r.created_at = $created_time
                                    """,
                                    name=name,
                                    project=project,
                                    field_name=f["field"],
                                    source_name=f["source"],
                                    created_time=datetime.now()
                                )
                            elif f["odfv"] is not None:
                                tx.run(
                                    f"""
                                    MATCH (p:Project {{ project_id: $project }})-[:CONTAINS]->(n:{label} {{ {id_field_name}: $name }})
                                    MATCH (fv:OnDemandFeatureView {{ feature_view_name: $odfv_name}})-[:PRODUCES]->(f:Field {{ field_name: $field_name}})
                                    MERGE (n)-[r:SERVES]->(f)
                                    ON CREATE SET r.created_at = $created_time
                                    """,
                                    name=name,
                                    project=project,
                                    field_name=f["field"],
                                    odfv_name=f["odfv"],
                                    created_time=datetime.now()
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

    def apply_data_source(self, data_source: DataSource, project: str, commit: bool = True, just_create: bool = False):
        if isinstance(data_source, (PushSource)):
            # self.apply_data_source(
            #     data_source=data_source.batch_source, 
            #     project=project, 
            #     commit=commit, 
            #     just_create=True
            # )
            with self.driver.session(database=self.database) as session:
                with session.begin_transaction() as tx:
                    created_at = datetime.now()
                    tx.run(
                        f"""
                        MATCH (p:Project {{project_id: $project}})
                        MERGE (p)-[r:CONTAINS]->(d:DataSource {{data_source_name: $data_source}})
                        ON CREATE SET r.created_at = $created_at;
                        """,
                        data_source=data_source.batch_source.name,
                        project=project,
                        created_at=created_at
                    )
        elif isinstance(data_source, (RequestSource)):
            # print(f"Features to apply: {data_source.schema}")
            # for field in data_source.schema:
            #     self.apply_field(
            #         field=field, 
            #         project=project, 
            #         parents={
            #             "ds": data_source.name
            #         }, 
            #         commit=commit
            #     )
            update_time = int(datetime.timestamp(datetime.now()))
            fields_to_apply = []
            for field in data_source.schema:
                field_data = {
                    'name': field.name,
                    'dtype': f"{field.dtype}",
                    'description': field.description or "",
                    'proto_data': field.to_proto().SerializeToString(),
                    'last_updated_timestamp': update_time,
                }
                fields_to_apply.append(field_data)

            with self.driver.session(database=self.database) as session:
                with session.begin_transaction() as tx:
                    created_at = datetime.now()
                    tx.run(
                        f"""
                        MATCH (p:Project {{project_id: $project}})-[:CONTAINS]->(d:DataSource {{data_source_name: $source}})
                        UNWIND $fields AS field_data
                        WITH field_data,p,d
                            MERGE (d)-[r:HAS]->(f:Field {{field_name:field_data.name}})<-[:CONTAINS]-(p)
                            ON CREATE SET r.created_at = $created_at
                            SET f.data_type = field_data.dtype,
                                f.description = field_data.description,
                                f.field_proto = field_data.proto_data,
                                f.last_updated_timestamp = field_data.last_updated_timestamp
                        """,
                        fields=fields_to_apply,
                        project=project,
                        created_at=created_at,
                        source=data_source.name
                    )
        return self._apply_object(
            label=data_sources, 
            project=project, 
            id_field_name="data_source_name", 
            obj=data_source, 
            proto_field_name="data_source_proto"
        )

    def apply_feature_view(self, feature_view: BaseFeatureView, project: str, commit: bool = True, just_create: bool = False):
        fv_label = self._infer_fv_label(feature_view)
          
        fields_to_apply = []  
        if fv_label == feature_views:
            # self.apply_fv_prerequisites()
            entities = feature_view.entities
            data_sources = []
            source_name = feature_view.batch_source.name
            if feature_view.stream_source:
                data_sources.append(feature_view.stream_source.name)
                source_name = feature_view.stream_source.name
            if feature_view.batch_source:
                data_sources.append(feature_view.batch_source.name)

            update_time = int(datetime.timestamp(datetime.now()))
            for field in feature_view.features:
                field_data = {
                    'name': field.name,
                    'dtype': f"{field.dtype}",
                    'description': field.description or "",
                    'proto_data': field.to_proto().SerializeToString(),
                    'last_updated_timestamp': update_time,
                    # 'parent': source_name
                }
                fields_to_apply.append(field_data)

            with self.driver.session(database=self.database) as session:
                with session.begin_transaction() as tx:
                    created_at = datetime.now()
                    tx.run(
                        f"""
                        MATCH (p:Project {{project_id: $project}})
                        UNWIND $data_sources AS ds
                        WITH ds,p
                            MERGE (p)-[r:CONTAINS]->(d:DataSource {{data_source_name: ds}})
                            ON CREATE SET r.created_at = $created_at;
                        """,
                        data_sources=data_sources,
                        project=project,
                        created_at=created_at
                    )
                    tx.run(
                        f"""
                        MATCH (p:Project {{project_id: $project}})
                        UNWIND $entities AS es
                        WITH es,p
                            MERGE (p)-[r:CONTAINS]->(e:Entity {{entity_name: es}})
                            ON CREATE SET r.created_at = $created_at;
                        """,
                        entities=entities,
                        project=project,
                        created_at=created_at
                    )
                    tx.run(
                        f"""
                        MATCH (p:Project {{project_id: $project}})-[:CONTAINS]->(d:DataSource {{data_source_name: $source}})
                        UNWIND $fields AS field_data
                        WITH field_data,p,d
                            MERGE (d)-[r:HAS]->(f:Field {{field_name:field_data.name}})<-[:CONTAINS]-(p)
                            ON CREATE SET r.created_at = $created_at
                            SET f.data_type = field_data.dtype,
                                f.description = field_data.description,
                                f.field_proto = field_data.proto_data,
                                f.last_updated_timestamp = field_data.last_updated_timestamp
                        """,
                        fields=fields_to_apply,
                        project=project,
                        created_at=created_at,
                        source=source_name
                    )

            # print(f"Batch source: {feature_view.batch_source}")
            
            # Apply batch source
            # self.apply_data_source(
            #     data_source=feature_view.batch_source, 
            #     project=project, 
            #     commit=commit,
            #     just_create=True
            # )   
            # if feature_view.stream_source:
                # print(f"Stream source: {feature_view.stream_source}")
                # Apply stream source
                # self.apply_data_source(
                #     data_source=feature_view.stream_source, 
                #     project=project, 
                #     commit=commit,
                #     just_create=True
                # )  

                # Apply features with batch_source as parent
                # print(f"Features to apply: {feature_view.features}")
                # for field in feature_view.features:
                #     self.apply_field(
                #         field=field, 
                #         project=project, 
                #         parents={
                #             "ds": feature_view.stream_source.name
                #         }, 
                #         commit=commit
                #     )
            # else:
                # Apply features with batch_source as parent
                # print(f"Features to apply: {feature_view.features}")
                # for field in feature_view.features:
                #     self.apply_field(
                #         field=field, 
                #         project=project, 
                #         parents={
                #             "ds": feature_view.batch_source.name
                #         }, 
                #         commit=commit
                #     )
        elif fv_label == on_demand_feature_views:
            # feature_view.infer_feature_dependencies()

            # data_sources = []
            # for rs in feature_view.source_request_sources.values():
            #     data_sources.append(rs.name)
            # feature_view_projections = []
            # for fv in feature_view.source_feature_view_projections.values():
            #     feature_view_projections.append(fv.name)

            # with self.driver.session(database=self.database) as session:
            #     with session.begin_transaction() as tx:
            #         created_at = datetime.now()
            #         tx.run(
            #             f"""
            #             MATCH (p:Project {{project_id: $project}})
            #             MERGE (p)-[:CONTAINS]->(odfv:OnDemandFeatureView {{feature_view_name: $name}})
            #             ON CREATE SET odfv.created_at = $created_at;
            #             """,
            #             name=feature_view.name,
            #             project=project,
            #             created_at=created_at
            #         )
            #         tx.run(
            #             f"""
            #             MATCH (p:Project {{project_id: $project}})
            #             UNWIND $data_sources AS ds
            #             WITH ds,p
            #                 MERGE (p)-[:CONTAINS]->(d:DataSource {{data_source_name: ds}})
            #                 ON CREATE SET d.created_at = $created_at;
            #             """,
            #             data_sources=data_sources,
            #             project=project,
            #             created_at=created_at
            #         )
            #         tx.run(
            #             f"""
            #             MATCH (p:Project {{project_id: $project}})
            #             UNWIND $feature_view_projections AS fvs
            #             WITH fvs,p
            #                 MERGE (p)-[:CONTAINS]->(fv:FeatureView {{feature_view_name: fvs}})
            #                 ON CREATE SET fv.created_at = $created_at;
            #             """,
            #             feature_view_projections=feature_view_projections,
            #             project=project,
            #             created_at=created_at
            #         )

            # Initial apply to create node
            self._apply_object(
                label=fv_label, 
                project=project, 
                id_field_name="feature_view_name", 
                obj=feature_view, 
                proto_field_name="feature_view_proto",
                #just_create=True
            )
            
            # print(f"{feature_view.feature_transformation.udf_string}")
            # print(f"Feature dependencies: {feature_view.feature_dependencies}")
            # Get feature views from feature_view_projections
            # fvs = feature_view.source_feature_view_projections.keys()
            # print(f"Feature view projections: {fvs}")
            # for fv in fvs:
            #     fv_object = self._get_feature_view(fv, project=project)

            # Apply feature views and request sources
            for rs in feature_view.source_request_sources.values():
                # Apply request source
                self.apply_data_source(
                    data_source=rs, 
                    project=project, 
                    commit=commit,
                    just_create=True
                )  

            for field in feature_view.features:
                self.apply_field(
                    field=field, 
                    project=project,
                    parents={
                        "odfv": feature_view.name
                    },
                    commit=commit
                )    
            
            # sources = {
            #     "fvs": list(feature_view.source_feature_view_projections.keys()),
            #     "rs": list(feature_view.source_request_sources.keys())
            # }
            
            # print(f"Sources: {sources}")     
            
        return self._apply_object(
            label=fv_label, 
            project=project, 
            id_field_name="feature_view_name", 
            obj=feature_view, 
            proto_field_name="feature_view_proto"
        )

    def apply_feature_service(self, feature_service: FeatureService, project: str, commit: bool = True):
        # Apply (on demand) feature views
        # print(f"Feature service _features: {feature_service._features}")
        for fv in feature_service._features:
            # print(f"Projection: {fv.projection.name} features: {fv.projection.features}")
            self.apply_feature_view(
                feature_view=fv,
                project=project,
                commit=commit,
                just_create=True
            )

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

    def apply_field(self, field: Field, project: str, parents: Optional[dict]=None, commit: bool = True, just_create: bool = False): 
        return self._apply_object(
            label="Field", 
            project=project, 
            id_field_name="field_name", 
            obj=field, 
            proto_field_name="field_proto",
            parents=parents
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

        with self.driver.session(database=self.database) as session:
            with session.begin_transaction() as tx:
                result = tx.run(
                    f"""
                    MATCH (p:Project {{ project_id: $project }})-[:CONTAINS]->(n:{label} {{ feature_view_name: $name }})
                    RETURN n
                    """, 
                    name=name, 
                    project=project
                )
                node = result.single()

                update_datetime = datetime.now()
                update_time = int(update_datetime.timestamp())
                if node:
                    tx.run(
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
        with self.driver.session(database=self.database) as session:
            with session.begin_transaction() as tx:
                result = tx.run(
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
                    tx.run(
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
                    tx.run(
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

        with self.driver.session(database=self.database) as session:
            with session.begin_transaction() as tx:
                result = tx.run(
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
    
    def _get_field(self, name: str, project: str) -> Field:
        return self._get_object(
            label="Field",
            name=name,
            project=project,
            proto_class=FieldProto,
            python_class=Field,
            id_field_name="field_name",
            proto_field_name="field_proto",
            not_found_exception=FieldNotFoundException
        )
            
    def _get_last_updated_metadata(self, project: str):
        with self.driver.session(database=self.database) as session:
            with session.begin_transaction() as tx:
                result = tx.run(
                    f"""
                    MATCH (p:Project {{ project_id: $project  }})-[:CONTAINS]->(n:ProjectMetadata {{ metadata_key: $key }})
                    RETURN n.last_updated_timestamp AS last_updated_timestamp
                    """, 
                    key=FeastMetadataKeys.LAST_UPDATED_TIMESTAMP.value, 
                    project=project
                )
                node = result.single()
                # print(f"Node: {node}")

                if not node:
                    # print("No metadata node found")
                    return None
                update_time = int(node["last_updated_timestamp"])
                # print(f"Found metadata node with timestamp: {update_time}")

        return datetime.fromtimestamp(update_time)

    def _get_all_projects(self) -> Set[str]:
        projects = set()
        with self.driver.session(database=self.database) as session:
            with session.begin_transaction() as tx:
                result = tx.run(
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

        with self.driver.session(database=self.database) as session:
            with session.begin_transaction() as tx:
                result = tx.run(
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
        with self.driver.session(database=self.database) as session:
            with session.begin_transaction() as tx:
                result = tx.run(
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
        with self.driver.session(database=self.database) as session:
            with session.begin_transaction() as tx:
                result = tx.run(
                    f"""
                    MATCH (p:Project {{ project_id: $project }})-[:CONTAINS]->(n:{label} {{ {id_field_name}: $name }})
                    DETACH DELETE n
                    RETURN COUNT(n) AS deleted_count
                    """, 
                    name=name, 
                    project=project
                )
                node = result.single()

                if node["deleted_count"] < 1 and not_found_exception:
                    raise not_found_exception(name, project)
                self._set_last_updated_metadata(datetime.now(), project)

        return node["deleted_count"]

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
                label=label,
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

    def _create_field_relationship(self, feature_name: str, dep_name: str, project: str, odfv: str) -> None:
        self._maybe_init_project_metadata(project)
        query = f"""
                MATCH (p:Project {{ project_id: {project} }})-[:CONTAINS]->(df:Field {{ field_name: {dep_name} }})
                MATCH (p)-[:CONTAINS]->(f:Field {{ field_name: {feature_name} }})
                MATCH (odfv:OnDemandFeatureView {{ feature_view_name: {odfv} }})-[:BASED_ON]->(s)
                MATCH (odfv)-[:PRODUCES]->(f)
                MATCH (s)-[:HAS]->(df)
                MERGE (df)-[r:USED_FOR]->(f)
                ON CREATE SET r.created_at = $created_time
                """
        # print(f"Query: {query}")

        with self.driver.session(database=self.database) as session:
            with session.begin_transaction() as tx:
                tx.run(
                    f"""
                    MATCH (p:Project {{ project_id: $project }})-[:CONTAINS]->(df:Field {{ field_name: $dep_name }})
                    MATCH (p)-[:CONTAINS]->(f:Field {{ field_name: $feature_name }})
                    MATCH (odfv:OnDemandFeatureView {{ feature_view_name: $odfv }})-[:BASED_ON]->(s)
                    MATCH (odfv)-[:PRODUCES]->(f)
                    MATCH (s)-[:HAS]->(df)
                    MERGE res=(df)-[r:USED_FOR]->(f)
                    ON CREATE SET r.created_at = $created_time
                    RETURN length(res)
                    """, 
                    feature_name=feature_name,
                    dep_name=dep_name, 
                    project=project,
                    odfv=odfv,
                    created_time=datetime.now()
                )
                    