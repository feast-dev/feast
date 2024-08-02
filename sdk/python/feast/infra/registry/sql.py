import logging
import uuid
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Set, Union

from pydantic import StrictStr
from sqlalchemy import (  # type: ignore
    BigInteger,
    Column,
    LargeBinary,
    MetaData,
    String,
    Table,
    create_engine,
    delete,
    insert,
    select,
    update,
)
from sqlalchemy.engine import Engine

from feast import utils
from feast.base_feature_view import BaseFeatureView
from feast.data_source import DataSource
from feast.entity import Entity
from feast.errors import (
    DataSourceObjectNotFoundException,
    EntityNotFoundException,
    FeatureServiceNotFoundException,
    FeatureViewNotFoundException,
    PermissionNotFoundException,
    SavedDatasetNotFound,
    ValidationReferenceNotFound,
)
from feast.feature_service import FeatureService
from feast.feature_view import FeatureView
from feast.infra.infra_object import Infra
from feast.infra.registry.caching_registry import CachingRegistry
from feast.on_demand_feature_view import OnDemandFeatureView
from feast.permissions.permission import Permission
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
from feast.protos.feast.core.Permission_pb2 import Permission as PermissionProto
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
from feast.utils import _utc_now

metadata = MetaData()

entities = Table(
    "entities",
    metadata,
    Column("entity_name", String(50), primary_key=True),
    Column("project_id", String(50), primary_key=True),
    Column("last_updated_timestamp", BigInteger, nullable=False),
    Column("entity_proto", LargeBinary, nullable=False),
)

data_sources = Table(
    "data_sources",
    metadata,
    Column("data_source_name", String(255), primary_key=True),
    Column("project_id", String(50), primary_key=True),
    Column("last_updated_timestamp", BigInteger, nullable=False),
    Column("data_source_proto", LargeBinary, nullable=False),
)

feature_views = Table(
    "feature_views",
    metadata,
    Column("feature_view_name", String(50), primary_key=True),
    Column("project_id", String(50), primary_key=True),
    Column("last_updated_timestamp", BigInteger, nullable=False),
    Column("materialized_intervals", LargeBinary, nullable=True),
    Column("feature_view_proto", LargeBinary, nullable=False),
    Column("user_metadata", LargeBinary, nullable=True),
)

stream_feature_views = Table(
    "stream_feature_views",
    metadata,
    Column("feature_view_name", String(50), primary_key=True),
    Column("project_id", String(50), primary_key=True),
    Column("last_updated_timestamp", BigInteger, nullable=False),
    Column("feature_view_proto", LargeBinary, nullable=False),
    Column("user_metadata", LargeBinary, nullable=True),
)

on_demand_feature_views = Table(
    "on_demand_feature_views",
    metadata,
    Column("feature_view_name", String(50), primary_key=True),
    Column("project_id", String(50), primary_key=True),
    Column("last_updated_timestamp", BigInteger, nullable=False),
    Column("feature_view_proto", LargeBinary, nullable=False),
    Column("user_metadata", LargeBinary, nullable=True),
)

feature_services = Table(
    "feature_services",
    metadata,
    Column("feature_service_name", String(50), primary_key=True),
    Column("project_id", String(50), primary_key=True),
    Column("last_updated_timestamp", BigInteger, nullable=False),
    Column("feature_service_proto", LargeBinary, nullable=False),
)

saved_datasets = Table(
    "saved_datasets",
    metadata,
    Column("saved_dataset_name", String(50), primary_key=True),
    Column("project_id", String(50), primary_key=True),
    Column("last_updated_timestamp", BigInteger, nullable=False),
    Column("saved_dataset_proto", LargeBinary, nullable=False),
)

validation_references = Table(
    "validation_references",
    metadata,
    Column("validation_reference_name", String(50), primary_key=True),
    Column("project_id", String(50), primary_key=True),
    Column("last_updated_timestamp", BigInteger, nullable=False),
    Column("validation_reference_proto", LargeBinary, nullable=False),
)

managed_infra = Table(
    "managed_infra",
    metadata,
    Column("infra_name", String(50), primary_key=True),
    Column("project_id", String(50), primary_key=True),
    Column("last_updated_timestamp", BigInteger, nullable=False),
    Column("infra_proto", LargeBinary, nullable=False),
)

permissions = Table(
    "permissions",
    metadata,
    Column("permission_name", String(255), primary_key=True),
    Column("project_id", String(50), primary_key=True),
    Column("last_updated_timestamp", BigInteger, nullable=False),
    Column("permission_proto", LargeBinary, nullable=False),
)


class FeastMetadataKeys(Enum):
    LAST_UPDATED_TIMESTAMP = "last_updated_timestamp"
    PROJECT_UUID = "project_uuid"


feast_metadata = Table(
    "feast_metadata",
    metadata,
    Column("project_id", String(50), primary_key=True),
    Column("metadata_key", String(50), primary_key=True),
    Column("metadata_value", String(50), nullable=False),
    Column("last_updated_timestamp", BigInteger, nullable=False),
)

logger = logging.getLogger(__name__)


class SqlRegistryConfig(RegistryConfig):
    registry_type: StrictStr = "sql"
    """ str: Provider name or a class name that implements Registry."""

    path: StrictStr = ""
    """ str: Path to metadata store.
    If registry_type is 'sql', then this is a database URL as expected by SQLAlchemy """

    sqlalchemy_config_kwargs: Dict[str, Any] = {"echo": False}
    """ Dict[str, Any]: Extra arguments to pass to SQLAlchemy.create_engine. """


class SqlRegistry(CachingRegistry):
    def __init__(
        self,
        registry_config: Optional[Union[RegistryConfig, SqlRegistryConfig]],
        project: str,
        repo_path: Optional[Path],
    ):
        assert registry_config is not None, "SqlRegistry needs a valid registry_config"

        self.engine: Engine = create_engine(
            registry_config.path, **registry_config.sqlalchemy_config_kwargs
        )
        metadata.create_all(self.engine)
        super().__init__(
            project=project,
            cache_ttl_seconds=registry_config.cache_ttl_seconds,
            cache_mode=registry_config.cache_mode,
        )

    def teardown(self):
        for t in {
            entities,
            data_sources,
            feature_views,
            feature_services,
            on_demand_feature_views,
            saved_datasets,
            validation_references,
            permissions,
        }:
            with self.engine.begin() as conn:
                stmt = delete(t)
                conn.execute(stmt)

    def _get_stream_feature_view(self, name: str, project: str):
        return self._get_object(
            table=stream_feature_views,
            name=name,
            project=project,
            proto_class=StreamFeatureViewProto,
            python_class=StreamFeatureView,
            id_field_name="feature_view_name",
            proto_field_name="feature_view_proto",
            not_found_exception=FeatureViewNotFoundException,
        )

    def _list_stream_feature_views(
        self, project: str, tags: Optional[dict[str, str]]
    ) -> List[StreamFeatureView]:
        return self._list_objects(
            stream_feature_views,
            project,
            StreamFeatureViewProto,
            StreamFeatureView,
            "feature_view_proto",
            tags=tags,
        )

    def apply_entity(self, entity: Entity, project: str, commit: bool = True):
        return self._apply_object(
            table=entities,
            project=project,
            id_field_name="entity_name",
            obj=entity,
            proto_field_name="entity_proto",
        )

    def _get_entity(self, name: str, project: str) -> Entity:
        return self._get_object(
            table=entities,
            name=name,
            project=project,
            proto_class=EntityProto,
            python_class=Entity,
            id_field_name="entity_name",
            proto_field_name="entity_proto",
            not_found_exception=EntityNotFoundException,
        )

    def _get_feature_view(self, name: str, project: str) -> FeatureView:
        return self._get_object(
            table=feature_views,
            name=name,
            project=project,
            proto_class=FeatureViewProto,
            python_class=FeatureView,
            id_field_name="feature_view_name",
            proto_field_name="feature_view_proto",
            not_found_exception=FeatureViewNotFoundException,
        )

    def _get_on_demand_feature_view(
        self, name: str, project: str
    ) -> OnDemandFeatureView:
        return self._get_object(
            table=on_demand_feature_views,
            name=name,
            project=project,
            proto_class=OnDemandFeatureViewProto,
            python_class=OnDemandFeatureView,
            id_field_name="feature_view_name",
            proto_field_name="feature_view_proto",
            not_found_exception=FeatureViewNotFoundException,
        )

    def _get_feature_service(self, name: str, project: str) -> FeatureService:
        return self._get_object(
            table=feature_services,
            name=name,
            project=project,
            proto_class=FeatureServiceProto,
            python_class=FeatureService,
            id_field_name="feature_service_name",
            proto_field_name="feature_service_proto",
            not_found_exception=FeatureServiceNotFoundException,
        )

    def _get_saved_dataset(self, name: str, project: str) -> SavedDataset:
        return self._get_object(
            table=saved_datasets,
            name=name,
            project=project,
            proto_class=SavedDatasetProto,
            python_class=SavedDataset,
            id_field_name="saved_dataset_name",
            proto_field_name="saved_dataset_proto",
            not_found_exception=SavedDatasetNotFound,
        )

    def _get_validation_reference(self, name: str, project: str) -> ValidationReference:
        return self._get_object(
            table=validation_references,
            name=name,
            project=project,
            proto_class=ValidationReferenceProto,
            python_class=ValidationReference,
            id_field_name="validation_reference_name",
            proto_field_name="validation_reference_proto",
            not_found_exception=ValidationReferenceNotFound,
        )

    def _list_validation_references(
        self, project: str, tags: Optional[dict[str, str]] = None
    ) -> List[ValidationReference]:
        return self._list_objects(
            table=validation_references,
            project=project,
            proto_class=ValidationReferenceProto,
            python_class=ValidationReference,
            proto_field_name="validation_reference_proto",
            tags=tags,
        )

    def _list_entities(
        self, project: str, tags: Optional[dict[str, str]]
    ) -> List[Entity]:
        return self._list_objects(
            entities, project, EntityProto, Entity, "entity_proto", tags=tags
        )

    def delete_entity(self, name: str, project: str, commit: bool = True):
        return self._delete_object(
            entities, name, project, "entity_name", EntityNotFoundException
        )

    def delete_feature_view(self, name: str, project: str, commit: bool = True):
        deleted_count = 0
        for table in {
            feature_views,
            on_demand_feature_views,
            stream_feature_views,
        }:
            deleted_count += self._delete_object(
                table, name, project, "feature_view_name", None
            )
        if deleted_count == 0:
            raise FeatureViewNotFoundException(name, project)

    def delete_feature_service(self, name: str, project: str, commit: bool = True):
        return self._delete_object(
            feature_services,
            name,
            project,
            "feature_service_name",
            FeatureServiceNotFoundException,
        )

    def _get_data_source(self, name: str, project: str) -> DataSource:
        return self._get_object(
            table=data_sources,
            name=name,
            project=project,
            proto_class=DataSourceProto,
            python_class=DataSource,
            id_field_name="data_source_name",
            proto_field_name="data_source_proto",
            not_found_exception=DataSourceObjectNotFoundException,
        )

    def _list_data_sources(
        self, project: str, tags: Optional[dict[str, str]]
    ) -> List[DataSource]:
        return self._list_objects(
            data_sources,
            project,
            DataSourceProto,
            DataSource,
            "data_source_proto",
            tags=tags,
        )

    def apply_data_source(
        self, data_source: DataSource, project: str, commit: bool = True
    ):
        return self._apply_object(
            data_sources, project, "data_source_name", data_source, "data_source_proto"
        )

    def apply_feature_view(
        self, feature_view: BaseFeatureView, project: str, commit: bool = True
    ):
        fv_table = self._infer_fv_table(feature_view)

        return self._apply_object(
            fv_table, project, "feature_view_name", feature_view, "feature_view_proto"
        )

    def apply_feature_service(
        self, feature_service: FeatureService, project: str, commit: bool = True
    ):
        return self._apply_object(
            feature_services,
            project,
            "feature_service_name",
            feature_service,
            "feature_service_proto",
        )

    def delete_data_source(self, name: str, project: str, commit: bool = True):
        with self.engine.begin() as conn:
            stmt = delete(data_sources).where(
                data_sources.c.data_source_name == name,
                data_sources.c.project_id == project,
            )
            rows = conn.execute(stmt)
            if rows.rowcount < 1:
                raise DataSourceObjectNotFoundException(name, project)

    def _list_feature_services(
        self, project: str, tags: Optional[dict[str, str]]
    ) -> List[FeatureService]:
        return self._list_objects(
            feature_services,
            project,
            FeatureServiceProto,
            FeatureService,
            "feature_service_proto",
            tags=tags,
        )

    def _list_feature_views(
        self, project: str, tags: Optional[dict[str, str]]
    ) -> List[FeatureView]:
        return self._list_objects(
            feature_views,
            project,
            FeatureViewProto,
            FeatureView,
            "feature_view_proto",
            tags=tags,
        )

    def _list_saved_datasets(
        self, project: str, tags: Optional[dict[str, str]] = None
    ) -> List[SavedDataset]:
        return self._list_objects(
            saved_datasets,
            project,
            SavedDatasetProto,
            SavedDataset,
            "saved_dataset_proto",
            tags=tags,
        )

    def _list_on_demand_feature_views(
        self, project: str, tags: Optional[dict[str, str]]
    ) -> List[OnDemandFeatureView]:
        return self._list_objects(
            on_demand_feature_views,
            project,
            OnDemandFeatureViewProto,
            OnDemandFeatureView,
            "feature_view_proto",
            tags=tags,
        )

    def _list_project_metadata(self, project: str) -> List[ProjectMetadata]:
        with self.engine.begin() as conn:
            stmt = select(feast_metadata).where(
                feast_metadata.c.project_id == project,
            )
            rows = conn.execute(stmt).all()
            if rows:
                project_metadata = ProjectMetadata(project_name=project)
                for row in rows:
                    if (
                        row._mapping["metadata_key"]
                        == FeastMetadataKeys.PROJECT_UUID.value
                    ):
                        project_metadata.project_uuid = row._mapping["metadata_value"]
                        break
                    # TODO(adchia): Add other project metadata in a structured way
                return [project_metadata]
        return []

    def apply_saved_dataset(
        self,
        saved_dataset: SavedDataset,
        project: str,
        commit: bool = True,
    ):
        return self._apply_object(
            saved_datasets,
            project,
            "saved_dataset_name",
            saved_dataset,
            "saved_dataset_proto",
        )

    def apply_validation_reference(
        self,
        validation_reference: ValidationReference,
        project: str,
        commit: bool = True,
    ):
        return self._apply_object(
            validation_references,
            project,
            "validation_reference_name",
            validation_reference,
            "validation_reference_proto",
        )

    def apply_materialization(
        self,
        feature_view: FeatureView,
        project: str,
        start_date: datetime,
        end_date: datetime,
        commit: bool = True,
    ):
        table = self._infer_fv_table(feature_view)
        python_class, proto_class = self._infer_fv_classes(feature_view)

        if python_class in {OnDemandFeatureView}:
            raise ValueError(
                f"Cannot apply materialization for feature {feature_view.name} of type {python_class}"
            )
        fv: Union[FeatureView, StreamFeatureView] = self._get_object(
            table,
            feature_view.name,
            project,
            proto_class,
            python_class,
            "feature_view_name",
            "feature_view_proto",
            FeatureViewNotFoundException,
        )
        fv.materialization_intervals.append((start_date, end_date))
        self._apply_object(
            table, project, "feature_view_name", fv, "feature_view_proto"
        )

    def delete_validation_reference(self, name: str, project: str, commit: bool = True):
        self._delete_object(
            validation_references,
            name,
            project,
            "validation_reference_name",
            ValidationReferenceNotFound,
        )

    def update_infra(self, infra: Infra, project: str, commit: bool = True):
        self._apply_object(
            table=managed_infra,
            project=project,
            id_field_name="infra_name",
            obj=infra,
            proto_field_name="infra_proto",
            name="infra_obj",
        )

    def _get_infra(self, project: str) -> Infra:
        infra_object = self._get_object(
            table=managed_infra,
            name="infra_obj",
            project=project,
            proto_class=InfraProto,
            python_class=Infra,
            id_field_name="infra_name",
            proto_field_name="infra_proto",
            not_found_exception=None,
        )
        if infra_object:
            return infra_object
        return Infra()

    def apply_user_metadata(
        self,
        project: str,
        feature_view: BaseFeatureView,
        metadata_bytes: Optional[bytes],
    ):
        table = self._infer_fv_table(feature_view)

        name = feature_view.name
        with self.engine.begin() as conn:
            stmt = select(table).where(
                getattr(table.c, "feature_view_name") == name,
                table.c.project_id == project,
            )
            row = conn.execute(stmt).first()
            update_datetime = _utc_now()
            update_time = int(update_datetime.timestamp())
            if row:
                values = {
                    "user_metadata": metadata_bytes,
                    "last_updated_timestamp": update_time,
                }
                update_stmt = (
                    update(table)
                    .where(
                        getattr(table.c, "feature_view_name") == name,
                        table.c.project_id == project,
                    )
                    .values(
                        values,
                    )
                )
                conn.execute(update_stmt)
            else:
                raise FeatureViewNotFoundException(feature_view.name, project=project)

    def _infer_fv_table(self, feature_view):
        if isinstance(feature_view, StreamFeatureView):
            table = stream_feature_views
        elif isinstance(feature_view, FeatureView):
            table = feature_views
        elif isinstance(feature_view, OnDemandFeatureView):
            table = on_demand_feature_views
        else:
            raise ValueError(f"Unexpected feature view type: {type(feature_view)}")
        return table

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

    def get_user_metadata(
        self, project: str, feature_view: BaseFeatureView
    ) -> Optional[bytes]:
        table = self._infer_fv_table(feature_view)

        name = feature_view.name
        with self.engine.begin() as conn:
            stmt = select(table).where(getattr(table.c, "feature_view_name") == name)
            row = conn.execute(stmt).first()
            if row:
                return row._mapping["user_metadata"]
            else:
                raise FeatureViewNotFoundException(feature_view.name, project=project)

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
                (self.list_permissions, r.permissions),
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

    def _apply_object(
        self,
        table: Table,
        project: str,
        id_field_name: str,
        obj: Any,
        proto_field_name: str,
        name: Optional[str] = None,
    ):
        self._maybe_init_project_metadata(project)

        name = name or (obj.name if hasattr(obj, "name") else None)
        assert name, f"name needs to be provided for {obj}"

        with self.engine.begin() as conn:
            update_datetime = _utc_now()
            update_time = int(update_datetime.timestamp())
            stmt = select(table).where(
                getattr(table.c, id_field_name) == name, table.c.project_id == project
            )
            row = conn.execute(stmt).first()
            if hasattr(obj, "last_updated_timestamp"):
                obj.last_updated_timestamp = update_datetime

            if row:
                if proto_field_name in [
                    "entity_proto",
                    "saved_dataset_proto",
                    "feature_view_proto",
                    "feature_service_proto",
                    "permission_proto",
                ]:
                    deserialized_proto = self.deserialize_registry_values(
                        row._mapping[proto_field_name], type(obj)
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
                values = {
                    proto_field_name: obj.to_proto().SerializeToString(),
                    "last_updated_timestamp": update_time,
                }
                update_stmt = (
                    update(table)
                    .where(
                        getattr(table.c, id_field_name) == name,
                        table.c.project_id == project,
                    )
                    .values(
                        values,
                    )
                )
                conn.execute(update_stmt)
            else:
                obj_proto = obj.to_proto()

                if hasattr(obj_proto, "meta") and hasattr(
                    obj_proto.meta, "created_timestamp"
                ):
                    obj_proto.meta.created_timestamp.FromDatetime(update_datetime)

                values = {
                    id_field_name: name,
                    proto_field_name: obj_proto.SerializeToString(),
                    "last_updated_timestamp": update_time,
                    "project_id": project,
                }
                insert_stmt = insert(table).values(
                    values,
                )
                conn.execute(insert_stmt)

            self._set_last_updated_metadata(update_datetime, project)

    def _maybe_init_project_metadata(self, project):
        # Initialize project metadata if needed
        with self.engine.begin() as conn:
            update_datetime = _utc_now()
            update_time = int(update_datetime.timestamp())
            stmt = select(feast_metadata).where(
                feast_metadata.c.metadata_key == FeastMetadataKeys.PROJECT_UUID.value,
                feast_metadata.c.project_id == project,
            )
            row = conn.execute(stmt).first()
            if not row:
                new_project_uuid = f"{uuid.uuid4()}"
                values = {
                    "metadata_key": FeastMetadataKeys.PROJECT_UUID.value,
                    "metadata_value": new_project_uuid,
                    "last_updated_timestamp": update_time,
                    "project_id": project,
                }
                insert_stmt = insert(feast_metadata).values(values)
                conn.execute(insert_stmt)

    def _delete_object(
        self,
        table: Table,
        name: str,
        project: str,
        id_field_name: str,
        not_found_exception: Optional[Callable],
    ):
        with self.engine.begin() as conn:
            stmt = delete(table).where(
                getattr(table.c, id_field_name) == name, table.c.project_id == project
            )
            rows = conn.execute(stmt)
            if rows.rowcount < 1 and not_found_exception:
                raise not_found_exception(name, project)
            self._set_last_updated_metadata(_utc_now(), project)

            return rows.rowcount

    def _get_object(
        self,
        table: Table,
        name: str,
        project: str,
        proto_class: Any,
        python_class: Any,
        id_field_name: str,
        proto_field_name: str,
        not_found_exception: Optional[Callable],
    ):
        self._maybe_init_project_metadata(project)

        with self.engine.begin() as conn:
            stmt = select(table).where(
                getattr(table.c, id_field_name) == name, table.c.project_id == project
            )
            row = conn.execute(stmt).first()
            if row:
                _proto = proto_class.FromString(row._mapping[proto_field_name])
                return python_class.from_proto(_proto)
        if not_found_exception:
            raise not_found_exception(name, project)
        else:
            return None

    def _list_objects(
        self,
        table: Table,
        project: str,
        proto_class: Any,
        python_class: Any,
        proto_field_name: str,
        tags: Optional[dict[str, str]] = None,
    ):
        self._maybe_init_project_metadata(project)
        with self.engine.begin() as conn:
            stmt = select(table).where(table.c.project_id == project)
            rows = conn.execute(stmt).all()
            if rows:
                objects = []
                for row in rows:
                    obj = python_class.from_proto(
                        proto_class.FromString(row._mapping[proto_field_name])
                    )
                    if utils.has_all_tags(obj.tags, tags):
                        objects.append(obj)
                return objects
        return []

    def _set_last_updated_metadata(self, last_updated: datetime, project: str):
        with self.engine.begin() as conn:
            stmt = select(feast_metadata).where(
                feast_metadata.c.metadata_key
                == FeastMetadataKeys.LAST_UPDATED_TIMESTAMP.value,
                feast_metadata.c.project_id == project,
            )
            row = conn.execute(stmt).first()

            update_time = int(last_updated.timestamp())

            values = {
                "metadata_key": FeastMetadataKeys.LAST_UPDATED_TIMESTAMP.value,
                "metadata_value": f"{update_time}",
                "last_updated_timestamp": update_time,
                "project_id": project,
            }
            if row:
                update_stmt = (
                    update(feast_metadata)
                    .where(
                        feast_metadata.c.metadata_key
                        == FeastMetadataKeys.LAST_UPDATED_TIMESTAMP.value,
                        feast_metadata.c.project_id == project,
                    )
                    .values(values)
                )
                conn.execute(update_stmt)
            else:
                insert_stmt = insert(feast_metadata).values(
                    values,
                )
                conn.execute(insert_stmt)

    def _get_last_updated_metadata(self, project: str):
        with self.engine.begin() as conn:
            stmt = select(feast_metadata).where(
                feast_metadata.c.metadata_key
                == FeastMetadataKeys.LAST_UPDATED_TIMESTAMP.value,
                feast_metadata.c.project_id == project,
            )
            row = conn.execute(stmt).first()
            if not row:
                return None
            update_time = int(row._mapping["last_updated_timestamp"])

            return datetime.fromtimestamp(update_time, tz=timezone.utc)

    def _get_all_projects(self) -> Set[str]:
        projects = set()
        with self.engine.begin() as conn:
            for table in {
                entities,
                data_sources,
                feature_views,
                on_demand_feature_views,
                stream_feature_views,
                permissions,
            }:
                stmt = select(table)
                rows = conn.execute(stmt).all()
                for row in rows:
                    projects.add(row._mapping["project_id"])

        return projects

    def _get_permission(self, name: str, project: str) -> Permission:
        return self._get_object(
            table=permissions,
            name=name,
            project=project,
            proto_class=PermissionProto,
            python_class=Permission,
            id_field_name="permission_name",
            proto_field_name="permission_proto",
            not_found_exception=PermissionNotFoundException,
        )

    def _list_permissions(
        self, project: str, tags: Optional[dict[str, str]]
    ) -> List[Permission]:
        return self._list_objects(
            permissions,
            project,
            PermissionProto,
            Permission,
            "permission_proto",
            tags=tags,
        )

    def apply_permission(
        self, permission: Permission, project: str, commit: bool = True
    ):
        return self._apply_object(
            permissions, project, "permission_name", permission, "permission_proto"
        )

    def delete_permission(self, name: str, project: str, commit: bool = True):
        with self.engine.begin() as conn:
            stmt = delete(permissions).where(
                permissions.c.permission_name == name,
                permissions.c.project_id == project,
            )
            rows = conn.execute(stmt)
            if rows.rowcount < 1:
                raise PermissionNotFoundException(name, project)
