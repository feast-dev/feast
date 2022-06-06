from datetime import datetime
from pathlib import Path
from threading import Lock
from typing import Any, List, Optional

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
from feast.on_demand_feature_view import OnDemandFeatureView
from feast.protos.feast.core.DataSource_pb2 import DataSource as DataSourceProto
from feast.protos.feast.core.Entity_pb2 import Entity as EntityProto
from feast.protos.feast.core.FeatureService_pb2 import (
    FeatureService as FeatureServiceProto,
)
from feast.protos.feast.core.FeatureView_pb2 import FeatureView as FeatureViewProto
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
from feast.registry import BaseRegistry
from feast.repo_config import RegistryConfig
from feast.request_feature_view import RequestFeatureView
from feast.saved_dataset import SavedDataset, ValidationReference
from feast.stream_feature_view import StreamFeatureView

metadata = MetaData()

entities = Table(
    "entities",
    metadata,
    Column("entity_name", String(50), primary_key=True),
    Column("last_updated_timestamp", BigInteger, nullable=False),
    Column("entity_proto", LargeBinary, nullable=False),
)

data_sources = Table(
    "data_sources",
    metadata,
    Column("data_source_name", String(50), primary_key=True),
    Column("last_updated_timestamp", BigInteger, nullable=False),
    Column("data_source_proto", LargeBinary, nullable=False),
)

feature_views = Table(
    "feature_views",
    metadata,
    Column("feature_view_name", String(50), primary_key=True),
    Column("last_updated_timestamp", BigInteger, nullable=False),
    Column("materialized_intervals", LargeBinary, nullable=True),
    Column("feature_view_proto", LargeBinary, nullable=False),
    Column("user_metadata", LargeBinary, nullable=True),
)

request_feature_views = Table(
    "request_feature_views",
    metadata,
    Column("feature_view_name", String(50), primary_key=True),
    Column("last_updated_timestamp", BigInteger, nullable=False),
    Column("feature_view_proto", LargeBinary, nullable=False),
    Column("user_metadata", LargeBinary, nullable=True),
)

streaming_feature_views = Table(
    "streaming_feature_views",
    metadata,
    Column("feature_view_name", String(50), primary_key=True),
    Column("last_updated_timestamp", BigInteger, nullable=False),
    Column("feature_view_proto", LargeBinary, nullable=False),
    Column("user_metadata", LargeBinary, nullable=True),
)

on_demand_feature_views = Table(
    "on_demand_feature_views",
    metadata,
    Column("feature_view_name", String(50), primary_key=True),
    Column("last_updated_timestamp", BigInteger, nullable=False),
    Column("feature_view_proto", LargeBinary, nullable=False),
    Column("user_metadata", LargeBinary, nullable=True),
)

feature_services = Table(
    "feature_services",
    metadata,
    Column("feature_service_name", String(50), primary_key=True),
    Column("last_updated_timestamp", BigInteger, nullable=False),
    Column("feature_service_proto", LargeBinary, nullable=False),
)

saved_datasets = Table(
    "saved_datasets",
    metadata,
    Column("saved_dataset_name", String(50), primary_key=True),
    Column("last_updated_timestamp", BigInteger, nullable=False),
    Column("saved_dataset_proto", LargeBinary, nullable=False),
)

validation_references = Table(
    "validation_references",
    metadata,
    Column("validation_reference_name", String(50), primary_key=True),
    Column("last_updated_timestamp", BigInteger, nullable=False),
    Column("validation_reference_proto", LargeBinary, nullable=False),
)


class SqlRegistry(BaseRegistry):
    def __init__(
        self, registry_config: Optional[RegistryConfig], repo_path: Optional[Path]
    ):
        assert registry_config is not None, "SqlRegistry needs a valid registry_config"
        self.engine: Engine = create_engine(registry_config.path, echo=False)
        metadata.create_all(self.engine)

        # _refresh_lock is not used by the SqlRegistry, but is present to conform to the
        # Registry class.
        # TODO: remove external references to _refresh_lock and remove field.
        self._refresh_lock = Lock()

    def teardown(self):
        for t in {
            entities,
            data_sources,
            feature_views,
            feature_services,
            on_demand_feature_views,
            request_feature_views,
            saved_datasets,
            validation_references,
        }:
            with self.engine.connect() as conn:
                stmt = delete(t)
                conn.execute(stmt)

    def refresh(self):
        pass

    def get_stream_feature_view(
        self, name: str, project: str, allow_cache: bool = False
    ):
        return self._get_object(
            streaming_feature_views,
            name,
            project,
            StreamFeatureViewProto,
            StreamFeatureView,
            "feature_view_name",
            "feature_view_proto",
            FeatureViewNotFoundException,
        )

    def list_stream_feature_views(
        self, project: str, allow_cache: bool = False
    ) -> List[StreamFeatureView]:
        return self._list_objects(
            streaming_feature_views,
            StreamFeatureViewProto,
            StreamFeatureView,
            "feature_view_proto",
        )

    def apply_entity(self, entity: Entity, project: str, commit: bool = True):
        return self._apply_object(entities, "entity_name", entity, "entity_proto")

    def get_entity(self, name: str, project: str, allow_cache: bool = False) -> Entity:
        return self._get_object(
            entities,
            name,
            project,
            EntityProto,
            Entity,
            "entity_name",
            "entity_proto",
            EntityNotFoundException,
        )

    def get_feature_view(
        self, name: str, project: str, allow_cache: bool = False
    ) -> FeatureView:
        return self._get_object(
            feature_views,
            name,
            project,
            FeatureViewProto,
            FeatureView,
            "feature_view_name",
            "feature_view_proto",
            FeatureViewNotFoundException,
        )

    def get_on_demand_feature_view(
        self, name: str, project: str, allow_cache: bool = False
    ) -> OnDemandFeatureView:
        return self._get_object(
            on_demand_feature_views,
            name,
            project,
            OnDemandFeatureViewProto,
            OnDemandFeatureView,
            "feature_view_name",
            "feature_view_proto",
            FeatureViewNotFoundException,
        )

    def get_request_feature_view(self, name: str, project: str):
        return self._get_object(
            request_feature_views,
            name,
            project,
            RequestFeatureViewProto,
            RequestFeatureView,
            "feature_view_name",
            "feature_view_proto",
            FeatureViewNotFoundException,
        )

    def get_feature_service(
        self, name: str, project: str, allow_cache: bool = False
    ) -> FeatureService:
        return self._get_object(
            feature_services,
            name,
            project,
            FeatureServiceProto,
            FeatureService,
            "feature_service_name",
            "feature_service_proto",
            FeatureServiceNotFoundException,
        )

    def get_saved_dataset(
        self, name: str, project: str, allow_cache: bool = False
    ) -> SavedDataset:
        return self._get_object(
            saved_datasets,
            name,
            project,
            SavedDatasetProto,
            SavedDataset,
            "saved_dataset_name",
            "saved_dataset_proto",
            SavedDatasetNotFound,
        )

    def get_validation_reference(
        self, name: str, project: str, allow_cache: bool = False
    ) -> ValidationReference:
        return self._get_object(
            validation_references,
            name,
            project,
            ValidationReferenceProto,
            ValidationReference,
            "validation_reference_name",
            "validation_reference_proto",
            ValidationReferenceNotFound,
        )

    def list_entities(self, project: str, allow_cache: bool = False) -> List[Entity]:
        return self._list_objects(entities, EntityProto, Entity, "entity_proto")

    def delete_entity(self, name: str, project: str, commit: bool = True):
        return self._delete_object(
            entities, name, project, "entity_name", EntityNotFoundException
        )

    def delete_feature_view(self, name: str, project: str, commit: bool = True):
        deleted_count = 0
        for table in {
            feature_views,
            request_feature_views,
            on_demand_feature_views,
            streaming_feature_views,
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

    def get_data_source(
        self, name: str, project: str, allow_cache: bool = False
    ) -> DataSource:
        return self._get_object(
            data_sources,
            name,
            project,
            DataSourceProto,
            DataSource,
            "data_source_name",
            "data_source_proto",
            DataSourceObjectNotFoundException,
        )

    def list_data_sources(
        self, project: str, allow_cache: bool = False
    ) -> List[DataSource]:
        return self._list_objects(
            data_sources, DataSourceProto, DataSource, "data_source_proto"
        )

    def apply_data_source(
        self, data_source: DataSource, project: str, commit: bool = True
    ):
        return self._apply_object(
            data_sources, "data_source_name", data_source, "data_source_proto"
        )

    def apply_feature_view(
        self, feature_view: BaseFeatureView, project: str, commit: bool = True
    ):
        if isinstance(feature_view, FeatureView):
            fv_table = feature_views
        elif isinstance(feature_view, OnDemandFeatureView):
            fv_table = on_demand_feature_views
        elif isinstance(feature_view, RequestFeatureView):
            fv_table = request_feature_views
        elif isinstance(feature_view, StreamFeatureView):
            fv_table = streaming_feature_views
        else:
            raise ValueError(f"Unexpected feature view type: {type(feature_view)}")

        return self._apply_object(
            fv_table, "feature_view_name", feature_view, "feature_view_proto"
        )

    def apply_feature_service(
        self, feature_service: FeatureService, project: str, commit: bool = True
    ):
        return self._apply_object(
            feature_services,
            "feature_service_name",
            feature_service,
            "feature_service_proto",
        )

    def delete_data_source(self, name: str, project: str, commit: bool = True):
        with self.engine.connect() as conn:
            stmt = delete(data_sources).where(data_sources.c.data_source_name == name)
            rows = conn.execute(stmt)
            if rows.rowcount < 1:
                raise DataSourceObjectNotFoundException(name, project)

    def list_feature_services(
        self, project: str, allow_cache: bool = False
    ) -> List[FeatureService]:
        return self._list_objects(
            feature_services,
            FeatureServiceProto,
            FeatureService,
            "feature_service_proto",
        )

    def list_feature_views(
        self, project: str, allow_cache: bool = False
    ) -> List[FeatureView]:
        return self._list_objects(
            feature_views, FeatureViewProto, FeatureView, "feature_view_proto"
        )

    def list_saved_datasets(
        self, project: str, allow_cache: bool = False
    ) -> List[SavedDataset]:
        return self._list_objects(
            saved_datasets, SavedDatasetProto, SavedDataset, "saved_dataset_proto"
        )

    def list_request_feature_views(
        self, project: str, allow_cache: bool = False
    ) -> List[RequestFeatureView]:
        return self._list_objects(
            request_feature_views,
            RequestFeatureViewProto,
            RequestFeatureView,
            "feature_view_proto",
        )

    def list_on_demand_feature_views(
        self, project: str, allow_cache: bool = False
    ) -> List[OnDemandFeatureView]:
        return self._list_objects(
            on_demand_feature_views,
            OnDemandFeatureViewProto,
            OnDemandFeatureView,
            "feature_view_proto",
        )

    def apply_saved_dataset(
        self, saved_dataset: SavedDataset, project: str, commit: bool = True,
    ):
        return self._apply_object(
            saved_datasets, "saved_dataset_name", saved_dataset, "saved_dataset_proto"
        )

    def apply_validation_reference(
        self,
        validation_reference: ValidationReference,
        project: str,
        commit: bool = True,
    ):
        return self._apply_object(
            validation_references,
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
        pass

    def delete_validation_reference(self, name: str, project: str, commit: bool = True):
        self._delete_object(
            validation_references,
            name,
            project,
            "validation_reference_name",
            ValidationReferenceNotFound,
        )

    def update_infra(self, infra: Infra, project: str, commit: bool = True):
        pass

    def get_infra(self, project: str, allow_cache: bool = False) -> Infra:
        pass

    def apply_user_metadata(
        self,
        project: str,
        feature_view: BaseFeatureView,
        metadata_bytes: Optional[bytes],
    ):
        if isinstance(feature_view, FeatureView):
            table = feature_views
        elif isinstance(feature_view, OnDemandFeatureView):
            table = on_demand_feature_views
        elif isinstance(feature_view, RequestFeatureView):
            table = request_feature_views
        elif isinstance(feature_view, StreamFeatureView):
            table = streaming_feature_views
        else:
            raise ValueError(f"Unexpected feature view type: {type(feature_view)}")

        name = feature_view.name
        with self.engine.connect() as conn:
            stmt = select(table).where(getattr(table.c, "feature_view_name") == name)
            row = conn.execute(stmt).first()
            update_datetime = datetime.utcnow()
            update_time = int(update_datetime.timestamp())
            if row:
                values = {
                    "user_metadata": metadata_bytes,
                    "last_updated_timestamp": update_time,
                }
                update_stmt = (
                    update(table)
                    .where(getattr(table.c, "feature_view_name") == name)
                    .values(values,)
                )
                conn.execute(update_stmt)
            else:
                raise FeatureViewNotFoundException(feature_view.name, project=project)

    def get_user_metadata(
        self, project: str, feature_view: BaseFeatureView
    ) -> Optional[bytes]:
        if isinstance(feature_view, FeatureView):
            table = feature_views
        elif isinstance(feature_view, OnDemandFeatureView):
            table = on_demand_feature_views
        elif isinstance(feature_view, RequestFeatureView):
            table = request_feature_views
        elif isinstance(feature_view, StreamFeatureView):
            table = streaming_feature_views
        else:
            raise ValueError(f"Unexpected feature view type: {type(feature_view)}")

        name = feature_view.name
        with self.engine.connect() as conn:
            stmt = select(table).where(getattr(table.c, "feature_view_name") == name)
            row = conn.execute(stmt).first()
            if row:
                return row["user_metadata"]
            else:
                raise FeatureViewNotFoundException(feature_view.name, project=project)

    def proto(self) -> RegistryProto:
        r = RegistryProto()
        project = ""
        # TODO(achal): Support Infra object, and last_updated_timestamp.
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
        ]:
            objs: List[Any] = lister(project)  # type: ignore
            registry_proto_field.extend([obj.to_proto() for obj in objs])

        return r

    def commit(self):
        pass

    def _apply_object(
        self, table, id_field_name, obj, proto_field_name,
    ):
        name = obj.name
        with self.engine.connect() as conn:
            stmt = select(table).where(getattr(table.c, id_field_name) == name)
            row = conn.execute(stmt).first()
            update_datetime = datetime.utcnow()
            update_time = int(update_datetime.timestamp())
            if hasattr(obj, "last_updated_timestamp"):
                obj.last_updated_timestamp = update_datetime

            if row:
                values = {
                    proto_field_name: obj.to_proto().SerializeToString(),
                    "last_updated_timestamp": update_time,
                }
                update_stmt = (
                    update(table)
                    .where(getattr(table.c, id_field_name) == name)
                    .values(values,)
                )
                conn.execute(update_stmt)
            else:
                values = {
                    id_field_name: name,
                    proto_field_name: obj.to_proto().SerializeToString(),
                    "last_updated_timestamp": update_time,
                }
                insert_stmt = insert(table).values(values,)
                conn.execute(insert_stmt)

    def _delete_object(self, table, name, project, id_field_name, not_found_exception):
        with self.engine.connect() as conn:
            stmt = delete(table).where(getattr(table.c, id_field_name) == name)
            rows = conn.execute(stmt)
            if rows.rowcount < 1 and not_found_exception:
                raise not_found_exception(name, project)
            return rows.rowcount

    def _get_object(
        self,
        table,
        name,
        project,
        proto_class,
        python_class,
        id_field_name,
        proto_field_name,
        not_found_exception,
    ):
        with self.engine.connect() as conn:
            stmt = select(table).where(getattr(table.c, id_field_name) == name)
            row = conn.execute(stmt).first()
            if row:
                _proto = proto_class.FromString(row[proto_field_name])
                return python_class.from_proto(_proto)
        raise not_found_exception(name, project)

    def _list_objects(self, table, proto_class, python_class, proto_field_name):
        with self.engine.connect() as conn:
            stmt = select(table)
            rows = conn.execute(stmt).all()
            if rows:
                return [
                    python_class.from_proto(
                        proto_class.FromString(row[proto_field_name])
                    )
                    for row in rows
                ]
        return []
