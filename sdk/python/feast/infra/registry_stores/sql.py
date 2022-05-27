from datetime import datetime
from pathlib import Path
from threading import Lock
from typing import List, Optional

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
from feast.errors import DataSourceObjectNotFoundException, EntityNotFoundException
from feast.feature_service import FeatureService
from feast.feature_view import FeatureView
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
from feast.protos.feast.core.RequestFeatureView_pb2 import (
    RequestFeatureView as RequestFeatureViewProto,
)
from feast.registry import Registry
from feast.repo_config import RegistryConfig
from feast.request_feature_view import RequestFeatureView
from feast.saved_dataset import SavedDataset

metadata = MetaData()

entities = Table(
    "entities",
    metadata,
    Column("entity_id", String, primary_key=True),
    Column("last_updated_timestamp", BigInteger, nullable=False),
    Column("entity_proto", LargeBinary, nullable=False),
)

data_sources = Table(
    "data_sources",
    metadata,
    Column("data_source_name", String, primary_key=True),
    Column("last_updated_timestamp", BigInteger, nullable=False),
    Column("data_source_proto", LargeBinary, nullable=False),
)

feature_views = Table(
    "feature_views",
    metadata,
    Column("feature_view_name", String, primary_key=True),
    Column("last_updated_timestamp", BigInteger, nullable=False),
    Column("materialized_intervals", LargeBinary, nullable=True),
    Column("feature_view_proto", LargeBinary, nullable=False),
)

request_feature_views = Table(
    "request_feature_views",
    metadata,
    Column("feature_view_name", String, primary_key=True),
    Column("last_updated_timestamp", BigInteger, nullable=False),
    Column("feature_view_proto", LargeBinary, nullable=False),
)

on_demand_feature_views = Table(
    "on_demand_feature_views",
    metadata,
    Column("feature_view_name", String, primary_key=True),
    Column("last_updated_timestamp", BigInteger, nullable=False),
    Column("feature_view_proto", LargeBinary, nullable=False),
)

feature_user_metadata = Table(
    "feature_metadata",
    metadata,
    Column("feature_name", String, primary_key=True),
    Column("last_updated_timestamp", BigInteger, nullable=False),
    Column("feature_metadata_binary", LargeBinary, nullable=False),
)

feature_services = Table(
    "feature_services",
    metadata,
    Column("feature_service_name", String, primary_key=True),
    Column("last_updated_timestamp", BigInteger, nullable=False),
    Column("feature_service_proto", LargeBinary, nullable=False),
)

APPLY_OPERATIONS = {"entity": (entities, entities.c.entity_id)}


class SqlRegistry(Registry):
    def __init__(
        self, registry_config: Optional[RegistryConfig], repo_path: Optional[Path]
    ):
        assert registry_config
        self.engine: Engine = create_engine(registry_config.path, echo=False)
        metadata.create_all(self.engine)
        self._refresh_lock = Lock()

    def teardown(self):
        for t in {
            feature_views,
            feature_services,
            data_sources,
            on_demand_feature_views,
            request_feature_views,
        }:
            with self.engine.connect() as conn:
                stmt = delete(t)
                conn.execute(stmt)

    def refresh(self):
        pass

    def apply_entity(self, entity: Entity, project: str, commit: bool = True):
        with self.engine.connect() as conn:
            stmt = select(entities).where(entities.c.entity_id == entity.name)
            entity.last_updated_timestamp = datetime.utcnow()
            row = conn.execute(stmt).first()
            if row:
                update_stmt = (
                    update(entities)
                    .where(entities.c.entity_id == entity.name,)
                    .values(
                        entity_proto=entity.to_proto().SerializeToString(),
                        last_updated_timestamp=int(
                            entity.last_updated_timestamp.timestamp()
                        ),
                    )
                )
                conn.execute(update_stmt)
            else:
                insert_stmt = insert(entities).values(
                    entity_id=entity.name,
                    entity_proto=entity.to_proto().SerializeToString(),
                    last_updated_timestamp=int(
                        entity.last_updated_timestamp.timestamp()
                    ),
                )
                conn.execute(insert_stmt)

    def get_entity(self, name: str, project: str, allow_cache: bool = False) -> Entity:
        with self.engine.connect() as conn:
            stmt = select(entities).where(entities.c.entity_id == name)
            row = conn.execute(stmt).first()
            if row:
                entity_proto = EntityProto.FromString(row["entity_proto"])
                return Entity.from_proto(entity_proto)
            raise EntityNotFoundException(name, project=project)

    def list_entities(self, project: str, allow_cache: bool = False) -> List[Entity]:
        with self.engine.connect() as conn:
            stmt = select(entities)
            rows = conn.execute(stmt).all()
            if rows:
                return [
                    Entity.from_proto(EntityProto.FromString(row["entity_proto"]))
                    for row in rows
                ]
        return []

    def delete_entity(self, name: str, project: str, commit: bool = True):
        with self.engine.connect() as conn:
            stmt = delete(entities).where(entities.c.entity_id == name)
            rows = conn.execute(stmt)
            if rows.rowcount < 1:
                raise EntityNotFoundException(name, project)

    def get_data_source(
        self, name: str, project: str, allow_cache: bool = False
    ) -> DataSource:
        with self.engine.connect() as conn:
            stmt = select(data_sources).where(data_sources.c.entity_id == name)
            row = conn.execute(stmt).first()
            if row:
                ds_proto = DataSourceProto.FromString(row["data_source_proto"])
                return DataSource.from_proto(ds_proto)
            raise DataSourceObjectNotFoundException(name, project=project)

    def list_data_sources(
        self, project: str, allow_cache: bool = False
    ) -> List[DataSource]:
        with self.engine.connect() as conn:
            stmt = select(data_sources)
            rows = conn.execute(stmt).all()
            if rows:
                return [
                    DataSource.from_proto(
                        DataSourceProto.FromString(row["data_source_proto"])
                    )
                    for row in rows
                ]
        return []

    def apply_data_source(
        self, data_source: DataSource, project: str, commit: bool = True
    ):
        with self.engine.connect() as conn:
            stmt = select(data_sources).where(
                data_sources.c.data_source_name == data_source.name
            )
            row = conn.execute(stmt).first()
            update_time = int(datetime.utcnow().timestamp())
            if row:
                update_stmt = (
                    update(data_sources)
                    .where(data_sources.c.data_source_name == data_source.name,)
                    .values(
                        data_source_proto=data_source.to_proto().SerializeToString(),
                        last_updated_timestamp=update_time,
                    )
                )
                conn.execute(update_stmt)
            else:
                insert_stmt = insert(data_sources).values(
                    data_source_name=data_source.name,
                    data_source_proto=data_source.to_proto().SerializeToString(),
                    last_updated_timestamp=update_time,
                )
                conn.execute(insert_stmt)

    def apply_feature_view(
        self, feature_view: BaseFeatureView, project: str, commit: bool = True
    ):
        if isinstance(feature_view, FeatureView):
            fv_table = feature_views
        elif isinstance(feature_view, OnDemandFeatureView):
            fv_table = on_demand_feature_views
        elif isinstance(feature_view, RequestFeatureView):
            fv_table = request_feature_views
        else:
            raise ValueError(f"Unexpected feature view type: {type(feature_view)}")

        with self.engine.connect() as conn:
            stmt = select(fv_table).where(
                fv_table.c.feature_view_name == feature_view.name
            )
            row = conn.execute(stmt).first()
            feature_view.last_updated_timestamp = datetime.utcnow()
            update_time = int(feature_view.last_updated_timestamp.timestamp())
            if row:
                update_stmt = (
                    update(fv_table)
                    .where(fv_table.c.feature_view_name == feature_view.name,)
                    .values(
                        feature_view_proto=feature_view.to_proto().SerializeToString(),
                        last_updated_timestamp=update_time,
                    )
                )
                conn.execute(update_stmt)
            else:
                insert_stmt = insert(fv_table).values(
                    feature_view_name=feature_view.name,
                    feature_view_proto=feature_view.to_proto().SerializeToString(),
                    last_updated_timestamp=update_time,
                )
                conn.execute(insert_stmt)

    def apply_feature_service(
        self, feature_service: FeatureService, project: str, commit: bool = True
    ):
        with self.engine.connect() as conn:
            stmt = select(feature_services).where(
                feature_services.c.feature_service_name == feature_service.name
            )
            row = conn.execute(stmt).first()
            feature_service.last_updated_timestamp = datetime.utcnow()
            update_time = int(feature_service.last_updated_timestamp.timestamp())
            if row:
                update_stmt = (
                    update(feature_services)
                    .where(
                        feature_services.c.feature_service_name == feature_service.name,
                    )
                    .values(
                        feature_service_proto=feature_service.to_proto().SerializeToString(),
                        last_updated_timestamp=update_time,
                    )
                )
                conn.execute(update_stmt)
            else:
                insert_stmt = insert(feature_services).values(
                    feature_service_name=feature_service.name,
                    feature_service_proto=feature_service.to_proto().SerializeToString(),
                    last_updated_timestamp=update_time,
                )
                conn.execute(insert_stmt)

    def delete_data_source(self, name: str, project: str, commit: bool = True):
        with self.engine.connect() as conn:
            stmt = delete(data_sources).where(data_sources.c.entity_id == name)
            rows = conn.execute(stmt)
            if rows.rowcount < 1:
                raise DataSourceObjectNotFoundException(name, project)

    def list_feature_services(
        self, project: str, allow_cache: bool = False
    ) -> List[FeatureService]:
        with self.engine.connect() as conn:
            stmt = select(feature_services)
            rows = conn.execute(stmt).all()
            if rows:
                return [
                    FeatureService.from_proto(
                        FeatureServiceProto.FromString(row["feature_service_proto"])
                    )
                    for row in rows
                ]
        return []

    def list_feature_views(
        self, project: str, allow_cache: bool = False
    ) -> List[FeatureView]:
        with self.engine.connect() as conn:
            stmt = select(feature_views)
            rows = conn.execute(stmt).all()
            if rows:
                return [
                    FeatureView.from_proto(
                        FeatureViewProto.FromString(row["feature_view_proto"])
                    )
                    for row in rows
                ]
        return []

    def list_saved_datasets(
        self, project: str, allow_cache: bool = False
    ) -> List[SavedDataset]:
        return []

    def list_request_feature_views(
        self, project: str, allow_cache: bool = False
    ) -> List[RequestFeatureView]:
        with self.engine.connect() as conn:
            stmt = select(request_feature_views)
            rows = conn.execute(stmt).all()
            if rows:
                return [
                    RequestFeatureView.from_proto(
                        RequestFeatureViewProto.FromString(row["feature_view_proto"])
                    )
                    for row in rows
                ]
        return []

    def list_on_demand_feature_views(
        self, project: str, allow_cache: bool = False
    ) -> List[OnDemandFeatureView]:
        with self.engine.connect() as conn:
            stmt = select(on_demand_feature_views)
            rows = conn.execute(stmt).all()
            if rows:
                return [
                    OnDemandFeatureView.from_proto(
                        OnDemandFeatureViewProto.FromString(row["feature_view_proto"])
                    )
                    for row in rows
                ]
        return []
