from datetime import datetime
from pathlib import Path
from typing import List, Optional

from sql import (  # type: ignore
    BIGINT,
    VARBINARY,
    Column,
    MetaData,
    String,
    Table,
    create_engine,
    delete,
    insert,
    select,
    update,
)
from sql.engine import Engine

from feast.data_source import DataSource
from feast.entity import Entity
from feast.errors import DataSourceObjectNotFoundException, EntityNotFoundException
from feast.protos.feast.core.DataSource_pb2 import DataSource as DataSourceProto
from feast.protos.feast.core.Entity_pb2 import Entity as EntityProto
from feast.registry import Registry
from feast.repo_config import RegistryConfig

metadata = MetaData()

entities = Table(
    "entities",
    metadata,
    Column("entity_id", String, primary_key=True),
    Column("last_updated_timestamp", BIGINT, nullable=False),
    Column("entity_proto", VARBINARY, nullable=False),
)

data_sources = Table(
    "data_sources",
    metadata,
    Column("data_source_name", String, primary_key=True),
    Column("last_updated_timestamp", BIGINT, nullable=False),
    Column("data_source_proto", VARBINARY, nullable=False),
)

feature_views = Table(
    "feature_views",
    metadata,
    Column("feature_view_name", String, primary_key=True),
    Column("last_updated_timestamp", BIGINT, nullable=False),
    Column("materialized_intervals", VARBINARY, nullable=False),
    Column("feature_view_proto", VARBINARY, nullable=False),
)

request_feature_views = Table(
    "request_feature_views",
    metadata,
    Column("feature_view_name", String, primary_key=True),
    Column("last_updated_timestamp", BIGINT, nullable=False),
    Column("feature_view_proto", VARBINARY, nullable=False),
)

on_demand_feature_views = Table(
    "on_demand_feature_views",
    metadata,
    Column("feature_view_name", String, primary_key=True),
    Column("last_updated_timestamp", BIGINT, nullable=False),
    Column("feature_view_proto", VARBINARY, nullable=False),
)

feature_user_metadata = Table(
    "feature_metadata",
    metadata,
    Column("feature_name", String, primary_key=True),
    Column("last_updated_timestamp", BIGINT, nullable=False),
    Column("feature_metadata_binary", VARBINARY, nullable=False),
)

feature_services = Table(
    "feature_services",
    metadata,
    Column("feature_service_name", String, primary_key=True),
    Column("last_updated_timestamp", BIGINT, nullable=False),
    Column("feature_service_proto", VARBINARY, nullable=False),
)

APPLY_OPERATIONS = {"entity": (entities, entities.c.entity_id)}


class SqlRegistry(Registry):
    def __init__(
        self, registry_config: Optional[RegistryConfig], repo_path: Optional[Path]
    ):
        assert registry_config
        self.engine: Engine = create_engine(registry_config.path, echo=True)
        metadata.create_all(self.engine)

    def teardown(self):
        super().teardown()

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
            stmt = select(data_sources).where(entities.c.entity_id == data_source.name)
            row = conn.execute(stmt).first()
            update_time = int(datetime.utcnow().timestamp())
            if row:
                update_stmt = (
                    update(data_sources)
                    .where(data_sources.c.entity_id == data_source.name,)
                    .values(
                        entity_proto=data_source.to_proto().SerializeToString(),
                        last_updated_timestamp=update_time,
                    )
                )
                conn.execute(update_stmt)
            else:
                insert_stmt = insert(data_sources).values(
                    entity_id=data_source.name,
                    entity_proto=data_source.to_proto().SerializeToString(),
                    last_updated_timestamp=update_time,
                )
                conn.execute(insert_stmt)

    def delete_data_source(self, name: str, project: str, commit: bool = True):
        with self.engine.connect() as conn:
            stmt = delete(data_sources).where(data_sources.c.entity_id == name)
            rows = conn.execute(stmt)
            if rows.rowcount < 1:
                raise DataSourceObjectNotFoundException(name, project)
