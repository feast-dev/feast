import logging
import os
import uuid
from binascii import hexlify
from datetime import datetime, timedelta, timezone
from enum import Enum
from threading import Lock
from typing import Any, Callable, List, Literal, Optional, Set, Union

from pydantic import ConfigDict, Field, StrictStr

import feast
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
from feast.infra.registry import proto_registry_utils
from feast.infra.registry.base_registry import BaseRegistry
from feast.infra.utils.snowflake.snowflake_utils import (
    GetSnowflakeConnection,
    execute_snowflake_statement,
)
from feast.on_demand_feature_view import OnDemandFeatureView
from feast.permissions.decision import DecisionStrategy
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
from feast.utils import _utc_now, has_all_tags

logger = logging.getLogger(__name__)


class FeastMetadataKeys(Enum):
    LAST_UPDATED_TIMESTAMP = "last_updated_timestamp"
    PROJECT_UUID = "project_uuid"
    PERMISSIONS_DECISION_STRATEGY = "permissions_decision_strategy"


class SnowflakeRegistryConfig(RegistryConfig):
    """Registry config for Snowflake"""

    registry_type: Literal["snowflake.registry"] = "snowflake.registry"
    """ Registry type selector """

    type: Literal["snowflake.registry"] = "snowflake.registry"
    """ Registry type selector """

    config_path: Optional[str] = os.path.expanduser("~/.snowsql/config")
    """ Snowflake config path -- absolute path required (Cant use ~) """

    account: Optional[str] = None
    """ Snowflake deployment identifier -- drop .snowflakecomputing.com """

    user: Optional[str] = None
    """ Snowflake user name """

    password: Optional[str] = None
    """ Snowflake password """

    role: Optional[str] = None
    """ Snowflake role name """

    warehouse: Optional[str] = None
    """ Snowflake warehouse name """

    authenticator: Optional[str] = None
    """ Snowflake authenticator name """

    private_key: Optional[str] = None
    """ Snowflake private key file path"""

    private_key_content: Optional[bytes] = None
    """ Snowflake private key stored as bytes"""

    private_key_passphrase: Optional[str] = None
    """ Snowflake private key file passphrase"""

    database: StrictStr
    """ Snowflake database name """

    schema_: Optional[str] = Field("PUBLIC", alias="schema")
    """ Snowflake schema name """
    model_config = ConfigDict(populate_by_name=True)


class SnowflakeRegistry(BaseRegistry):
    def __init__(
        self,
        registry_config,
        project: str,
        repo_path,
    ):
        assert registry_config is not None and isinstance(
            registry_config, SnowflakeRegistryConfig
        ), "SnowflakeRegistry needs a valid registry_config, a path does not work"

        self.registry_config = registry_config
        self.registry_path = (
            f'"{self.registry_config.database}"."{self.registry_config.schema_}"'
        )

        with GetSnowflakeConnection(self.registry_config) as conn:
            sql_function_file = f"{os.path.dirname(feast.__file__)}/infra/utils/snowflake/registry/snowflake_table_creation.sql"
            with open(sql_function_file, "r") as file:
                sql_file = file.read()
                sql_cmds = sql_file.split(";")
                for command in sql_cmds:
                    query = command.replace("REGISTRY_PATH", f"{self.registry_path}")
                    execute_snowflake_statement(conn, query)

        self.cached_registry_proto = self.proto()
        proto_registry_utils.init_project_metadata(self.cached_registry_proto, project)
        self.cached_registry_proto_created = _utc_now()
        self._refresh_lock = Lock()
        self.cached_registry_proto_ttl = timedelta(
            seconds=registry_config.cache_ttl_seconds
            if registry_config.cache_ttl_seconds is not None
            else 0
        )
        self.project = project

    def refresh(self, project: Optional[str] = None):
        if project:
            project_metadata = proto_registry_utils.get_project_metadata(
                registry_proto=self.cached_registry_proto, project=project
            )
            if not project_metadata:
                proto_registry_utils.init_project_metadata(
                    self.cached_registry_proto, project
                )
        self.cached_registry_proto = self.proto()
        self.cached_registry_proto_created = _utc_now()

    def _refresh_cached_registry_if_necessary(self):
        with self._refresh_lock:
            expired = (
                self.cached_registry_proto is None
                or self.cached_registry_proto_created is None
            ) or (
                self.cached_registry_proto_ttl.total_seconds()
                > 0  # 0 ttl means infinity
                and (
                    _utc_now()
                    > (
                        self.cached_registry_proto_created
                        + self.cached_registry_proto_ttl
                    )
                )
            )

            if expired:
                logger.info("Registry cache expired, so refreshing")
                self.refresh()

    def teardown(self):
        with GetSnowflakeConnection(self.registry_config) as conn:
            sql_function_file = f"{os.path.dirname(feast.__file__)}/infra/utils/snowflake/registry/snowflake_table_deletion.sql"
            with open(sql_function_file, "r") as file:
                sqlFile = file.read()
                sqlCommands = sqlFile.split(";")
                for command in sqlCommands:
                    query = command.replace("REGISTRY_PATH", f"{self.registry_path}")
                    execute_snowflake_statement(conn, query)

    # apply operations
    def apply_data_source(
        self, data_source: DataSource, project: str, commit: bool = True
    ):
        return self._apply_object(
            "DATA_SOURCES",
            project,
            "DATA_SOURCE_NAME",
            data_source,
            "DATA_SOURCE_PROTO",
        )

    def apply_entity(self, entity: Entity, project: str, commit: bool = True):
        return self._apply_object(
            "ENTITIES", project, "ENTITY_NAME", entity, "ENTITY_PROTO"
        )

    def apply_feature_service(
        self, feature_service: FeatureService, project: str, commit: bool = True
    ):
        return self._apply_object(
            "FEATURE_SERVICES",
            project,
            "FEATURE_SERVICE_NAME",
            feature_service,
            "FEATURE_SERVICE_PROTO",
        )

    def apply_feature_view(
        self, feature_view: BaseFeatureView, project: str, commit: bool = True
    ):
        fv_table_str = self._infer_fv_table(feature_view)
        fv_column_name = fv_table_str[:-1]
        return self._apply_object(
            fv_table_str,
            project,
            f"{fv_column_name}_NAME",
            feature_view,
            f"{fv_column_name}_PROTO",
        )

    def apply_saved_dataset(
        self,
        saved_dataset: SavedDataset,
        project: str,
        commit: bool = True,
    ):
        return self._apply_object(
            "SAVED_DATASETS",
            project,
            "SAVED_DATASET_NAME",
            saved_dataset,
            "SAVED_DATASET_PROTO",
        )

    def apply_validation_reference(
        self,
        validation_reference: ValidationReference,
        project: str,
        commit: bool = True,
    ):
        return self._apply_object(
            "VALIDATION_REFERENCES",
            project,
            "VALIDATION_REFERENCE_NAME",
            validation_reference,
            "VALIDATION_REFERENCE_PROTO",
        )

    def update_infra(self, infra: Infra, project: str, commit: bool = True):
        self._apply_object(
            "MANAGED_INFRA",
            project,
            "INFRA_NAME",
            infra,
            "INFRA_PROTO",
            name="infra_obj",
        )

    def _apply_object(
        self,
        table: str,
        project: str,
        id_field_name: str,
        obj: Any,
        proto_field_name: str,
        name: Optional[str] = None,
    ):
        self._maybe_init_project_metadata(project)

        name = name or (obj.name if hasattr(obj, "name") else None)
        assert name, f"name needs to be provided for {obj}"

        update_datetime = _utc_now()
        if hasattr(obj, "last_updated_timestamp"):
            obj.last_updated_timestamp = update_datetime

        with GetSnowflakeConnection(self.registry_config) as conn:
            query = f"""
                SELECT
                    project_id
                FROM
                    {self.registry_path}."{table}"
                WHERE
                    project_id = '{project}'
                    AND {id_field_name.lower()} = '{name}'
                LIMIT 1
            """
            df = execute_snowflake_statement(conn, query).fetch_pandas_all()

            if not df.empty:
                proto = hexlify(obj.to_proto().SerializeToString()).__str__()[1:]
                query = f"""
                    UPDATE {self.registry_path}."{table}"
                        SET
                            {proto_field_name} = TO_BINARY({proto}),
                            last_updated_timestamp = CURRENT_TIMESTAMP()
                        WHERE
                            {id_field_name.lower()} = '{name}'
                """
                execute_snowflake_statement(conn, query)

            else:
                obj_proto = obj.to_proto()

                if hasattr(obj_proto, "meta") and hasattr(
                    obj_proto.meta, "created_timestamp"
                ):
                    obj_proto.meta.created_timestamp.FromDatetime(update_datetime)

                proto = hexlify(obj_proto.SerializeToString()).__str__()[1:]
                if table == "FEATURE_VIEWS":
                    query = f"""
                        INSERT INTO {self.registry_path}."{table}"
                            VALUES
                            ('{name}', '{project}', CURRENT_TIMESTAMP(), TO_BINARY({proto}), '', '')
                    """
                elif "_FEATURE_VIEWS" in table:
                    query = f"""
                        INSERT INTO {self.registry_path}."{table}"
                            VALUES
                            ('{name}', '{project}', CURRENT_TIMESTAMP(), TO_BINARY({proto}), '')
                    """
                else:
                    query = f"""
                        INSERT INTO {self.registry_path}."{table}"
                            VALUES
                            ('{name}', '{project}', CURRENT_TIMESTAMP(), TO_BINARY({proto}))
                    """
                execute_snowflake_statement(conn, query)

            self._set_last_updated_metadata(update_datetime, project)

    def apply_permission(
        self, permission: Permission, project: str, commit: bool = True
    ):
        return self._apply_object(
            "PERMISSIONS",
            project,
            "PERMISSION_NAME",
            permission,
            "PERMISSION_PROTO",
        )

    # delete operations
    def delete_data_source(self, name: str, project: str, commit: bool = True):
        return self._delete_object(
            "DATA_SOURCES",
            name,
            project,
            "DATA_SOURCE_NAME",
            DataSourceObjectNotFoundException,
        )

    def delete_entity(self, name: str, project: str, commit: bool = True):
        return self._delete_object(
            "ENTITIES", name, project, "ENTITY_NAME", EntityNotFoundException
        )

    def delete_feature_service(self, name: str, project: str, commit: bool = True):
        return self._delete_object(
            "FEATURE_SERVICES",
            name,
            project,
            "FEATURE_SERVICE_NAME",
            FeatureServiceNotFoundException,
        )

    # can you have featureviews with the same name
    def delete_feature_view(self, name: str, project: str, commit: bool = True):
        deleted_count = 0
        for table in {
            "FEATURE_VIEWS",
            "ON_DEMAND_FEATURE_VIEWS",
            "STREAM_FEATURE_VIEWS",
        }:
            deleted_count += self._delete_object(
                table, name, project, "FEATURE_VIEW_NAME", None
            )
        if deleted_count == 0:
            raise FeatureViewNotFoundException(name, project)

    def delete_saved_dataset(self, name: str, project: str, allow_cache: bool = False):
        self._delete_object(
            "SAVED_DATASETS",
            name,
            project,
            "SAVED_DATASET_NAME",
            SavedDatasetNotFound,
        )

    def delete_validation_reference(self, name: str, project: str, commit: bool = True):
        self._delete_object(
            "VALIDATION_REFERENCES",
            name,
            project,
            "VALIDATION_REFERENCE_NAME",
            ValidationReferenceNotFound,
        )

    def _delete_object(
        self,
        table: str,
        name: str,
        project: str,
        id_field_name: str,
        not_found_exception: Optional[Callable],
    ):
        with GetSnowflakeConnection(self.registry_config) as conn:
            query = f"""
                DELETE FROM {self.registry_path}."{table}"
                WHERE
                    project_id = '{project}'
                    AND {id_field_name.lower()} = '{name}'
            """
            cursor = execute_snowflake_statement(conn, query)

            if cursor.rowcount < 1 and not_found_exception:  # type: ignore
                raise not_found_exception(name, project)
            self._set_last_updated_metadata(_utc_now(), project)

            return cursor.rowcount

    def delete_permission(self, name: str, project: str, commit: bool = True):
        return self._delete_object(
            "PERMISSIONS",
            name,
            project,
            "PERMISSION_NAME",
            PermissionNotFoundException,
        )

    # get operations
    def get_data_source(
        self, name: str, project: str, allow_cache: bool = False
    ) -> DataSource:
        if allow_cache:
            self._refresh_cached_registry_if_necessary()
            return proto_registry_utils.get_data_source(
                self.cached_registry_proto, name, project
            )
        return self._get_object(
            "DATA_SOURCES",
            name,
            project,
            DataSourceProto,
            DataSource,
            "DATA_SOURCE_NAME",
            "DATA_SOURCE_PROTO",
            DataSourceObjectNotFoundException,
        )

    def get_entity(self, name: str, project: str, allow_cache: bool = False) -> Entity:
        if allow_cache:
            self._refresh_cached_registry_if_necessary()
            return proto_registry_utils.get_entity(
                self.cached_registry_proto, name, project
            )
        return self._get_object(
            "ENTITIES",
            name,
            project,
            EntityProto,
            Entity,
            "ENTITY_NAME",
            "ENTITY_PROTO",
            EntityNotFoundException,
        )

    def get_feature_service(
        self, name: str, project: str, allow_cache: bool = False
    ) -> FeatureService:
        if allow_cache:
            self._refresh_cached_registry_if_necessary()
            return proto_registry_utils.get_feature_service(
                self.cached_registry_proto, name, project
            )
        return self._get_object(
            "FEATURE_SERVICES",
            name,
            project,
            FeatureServiceProto,
            FeatureService,
            "FEATURE_SERVICE_NAME",
            "FEATURE_SERVICE_PROTO",
            FeatureServiceNotFoundException,
        )

    def get_feature_view(
        self, name: str, project: str, allow_cache: bool = False
    ) -> FeatureView:
        if allow_cache:
            self._refresh_cached_registry_if_necessary()
            return proto_registry_utils.get_feature_view(
                self.cached_registry_proto, name, project
            )
        return self._get_object(
            "FEATURE_VIEWS",
            name,
            project,
            FeatureViewProto,
            FeatureView,
            "FEATURE_VIEW_NAME",
            "FEATURE_VIEW_PROTO",
            FeatureViewNotFoundException,
        )

    def get_infra(self, project: str, allow_cache: bool = False) -> Infra:
        infra_object = self._get_object(
            "MANAGED_INFRA",
            "infra_obj",
            project,
            InfraProto,
            Infra,
            "INFRA_NAME",
            "INFRA_PROTO",
            None,
        )
        infra_object = infra_object or InfraProto()
        return Infra.from_proto(infra_object)

    def get_on_demand_feature_view(
        self, name: str, project: str, allow_cache: bool = False
    ) -> OnDemandFeatureView:
        if allow_cache:
            self._refresh_cached_registry_if_necessary()
            return proto_registry_utils.get_on_demand_feature_view(
                self.cached_registry_proto, name, project
            )
        return self._get_object(
            "ON_DEMAND_FEATURE_VIEWS",
            name,
            project,
            OnDemandFeatureViewProto,
            OnDemandFeatureView,
            "ON_DEMAND_FEATURE_VIEW_NAME",
            "ON_DEMAND_FEATURE_VIEW_PROTO",
            FeatureViewNotFoundException,
        )

    def get_saved_dataset(
        self, name: str, project: str, allow_cache: bool = False
    ) -> SavedDataset:
        if allow_cache:
            self._refresh_cached_registry_if_necessary()
            return proto_registry_utils.get_saved_dataset(
                self.cached_registry_proto, name, project
            )
        return self._get_object(
            "SAVED_DATASETS",
            name,
            project,
            SavedDatasetProto,
            SavedDataset,
            "SAVED_DATASET_NAME",
            "SAVED_DATASET_PROTO",
            SavedDatasetNotFound,
        )

    def get_stream_feature_view(
        self, name: str, project: str, allow_cache: bool = False
    ):
        if allow_cache:
            self._refresh_cached_registry_if_necessary()
            return proto_registry_utils.get_stream_feature_view(
                self.cached_registry_proto, name, project
            )
        return self._get_object(
            "STREAM_FEATURE_VIEWS",
            name,
            project,
            StreamFeatureViewProto,
            StreamFeatureView,
            "STREAM_FEATURE_VIEW_NAME",
            "STREAM_FEATURE_VIEW_PROTO",
            FeatureViewNotFoundException,
        )

    def get_validation_reference(
        self, name: str, project: str, allow_cache: bool = False
    ) -> ValidationReference:
        if allow_cache:
            self._refresh_cached_registry_if_necessary()
            return proto_registry_utils.get_validation_reference(
                self.cached_registry_proto, name, project
            )
        return self._get_object(
            "VALIDATION_REFERENCES",
            name,
            project,
            ValidationReferenceProto,
            ValidationReference,
            "VALIDATION_REFERENCE_NAME",
            "VALIDATION_REFERENCE_PROTO",
            ValidationReferenceNotFound,
        )

    def _get_object(
        self,
        table: str,
        name: str,
        project: str,
        proto_class: Any,
        python_class: Any,
        id_field_name: str,
        proto_field_name: str,
        not_found_exception: Optional[Callable],
    ):
        self._maybe_init_project_metadata(project)
        with GetSnowflakeConnection(self.registry_config) as conn:
            query = f"""
                SELECT
                    {proto_field_name}
                FROM
                    {self.registry_path}."{table}"
                WHERE
                    project_id = '{project}'
                    AND {id_field_name.lower()} = '{name}'
                LIMIT 1
            """
            df = execute_snowflake_statement(conn, query).fetch_pandas_all()

        if not df.empty:
            _proto = proto_class.FromString(df.squeeze())
            return python_class.from_proto(_proto)
        elif not_found_exception:
            raise not_found_exception(name, project)
        else:
            return None

    def get_permission(
        self, name: str, project: str, allow_cache: bool = False
    ) -> Permission:
        if allow_cache:
            self._refresh_cached_registry_if_necessary()
            return proto_registry_utils.get_permission(
                self.cached_registry_proto, name, project
            )
        return self._get_object(
            "PERMISSIONS",
            name,
            project,
            PermissionProto,
            Permission,
            "PERMISSION_NAME",
            "PERMISSION_PROTO",
            PermissionNotFoundException,
        )

    # list operations
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
        return self._list_objects(
            "DATA_SOURCES",
            project,
            DataSourceProto,
            DataSource,
            "DATA_SOURCE_PROTO",
            tags=tags,
        )

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
        return self._list_objects(
            "ENTITIES", project, EntityProto, Entity, "ENTITY_PROTO", tags=tags
        )

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
        return self._list_objects(
            "FEATURE_SERVICES",
            project,
            FeatureServiceProto,
            FeatureService,
            "FEATURE_SERVICE_PROTO",
            tags=tags,
        )

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
        return self._list_objects(
            "FEATURE_VIEWS",
            project,
            FeatureViewProto,
            FeatureView,
            "FEATURE_VIEW_PROTO",
            tags=tags,
        )

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
        return self._list_objects(
            "ON_DEMAND_FEATURE_VIEWS",
            project,
            OnDemandFeatureViewProto,
            OnDemandFeatureView,
            "ON_DEMAND_FEATURE_VIEW_PROTO",
            tags=tags,
        )

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
        return self._list_objects(
            "SAVED_DATASETS",
            project,
            SavedDatasetProto,
            SavedDataset,
            "SAVED_DATASET_PROTO",
            tags=tags,
        )

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
        return self._list_objects(
            "STREAM_FEATURE_VIEWS",
            project,
            StreamFeatureViewProto,
            StreamFeatureView,
            "STREAM_FEATURE_VIEW_PROTO",
            tags=tags,
        )

    def list_validation_references(
        self,
        project: str,
        allow_cache: bool = False,
        tags: Optional[dict[str, str]] = None,
    ) -> List[ValidationReference]:
        return self._list_objects(
            "VALIDATION_REFERENCES",
            project,
            ValidationReferenceProto,
            ValidationReference,
            "VALIDATION_REFERENCE_PROTO",
            tags=tags,
        )

    def _list_objects(
        self,
        table: str,
        project: str,
        proto_class: Any,
        python_class: Any,
        proto_field_name: str,
        tags: Optional[dict[str, str]] = None,
    ):
        self._maybe_init_project_metadata(project)
        with GetSnowflakeConnection(self.registry_config) as conn:
            query = f"""
                SELECT
                    {proto_field_name}
                FROM
                    {self.registry_path}."{table}"
                WHERE
                    project_id = '{project}'
            """
            df = execute_snowflake_statement(conn, query).fetch_pandas_all()
            if not df.empty:
                objects = []
                for row in df.iterrows():
                    obj = python_class.from_proto(
                        proto_class.FromString(row[1][proto_field_name])
                    )
                    if has_all_tags(obj.tags, tags):
                        objects.append(obj)
                return objects
        return []

    def list_permissions(
        self,
        project: str,
        allow_cache: bool = False,
        tags: Optional[dict[str, str]] = None,
    ) -> List[Permission]:
        if allow_cache:
            self._refresh_cached_registry_if_necessary()
            return proto_registry_utils.list_permissions(
                self.cached_registry_proto, project
            )
        return self._list_objects(
            "PERMISSIONS",
            project,
            PermissionProto,
            Permission,
            "PERMISSION_PROTO",
            tags,
        )

    def apply_materialization(
        self,
        feature_view: FeatureView,
        project: str,
        start_date: datetime,
        end_date: datetime,
        commit: bool = True,
    ):
        fv_table_str = self._infer_fv_table(feature_view)
        fv_column_name = fv_table_str[:-1]
        python_class, proto_class = self._infer_fv_classes(feature_view)

        if python_class in {OnDemandFeatureView}:
            raise ValueError(
                f"Cannot apply materialization for feature {feature_view.name} of type {python_class}"
            )
        fv: Union[FeatureView, StreamFeatureView] = self._get_object(
            fv_table_str,
            feature_view.name,
            project,
            proto_class,
            python_class,
            f"{fv_column_name}_NAME",
            f"{fv_column_name}_PROTO",
            FeatureViewNotFoundException,
        )
        fv.materialization_intervals.append((start_date, end_date))
        self._apply_object(
            fv_table_str,
            project,
            f"{fv_column_name}_NAME",
            fv,
            f"{fv_column_name}_PROTO",
        )

    def list_project_metadata(
        self, project: str, allow_cache: bool = False
    ) -> List[ProjectMetadata]:
        if allow_cache:
            self._refresh_cached_registry_if_necessary()
            return proto_registry_utils.list_project_metadata(
                self.cached_registry_proto, project
            )
        with GetSnowflakeConnection(self.registry_config) as conn:
            query = f"""
                SELECT
                    metadata_key,
                    metadata_value
                FROM
                    {self.registry_path}."FEAST_METADATA"
                WHERE
                    project_id = '{project}'
            """
            df = execute_snowflake_statement(conn, query).fetch_pandas_all()

            if not df.empty:
                project_metadata = ProjectMetadata(project_name=project)
                for row in df.iterrows():
                    if row[1]["METADATA_KEY"] == FeastMetadataKeys.PROJECT_UUID.value:
                        project_metadata.project_uuid = row[1]["METADATA_VALUE"]
                    elif (
                        row[1]["METADATA_KEY"]
                        == FeastMetadataKeys.PERMISSIONS_DECISION_STRATEGY.value
                    ):
                        project_metadata.decision_strategy = DecisionStrategy(
                            row[1]["METADATA_VALUE"]
                        )
                    # TODO(adchia): Add other project metadata in a structured way
                return [project_metadata]
        return []

    def apply_user_metadata(
        self,
        project: str,
        feature_view: BaseFeatureView,
        metadata_bytes: Optional[bytes],
    ):
        fv_table_str = self._infer_fv_table(feature_view)
        fv_column_name = fv_table_str[:-1].lower()
        with GetSnowflakeConnection(self.registry_config) as conn:
            query = f"""
                SELECT
                    project_id
                FROM
                    {self.registry_path}."{fv_table_str}"
                WHERE
                    project_id = '{project}'
                    AND {fv_column_name}_name = '{feature_view.name}'
                LIMIT 1
            """
            df = execute_snowflake_statement(conn, query).fetch_pandas_all()

            if not df.empty:
                if metadata_bytes:
                    metadata_hex = hexlify(metadata_bytes).__str__()[1:]
                else:
                    metadata_hex = "''"
                query = f"""
                    UPDATE {self.registry_path}."{fv_table_str}"
                        SET
                            user_metadata = TO_BINARY({metadata_hex}),
                            last_updated_timestamp = CURRENT_TIMESTAMP()
                        WHERE
                            project_id = '{project}'
                            AND {fv_column_name}_name = '{feature_view.name}'
                """
                execute_snowflake_statement(conn, query)
            else:
                raise FeatureViewNotFoundException(feature_view.name, project=project)

    def get_user_metadata(
        self, project: str, feature_view: BaseFeatureView
    ) -> Optional[bytes]:
        fv_table_str = self._infer_fv_table(feature_view)
        fv_column_name = fv_table_str[:-1].lower()
        with GetSnowflakeConnection(self.registry_config) as conn:
            query = f"""
                SELECT
                    user_metadata
                FROM
                    {self.registry_path}."{fv_table_str}"
                WHERE
                    {fv_column_name}_name = '{feature_view.name}'
                LIMIT 1
            """
            df = execute_snowflake_statement(conn, query).fetch_pandas_all()

        if not df.empty:
            return df.squeeze()
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

    def _get_all_projects(self) -> Set[str]:
        projects = set()

        base_tables = [
            "DATA_SOURCES",
            "ENTITIES",
            "FEATURE_VIEWS",
            "ON_DEMAND_FEATURE_VIEWS",
            "STREAM_FEATURE_VIEWS",
            "PERMISSIONS",
        ]

        with GetSnowflakeConnection(self.registry_config) as conn:
            for table in base_tables:
                query = (
                    f'SELECT DISTINCT project_id FROM {self.registry_path}."{table}"'
                )
                df = execute_snowflake_statement(conn, query).fetch_pandas_all()

                for row in df.iterrows():
                    projects.add(row[1]["PROJECT_ID"])

        return projects

    def _get_last_updated_metadata(self, project: str):
        with GetSnowflakeConnection(self.registry_config) as conn:
            query = f"""
                SELECT
                    metadata_value
                FROM
                    {self.registry_path}."FEAST_METADATA"
                WHERE
                    project_id = '{project}'
                    AND metadata_key = '{FeastMetadataKeys.LAST_UPDATED_TIMESTAMP.value}'
                LIMIT 1
            """
            df = execute_snowflake_statement(conn, query).fetch_pandas_all()

        if df.empty:
            return None

        return datetime.fromtimestamp(int(df.squeeze()), tz=timezone.utc)

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

    def _infer_fv_table(self, feature_view) -> str:
        if isinstance(feature_view, StreamFeatureView):
            table = "STREAM_FEATURE_VIEWS"
        elif isinstance(feature_view, FeatureView):
            table = "FEATURE_VIEWS"
        elif isinstance(feature_view, OnDemandFeatureView):
            table = "ON_DEMAND_FEATURE_VIEWS"
        else:
            raise ValueError(f"Unexpected feature view type: {type(feature_view)}")
        return table

    def _maybe_init_project_metadata(self, project):
        with GetSnowflakeConnection(self.registry_config) as conn:
            query = f"""
                SELECT
                    metadata_value
                FROM
                    {self.registry_path}."FEAST_METADATA"
                WHERE
                    project_id = '{project}'
                    AND metadata_key = '{FeastMetadataKeys.PROJECT_UUID.value}'
                LIMIT 1
            """
            df = execute_snowflake_statement(conn, query).fetch_pandas_all()

            if df.empty:
                new_project_uuid = f"{uuid.uuid4()}"
                query = f"""
                    INSERT INTO {self.registry_path}."FEAST_METADATA"
                        VALUES
                        ('{project}', '{FeastMetadataKeys.PROJECT_UUID.value}', '{new_project_uuid}', CURRENT_TIMESTAMP()),
                        ('{project}', '{FeastMetadataKeys.PERMISSIONS_DECISION_STRATEGY.value}', '{DecisionStrategy.UNANIMOUS.value}', CURRENT_TIMESTAMP())
                """
                execute_snowflake_statement(conn, query)

    def _set_last_updated_metadata(self, last_updated: datetime, project: str):
        with GetSnowflakeConnection(self.registry_config) as conn:
            query = f"""
                SELECT
                    project_id
                FROM
                    {self.registry_path}."FEAST_METADATA"
                WHERE
                    project_id = '{project}'
                    AND metadata_key = '{FeastMetadataKeys.LAST_UPDATED_TIMESTAMP.value}'
                LIMIT 1
            """
            df = execute_snowflake_statement(conn, query).fetch_pandas_all()

            update_time = int(last_updated.timestamp())
            if not df.empty:
                query = f"""
                    UPDATE {self.registry_path}."FEAST_METADATA"
                        SET
                            project_id = '{project}',
                            metadata_key = '{FeastMetadataKeys.LAST_UPDATED_TIMESTAMP.value}',
                            metadata_value = '{update_time}',
                            last_updated_timestamp = CURRENT_TIMESTAMP()
                        WHERE
                            project_id = '{project}'
                            AND metadata_key = '{FeastMetadataKeys.LAST_UPDATED_TIMESTAMP.value}'
                """
                execute_snowflake_statement(conn, query)

            else:
                query = f"""
                    INSERT INTO {self.registry_path}."FEAST_METADATA"
                        VALUES
                        ('{project}', '{FeastMetadataKeys.LAST_UPDATED_TIMESTAMP.value}', '{update_time}', CURRENT_TIMESTAMP())
                """
                execute_snowflake_statement(conn, query)

    def commit(self):
        pass
