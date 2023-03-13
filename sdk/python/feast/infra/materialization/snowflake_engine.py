import os
import shutil
from dataclasses import dataclass
from datetime import datetime
from typing import Callable, List, Literal, Optional, Sequence, Union

import click
import pandas as pd
from colorama import Fore, Style
from pydantic import Field, StrictStr
from pytz import utc
from tqdm import tqdm

import feast
from feast.batch_feature_view import BatchFeatureView
from feast.entity import Entity
from feast.feature_view import FeatureView
from feast.infra.materialization.batch_materialization_engine import (
    BatchMaterializationEngine,
    MaterializationJob,
    MaterializationJobStatus,
    MaterializationTask,
)
from feast.infra.offline_stores.offline_store import OfflineStore
from feast.infra.online_stores.online_store import OnlineStore
from feast.infra.registry.base_registry import BaseRegistry
from feast.infra.utils.snowflake.snowflake_utils import (
    GetSnowflakeConnection,
    _run_snowflake_field_mapping,
    assert_snowflake_feature_names,
    execute_snowflake_statement,
    get_snowflake_online_store_path,
    package_snowpark_zip,
)
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from feast.repo_config import FeastConfigBaseModel, RepoConfig
from feast.stream_feature_view import StreamFeatureView
from feast.type_map import _convert_value_name_to_snowflake_udf
from feast.utils import _coerce_datetime, _get_column_names


class SnowflakeMaterializationEngineConfig(FeastConfigBaseModel):
    """Batch Materialization Engine config for Snowflake Snowpark Python UDFs"""

    type: Literal["snowflake.engine"] = "snowflake.engine"
    """ Type selector"""

    config_path: Optional[str] = os.path.expanduser("~/.snowsql/config")
    """ Snowflake config path -- absolute path required (Cant use ~)"""

    account: Optional[str] = None
    """ Snowflake deployment identifier -- drop .snowflakecomputing.com"""

    user: Optional[str] = None
    """ Snowflake user name """

    password: Optional[str] = None
    """ Snowflake password """

    role: Optional[str] = None
    """ Snowflake role name"""

    warehouse: Optional[str] = None
    """ Snowflake warehouse name """

    authenticator: Optional[str] = None
    """ Snowflake authenticator name """

    database: StrictStr
    """ Snowflake database name """

    schema_: Optional[str] = Field("PUBLIC", alias="schema")
    """ Snowflake schema name """

    class Config:
        allow_population_by_field_name = True


@dataclass
class SnowflakeMaterializationJob(MaterializationJob):
    def __init__(
        self,
        job_id: str,
        status: MaterializationJobStatus,
        error: Optional[BaseException] = None,
    ) -> None:
        super().__init__()
        self._job_id: str = job_id
        self._status: MaterializationJobStatus = status
        self._error: Optional[BaseException] = error

    def status(self) -> MaterializationJobStatus:
        return self._status

    def error(self) -> Optional[BaseException]:
        return self._error

    def should_be_retried(self) -> bool:
        return False

    def job_id(self) -> str:
        return self._job_id

    def url(self) -> Optional[str]:
        return None


class SnowflakeMaterializationEngine(BatchMaterializationEngine):
    def update(
        self,
        project: str,
        views_to_delete: Sequence[
            Union[BatchFeatureView, StreamFeatureView, FeatureView]
        ],
        views_to_keep: Sequence[
            Union[BatchFeatureView, StreamFeatureView, FeatureView]
        ],
        entities_to_delete: Sequence[Entity],
        entities_to_keep: Sequence[Entity],
    ):
        stage_context = f'"{self.repo_config.batch_engine.database}"."{self.repo_config.batch_engine.schema_}"'
        stage_path = f'{stage_context}."feast_{project}"'
        with GetSnowflakeConnection(self.repo_config.batch_engine) as conn:
            query = f"SHOW STAGES IN {stage_context}"
            cursor = execute_snowflake_statement(conn, query)
            stage_list = pd.DataFrame(
                cursor.fetchall(),
                columns=[column.name for column in cursor.description],
            )

            # if the stage already exists,
            # assumes that the materialization functions have been deployed
            if f"feast_{project}" in stage_list["name"].tolist():
                click.echo(
                    f"Materialization functions for {Style.BRIGHT + Fore.GREEN}{project}{Style.RESET_ALL} already detected."
                )
                click.echo()
                return None

            click.echo(
                f"Deploying materialization functions for {Style.BRIGHT + Fore.GREEN}{project}{Style.RESET_ALL}"
            )
            click.echo()

            query = f"CREATE STAGE {stage_path}"
            execute_snowflake_statement(conn, query)

            copy_path, zip_path = package_snowpark_zip(project)
            query = f"PUT file://{zip_path} @{stage_path}"
            execute_snowflake_statement(conn, query)

            shutil.rmtree(copy_path)

            # Execute snowflake python udf creation functions
            sql_function_file = f"{os.path.dirname(feast.__file__)}/infra/utils/snowflake/snowpark/snowflake_python_udfs_creation.sql"
            with open(sql_function_file, "r") as file:
                sqlFile = file.read()

                sqlCommands = sqlFile.split(";")
                for command in sqlCommands:
                    command = command.replace("STAGE_HOLDER", f"{stage_path}")
                    query = command.replace("PROJECT_NAME", f"{project}")
                    execute_snowflake_statement(conn, query)

        return None

    def teardown_infra(
        self,
        project: str,
        fvs: Sequence[Union[BatchFeatureView, StreamFeatureView, FeatureView]],
        entities: Sequence[Entity],
    ):

        stage_path = f'"{self.repo_config.batch_engine.database}"."{self.repo_config.batch_engine.schema_}"."feast_{project}"'
        with GetSnowflakeConnection(self.repo_config.batch_engine) as conn:
            query = f"DROP STAGE IF EXISTS {stage_path}"
            execute_snowflake_statement(conn, query)

            # Execute snowflake python udf deletion functions
            sql_function_file = f"{os.path.dirname(feast.__file__)}/infra/utils/snowflake/snowpark/snowflake_python_udfs_deletion.sql"
            with open(sql_function_file, "r") as file:
                sqlFile = file.read()

                sqlCommands = sqlFile.split(";")
                for command in sqlCommands:
                    query = command.replace("PROJECT_NAME", f"{project}")
                    execute_snowflake_statement(conn, query)

        return None

    def __init__(
        self,
        *,
        repo_config: RepoConfig,
        offline_store: OfflineStore,
        online_store: OnlineStore,
        **kwargs,
    ):
        assert (
            repo_config.offline_store.type == "snowflake.offline"
        ), "To use SnowflakeMaterializationEngine, you must use Snowflake as an offline store."

        super().__init__(
            repo_config=repo_config,
            offline_store=offline_store,
            online_store=online_store,
            **kwargs,
        )

    def materialize(
        self, registry, tasks: List[MaterializationTask]
    ) -> List[MaterializationJob]:
        return [
            self._materialize_one(
                registry,
                task.feature_view,
                task.start_time,
                task.end_time,
                task.project,
                task.tqdm_builder,
            )
            for task in tasks
        ]

    def _materialize_one(
        self,
        registry: BaseRegistry,
        feature_view: Union[BatchFeatureView, StreamFeatureView, FeatureView],
        start_date: datetime,
        end_date: datetime,
        project: str,
        tqdm_builder: Callable[[int], tqdm],
    ):
        assert isinstance(feature_view, BatchFeatureView) or isinstance(
            feature_view, FeatureView
        ), "Snowflake can only materialize FeatureView & BatchFeatureView feature view types."

        entities = []
        for entity_name in feature_view.entities:
            entities.append(registry.get_entity(entity_name, project))

        (
            join_key_columns,
            feature_name_columns,
            timestamp_field,
            created_timestamp_column,
        ) = _get_column_names(feature_view, entities)

        job_id = f"{feature_view.name}-{start_date}-{end_date}"

        try:
            offline_job = self.offline_store.pull_latest_from_table_or_query(
                config=self.repo_config,
                data_source=feature_view.batch_source,
                join_key_columns=join_key_columns,
                feature_name_columns=feature_name_columns,
                timestamp_field=timestamp_field,
                created_timestamp_column=created_timestamp_column,
                start_date=start_date,
                end_date=end_date,
            )

            # Lets check and see if we can skip this query, because the table hasnt changed
            # since before the start date of this query
            with GetSnowflakeConnection(self.repo_config.offline_store) as conn:
                query = f"""SELECT SYSTEM$LAST_CHANGE_COMMIT_TIME('{feature_view.batch_source.get_table_query_string()}') AS last_commit_change_time"""
                last_commit_change_time = (
                    execute_snowflake_statement(conn, query).fetchall()[0][0]
                    / 1_000_000_000
                )
            if last_commit_change_time < start_date.astimezone(tz=utc).timestamp():
                return SnowflakeMaterializationJob(
                    job_id=job_id, status=MaterializationJobStatus.SUCCEEDED
                )

            fv_latest_values_sql = offline_job.to_sql()

            if feature_view.batch_source.field_mapping is not None:
                fv_latest_mapped_values_sql = _run_snowflake_field_mapping(
                    fv_latest_values_sql, feature_view.batch_source.field_mapping
                )

            fv_to_proto_sql = self.generate_snowflake_materialization_query(
                self.repo_config,
                fv_latest_mapped_values_sql,
                feature_view,
                project,
            )

            if self.repo_config.online_store.type == "snowflake.online":
                self.materialize_to_snowflake_online_store(
                    self.repo_config,
                    fv_to_proto_sql,
                    feature_view,
                    project,
                )
            else:
                self.materialize_to_external_online_store(
                    self.repo_config,
                    fv_to_proto_sql,
                    feature_view,
                    tqdm_builder,
                )

            return SnowflakeMaterializationJob(
                job_id=job_id, status=MaterializationJobStatus.SUCCEEDED
            )
        except BaseException as e:
            return SnowflakeMaterializationJob(
                job_id=job_id, status=MaterializationJobStatus.ERROR, error=e
            )

    def generate_snowflake_materialization_query(
        self,
        repo_config: RepoConfig,
        fv_latest_mapped_values_sql: str,
        feature_view: Union[BatchFeatureView, FeatureView],
        project: str,
    ) -> str:

        if feature_view.batch_source.created_timestamp_column:
            fv_created_str = f',"{feature_view.batch_source.created_timestamp_column}"'
        else:
            fv_created_str = None

        join_keys = [entity.name for entity in feature_view.entity_columns]
        join_keys_type = [
            entity.dtype.to_value_type().name for entity in feature_view.entity_columns
        ]

        entity_names = "ARRAY_CONSTRUCT('" + "', '".join(join_keys) + "')"
        entity_data = 'ARRAY_CONSTRUCT("' + '", "'.join(join_keys) + '")'
        entity_types = "ARRAY_CONSTRUCT('" + "', '".join(join_keys_type) + "')"

        """
        Generate the SQL that maps the feature given ValueType to the correct python
        UDF serialization function.
        """
        feature_sql_list = []
        for feature in feature_view.features:
            feature_value_type_name = feature.dtype.to_value_type().name

            feature_sql = _convert_value_name_to_snowflake_udf(
                feature_value_type_name, project
            )

            if feature_value_type_name == "UNIX_TIMESTAMP":
                feature_sql = f'{feature_sql}(DATE_PART(EPOCH_NANOSECOND, "{feature.name}"::TIMESTAMP_LTZ)) AS "{feature.name}"'
            elif feature_value_type_name == "DOUBLE":
                feature_sql = (
                    f'{feature_sql}("{feature.name}"::DOUBLE) AS "{feature.name}"'
                )
            else:
                feature_sql = f'{feature_sql}("{feature.name}") AS "{feature.name}"'

            feature_sql_list.append(feature_sql)

        features_str = ",\n".join(feature_sql_list)

        if repo_config.online_store.type == "snowflake.online":
            serial_func = f"feast_{project}_serialize_entity_keys"
        else:
            serial_func = f"feast_{project}_entity_key_proto_to_string"

        fv_to_proto_sql = f"""
            SELECT
              {serial_func.upper()}({entity_names}, {entity_data}, {entity_types}) AS "entity_key",
              {features_str},
              "{feature_view.batch_source.timestamp_field}"
              {fv_created_str if fv_created_str else ''}
            FROM (
              {fv_latest_mapped_values_sql}
            )
        """

        return fv_to_proto_sql

    def materialize_to_snowflake_online_store(
        self,
        repo_config: RepoConfig,
        materialization_sql: str,
        feature_view: Union[BatchFeatureView, FeatureView],
        project: str,
    ) -> None:
        assert_snowflake_feature_names(feature_view)

        feature_names_str = '", "'.join(
            [feature.name for feature in feature_view.features]
        )

        if feature_view.batch_source.created_timestamp_column:
            fv_created_str = f',"{feature_view.batch_source.created_timestamp_column}"'
        else:
            fv_created_str = None

        online_path = get_snowflake_online_store_path(repo_config, feature_view)
        online_table = (
            f'{online_path}."[online-transient] {project}_{feature_view.name}"'
        )

        query = f"""
            MERGE INTO {online_table} online_table
              USING (
                SELECT
                  "entity_key" || TO_BINARY("feature_name", 'UTF-8') AS "entity_feature_key",
                  "entity_key",
                  "feature_name",
                  "feature_value" AS "value",
                  "{feature_view.batch_source.timestamp_field}" AS "event_ts"
                  {fv_created_str + ' AS "created_ts"' if fv_created_str else ''}
                FROM (
                  {materialization_sql}
                )
                UNPIVOT("feature_value" FOR "feature_name" IN ("{feature_names_str}"))
              ) AS latest_values ON online_table."entity_feature_key" = latest_values."entity_feature_key"
              WHEN MATCHED THEN
                UPDATE SET
                  online_table."entity_key" = latest_values."entity_key",
                  online_table."feature_name" = latest_values."feature_name",
                  online_table."value" = latest_values."value",
                  online_table."event_ts" = latest_values."event_ts"
                  {',online_table."created_ts" = latest_values."created_ts"' if fv_created_str else ''}
              WHEN NOT MATCHED THEN
                INSERT ("entity_feature_key", "entity_key", "feature_name", "value", "event_ts" {', "created_ts"' if fv_created_str else ''})
                VALUES (
                  latest_values."entity_feature_key",
                  latest_values."entity_key",
                  latest_values."feature_name",
                  latest_values."value",
                  latest_values."event_ts"
                  {',latest_values."created_ts"' if fv_created_str else ''}
                )
        """

        with GetSnowflakeConnection(repo_config.batch_engine) as conn:
            query_id = execute_snowflake_statement(conn, query).sfqid

        click.echo(
            f"Snowflake Query ID: {Style.BRIGHT + Fore.GREEN}{query_id}{Style.RESET_ALL}"
        )
        return None

    def materialize_to_external_online_store(
        self,
        repo_config: RepoConfig,
        materialization_sql: str,
        feature_view: Union[StreamFeatureView, FeatureView],
        tqdm_builder: Callable[[int], tqdm],
    ) -> None:

        feature_names = [feature.name for feature in feature_view.features]

        with GetSnowflakeConnection(repo_config.batch_engine) as conn:
            query = materialization_sql
            cursor = execute_snowflake_statement(conn, query)
            for i, df in enumerate(cursor.fetch_pandas_batches()):
                click.echo(
                    f"Snowflake: Processing Materialization ResultSet Batch #{i+1}"
                )

                entity_keys = (
                    df["entity_key"].apply(EntityKeyProto.FromString).to_numpy()
                )

                for feature in feature_names:
                    df[feature] = df[feature].apply(ValueProto.FromString)

                features = df[feature_names].to_dict("records")

                event_timestamps = [
                    _coerce_datetime(val)
                    for val in pd.to_datetime(
                        df[feature_view.batch_source.timestamp_field]
                    )
                ]

                if feature_view.batch_source.created_timestamp_column:
                    created_timestamps = [
                        _coerce_datetime(val)
                        for val in pd.to_datetime(
                            df[feature_view.batch_source.created_timestamp_column]
                        )
                    ]
                else:
                    created_timestamps = [None] * df.shape[0]

                rows_to_write = list(
                    zip(
                        entity_keys,
                        features,
                        event_timestamps,
                        created_timestamps,
                    )
                )

                with tqdm_builder(len(rows_to_write)) as pbar:
                    self.online_store.online_write_batch(
                        repo_config,
                        feature_view,
                        rows_to_write,
                        lambda x: pbar.update(x),
                    )
        return None
