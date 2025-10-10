import contextlib
import json
import os
import uuid
import warnings
from datetime import datetime, timezone
from functools import reduce
from pathlib import Path
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    ContextManager,
    Dict,
    Iterator,
    List,
    Literal,
    Optional,
    Tuple,
    Union,
    cast,
)

import numpy as np
import pandas as pd
import pyarrow
from pydantic import ConfigDict, Field, StrictStr

from feast import OnDemandFeatureView
from feast.data_source import DataSource
from feast.errors import EntitySQLEmptyResults, InvalidEntityType
from feast.feature_logging import LoggingConfig, LoggingSource
from feast.feature_view import DUMMY_ENTITY_ID, DUMMY_ENTITY_VAL, FeatureView
from feast.infra.offline_stores import offline_utils
from feast.infra.offline_stores.offline_store import (
    OfflineStore,
    RetrievalJob,
    RetrievalMetadata,
)
from feast.infra.offline_stores.offline_utils import get_timestamp_filter_sql
from feast.infra.offline_stores.snowflake_source import (
    SavedDatasetSnowflakeStorage,
    SnowflakeLoggingDestination,
    SnowflakeSource,
)
from feast.infra.registry.base_registry import BaseRegistry
from feast.infra.utils.snowflake.snowflake_utils import (
    GetSnowflakeConnection,
    execute_snowflake_statement,
    write_pandas,
    write_parquet,
)
from feast.repo_config import FeastConfigBaseModel, RepoConfig
from feast.saved_dataset import SavedDatasetStorage
from feast.types import (
    Array,
    Bool,
    Bytes,
    Float32,
    Float64,
    Int32,
    Int64,
    String,
    UnixTimestamp,
)

try:
    from snowflake.connector import SnowflakeConnection
except ImportError as e:
    from feast.errors import FeastExtrasDependencyImportError

    raise FeastExtrasDependencyImportError("snowflake", str(e))

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

warnings.filterwarnings("ignore", category=DeprecationWarning)


class SnowflakeOfflineStoreConfig(FeastConfigBaseModel):
    """Offline store config for Snowflake"""

    type: Literal["snowflake.offline"] = "snowflake.offline"
    """ Offline store type selector """

    config_path: Optional[str] = os.path.expanduser("~/.snowsql/config")
    """ Snowflake snowsql config path -- absolute path required (Cant use ~)"""

    connection_name: Optional[str] = None
    """ Snowflake connector connection name -- typically defined in ~/.snowflake/connections.toml """

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

    storage_integration_name: Optional[str] = None
    """ Storage integration name in snowflake """

    blob_export_location: Optional[str] = None
    """ Location (in S3, Google storage or Azure storage) where data is offloaded """

    convert_timestamp_columns: Optional[bool] = None
    """ Convert timestamp columns on export to a Parquet-supported format """

    max_file_size: Optional[int] = None
    """ Upper size limit (in bytes) of each file that is offloaded. Default: 16777216"""
    model_config = ConfigDict(populate_by_name=True, extra="allow")


class SnowflakeOfflineStore(OfflineStore):
    @staticmethod
    def pull_latest_from_table_or_query(
        config: RepoConfig,
        data_source: DataSource,
        join_key_columns: List[str],
        feature_name_columns: List[str],
        timestamp_field: str,
        created_timestamp_column: Optional[str],
        start_date: datetime,
        end_date: datetime,
    ) -> RetrievalJob:
        assert isinstance(config.offline_store, SnowflakeOfflineStoreConfig)
        assert isinstance(data_source, SnowflakeSource)

        from_expression = data_source.get_table_query_string()
        if not data_source.database and not data_source.schema and data_source.table:
            from_expression = f'"{config.offline_store.database}"."{config.offline_store.schema_}".{from_expression}'
        if not data_source.database and data_source.schema and data_source.table:
            from_expression = f'"{config.offline_store.database}".{from_expression}'

        if join_key_columns:
            partition_by_join_key_string = '"' + '", "'.join(join_key_columns) + '"'
            partition_by_join_key_string = (
                "PARTITION BY " + partition_by_join_key_string
            )
        else:
            partition_by_join_key_string = ""

        timestamp_columns = [timestamp_field]
        if created_timestamp_column:
            timestamp_columns.append(created_timestamp_column)

        timestamp_desc_string = '"' + '" DESC, "'.join(timestamp_columns) + '" DESC'
        field_string = (
            '"'
            + '", "'.join(join_key_columns + feature_name_columns + timestamp_columns)
            + '"'
        )

        if config.offline_store.convert_timestamp_columns:
            select_fields = list(
                map(
                    lambda field_name: f'"{field_name}"',
                    join_key_columns + feature_name_columns,
                )
            )
            select_timestamps = list(
                map(
                    lambda field_name: f"TO_VARCHAR({field_name}, 'YYYY-MM-DD\"T\"HH24:MI:SS.FFTZH:TZM') AS {field_name}",
                    timestamp_columns,
                )
            )
            inner_field_string = ", ".join(select_fields + select_timestamps)
        else:
            select_fields = list(
                map(
                    lambda field_name: f'"{field_name}"',
                    join_key_columns + feature_name_columns + timestamp_columns,
                )
            )
            inner_field_string = ", ".join(select_fields)

        with GetSnowflakeConnection(config.offline_store) as conn:
            snowflake_conn = conn

        start_date = start_date.astimezone(tz=timezone.utc)
        end_date = end_date.astimezone(tz=timezone.utc)

        query = f"""
            SELECT
                {field_string}
                {f''', TRIM({repr(DUMMY_ENTITY_VAL)}::VARIANT,'"') AS "{DUMMY_ENTITY_ID}"''' if not join_key_columns else ""}
            FROM (
                SELECT {inner_field_string},
                ROW_NUMBER() OVER({partition_by_join_key_string} ORDER BY {timestamp_desc_string}) AS "_feast_row"
                FROM {from_expression}
                WHERE "{timestamp_field}" BETWEEN TIMESTAMP '{start_date}' AND TIMESTAMP '{end_date}'
            )
            WHERE "_feast_row" = 1
            """

        return SnowflakeRetrievalJob(
            query=query,
            snowflake_conn=snowflake_conn,
            config=config,
            full_feature_names=False,
            on_demand_feature_views=None,
        )

    @staticmethod
    def pull_all_from_table_or_query(
        config: RepoConfig,
        data_source: DataSource,
        join_key_columns: List[str],
        feature_name_columns: List[str],
        timestamp_field: str,
        created_timestamp_column: Optional[str] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
    ) -> RetrievalJob:
        assert isinstance(config.offline_store, SnowflakeOfflineStoreConfig)
        assert isinstance(data_source, SnowflakeSource)

        from_expression = data_source.get_table_query_string()
        if not data_source.database and not data_source.schema and data_source.table:
            from_expression = f'"{config.offline_store.database}"."{config.offline_store.schema_}".{from_expression}'
        if not data_source.database and data_source.schema and data_source.table:
            from_expression = f'"{config.offline_store.database}".{from_expression}'

        timestamp_fields = [timestamp_field]
        if created_timestamp_column:
            timestamp_fields.append(created_timestamp_column)
        field_string = (
            '"'
            + '", "'.join(join_key_columns + feature_name_columns + timestamp_fields)
            + '"'
        )

        with GetSnowflakeConnection(config.offline_store) as conn:
            snowflake_conn = conn

        timestamp_filter = get_timestamp_filter_sql(
            start_date, end_date, timestamp_field, tz=timezone.utc
        )

        query = f"""
            SELECT {field_string}
            FROM {from_expression}
            WHERE {timestamp_filter}
        """

        return SnowflakeRetrievalJob(
            query=query,
            snowflake_conn=snowflake_conn,
            config=config,
            full_feature_names=False,
        )

    @staticmethod
    def get_historical_features(
        config: RepoConfig,
        feature_views: List[FeatureView],
        feature_refs: List[str],
        entity_df: Union[pd.DataFrame, str],
        registry: BaseRegistry,
        project: str,
        full_feature_names: bool = False,
    ) -> RetrievalJob:
        assert isinstance(config.offline_store, SnowflakeOfflineStoreConfig)
        for fv in feature_views:
            assert isinstance(fv.batch_source, SnowflakeSource)

        with GetSnowflakeConnection(config.offline_store) as conn:
            snowflake_conn = conn

        entity_schema = _get_entity_schema(entity_df, snowflake_conn, config)

        entity_df_event_timestamp_col = (
            offline_utils.infer_event_timestamp_from_entity_df(entity_schema)
        )

        entity_df_event_timestamp_range = _get_entity_df_event_timestamp_range(
            entity_df,
            entity_df_event_timestamp_col,
            snowflake_conn,
        )

        @contextlib.contextmanager
        def query_generator() -> Iterator[str]:
            table_name = offline_utils.get_temp_entity_table_name()

            _upload_entity_df(entity_df, snowflake_conn, config, table_name)

            expected_join_keys = offline_utils.get_expected_join_keys(
                project, feature_views, registry
            )

            offline_utils.assert_expected_columns_in_entity_df(
                entity_schema, expected_join_keys, entity_df_event_timestamp_col
            )

            # Build a query context containing all information required to template the Snowflake SQL query
            query_context = offline_utils.get_feature_view_query_context(
                feature_refs,
                feature_views,
                registry,
                project,
                entity_df_event_timestamp_range,
            )

            query_context = _fix_entity_selections_identifiers(query_context)

            # Generate the Snowflake SQL query from the query context
            query = offline_utils.build_point_in_time_query(
                query_context,
                left_table_query_string=table_name,
                entity_df_event_timestamp_col=entity_df_event_timestamp_col,
                entity_df_columns=entity_schema.keys(),
                query_template=MULTIPLE_FEATURE_VIEW_POINT_IN_TIME_JOIN,
                full_feature_names=full_feature_names,
            )

            yield query

        return SnowflakeRetrievalJob(
            query=query_generator,
            snowflake_conn=snowflake_conn,
            config=config,
            full_feature_names=full_feature_names,
            on_demand_feature_views=OnDemandFeatureView.get_requested_odfvs(
                feature_refs, project, registry
            ),
            feature_views=feature_views,
            metadata=RetrievalMetadata(
                features=feature_refs,
                keys=list(entity_schema.keys() - {entity_df_event_timestamp_col}),
                min_event_timestamp=entity_df_event_timestamp_range[0],
                max_event_timestamp=entity_df_event_timestamp_range[1],
            ),
        )

    @staticmethod
    def write_logged_features(
        config: RepoConfig,
        data: Union[pyarrow.Table, Path],
        source: LoggingSource,
        logging_config: LoggingConfig,
        registry: BaseRegistry,
    ):
        assert isinstance(logging_config.destination, SnowflakeLoggingDestination)

        with GetSnowflakeConnection(config.offline_store) as conn:
            snowflake_conn = conn

        if isinstance(data, Path):
            write_parquet(
                snowflake_conn,
                data,
                source.get_schema(registry),
                table_name=logging_config.destination.table_name,
                auto_create_table=True,
            )
        else:
            write_pandas(
                snowflake_conn,
                data.to_pandas(),
                table_name=logging_config.destination.table_name,
                auto_create_table=True,
            )

    @staticmethod
    def offline_write_batch(
        config: RepoConfig,
        feature_view: FeatureView,
        table: pyarrow.Table,
        progress: Optional[Callable[[int], Any]],
    ):
        assert isinstance(config.offline_store, SnowflakeOfflineStoreConfig)
        assert isinstance(feature_view.batch_source, SnowflakeSource)

        pa_schema, column_names = offline_utils.get_pyarrow_schema_from_batch_source(
            config, feature_view.batch_source
        )
        if column_names != table.column_names:
            raise ValueError(
                f"The input pyarrow table has schema {table.schema} with the incorrect columns {table.column_names}. "
                f"The schema is expected to be {pa_schema} with the columns (in this exact order) to be {column_names}."
            )

        if table.schema != pa_schema:
            table = table.cast(pa_schema)

        with GetSnowflakeConnection(config.offline_store) as conn:
            snowflake_conn = conn

        write_pandas(
            snowflake_conn,
            table.to_pandas(),
            table_name=feature_view.batch_source.table,
            auto_create_table=True,
        )


class SnowflakeRetrievalJob(RetrievalJob):
    def __init__(
        self,
        query: Union[str, Callable[[], ContextManager[str]]],
        snowflake_conn: SnowflakeConnection,
        config: RepoConfig,
        full_feature_names: bool,
        on_demand_feature_views: Optional[List[OnDemandFeatureView]] = None,
        feature_views: Optional[List[FeatureView]] = None,
        metadata: Optional[RetrievalMetadata] = None,
    ):
        if feature_views is None:
            feature_views = []
        if not isinstance(query, str):
            self._query_generator = query
        else:

            @contextlib.contextmanager
            def query_generator() -> Iterator[str]:
                assert isinstance(query, str)
                yield query

            self._query_generator = query_generator

        self.snowflake_conn = snowflake_conn
        self.config = config
        self._full_feature_names = full_feature_names
        self._on_demand_feature_views = on_demand_feature_views or []
        self._feature_views = feature_views
        self._metadata = metadata
        self.export_path: Optional[str]
        if self.config.offline_store.blob_export_location:
            self.export_path = f"{self.config.offline_store.blob_export_location}/{self.config.project}/{uuid.uuid4()}"
        else:
            self.export_path = None

    @property
    def full_feature_names(self) -> bool:
        return self._full_feature_names

    @property
    def on_demand_feature_views(self) -> List[OnDemandFeatureView]:
        return self._on_demand_feature_views

    def _to_df_internal(self, timeout: Optional[int] = None) -> pd.DataFrame:
        df = execute_snowflake_statement(
            self.snowflake_conn, self.to_sql()
        ).fetch_pandas_all()

        for feature_view in self._feature_views:
            for feature in feature_view.features:
                if feature.dtype in [
                    Array(String),
                    Array(Bytes),
                    Array(Int32),
                    Array(Int64),
                    Array(UnixTimestamp),
                    Array(Float64),
                    Array(Float32),
                    Array(Bool),
                ]:
                    df[feature.name] = [
                        json.loads(x) if x else None for x in df[feature.name]
                    ]

        return df

    def _to_arrow_internal(self, timeout: Optional[int] = None) -> pyarrow.Table:
        return execute_snowflake_statement(
            self.snowflake_conn, self.to_sql()
        ).fetch_arrow_all(force_return_table=True)

    def to_sql(self) -> str:
        """
        Returns the SQL query that will be executed in Snowflake to build the historical feature table.
        """
        with self._query_generator() as query:
            return query

    def to_snowflake(
        self, table_name: str, allow_overwrite: bool = False, temporary: bool = False
    ) -> None:
        """Save dataset as a new Snowflake table"""
        if self.on_demand_feature_views:
            transformed_df = self.to_df()

            if allow_overwrite:
                query = f'DROP TABLE IF EXISTS "{table_name}"'
                execute_snowflake_statement(self.snowflake_conn, query)

            write_pandas(
                self.snowflake_conn,
                transformed_df,
                table_name,
                auto_create_table=True,
                create_temp_table=temporary,
            )

        else:
            query = f'CREATE {"OR REPLACE" if allow_overwrite else ""} {"TEMPORARY" if temporary else ""} TABLE {"IF NOT EXISTS" if not allow_overwrite else ""} "{table_name}" AS ({self.to_sql()});\n'
            execute_snowflake_statement(self.snowflake_conn, query)

        return None

    def to_arrow_batches(self) -> Iterator[pyarrow.Table]:
        table_name = "temp_arrow_batches_" + uuid.uuid4().hex

        self.to_snowflake(table_name=table_name, allow_overwrite=True, temporary=True)

        query = f'SELECT * FROM "{table_name}"'
        arrow_batches = execute_snowflake_statement(
            self.snowflake_conn, query
        ).fetch_arrow_batches()

        return arrow_batches

    def to_pandas_batches(self) -> Iterator[pd.DataFrame]:
        table_name = "temp_pandas_batches_" + uuid.uuid4().hex

        self.to_snowflake(table_name=table_name, allow_overwrite=True, temporary=True)

        query = f'SELECT * FROM "{table_name}"'
        arrow_batches = execute_snowflake_statement(
            self.snowflake_conn, query
        ).fetch_pandas_batches()

        return arrow_batches

    def to_spark_df(self, spark_session: "SparkSession") -> "DataFrame":
        """
        Method to convert snowflake query results to pyspark data frame.

        Args:
            spark_session: spark Session variable of current environment.

        Returns:
            spark_df: A pyspark dataframe.
        """

        try:
            from pyspark.sql import DataFrame
        except ImportError as e:
            from feast.errors import FeastExtrasDependencyImportError

            raise FeastExtrasDependencyImportError("spark", str(e))

        spark_session.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")

        # This can be improved by parallelizing the read of chunks
        pandas_batches = self.to_pandas_batches()

        spark_df = reduce(
            DataFrame.unionAll,
            [spark_session.createDataFrame(batch) for batch in pandas_batches],
        )
        return spark_df

    def persist(
        self,
        storage: SavedDatasetStorage,
        allow_overwrite: bool = False,
        timeout: Optional[int] = None,
    ):
        assert isinstance(storage, SavedDatasetSnowflakeStorage)

        self.to_snowflake(
            table_name=storage.snowflake_options.table, allow_overwrite=allow_overwrite
        )

    @property
    def metadata(self) -> Optional[RetrievalMetadata]:
        return self._metadata

    def supports_remote_storage_export(self) -> bool:
        return (
            self.config.offline_store.storage_integration_name
            and self.config.offline_store.blob_export_location
        )

    def to_remote_storage(self) -> List[str]:
        if not self.export_path:
            raise ValueError(
                "to_remote_storage() requires `blob_export_location` to be specified in config"
            )
        if not self.config.offline_store.storage_integration_name:
            raise ValueError(
                "to_remote_storage() requires `storage_integration_name` to be specified in config"
            )

        table = f"temporary_{uuid.uuid4().hex}"
        self.to_snowflake(table, temporary=True)

        query = f"""
            COPY INTO '{self.export_path}/{table}' FROM "{self.config.offline_store.database}"."{self.config.offline_store.schema_}"."{table}"\n
              STORAGE_INTEGRATION = {self.config.offline_store.storage_integration_name}\n
              FILE_FORMAT = (TYPE = PARQUET)
              DETAILED_OUTPUT = TRUE
              HEADER = TRUE
        """
        if (max_file_size := self.config.offline_store.max_file_size) is not None:
            query += f"\nMAX_FILE_SIZE = {max_file_size}"

        cursor = execute_snowflake_statement(self.snowflake_conn, query)
        # s3gov schema is used by Snowflake in AWS govcloud regions
        # remove gov portion from schema and pass it to online store upload
        native_export_path = self.export_path.replace("s3gov://", "s3://")
        return self._get_file_names_from_copy_into(cursor, native_export_path)

    def _get_file_names_from_copy_into(self, cursor, native_export_path) -> List[str]:
        file_name_column_index = [
            idx for idx, rm in enumerate(cursor.description) if rm.name == "FILE_NAME"
        ][0]
        return [
            f"{native_export_path}/{row[file_name_column_index]}"
            for row in cursor.fetchall()
        ]


def _get_entity_schema(
    entity_df: Union[pd.DataFrame, str],
    snowflake_conn: SnowflakeConnection,
    config: RepoConfig,
) -> Dict[str, np.dtype]:
    if isinstance(entity_df, pd.DataFrame):
        return dict(zip(entity_df.columns, entity_df.dtypes))

    else:
        query = f"SELECT * FROM ({entity_df}) LIMIT 1"
        limited_entity_df = execute_snowflake_statement(
            snowflake_conn, query
        ).fetch_pandas_all()

        return dict(zip(limited_entity_df.columns, limited_entity_df.dtypes))


def _upload_entity_df(
    entity_df: Union[pd.DataFrame, str],
    snowflake_conn: SnowflakeConnection,
    config: RepoConfig,
    table_name: str,
) -> None:
    if isinstance(entity_df, pd.DataFrame):
        # Write the data from the DataFrame to the table
        # Known issues with following entity data types: BINARY
        write_pandas(
            snowflake_conn,
            entity_df,
            table_name,
            auto_create_table=True,
            create_temp_table=True,
        )

        return None
    elif isinstance(entity_df, str):
        # If the entity_df is a string (SQL query), create a Snowflake table out of it,
        query = f'CREATE TEMPORARY TABLE "{table_name}" AS ({entity_df})'
        execute_snowflake_statement(snowflake_conn, query)

        return None
    else:
        raise InvalidEntityType(type(entity_df))


def _fix_entity_selections_identifiers(query_context) -> list:
    for i, qc in enumerate(query_context):
        for j, es in enumerate(qc.entity_selections):
            query_context[i].entity_selections[j] = f'"{es}"'.replace(" AS ", '" AS "')

    return query_context


def _get_entity_df_event_timestamp_range(
    entity_df: Union[pd.DataFrame, str],
    entity_df_event_timestamp_col: str,
    snowflake_conn: SnowflakeConnection,
) -> Tuple[datetime, datetime]:
    if isinstance(entity_df, pd.DataFrame):
        entity_df_event_timestamp = entity_df.loc[
            :, entity_df_event_timestamp_col
        ].infer_objects()
        if pd.api.types.is_string_dtype(entity_df_event_timestamp):
            entity_df_event_timestamp = pd.to_datetime(
                entity_df_event_timestamp, utc=True
            )
        entity_df_event_timestamp_range = (
            entity_df_event_timestamp.min().to_pydatetime(),
            entity_df_event_timestamp.max().to_pydatetime(),
        )
    elif isinstance(entity_df, str):
        # If the entity_df is a string (SQL query), determine range
        # from table
        query = f'SELECT MIN("{entity_df_event_timestamp_col}") AS "min_value", MAX("{entity_df_event_timestamp_col}") AS "max_value" FROM ({entity_df})'
        results = execute_snowflake_statement(snowflake_conn, query).fetchall()

        entity_df_event_timestamp_range = cast(Tuple[datetime, datetime], results[0])
        if (
            entity_df_event_timestamp_range[0] is None
            or entity_df_event_timestamp_range[1] is None
        ):
            raise EntitySQLEmptyResults(entity_df)
    else:
        raise InvalidEntityType(type(entity_df))

    return entity_df_event_timestamp_range


MULTIPLE_FEATURE_VIEW_POINT_IN_TIME_JOIN = """
/*
 0. Compute a deterministic hash for the `left_table_query_string` that will be used throughout
 all the logic as the field to GROUP BY the data.
*/
WITH "entity_dataframe" AS (
    SELECT *,
        "{{entity_df_event_timestamp_col}}" AS "entity_timestamp"
        {% for featureview in featureviews %}
            {% if featureview.entities %}
            ,(
                {% for entity in featureview.entities %}
                    CAST("{{entity}}" AS VARCHAR) ||
                {% endfor %}
                CAST("{{entity_df_event_timestamp_col}}" AS VARCHAR)
            ) AS "{{featureview.name}}__entity_row_unique_id"
            {% else %}
            ,CAST("{{entity_df_event_timestamp_col}}" AS VARCHAR) AS "{{featureview.name}}__entity_row_unique_id"
            {% endif %}
        {% endfor %}
    FROM "{{ left_table_query_string }}"
),

{% for featureview in featureviews %}

/*
 1. Only select the required columns with entities of the featureview.
*/

"{{ featureview.name }}__entity_dataframe" AS (
    SELECT
        {{ featureview.entities | map('tojson') | join(', ')}}{% if featureview.entities %},{% else %}{% endif %}
        "entity_timestamp",
        "{{featureview.name}}__entity_row_unique_id"
    FROM "entity_dataframe"
    GROUP BY
        {{ featureview.entities | map('tojson') | join(', ')}}{% if featureview.entities %},{% else %}{% endif %}
        "entity_timestamp",
        "{{featureview.name}}__entity_row_unique_id"
),

/*
2. Use subquery to prepare event_timestamp, created_timestamp, entity columns and feature columns.
*/

"{{ featureview.name }}__subquery" AS (
    SELECT
        "{{ featureview.timestamp_field }}" as "event_timestamp",
        {{'"' ~ featureview.created_timestamp_column ~ '" as "created_timestamp",' if featureview.created_timestamp_column else '' }}
        {{featureview.entity_selections | join(', ')}}{% if featureview.entity_selections %},{% else %}{% endif %}
        {% for feature in featureview.features %}
            "{{ feature }}" as {% if full_feature_names %}"{{ featureview.name }}__{{featureview.field_mapping.get(feature, feature)}}"{% else %}"{{ featureview.field_mapping.get(feature, feature) }}"{% endif %}{% if loop.last %}{% else %}, {% endif %}
        {% endfor %}
    FROM {{ featureview.table_subquery }}
),

/*
3. If the `created_timestamp_column` has been set, we need to
deduplicate the data first. This is done by calculating the
`MAX(created_at_timestamp)` for each event_timestamp and joining back on the subquery.
Otherwise, the ASOF JOIN can have unstable side effects
https://docs.snowflake.com/en/sql-reference/constructs/asof-join#expected-behavior-when-ties-exist-in-the-right-table
*/

{% if featureview.created_timestamp_column %}
"{{ featureview.name }}__dedup" AS (
    SELECT *
    FROM "{{ featureview.name }}__subquery"
    INNER JOIN (
        SELECT
            {{ featureview.entities | map('tojson') | join(', ')}}{% if featureview.entities %},{% else %}{% endif %}
            "event_timestamp",
            MAX("created_timestamp") AS "created_timestamp"
        FROM "{{ featureview.name }}__subquery"
        GROUP BY {{ featureview.entities | map('tojson') | join(', ')}}{% if featureview.entities %},{% else %}{% endif %} "event_timestamp"
        )
    USING({{ featureview.entities | map('tojson') | join(', ')}}{% if featureview.entities %},{% else %}{% endif %} "event_timestamp", "created_timestamp")
),
{% endif %}

/*
4. Make ASOF JOIN of deduplicated feature CTE on reduced entity dataframe.
*/

"{{ featureview.name }}__asof_join" AS (
    SELECT
        e.*,
        v.*
    FROM "{{ featureview.name }}__entity_dataframe" e
    ASOF JOIN {% if featureview.created_timestamp_column %}"{{ featureview.name }}__dedup"{% else %}"{{ featureview.name }}__subquery"{% endif %} v
    MATCH_CONDITION (e."entity_timestamp" >= v."event_timestamp")
    {% if featureview.entities %} USING({{ featureview.entities | map('tojson') | join(', ')}}) {% endif %}
),

/*
5. If TTL is configured filter the CTE to remove rows where the feature values are older than the configured ttl.
*/

"{{ featureview.name }}__ttl" AS (
    SELECT *
    FROM "{{ featureview.name }}__asof_join"
    {% if featureview.ttl == 0 %}{% else %}
    WHERE "event_timestamp" >= TIMESTAMPADD(second,-{{ featureview.ttl }},"entity_timestamp")
    {% endif %}
){% if loop.last %}{% else %}, {% endif %}

{% endfor %}
/*
 Join the outputs of multiple time travel joins to a single table.
 The entity_dataframe dataset being our source of truth here.
 */

SELECT "{{ final_output_feature_names | join('", "')}}"
FROM "entity_dataframe"
{% for featureview in featureviews %}
LEFT JOIN (
    SELECT
        "{{featureview.name}}__entity_row_unique_id"
        {% for feature in featureview.features %}
            ,{% if full_feature_names %}"{{ featureview.name }}__{{featureview.field_mapping.get(feature, feature)}}"{% else %}"{{ featureview.field_mapping.get(feature, feature) }}"{% endif %}
        {% endfor %}
    FROM "{{ featureview.name }}__ttl"
) "{{ featureview.name }}__ttl" USING ("{{featureview.name}}__entity_row_unique_id")
{% endfor %}
"""
