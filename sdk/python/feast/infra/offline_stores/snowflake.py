import contextlib
import os
import uuid
import warnings
from datetime import datetime
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
    Optional,
    Tuple,
    Union,
    cast,
)

import numpy as np
import pandas as pd
import pyarrow
from pydantic import Field, StrictStr
from pydantic.typing import Literal
from pytz import utc

from feast import OnDemandFeatureView
from feast.data_source import DataSource
from feast.errors import (
    EntitySQLEmptyResults,
    InvalidEntityType,
    InvalidSparkSessionException,
)
from feast.feature_logging import LoggingConfig, LoggingSource
from feast.feature_view import DUMMY_ENTITY_ID, DUMMY_ENTITY_VAL, FeatureView
from feast.infra.offline_stores import offline_utils
from feast.infra.offline_stores.offline_store import (
    OfflineStore,
    RetrievalJob,
    RetrievalMetadata,
)
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
from feast.usage import log_exceptions_and_usage

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
    """ Snowflake config path -- absolute path required (Cant use ~)"""

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

    database: StrictStr
    """ Snowflake database name """

    schema_: Optional[str] = Field("PUBLIC", alias="schema")
    """ Snowflake schema name """

    storage_integration_name: Optional[str] = None
    """ Storage integration name in snowflake """

    blob_export_location: Optional[str] = None
    """ Location (in S3, Google storage or Azure storage) where data is offloaded """

    class Config:
        allow_population_by_field_name = True


class SnowflakeOfflineStore(OfflineStore):
    @staticmethod
    @log_exceptions_and_usage(offline_store="snowflake")
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
        if not data_source.database and data_source.table:
            from_expression = f'"{config.offline_store.database}"."{config.offline_store.schema_}".{from_expression}'

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

        if data_source.snowflake_options.warehouse:
            config.offline_store.warehouse = data_source.snowflake_options.warehouse

        with GetSnowflakeConnection(config.offline_store) as conn:
            snowflake_conn = conn

        start_date = start_date.astimezone(tz=utc)
        end_date = end_date.astimezone(tz=utc)

        query = f"""
            SELECT
                {field_string}
                {f''', TRIM({repr(DUMMY_ENTITY_VAL)}::VARIANT,'"') AS "{DUMMY_ENTITY_ID}"''' if not join_key_columns else ""}
            FROM (
                SELECT {field_string},
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
    @log_exceptions_and_usage(offline_store="snowflake")
    def pull_all_from_table_or_query(
        config: RepoConfig,
        data_source: DataSource,
        join_key_columns: List[str],
        feature_name_columns: List[str],
        timestamp_field: str,
        start_date: datetime,
        end_date: datetime,
    ) -> RetrievalJob:
        assert isinstance(config.offline_store, SnowflakeOfflineStoreConfig)
        assert isinstance(data_source, SnowflakeSource)

        from_expression = data_source.get_table_query_string()
        if not data_source.database and data_source.table:
            from_expression = f'"{config.offline_store.database}"."{config.offline_store.schema_}".{from_expression}'

        field_string = (
            '"'
            + '", "'.join(join_key_columns + feature_name_columns + [timestamp_field])
            + '"'
        )

        if data_source.snowflake_options.warehouse:
            config.offline_store.warehouse = data_source.snowflake_options.warehouse

        with GetSnowflakeConnection(config.offline_store) as conn:
            snowflake_conn = conn

        start_date = start_date.astimezone(tz=utc)
        end_date = end_date.astimezone(tz=utc)

        query = f"""
            SELECT {field_string}
            FROM {from_expression}
            WHERE "{timestamp_field}" BETWEEN TIMESTAMP '{start_date}' AND TIMESTAMP '{end_date}'
        """

        return SnowflakeRetrievalJob(
            query=query,
            snowflake_conn=snowflake_conn,
            config=config,
            full_feature_names=False,
        )

    @staticmethod
    @log_exceptions_and_usage(offline_store="snowflake")
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
        metadata: Optional[RetrievalMetadata] = None,
    ):

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
        with self._query_generator() as query:

            df = execute_snowflake_statement(
                self.snowflake_conn, query
            ).fetch_pandas_all()

        return df

    def _to_arrow_internal(self, timeout: Optional[int] = None) -> pyarrow.Table:
        with self._query_generator() as query:

            pa_table = execute_snowflake_statement(
                self.snowflake_conn, query
            ).fetch_arrow_all()

            if pa_table:
                return pa_table
            else:
                empty_result = execute_snowflake_statement(self.snowflake_conn, query)

                return pyarrow.Table.from_pandas(
                    pd.DataFrame(columns=[md.name for md in empty_result.description])
                )

    def to_snowflake(self, table_name: str, temporary=False) -> None:
        """Save dataset as a new Snowflake table"""
        if self.on_demand_feature_views:
            transformed_df = self.to_df()

            write_pandas(
                self.snowflake_conn, transformed_df, table_name, auto_create_table=True
            )

            return None

        with self._query_generator() as query:
            query = f'CREATE {"TEMPORARY" if temporary else ""} TABLE IF NOT EXISTS "{table_name}" AS ({query});\n'

            execute_snowflake_statement(self.snowflake_conn, query)

    def to_sql(self) -> str:
        """
        Returns the SQL query that will be executed in Snowflake to build the historical feature table.
        """
        with self._query_generator() as query:
            return query

    def to_spark_df(self, spark_session: "SparkSession") -> "DataFrame":
        """
        Method to convert snowflake query results to pyspark data frame.

        Args:
            spark_session: spark Session variable of current environment.

        Returns:
            spark_df: A pyspark dataframe.
        """

        try:
            from pyspark.sql import DataFrame, SparkSession
        except ImportError as e:
            from feast.errors import FeastExtrasDependencyImportError

            raise FeastExtrasDependencyImportError("spark", str(e))

        if isinstance(spark_session, SparkSession):
            with self._query_generator() as query:

                arrow_batches = execute_snowflake_statement(
                    self.snowflake_conn, query
                ).fetch_arrow_batches()

                if arrow_batches:
                    spark_df = reduce(
                        DataFrame.unionAll,
                        [
                            spark_session.createDataFrame(batch.to_pandas())
                            for batch in arrow_batches
                        ],
                    )

                    return spark_df

                else:
                    raise EntitySQLEmptyResults(query)

        else:
            raise InvalidSparkSessionException(spark_session)

    def persist(self, storage: SavedDatasetStorage, allow_overwrite: bool = False):
        assert isinstance(storage, SavedDatasetSnowflakeStorage)
        self.to_snowflake(table_name=storage.snowflake_options.table)

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
        self.to_snowflake(table)

        query = f"""
            COPY INTO '{self.config.offline_store.blob_export_location}/{table}' FROM "{self.config.offline_store.database}"."{self.config.offline_store.schema_}"."{table}"\n
              STORAGE_INTEGRATION = {self.config.offline_store.storage_integration_name}\n
              FILE_FORMAT = (TYPE = PARQUET)
              DETAILED_OUTPUT = TRUE
              HEADER = TRUE
        """
        cursor = execute_snowflake_statement(self.snowflake_conn, query)

        file_name_column_index = [
            idx for idx, rm in enumerate(cursor.description) if rm.name == "FILE_NAME"
        ][0]
        return [
            f"{self.export_path}/{row[file_name_column_index]}"
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
 Compute a deterministic hash for the `left_table_query_string` that will be used throughout
 all the logic as the field to GROUP BY the data
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
 This query template performs the point-in-time correctness join for a single feature set table
 to the provided entity table.

 1. We first join the current feature_view to the entity dataframe that has been passed.
 This JOIN has the following logic:
    - For each row of the entity dataframe, only keep the rows where the `timestamp_field`
    is less than the one provided in the entity dataframe
    - If there a TTL for the current feature_view, also keep the rows where the `timestamp_field`
    is higher the the one provided minus the TTL
    - For each row, Join on the entity key and retrieve the `entity_row_unique_id` that has been
    computed previously

 The output of this CTE will contain all the necessary information and already filtered out most
 of the data that is not relevant.
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
    WHERE "{{ featureview.timestamp_field }}" <= '{{ featureview.max_event_timestamp }}'
    {% if featureview.ttl == 0 %}{% else %}
    AND "{{ featureview.timestamp_field }}" >= '{{ featureview.min_event_timestamp }}'
    {% endif %}
),

"{{ featureview.name }}__base" AS (
    SELECT
        "subquery".*,
        "entity_dataframe"."entity_timestamp",
        "entity_dataframe"."{{featureview.name}}__entity_row_unique_id"
    FROM "{{ featureview.name }}__subquery" AS "subquery"
    INNER JOIN "{{ featureview.name }}__entity_dataframe" AS "entity_dataframe"
    ON TRUE
        AND "subquery"."event_timestamp" <= "entity_dataframe"."entity_timestamp"

        {% if featureview.ttl == 0 %}{% else %}
        AND "subquery"."event_timestamp" >= TIMESTAMPADD(second,-{{ featureview.ttl }},"entity_dataframe"."entity_timestamp")
        {% endif %}

        {% for entity in featureview.entities %}
        AND "subquery"."{{ entity }}" = "entity_dataframe"."{{ entity }}"
        {% endfor %}
),

/*
 2. If the `created_timestamp_column` has been set, we need to
 deduplicate the data first. This is done by calculating the
 `MAX(created_at_timestamp)` for each event_timestamp.
 We then join the data on the next CTE
*/
{% if featureview.created_timestamp_column %}
"{{ featureview.name }}__dedup" AS (
    SELECT
        "{{featureview.name}}__entity_row_unique_id",
        "event_timestamp",
        MAX("created_timestamp") AS "created_timestamp"
    FROM "{{ featureview.name }}__base"
    GROUP BY "{{featureview.name}}__entity_row_unique_id", "event_timestamp"
),
{% endif %}

/*
 3. The data has been filtered during the first CTE "*__base"
 Thus we only need to compute the latest timestamp of each feature.
*/
"{{ featureview.name }}__latest" AS (
    SELECT
        "event_timestamp",
        {% if featureview.created_timestamp_column %}"created_timestamp",{% endif %}
        "{{featureview.name}}__entity_row_unique_id"
    FROM
    (
        SELECT *,
            ROW_NUMBER() OVER(
                PARTITION BY "{{featureview.name}}__entity_row_unique_id"
                ORDER BY "event_timestamp" DESC{% if featureview.created_timestamp_column %},"created_timestamp" DESC{% endif %}
            ) AS "row_number"
        FROM "{{ featureview.name }}__base"
        {% if featureview.created_timestamp_column %}
            INNER JOIN "{{ featureview.name }}__dedup"
            USING ("{{featureview.name}}__entity_row_unique_id", "event_timestamp", "created_timestamp")
        {% endif %}
    )
    WHERE "row_number" = 1
),

/*
 4. Once we know the latest value of each feature for a given timestamp,
 we can join again the data back to the original "base" dataset
*/
"{{ featureview.name }}__cleaned" AS (
    SELECT "base".*
    FROM "{{ featureview.name }}__base" AS "base"
    INNER JOIN "{{ featureview.name }}__latest"
    USING(
        "{{featureview.name}}__entity_row_unique_id",
        "event_timestamp"
        {% if featureview.created_timestamp_column %}
            ,"created_timestamp"
        {% endif %}
    )
){% if loop.last %}{% else %}, {% endif %}


{% endfor %}
/*
 Joins the outputs of multiple time travel joins to a single table.
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
    FROM "{{ featureview.name }}__cleaned"
) "{{ featureview.name }}__cleaned" USING ("{{featureview.name}}__entity_row_unique_id")
{% endfor %}
"""
