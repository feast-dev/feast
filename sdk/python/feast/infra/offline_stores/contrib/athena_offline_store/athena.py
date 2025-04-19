import contextlib
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import (
    Callable,
    ContextManager,
    Dict,
    Iterator,
    List,
    Literal,
    Optional,
    Tuple,
    Union,
)

import numpy as np
import pandas as pd
import pyarrow
import pyarrow as pa
from pydantic import StrictStr

from feast import OnDemandFeatureView
from feast.data_source import DataSource
from feast.errors import InvalidEntityType
from feast.feature_logging import LoggingConfig, LoggingSource
from feast.feature_view import DUMMY_ENTITY_ID, DUMMY_ENTITY_VAL, FeatureView
from feast.infra.offline_stores import offline_utils
from feast.infra.offline_stores.contrib.athena_offline_store.athena_source import (
    AthenaLoggingDestination,
    AthenaSource,
    SavedDatasetAthenaStorage,
)
from feast.infra.offline_stores.offline_store import (
    OfflineStore,
    RetrievalJob,
    RetrievalMetadata,
)
from feast.infra.offline_stores.offline_utils import get_timestamp_filter_sql
from feast.infra.registry.base_registry import BaseRegistry
from feast.infra.utils import aws_utils
from feast.repo_config import FeastConfigBaseModel, RepoConfig
from feast.saved_dataset import SavedDatasetStorage


class AthenaOfflineStoreConfig(FeastConfigBaseModel):
    """Offline store config for AWS Athena"""

    type: Literal["athena"] = "athena"
    """ Offline store type selector"""

    data_source: StrictStr
    """ athena data source ex) AwsDataCatalog """

    region: StrictStr
    """ Athena's AWS region """

    database: StrictStr
    """ Athena database name """

    workgroup: StrictStr
    """ Athena workgroup name """

    s3_staging_location: StrictStr
    """ S3 path for importing & exporting data to Athena """


class AthenaOfflineStore(OfflineStore):
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
        assert isinstance(config.offline_store, AthenaOfflineStoreConfig)
        assert isinstance(data_source, AthenaSource)

        from_expression = data_source.get_table_query_string(config)

        partition_by_join_key_string = ", ".join(join_key_columns)
        if partition_by_join_key_string != "":
            partition_by_join_key_string = (
                "PARTITION BY " + partition_by_join_key_string
            )
        timestamp_columns = [timestamp_field]
        if created_timestamp_column:
            timestamp_columns.append(created_timestamp_column)
        timestamp_desc_string = " DESC, ".join(timestamp_columns) + " DESC"
        field_string = ", ".join(
            join_key_columns + feature_name_columns + timestamp_columns
        )

        date_partition_column = data_source.date_partition_column

        athena_client = aws_utils.get_athena_data_client(config.offline_store.region)
        s3_resource = aws_utils.get_s3_resource(config.offline_store.region)

        start_date = start_date.astimezone(tz=timezone.utc)
        end_date = end_date.astimezone(tz=timezone.utc)

        query = f"""
            SELECT
                {field_string}
                {f", {repr(DUMMY_ENTITY_VAL)} AS {DUMMY_ENTITY_ID}" if not join_key_columns else ""}
            FROM (
                SELECT {field_string},
                ROW_NUMBER() OVER({partition_by_join_key_string} ORDER BY {timestamp_desc_string}) AS _feast_row
                FROM {from_expression}
                WHERE {timestamp_field} BETWEEN TIMESTAMP '{start_date.strftime("%Y-%m-%d %H:%M:%S")}' AND TIMESTAMP '{end_date.strftime("%Y-%m-%d %H:%M:%S")}'
                {"AND " + date_partition_column + " >= '" + start_date.strftime("%Y-%m-%d") + "' AND " + date_partition_column + " <= '" + end_date.strftime("%Y-%m-%d") + "' " if date_partition_column != "" and date_partition_column is not None else ""}
            )
            WHERE _feast_row = 1
            """
        # When materializing a single feature view, we don't need full feature names. On demand transforms aren't materialized
        return AthenaRetrievalJob(
            query=query,
            athena_client=athena_client,
            s3_resource=s3_resource,
            config=config,
            full_feature_names=False,
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
        assert isinstance(config.offline_store, AthenaOfflineStoreConfig)
        assert isinstance(data_source, AthenaSource)
        from_expression = data_source.get_table_query_string(config)

        timestamp_fields = [timestamp_field]
        if created_timestamp_column:
            timestamp_fields.append(created_timestamp_column)
        field_string = ", ".join(
            join_key_columns + feature_name_columns + timestamp_fields
        )

        athena_client = aws_utils.get_athena_data_client(config.offline_store.region)
        s3_resource = aws_utils.get_s3_resource(config.offline_store.region)

        date_partition_column = data_source.date_partition_column

        start_date_str = None
        if start_date:
            start_date_str = start_date.astimezone(tz=timezone.utc).strftime(
                "%Y-%m-%d %H:%M:%S.%f"
            )[:-3]
        end_date_str = None
        if end_date:
            end_date_str = end_date.astimezone(tz=timezone.utc).strftime(
                "%Y-%m-%d %H:%M:%S.%f"
            )[:-3]

        timestamp_filter = get_timestamp_filter_sql(
            start_date_str,
            end_date_str,
            timestamp_field,
            date_partition_column,
            cast_style="raw",
            quote_fields=False,
        )

        query = f"""
            SELECT {field_string}
            FROM {from_expression}
            WHERE {timestamp_filter}
        """

        return AthenaRetrievalJob(
            query=query,
            athena_client=athena_client,
            s3_resource=s3_resource,
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
        assert isinstance(config.offline_store, AthenaOfflineStoreConfig)
        for fv in feature_views:
            assert isinstance(fv.batch_source, AthenaSource)

        athena_client = aws_utils.get_athena_data_client(config.offline_store.region)
        s3_resource = aws_utils.get_s3_resource(config.offline_store.region)

        # get pandas dataframe consisting of 1 row (LIMIT 1) and generate the schema out of it
        entity_schema = _get_entity_schema(
            entity_df, athena_client, config, s3_resource
        )

        # find timestamp column of entity df.(default = "event_timestamp"). Exception occurs if there are more than two timestamp columns.
        entity_df_event_timestamp_col = (
            offline_utils.infer_event_timestamp_from_entity_df(entity_schema)
        )

        # get min,max of event_timestamp.
        entity_df_event_timestamp_range = _get_entity_df_event_timestamp_range(
            entity_df,
            entity_df_event_timestamp_col,
            athena_client,
            config,
        )

        @contextlib.contextmanager
        def query_generator() -> Iterator[str]:
            table_name = offline_utils.get_temp_entity_table_name()

            _upload_entity_df(entity_df, athena_client, config, s3_resource, table_name)

            expected_join_keys = offline_utils.get_expected_join_keys(
                project, feature_views, registry
            )

            offline_utils.assert_expected_columns_in_entity_df(
                entity_schema, expected_join_keys, entity_df_event_timestamp_col
            )

            # Build a query context containing all information required to template the Athena SQL query
            query_context = offline_utils.get_feature_view_query_context(
                feature_refs,
                feature_views,
                registry,
                project,
                entity_df_event_timestamp_range,
            )

            # Generate the Athena SQL query from the query context
            query = offline_utils.build_point_in_time_query(
                query_context,
                left_table_query_string=table_name,
                entity_df_event_timestamp_col=entity_df_event_timestamp_col,
                entity_df_columns=entity_schema.keys(),
                query_template=MULTIPLE_FEATURE_VIEW_POINT_IN_TIME_JOIN,
                full_feature_names=full_feature_names,
            )

            try:
                yield query
            finally:
                # Always clean up the temp Athena table
                aws_utils.execute_athena_query(
                    athena_client,
                    config.offline_store.data_source,
                    config.offline_store.database,
                    config.offline_store.workgroup,
                    f"DROP TABLE IF EXISTS {config.offline_store.database}.{table_name}",
                )

                bucket = config.offline_store.s3_staging_location.replace(
                    "s3://", ""
                ).split("/", 1)[0]
                aws_utils.delete_s3_directory(
                    s3_resource, bucket, "entity_df/" + table_name + "/"
                )

        return AthenaRetrievalJob(
            query=query_generator,
            athena_client=athena_client,
            s3_resource=s3_resource,
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
        destination = logging_config.destination
        assert isinstance(destination, AthenaLoggingDestination)

        athena_client = aws_utils.get_athena_data_client(config.offline_store.region)
        s3_resource = aws_utils.get_s3_resource(config.offline_store.region)
        if isinstance(data, Path):
            s3_path = f"{config.offline_store.s3_staging_location}/logged_features/{uuid.uuid4()}"
        else:
            s3_path = f"{config.offline_store.s3_staging_location}/logged_features/{uuid.uuid4()}.parquet"

        aws_utils.upload_arrow_table_to_athena(
            table=data,
            athena_client=athena_client,
            data_source=config.offline_store.data_source,
            database=config.offline_store.database,
            workgroup=config.offline_store.workgroup,
            s3_resource=s3_resource,
            s3_path=s3_path,
            table_name=destination.table_name,
            schema=source.get_schema(registry),
            fail_if_exists=False,
        )


class AthenaRetrievalJob(RetrievalJob):
    def __init__(
        self,
        query: Union[str, Callable[[], ContextManager[str]]],
        athena_client,
        s3_resource,
        config: RepoConfig,
        full_feature_names: bool,
        on_demand_feature_views: Optional[List[OnDemandFeatureView]] = None,
        metadata: Optional[RetrievalMetadata] = None,
    ):
        """Initialize AthenaRetrievalJob object.

        Args:
            query: Athena SQL query to execute. Either a string, or a generator function that handles the artifact cleanup.
            athena_client: boto3 athena client
            s3_resource: boto3 s3 resource object
            config: Feast repo config
            full_feature_names: Whether to add the feature view prefixes to the feature names
            on_demand_feature_views (optional): A list of on demand transforms to apply at retrieval time
        """

        if not isinstance(query, str):
            self._query_generator = query
        else:

            @contextlib.contextmanager
            def query_generator() -> Iterator[str]:
                assert isinstance(query, str)
                yield query

            self._query_generator = query_generator
        self._athena_client = athena_client
        self._s3_resource = s3_resource
        self._config = config
        self._full_feature_names = full_feature_names
        self._on_demand_feature_views = on_demand_feature_views or []
        self._metadata = metadata

    @property
    def full_feature_names(self) -> bool:
        return self._full_feature_names

    @property
    def on_demand_feature_views(self) -> List[OnDemandFeatureView]:
        return self._on_demand_feature_views

    def get_temp_s3_path(self) -> str:
        return (
            self._config.offline_store.s3_staging_location
            + "/unload/"
            + str(uuid.uuid4())
        )

    def get_temp_table_dml_header(
        self, temp_table_name: str, temp_external_location: str
    ) -> str:
        temp_table_dml_header = f"""
                                    CREATE TABLE {temp_table_name}
                                    WITH (
                                    external_location = '{temp_external_location}',
                                    format = 'parquet',
                                    write_compression = 'snappy'
                                    )
                                    as
                                """
        return temp_table_dml_header

    def _to_df_internal(self, timeout: Optional[int] = None) -> pd.DataFrame:
        with self._query_generator() as query:
            temp_table_name = "_" + str(uuid.uuid4()).replace("-", "")
            temp_external_location = self.get_temp_s3_path()
            return aws_utils.unload_athena_query_to_df(
                self._athena_client,
                self._config.offline_store.data_source,
                self._config.offline_store.database,
                self._config.offline_store.workgroup,
                self._s3_resource,
                temp_external_location,
                self.get_temp_table_dml_header(temp_table_name, temp_external_location)
                + query,
                temp_table_name,
            )

    def _to_arrow_internal(self, timeout: Optional[int] = None) -> pa.Table:
        with self._query_generator() as query:
            temp_table_name = "_" + str(uuid.uuid4()).replace("-", "")
            temp_external_location = self.get_temp_s3_path()
            return aws_utils.unload_athena_query_to_pa(
                self._athena_client,
                self._config.offline_store.data_source,
                self._config.offline_store.database,
                self._config.offline_store.workgroup,
                self._s3_resource,
                temp_external_location,
                self.get_temp_table_dml_header(temp_table_name, temp_external_location)
                + query,
                temp_table_name,
            )

    @property
    def metadata(self) -> Optional[RetrievalMetadata]:
        return self._metadata

    def persist(
        self,
        storage: SavedDatasetStorage,
        allow_overwrite: Optional[bool] = False,
        timeout: Optional[int] = None,
    ):
        assert isinstance(storage, SavedDatasetAthenaStorage)
        self.to_athena(table_name=storage.athena_options.table)

    def to_athena(self, table_name: str) -> None:
        if self.on_demand_feature_views:
            transformed_df = self.to_df()

            _upload_entity_df(
                transformed_df,
                self._athena_client,
                self._config,
                self._s3_resource,
                table_name,
            )

            return

        with self._query_generator() as query:
            query = f'CREATE TABLE "{table_name}" AS ({query});\n'

            aws_utils.execute_athena_query(
                self._athena_client,
                self._config.offline_store.data_source,
                self._config.offline_store.database,
                self._config.offline_store.workgroup,
                query,
            )


def _upload_entity_df(
    entity_df: Union[pd.DataFrame, str],
    athena_client,
    config: RepoConfig,
    s3_resource,
    table_name: str,
):
    if isinstance(entity_df, pd.DataFrame):
        # If the entity_df is a pandas dataframe, upload it to Athena
        aws_utils.upload_df_to_athena(
            athena_client,
            config.offline_store.data_source,
            config.offline_store.database,
            config.offline_store.workgroup,
            s3_resource,
            f"{config.offline_store.s3_staging_location}/entity_df/{table_name}/{table_name}.parquet",
            table_name,
            entity_df,
        )
    elif isinstance(entity_df, str):
        # If the entity_df is a string (SQL query), create a Athena table out of it
        aws_utils.execute_athena_query(
            athena_client,
            config.offline_store.data_source,
            config.offline_store.database,
            config.offline_store.workgroup,
            f"CREATE TABLE {table_name} AS ({entity_df})",
        )
    else:
        raise InvalidEntityType(type(entity_df))


def _get_entity_schema(
    entity_df: Union[pd.DataFrame, str],
    athena_client,
    config: RepoConfig,
    s3_resource,
) -> Dict[str, np.dtype]:
    if isinstance(entity_df, pd.DataFrame):
        return dict(zip(entity_df.columns, entity_df.dtypes))

    elif isinstance(entity_df, str):
        # get pandas dataframe consisting of 1 row (LIMIT 1) and generate the schema out of it
        entity_df_sample = AthenaRetrievalJob(
            f"SELECT * FROM ({entity_df}) LIMIT 1",
            athena_client,
            s3_resource,
            config,
            full_feature_names=False,
        ).to_df()
        return dict(zip(entity_df_sample.columns, entity_df_sample.dtypes))
    else:
        raise InvalidEntityType(type(entity_df))


def _get_entity_df_event_timestamp_range(
    entity_df: Union[pd.DataFrame, str],
    entity_df_event_timestamp_col: str,
    athena_client,
    config: RepoConfig,
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
        statement_id = aws_utils.execute_athena_query(
            athena_client,
            config.offline_store.data_source,
            config.offline_store.database,
            config.offline_store.workgroup,
            f"SELECT MIN({entity_df_event_timestamp_col}) AS min, MAX({entity_df_event_timestamp_col}) AS max "
            f"FROM ({entity_df})",
        )
        res = aws_utils.get_athena_query_result(athena_client, statement_id)
        entity_df_event_timestamp_range = (
            datetime.strptime(
                res["Rows"][1]["Data"][0]["VarCharValue"], "%Y-%m-%d %H:%M:%S.%f"
            ),
            datetime.strptime(
                res["Rows"][1]["Data"][1]["VarCharValue"], "%Y-%m-%d %H:%M:%S.%f"
            ),
        )
    else:
        raise InvalidEntityType(type(entity_df))

    return entity_df_event_timestamp_range


MULTIPLE_FEATURE_VIEW_POINT_IN_TIME_JOIN = """
/*
 Compute a deterministic hash for the `left_table_query_string` that will be used throughout
 all the logic as the field to GROUP BY the data
*/
WITH entity_dataframe AS (
    SELECT *,
        {{entity_df_event_timestamp_col}} AS entity_timestamp
        {% for featureview in featureviews %}
            {% if featureview.entities %}
            ,(
                {% for entity in featureview.entities %}
                    CAST({{entity}} as VARCHAR) ||
                {% endfor %}
                CAST({{entity_df_event_timestamp_col}} AS VARCHAR)
            ) AS {{featureview.name}}__entity_row_unique_id
            {% else %}
            ,CAST({{entity_df_event_timestamp_col}} AS VARCHAR) AS {{featureview.name}}__entity_row_unique_id
            {% endif %}
        {% endfor %}
    FROM {{ left_table_query_string }}
),

{% for featureview in featureviews %}

{{ featureview.name }}__entity_dataframe AS (
    SELECT
        {{ featureview.entities | join(', ')}}{% if featureview.entities %},{% else %}{% endif %}
        entity_timestamp,
        {{featureview.name}}__entity_row_unique_id
    FROM entity_dataframe
    GROUP BY
        {{ featureview.entities | join(', ')}}{% if featureview.entities %},{% else %}{% endif %}
        entity_timestamp,
        {{featureview.name}}__entity_row_unique_id
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

{{ featureview.name }}__subquery AS (
    SELECT
        {{ featureview.timestamp_field }} as event_timestamp,
        {{ featureview.created_timestamp_column ~ ' as created_timestamp,' if featureview.created_timestamp_column else '' }}
        {{ featureview.entity_selections | join(', ')}}{% if featureview.entity_selections %},{% else %}{% endif %}
        {% for feature in featureview.features %}
            {{ feature }} as {% if full_feature_names %}{{ featureview.name }}__{{featureview.field_mapping.get(feature, feature)}}{% else %}{{ featureview.field_mapping.get(feature, feature) }}{% endif %}{% if loop.last %}{% else %}, {% endif %}
        {% endfor %}
    FROM {{ featureview.table_subquery }}
   WHERE {{ featureview.timestamp_field }} <= from_iso8601_timestamp('{{ featureview.max_event_timestamp }}')
    {% if featureview.date_partition_column != "" and featureview.date_partition_column is not none %}
      AND {{ featureview.date_partition_column }} <= '{{ featureview.max_event_timestamp[:10] }}'
    {% endif %}

    {% if featureview.ttl == 0 %}{% else %}
    AND {{ featureview.timestamp_field }} >= from_iso8601_timestamp('{{ featureview.min_event_timestamp }}')
        {% if featureview.date_partition_column != "" and featureview.date_partition_column is not none %}
          AND {{ featureview.date_partition_column }} >= '{{ featureview.min_event_timestamp[:10] }}'
        {% endif %}
    {% endif %}

),

{{ featureview.name }}__base AS (
    SELECT
        subquery.*,
        entity_dataframe.entity_timestamp,
        entity_dataframe.{{featureview.name}}__entity_row_unique_id
    FROM {{ featureview.name }}__subquery AS subquery
    INNER JOIN {{ featureview.name }}__entity_dataframe AS entity_dataframe
    ON TRUE
        AND subquery.event_timestamp <= entity_dataframe.entity_timestamp

        {% if featureview.ttl == 0 %}{% else %}
        AND subquery.event_timestamp >= entity_dataframe.entity_timestamp - {{ featureview.ttl }} * interval '1' second
        {% endif %}

        {% for entity in featureview.entities %}
        AND subquery.{{ entity }} = entity_dataframe.{{ entity }}
        {% endfor %}
),

/*
 2. If the `created_timestamp_column` has been set, we need to
 deduplicate the data first. This is done by calculating the
 `MAX(created_at_timestamp)` for each event_timestamp.
 We then join the data on the next CTE
*/
{% if featureview.created_timestamp_column %}
{{ featureview.name }}__dedup AS (
    SELECT
        {{featureview.name}}__entity_row_unique_id,
        event_timestamp,
        MAX(created_timestamp) as created_timestamp
    FROM {{ featureview.name }}__base
    GROUP BY {{featureview.name}}__entity_row_unique_id, event_timestamp
),
{% endif %}

/*
 3. The data has been filtered during the first CTE "*__base"
 Thus we only need to compute the latest timestamp of each feature.
*/
{{ featureview.name }}__latest AS (
    SELECT
        event_timestamp,
        {% if featureview.created_timestamp_column %}created_timestamp,{% endif %}
        {{featureview.name}}__entity_row_unique_id
    FROM
    (
        SELECT base.*,
            ROW_NUMBER() OVER(
                PARTITION BY base.{{featureview.name}}__entity_row_unique_id
                ORDER BY base.event_timestamp DESC{% if featureview.created_timestamp_column %},base.created_timestamp DESC{% endif %}
            ) AS row_number
        FROM {{ featureview.name }}__base as base
        {% if featureview.created_timestamp_column %}
            INNER JOIN {{ featureview.name }}__dedup as dedup
            ON TRUE
            AND base.{{featureview.name}}__entity_row_unique_id = dedup.{{featureview.name}}__entity_row_unique_id
            AND base.event_timestamp = dedup.event_timestamp
            AND base.created_timestamp = dedup.created_timestamp
        {% endif %}
    )
    WHERE row_number = 1
),

/*
 4. Once we know the latest value of each feature for a given timestamp,
 we can join again the data back to the original "base" dataset
*/
{{ featureview.name }}__cleaned AS (
    SELECT base.*
    FROM {{ featureview.name }}__base as base
    INNER JOIN {{ featureview.name }}__latest as latest
    ON TRUE
        AND base.{{featureview.name}}__entity_row_unique_id = latest.{{featureview.name}}__entity_row_unique_id
        AND base.event_timestamp = latest.event_timestamp
        {% if featureview.created_timestamp_column %}
        AND base.created_timestamp = latest.created_timestamp
        {% endif %}
){% if loop.last %}{% else %}, {% endif %}


{% endfor %}
/*
 Joins the outputs of multiple time travel joins to a single table.
 The entity_dataframe dataset being our source of truth here.
 */

SELECT {{ final_output_feature_names | join(', ')}}
FROM entity_dataframe as entity_df
{% for featureview in featureviews %}
LEFT JOIN (
    SELECT
        {{featureview.name}}__entity_row_unique_id
        {% for feature in featureview.features %}
            ,{% if full_feature_names %}{{ featureview.name }}__{{featureview.field_mapping.get(feature, feature)}}{% else %}{{ featureview.field_mapping.get(feature, feature) }}{% endif %}
        {% endfor %}
    FROM {{ featureview.name }}__cleaned
) as cleaned
ON TRUE
AND entity_df.{{featureview.name}}__entity_row_unique_id = cleaned.{{featureview.name}}__entity_row_unique_id
{% endfor %}
"""
