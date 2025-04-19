import contextlib
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import (
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
)

import numpy as np
import pandas as pd
import pyarrow
import pyarrow as pa
from dateutil import parser
from pydantic import StrictStr, model_validator

from feast import OnDemandFeatureView, RedshiftSource
from feast.data_source import DataSource
from feast.errors import InvalidEntityType
from feast.feature_logging import LoggingConfig, LoggingSource
from feast.feature_view import DUMMY_ENTITY_ID, DUMMY_ENTITY_VAL, FeatureView
from feast.infra.offline_stores import offline_utils
from feast.infra.offline_stores.offline_store import (
    OfflineStore,
    RetrievalJob,
    RetrievalMetadata,
)
from feast.infra.offline_stores.offline_utils import get_timestamp_filter_sql
from feast.infra.offline_stores.redshift_source import (
    RedshiftLoggingDestination,
    SavedDatasetRedshiftStorage,
)
from feast.infra.registry.base_registry import BaseRegistry
from feast.infra.utils import aws_utils
from feast.repo_config import FeastConfigBaseModel, RepoConfig
from feast.saved_dataset import SavedDatasetStorage


class RedshiftOfflineStoreConfig(FeastConfigBaseModel):
    """Offline store config for AWS Redshift"""

    type: Literal["redshift"] = "redshift"
    """ Offline store type selector"""

    cluster_id: Optional[StrictStr] = None
    """ Redshift cluster identifier, for provisioned clusters """

    user: Optional[StrictStr] = None
    """ Redshift user name, only required for provisioned clusters """

    workgroup: Optional[StrictStr] = None
    """ Redshift workgroup identifier, for serverless """

    region: StrictStr
    """ Redshift cluster's AWS region """

    database: StrictStr
    """ Redshift database name """

    s3_staging_location: StrictStr
    """ S3 path for importing & exporting data to Redshift """

    iam_role: StrictStr
    """ IAM Role for Redshift, granting it access to S3 """

    @model_validator(mode="after")
    def require_cluster_and_user_or_workgroup(self):
        """
        Provisioned Redshift clusters:  Require cluster_id and user, ignore workgroup
        Serverless Redshift:  Require workgroup, ignore cluster_id and user
        """
        cluster_id, user, workgroup = (
            self.cluster_id,
            self.user,
            self.workgroup,
        )
        if not (cluster_id and user) and not workgroup:
            raise ValueError(
                "please specify either cluster_id & user if using provisioned clusters, or workgroup if using serverless"
            )
        elif cluster_id and workgroup:
            raise ValueError("cannot specify both cluster_id and workgroup")

        return self


class RedshiftOfflineStore(OfflineStore):
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
        assert isinstance(data_source, RedshiftSource)
        assert isinstance(config.offline_store, RedshiftOfflineStoreConfig)

        from_expression = data_source.get_table_query_string()

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

        redshift_client = aws_utils.get_redshift_data_client(
            config.offline_store.region
        )
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
                WHERE {timestamp_field} BETWEEN TIMESTAMP '{start_date}' AND TIMESTAMP '{end_date}'
            )
            WHERE _feast_row = 1
            """
        # When materializing a single feature view, we don't need full feature names. On demand transforms aren't materialized
        return RedshiftRetrievalJob(
            query=query,
            redshift_client=redshift_client,
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
        assert isinstance(config.offline_store, RedshiftOfflineStoreConfig)
        assert isinstance(data_source, RedshiftSource)
        from_expression = data_source.get_table_query_string()

        timestamp_fields = [timestamp_field]
        if created_timestamp_column:
            timestamp_fields.append(created_timestamp_column)
        field_string = ", ".join(
            join_key_columns + feature_name_columns + timestamp_fields
        )

        redshift_client = aws_utils.get_redshift_data_client(
            config.offline_store.region
        )
        s3_resource = aws_utils.get_s3_resource(config.offline_store.region)

        timestamp_filter = get_timestamp_filter_sql(
            start_date,
            end_date,
            timestamp_field,
            tz=timezone.utc,
        )

        query = f"""
            SELECT {field_string}
            FROM {from_expression}
            WHERE {timestamp_filter}
        """

        return RedshiftRetrievalJob(
            query=query,
            redshift_client=redshift_client,
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
        assert isinstance(config.offline_store, RedshiftOfflineStoreConfig)
        for fv in feature_views:
            assert isinstance(fv.batch_source, RedshiftSource)

        redshift_client = aws_utils.get_redshift_data_client(
            config.offline_store.region
        )
        s3_resource = aws_utils.get_s3_resource(config.offline_store.region)

        entity_schema = _get_entity_schema(
            entity_df, redshift_client, config, s3_resource
        )

        entity_df_event_timestamp_col = (
            offline_utils.infer_event_timestamp_from_entity_df(entity_schema)
        )

        entity_df_event_timestamp_range = _get_entity_df_event_timestamp_range(
            entity_df,
            entity_df_event_timestamp_col,
            redshift_client,
            config,
        )

        @contextlib.contextmanager
        def query_generator() -> Iterator[str]:
            table_name = offline_utils.get_temp_entity_table_name()

            _upload_entity_df(
                entity_df, redshift_client, config, s3_resource, table_name
            )

            expected_join_keys = offline_utils.get_expected_join_keys(
                project, feature_views, registry
            )

            offline_utils.assert_expected_columns_in_entity_df(
                entity_schema, expected_join_keys, entity_df_event_timestamp_col
            )

            # Build a query context containing all information required to template the Redshift SQL query
            query_context = offline_utils.get_feature_view_query_context(
                feature_refs,
                feature_views,
                registry,
                project,
                entity_df_event_timestamp_range,
            )

            # Generate the Redshift SQL query from the query context
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
                # Always clean up the uploaded Redshift table
                aws_utils.execute_redshift_statement(
                    redshift_client,
                    config.offline_store.cluster_id,
                    config.offline_store.workgroup,
                    config.offline_store.database,
                    config.offline_store.user,
                    f"DROP TABLE IF EXISTS {table_name}",
                )

        return RedshiftRetrievalJob(
            query=query_generator,
            redshift_client=redshift_client,
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
        assert isinstance(destination, RedshiftLoggingDestination)

        redshift_client = aws_utils.get_redshift_data_client(
            config.offline_store.region
        )
        s3_resource = aws_utils.get_s3_resource(config.offline_store.region)
        if isinstance(data, Path):
            s3_path = f"{config.offline_store.s3_staging_location}/logged_features/{uuid.uuid4()}"
        else:
            s3_path = f"{config.offline_store.s3_staging_location}/logged_features/{uuid.uuid4()}.parquet"

        aws_utils.upload_arrow_table_to_redshift(
            table=data,
            redshift_data_client=redshift_client,
            cluster_id=config.offline_store.cluster_id,
            workgroup=config.offline_store.workgroup,
            database=config.offline_store.database,
            user=config.offline_store.user,
            s3_resource=s3_resource,
            s3_path=s3_path,
            iam_role=config.offline_store.iam_role,
            table_name=destination.table_name,
            schema=source.get_schema(registry),
            fail_if_exists=False,
        )

    @staticmethod
    def offline_write_batch(
        config: RepoConfig,
        feature_view: FeatureView,
        table: pyarrow.Table,
        progress: Optional[Callable[[int], Any]],
    ):
        assert isinstance(config.offline_store, RedshiftOfflineStoreConfig)
        assert isinstance(feature_view.batch_source, RedshiftSource)

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

        redshift_options = feature_view.batch_source.redshift_options
        redshift_client = aws_utils.get_redshift_data_client(
            config.offline_store.region
        )
        s3_resource = aws_utils.get_s3_resource(config.offline_store.region)

        aws_utils.upload_arrow_table_to_redshift(
            table=table,
            redshift_data_client=redshift_client,
            cluster_id=config.offline_store.cluster_id,
            workgroup=config.offline_store.workgroup,
            database=redshift_options.database
            # Users can define database in the source if needed but it's not required.
            or config.offline_store.database,
            user=config.offline_store.user,
            s3_resource=s3_resource,
            s3_path=f"{config.offline_store.s3_staging_location}/push/{uuid.uuid4()}.parquet",
            iam_role=config.offline_store.iam_role,
            table_name=redshift_options.fully_qualified_table_name,
            schema=pa_schema,
            fail_if_exists=False,
        )


class RedshiftRetrievalJob(RetrievalJob):
    def __init__(
        self,
        query: Union[str, Callable[[], ContextManager[str]]],
        redshift_client,
        s3_resource,
        config: RepoConfig,
        full_feature_names: bool,
        on_demand_feature_views: Optional[List[OnDemandFeatureView]] = None,
        metadata: Optional[RetrievalMetadata] = None,
    ):
        """Initialize RedshiftRetrievalJob object.

        Args:
            query: Redshift SQL query to execute. Either a string, or a generator function that handles the artifact cleanup.
            redshift_client: boto3 redshift-data client
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
        self._redshift_client = redshift_client
        self._s3_resource = s3_resource
        self._config = config
        self._s3_path = (
            self._config.offline_store.s3_staging_location
            + "/unload/"
            + str(uuid.uuid4())
        )
        self._full_feature_names = full_feature_names
        self._on_demand_feature_views = on_demand_feature_views or []
        self._metadata = metadata

    @property
    def full_feature_names(self) -> bool:
        return self._full_feature_names

    @property
    def on_demand_feature_views(self) -> List[OnDemandFeatureView]:
        return self._on_demand_feature_views

    def _to_df_internal(self, timeout: Optional[int] = None) -> pd.DataFrame:
        with self._query_generator() as query:
            return aws_utils.unload_redshift_query_to_df(
                self._redshift_client,
                self._config.offline_store.cluster_id,
                self._config.offline_store.workgroup,
                self._config.offline_store.database,
                self._config.offline_store.user,
                self._s3_resource,
                self._s3_path,
                self._config.offline_store.iam_role,
                query,
            )

    def _to_arrow_internal(self, timeout: Optional[int] = None) -> pa.Table:
        with self._query_generator() as query:
            return aws_utils.unload_redshift_query_to_pa(
                self._redshift_client,
                self._config.offline_store.cluster_id,
                self._config.offline_store.workgroup,
                self._config.offline_store.database,
                self._config.offline_store.user,
                self._s3_resource,
                self._s3_path,
                self._config.offline_store.iam_role,
                query,
            )

    def to_s3(self) -> str:
        """Export dataset to S3 in Parquet format and return path"""
        if self.on_demand_feature_views:
            transformed_df = self.to_df()
            aws_utils.upload_df_to_s3(self._s3_resource, self._s3_path, transformed_df)
            return self._s3_path

        with self._query_generator() as query:
            aws_utils.execute_redshift_query_and_unload_to_s3(
                self._redshift_client,
                self._config.offline_store.cluster_id,
                self._config.offline_store.workgroup,
                self._config.offline_store.database,
                self._config.offline_store.user,
                self._s3_path,
                self._config.offline_store.iam_role,
                query,
            )
            return self._s3_path

    def to_redshift(self, table_name: str) -> None:
        """Save dataset as a new Redshift table"""
        if self.on_demand_feature_views:
            transformed_df = self.to_df()
            aws_utils.upload_df_to_redshift(
                self._redshift_client,
                self._config.offline_store.cluster_id,
                self._config.offline_store.workgroup,
                self._config.offline_store.database,
                self._config.offline_store.user,
                self._s3_resource,
                f"{self._config.offline_store.s3_staging_location}/features_df/{table_name}.parquet",
                self._config.offline_store.iam_role,
                table_name,
                transformed_df,
            )
            return

        with self._query_generator() as query:
            query = f'CREATE TABLE "{table_name}" AS ({query});\n'

            aws_utils.execute_redshift_statement(
                self._redshift_client,
                self._config.offline_store.cluster_id,
                self._config.offline_store.workgroup,
                self._config.offline_store.database,
                self._config.offline_store.user,
                query,
            )

    def persist(
        self,
        storage: SavedDatasetStorage,
        allow_overwrite: Optional[bool] = False,
        timeout: Optional[int] = None,
    ):
        assert isinstance(storage, SavedDatasetRedshiftStorage)
        self.to_redshift(table_name=storage.redshift_options.table)

    @property
    def metadata(self) -> Optional[RetrievalMetadata]:
        return self._metadata

    def supports_remote_storage_export(self) -> bool:
        return True

    def to_remote_storage(self) -> List[str]:
        path = self.to_s3()
        return aws_utils.list_s3_files(self._config.offline_store.region, path)


def _upload_entity_df(
    entity_df: Union[pd.DataFrame, str],
    redshift_client,
    config: RepoConfig,
    s3_resource,
    table_name: str,
):
    if isinstance(entity_df, pd.DataFrame):
        # If the entity_df is a pandas dataframe, upload it to Redshift
        aws_utils.upload_df_to_redshift(
            redshift_client,
            config.offline_store.cluster_id,
            config.offline_store.workgroup,
            config.offline_store.database,
            config.offline_store.user,
            s3_resource,
            f"{config.offline_store.s3_staging_location}/entity_df/{table_name}.parquet",
            config.offline_store.iam_role,
            table_name,
            entity_df,
        )
    elif isinstance(entity_df, str):
        # If the entity_df is a string (SQL query), create a Redshift table out of it
        aws_utils.execute_redshift_statement(
            redshift_client,
            config.offline_store.cluster_id,
            config.offline_store.workgroup,
            config.offline_store.database,
            config.offline_store.user,
            f"CREATE TABLE {table_name} AS ({entity_df})",
        )
    else:
        raise InvalidEntityType(type(entity_df))


def _get_entity_schema(
    entity_df: Union[pd.DataFrame, str],
    redshift_client,
    config: RepoConfig,
    s3_resource,
) -> Dict[str, np.dtype]:
    if isinstance(entity_df, pd.DataFrame):
        return dict(zip(entity_df.columns, entity_df.dtypes))

    elif isinstance(entity_df, str):
        # get pandas dataframe consisting of 1 row (LIMIT 1) and generate the schema out of it
        entity_df_sample = RedshiftRetrievalJob(
            f"SELECT * FROM ({entity_df}) LIMIT 1",
            redshift_client,
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
    redshift_client,
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
        statement_id = aws_utils.execute_redshift_statement(
            redshift_client,
            config.offline_store.cluster_id,
            config.offline_store.workgroup,
            config.offline_store.database,
            config.offline_store.user,
            f"SELECT MIN({entity_df_event_timestamp_col}) AS min, MAX({entity_df_event_timestamp_col}) AS max "
            f"FROM ({entity_df})",
        )
        res = aws_utils.get_redshift_statement_result(redshift_client, statement_id)[
            "Records"
        ][0]
        entity_df_event_timestamp_range = (
            parser.parse(res[0]["stringValue"]),
            parser.parse(res[1]["stringValue"]),
        )
    else:
        raise InvalidEntityType(type(entity_df))

    return entity_df_event_timestamp_range


# This query is based on sdk/python/feast/infra/offline_stores/bigquery.py:MULTIPLE_FEATURE_VIEW_POINT_IN_TIME_JOIN
# There are couple of changes from BigQuery:
# 1. Use VARCHAR instead of STRING type
# 2. Use "t - x * interval '1' second" instead of "Timestamp_sub(...)"
# 3. Replace `SELECT * EXCEPT (...)` with `SELECT *`, because `EXCEPT` is not supported by Redshift.
#    Instead, we drop the column later after creating the table out of the query.
# We need to keep this query in sync with BigQuery.

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
    WHERE {{ featureview.timestamp_field }} <= '{{ featureview.max_event_timestamp }}'
    {% if featureview.ttl == 0 %}{% else %}
    AND {{ featureview.timestamp_field }} >= '{{ featureview.min_event_timestamp }}'
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
        SELECT *,
            ROW_NUMBER() OVER(
                PARTITION BY {{featureview.name}}__entity_row_unique_id
                ORDER BY event_timestamp DESC{% if featureview.created_timestamp_column %},created_timestamp DESC{% endif %}
            ) AS row_number
        FROM {{ featureview.name }}__base
        {% if featureview.created_timestamp_column %}
            INNER JOIN {{ featureview.name }}__dedup
            USING ({{featureview.name}}__entity_row_unique_id, event_timestamp, created_timestamp)
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
    INNER JOIN {{ featureview.name }}__latest
    USING(
        {{featureview.name}}__entity_row_unique_id,
        event_timestamp
        {% if featureview.created_timestamp_column %}
            ,created_timestamp
        {% endif %}
    )
){% if loop.last %}{% else %}, {% endif %}


{% endfor %}
/*
 Joins the outputs of multiple time travel joins to a single table.
 The entity_dataframe dataset being our source of truth here.
 */

SELECT {{ final_output_feature_names | join(', ')}}
FROM entity_dataframe
{% for featureview in featureviews %}
LEFT JOIN (
    SELECT
        {{featureview.name}}__entity_row_unique_id
        {% for feature in featureview.features %}
            ,{% if full_feature_names %}{{ featureview.name }}__{{featureview.field_mapping.get(feature, feature)}}{% else %}{{ featureview.field_mapping.get(feature, feature) }}{% endif %}
        {% endfor %}
    FROM {{ featureview.name }}__cleaned
) USING ({{featureview.name}}__entity_row_unique_id)
{% endfor %}
"""
