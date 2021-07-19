import uuid
from datetime import datetime
from typing import Callable, Dict, Iterable, List, Optional, Tuple, Union

import pandas as pd
import pyarrow as pa
from pydantic import StrictStr
from pydantic.typing import Literal

from feast import type_map
from feast.data_source import DataSource
from feast.errors import DataSourceNotFoundException, RedshiftCredentialsError
from feast.feature_view import FeatureView
from feast.infra.offline_stores.offline_store import OfflineStore, RetrievalJob
from feast.infra.utils import aws_utils, common_utils
from feast.protos.feast.core.DataSource_pb2 import DataSource as DataSourceProto
from feast.registry import Registry
from feast.repo_config import FeastConfigBaseModel, RepoConfig
from feast.value_type import ValueType


class RedshiftOfflineStoreConfig(FeastConfigBaseModel):
    """ Offline store config for AWS Redshift """

    type: Literal["redshift"] = "redshift"
    """ Offline store type selector"""

    cluster_id: StrictStr
    """ Redshift cluster identifier """

    region: StrictStr
    """ Redshift cluster's AWS region """

    user: StrictStr
    """ Redshift user name """

    database: StrictStr
    """ Redshift database name """

    s3_staging_location: StrictStr
    """ S3 path for importing & exporting data to Redshift """

    iam_role: StrictStr
    """ IAM Role for Redshift, granting it access to S3 """


class RedshiftOfflineStore(OfflineStore):
    @staticmethod
    def pull_latest_from_table_or_query(
        config: RepoConfig,
        data_source: DataSource,
        join_key_columns: List[str],
        feature_name_columns: List[str],
        event_timestamp_column: str,
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
        timestamp_columns = [event_timestamp_column]
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

        query = f"""
            SELECT {field_string}
            FROM (
                SELECT {field_string},
                ROW_NUMBER() OVER({partition_by_join_key_string} ORDER BY {timestamp_desc_string}) AS _feast_row
                FROM {from_expression}
                WHERE {event_timestamp_column} BETWEEN TIMESTAMP '{start_date}' AND TIMESTAMP '{end_date}'
            )
            WHERE _feast_row = 1
            """
        return RedshiftRetrievalJob(
            query=query,
            redshift_client=redshift_client,
            s3_resource=s3_resource,
            config=config,
        )

    @staticmethod
    def get_historical_features(
        config: RepoConfig,
        feature_views: List[FeatureView],
        feature_refs: List[str],
        entity_df: Union[pd.DataFrame, str],
        registry: Registry,
        project: str,
        full_feature_names: bool = False,
    ) -> RetrievalJob:
        assert isinstance(config.offline_store, RedshiftOfflineStoreConfig)

        redshift_client = aws_utils.get_redshift_data_client(
            config.offline_store.region
        )
        s3_resource = aws_utils.get_s3_resource(config.offline_store.region)

        # Generate random table name for uploading the entity dataframe
        table_name = "feast_entity_df_" + uuid.uuid4().hex

        aws_utils.upload_df_to_redshift(
            redshift_client,
            config.offline_store.cluster_id,
            config.offline_store.database,
            config.offline_store.user,
            s3_resource,
            f"{config.offline_store.s3_staging_location}/entity_df/{table_name}.parquet",
            config.offline_store.iam_role,
            table_name,
            entity_df,
        )

        entity_df_event_timestamp_col = common_utils.infer_event_timestamp_from_entity_df(
            entity_df
        )

        expected_join_keys = common_utils.get_expected_join_keys(
            project, feature_views, registry
        )

        common_utils.assert_expected_columns_in_entity_df(
            entity_df, expected_join_keys, entity_df_event_timestamp_col
        )

        # Build a query context containing all information required to template the BigQuery SQL query
        query_context = common_utils.get_feature_view_query_context(
            feature_refs, feature_views, registry, project,
        )

        # Infer min and max timestamps from entity_df to limit data read in BigQuery SQL query
        min_timestamp, max_timestamp = common_utils.get_entity_df_timestamp_bounds(
            entity_df, entity_df_event_timestamp_col
        )

        # Generate the BigQuery SQL query from the query context
        query = common_utils.build_point_in_time_query(
            query_context,
            min_timestamp=min_timestamp,
            max_timestamp=max_timestamp,
            left_table_query_string=table_name,
            entity_df_event_timestamp_col=entity_df_event_timestamp_col,
            query_template=MULTIPLE_FEATURE_VIEW_POINT_IN_TIME_JOIN,
            full_feature_names=full_feature_names,
        )

        return RedshiftRetrievalJob(
            query=query,
            redshift_client=redshift_client,
            s3_resource=s3_resource,
            config=config,
            drop_columns=["entity_row_unique_id"],
        )


class RedshiftRetrievalJob(RetrievalJob):
    def __init__(
        self,
        query: str,
        redshift_client,
        s3_resource,
        config: RepoConfig,
        drop_columns: Optional[List[str]] = None,
    ):
        """Initialize RedshiftRetrievalJob object.

        Args:
            query: Redshift SQL query to execute.
            redshift_client: boto3 redshift-data client
            s3_resource: boto3 s3 resource object
            config: Feast repo config
            drop_columns: Optionally a list of columns to drop before unloading to S3.
                          This is a convenient field, since "SELECT ... EXCEPT col" isn't supported in Redshift.
        """
        self.query = query
        self._redshift_client = redshift_client
        self._s3_resource = s3_resource
        self._config = config
        self._s3_path = (
            self._config.offline_store.s3_staging_location
            + "/unload/"
            + str(uuid.uuid4())
        )
        self._drop_columns = drop_columns

    def to_df(self) -> pd.DataFrame:
        return aws_utils.unload_redshift_query_to_df(
            self._redshift_client,
            self._config.offline_store.cluster_id,
            self._config.offline_store.database,
            self._config.offline_store.user,
            self._s3_resource,
            self._s3_path,
            self._config.offline_store.iam_role,
            self.query,
            self._drop_columns,
        )

    def to_arrow(self) -> pa.Table:
        return aws_utils.unload_redshift_query_to_pa(
            self._redshift_client,
            self._config.offline_store.cluster_id,
            self._config.offline_store.database,
            self._config.offline_store.user,
            self._s3_resource,
            self._s3_path,
            self._config.offline_store.iam_role,
            self.query,
            self._drop_columns,
        )

    def to_s3(self) -> str:
        """ Export dataset to S3 in Parquet format and return path """
        aws_utils.execute_redshift_query_and_unload_to_s3(
            self._redshift_client,
            self._config.offline_store.cluster_id,
            self._config.offline_store.database,
            self._config.offline_store.user,
            self._s3_path,
            self._config.offline_store.iam_role,
            self.query,
            self._drop_columns,
        )
        return self._s3_path

    def to_redshift(self, table_name: str) -> None:
        """ Save dataset as a new Redshift table """
        query = f'CREATE TABLE "{table_name}" AS ({self.query});\n'
        if self._drop_columns is not None:
            for column in self._drop_columns:
                query += f"ALTER TABLE {table_name} DROP COLUMN {column};\n"

        aws_utils.execute_redshift_statement(
            self._redshift_client,
            self._config.offline_store.cluster_id,
            self._config.offline_store.database,
            self._config.offline_store.user,
            query,
        )


# This is based on sdk/python/feast/infra/offline_stores/bigquery.py:MULTIPLE_FEATURE_VIEW_POINT_IN_TIME_JOIN
# There are couple of changes from BigQuery:
# 1. Use VARCHAR instead of STRING type
# 2. Use DATEADD(...) instead of Timestamp_sub(...)
# 3. Replace `SELECT * EXCEPT (...)` with `SELECT *`, because `EXCEPT` is not supported by Redshift.
#    Instead, we drop the column later after creating the table out of the query.
# We need to keep this query in sync with BigQuery.

MULTIPLE_FEATURE_VIEW_POINT_IN_TIME_JOIN = """
/*
 Compute a deterministic hash for the `left_table_query_string` that will be used throughout
 all the logic as the field to GROUP BY the data
*/
WITH entity_dataframe AS (
    SELECT
        *,
        CONCAT(
            {% for entity_key in unique_entity_keys %}
                CAST({{entity_key}} AS VARCHAR),
            {% endfor %}
            CAST({{entity_df_event_timestamp_col}} AS VARCHAR)
        ) AS entity_row_unique_id
    FROM {{ left_table_query_string }}
),

{% for featureview in featureviews %}

/*
 This query template performs the point-in-time correctness join for a single feature set table
 to the provided entity table.

 1. We first join the current feature_view to the entity dataframe that has been passed.
 This JOIN has the following logic:
    - For each row of the entity dataframe, only keep the rows where the `event_timestamp_column`
    is less than the one provided in the entity dataframe
    - If there a TTL for the current feature_view, also keep the rows where the `event_timestamp_column`
    is higher the the one provided minus the TTL
    - For each row, Join on the entity key and retrieve the `entity_row_unique_id` that has been
    computed previously

 The output of this CTE will contain all the necessary information and already filtered out most
 of the data that is not relevant.
*/

{{ featureview.name }}__subquery AS (
    SELECT
        {{ featureview.event_timestamp_column }} as event_timestamp,
        {{ featureview.created_timestamp_column ~ ' as created_timestamp,' if featureview.created_timestamp_column else '' }}
        {{ featureview.entity_selections | join(', ')}},
        {% for feature in featureview.features %}
            {{ feature }} as {% if full_feature_names %}{{ featureview.name }}__{{feature}}{% else %}{{ feature }}{% endif %}{% if loop.last %}{% else %}, {% endif %}
        {% endfor %}
    FROM {{ featureview.table_subquery }}
    WHERE {{ featureview.event_timestamp_column }} <= '{{max_timestamp}}'
    {% if featureview.ttl == 0 %}{% else %}
    AND {{ featureview.event_timestamp_column }} >= DATEADD(second, {{ -featureview.ttl }} ,'{{min_timestamp}}')
    {% endif %}
),

{{ featureview.name }}__base AS (
    SELECT
        subquery.*,
        entity_dataframe.{{entity_df_event_timestamp_col}} AS entity_timestamp,
        entity_dataframe.entity_row_unique_id
    FROM {{ featureview.name }}__subquery AS subquery
    INNER JOIN entity_dataframe
    ON TRUE
        AND subquery.event_timestamp <= entity_dataframe.{{entity_df_event_timestamp_col}}

        {% if featureview.ttl == 0 %}{% else %}
        AND subquery.event_timestamp >= DATEADD(second, {{ -featureview.ttl }}, entity_dataframe.{{entity_df_event_timestamp_col}})
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
        entity_row_unique_id,
        event_timestamp,
        MAX(created_timestamp) as created_timestamp
    FROM {{ featureview.name }}__base
    GROUP BY entity_row_unique_id, event_timestamp
),
{% endif %}

/*
 3. The data has been filtered during the first CTE "*__base"
 Thus we only need to compute the latest timestamp of each feature.
*/
{{ featureview.name }}__latest AS (
    SELECT
        entity_row_unique_id,
        MAX(event_timestamp) AS event_timestamp
        {% if featureview.created_timestamp_column %}
            ,ANY_VALUE(created_timestamp) AS created_timestamp
        {% endif %}

    FROM {{ featureview.name }}__base
    {% if featureview.created_timestamp_column %}
        INNER JOIN {{ featureview.name }}__dedup
        USING (entity_row_unique_id, event_timestamp, created_timestamp)
    {% endif %}

    GROUP BY entity_row_unique_id
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
        entity_row_unique_id,
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

SELECT *
FROM entity_dataframe
{% for featureview in featureviews %}
LEFT JOIN (
    SELECT
        entity_row_unique_id
        {% for feature in featureview.features %}
            ,{% if full_feature_names %}{{ featureview.name }}__{{feature}}{% else %}{{ feature }}{% endif %}
        {% endfor %}
    FROM {{ featureview.name }}__cleaned
) USING (entity_row_unique_id)
{% endfor %}
"""


class RedshiftSource(DataSource):
    def __init__(
        self,
        event_timestamp_column: Optional[str] = "",
        table: Optional[str] = None,
        created_timestamp_column: Optional[str] = "",
        field_mapping: Optional[Dict[str, str]] = None,
        date_partition_column: Optional[str] = "",
        query: Optional[str] = None,
    ):
        super().__init__(
            event_timestamp_column,
            created_timestamp_column,
            field_mapping,
            date_partition_column,
        )

        self._redshift_options = RedshiftOptions(table=table, query=query)

    @staticmethod
    def from_proto(data_source: DataSourceProto):
        return RedshiftSource(
            field_mapping=dict(data_source.field_mapping),
            table=data_source.redshift_options.table,
            event_timestamp_column=data_source.event_timestamp_column,
            created_timestamp_column=data_source.created_timestamp_column,
            date_partition_column=data_source.date_partition_column,
            query=data_source.redshift_options.query,
        )

    def __eq__(self, other):
        if not isinstance(other, RedshiftSource):
            raise TypeError(
                "Comparisons should only involve RedshiftSource class objects."
            )

        return (
            self.redshift_options.table == other.redshift_options.table
            and self.redshift_options.query == other.redshift_options.query
            and self.event_timestamp_column == other.event_timestamp_column
            and self.created_timestamp_column == other.created_timestamp_column
            and self.field_mapping == other.field_mapping
        )

    @property
    def table(self):
        return self._redshift_options.table

    @property
    def query(self):
        return self._redshift_options.query

    @property
    def redshift_options(self):
        """
        Returns the Redshift options of this data source
        """
        return self._redshift_options

    @redshift_options.setter
    def redshift_options(self, _redshift_options):
        """
        Sets the Redshift options of this data source
        """
        self._redshift_options = _redshift_options

    def to_proto(self) -> DataSourceProto:
        data_source_proto = DataSourceProto(
            type=DataSourceProto.BATCH_REDSHIFT,
            field_mapping=self.field_mapping,
            redshift_options=self.redshift_options.to_proto(),
        )

        data_source_proto.event_timestamp_column = self.event_timestamp_column
        data_source_proto.created_timestamp_column = self.created_timestamp_column
        data_source_proto.date_partition_column = self.date_partition_column

        return data_source_proto

    def validate(self, config: RepoConfig):
        # As long as the query gets successfully executed, or the table exists,
        # the data source is validated. We don't need the results though.
        # TODO: uncomment this
        # self.get_table_column_names_and_types(config)
        print("Validate", self.get_table_column_names_and_types(config))

    def get_table_query_string(self) -> str:
        """Returns a string that can directly be used to reference this table in SQL"""
        if self.table:
            return f'"{self.table}"'
        else:
            return f"({self.query})"

    @staticmethod
    def source_datatype_to_feast_value_type() -> Callable[[str], ValueType]:
        return type_map.redshift_to_feast_value_type

    def get_table_column_names_and_types(
        self, config: RepoConfig
    ) -> Iterable[Tuple[str, str]]:
        from botocore.exceptions import ClientError

        from feast.infra.offline_stores.redshift import RedshiftOfflineStoreConfig
        from feast.infra.utils import aws_utils

        assert isinstance(config.offline_store, RedshiftOfflineStoreConfig)

        client = aws_utils.get_redshift_data_client(config.offline_store.region)

        if self.table is not None:
            try:
                table = client.describe_table(
                    ClusterIdentifier=config.offline_store.cluster_id,
                    Database=config.offline_store.database,
                    DbUser=config.offline_store.user,
                    Table=self.table,
                )
            except ClientError as e:
                if e.response["Error"]["Code"] == "ValidationException":
                    raise RedshiftCredentialsError() from e
                raise

            # The API returns valid JSON with empty column list when the table doesn't exist
            if len(table["ColumnList"]) == 0:
                raise DataSourceNotFoundException(self.table)

            columns = table["ColumnList"]
        else:
            statement_id = aws_utils.execute_redshift_statement(
                client,
                config.offline_store.cluster_id,
                config.offline_store.database,
                config.offline_store.user,
                f"SELECT * FROM ({self.query}) LIMIT 1",
            )
            columns = aws_utils.get_redshift_statement_result(client, statement_id)[
                "ColumnMetadata"
            ]

        return [(column["name"], column["typeName"].upper()) for column in columns]


class RedshiftOptions:
    """
    DataSource Redshift options used to source features from Redshift query
    """

    def __init__(self, table: Optional[str], query: Optional[str]):
        self._table = table
        self._query = query

    @property
    def query(self):
        """
        Returns the Redshift SQL query referenced by this source
        """
        return self._query

    @query.setter
    def query(self, query):
        """
        Sets the Redshift SQL query referenced by this source
        """
        self._query = query

    @property
    def table(self):
        """
        Returns the table name of this Redshift table
        """
        return self._table

    @table.setter
    def table(self, table_name):
        """
        Sets the table ref of this Redshift table
        """
        self._table = table_name

    @classmethod
    def from_proto(cls, redshift_options_proto: DataSourceProto.RedshiftOptions):
        """
        Creates a RedshiftOptions from a protobuf representation of a Redshift option

        Args:
            redshift_options_proto: A protobuf representation of a DataSource

        Returns:
            Returns a RedshiftOptions object based on the redshift_options protobuf
        """

        redshift_options = cls(
            table=redshift_options_proto.table, query=redshift_options_proto.query,
        )

        return redshift_options

    def to_proto(self) -> DataSourceProto.RedshiftOptions:
        """
        Converts an RedshiftOptionsProto object to its protobuf representation.

        Returns:
            RedshiftOptionsProto protobuf
        """

        redshift_options_proto = DataSourceProto.RedshiftOptions(
            table=self.table, query=self.query,
        )

        return redshift_options_proto
