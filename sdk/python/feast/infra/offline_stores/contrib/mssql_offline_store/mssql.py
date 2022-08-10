# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

from dataclasses import asdict, dataclass
from datetime import datetime, timedelta
from typing import (
    Dict,
    List,
    Optional,
    Set,
    Union,
)
from feast.infra.offline_stores import offline_utils
from feast.on_demand_feature_view import OnDemandFeatureView

import numpy as np
import pandas
import pyarrow
from jinja2 import BaseLoader, Environment
from pydantic.types import StrictStr
from pydantic.typing import Literal
from sqlalchemy import create_engine
import sqlalchemy
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

from feast import errors
from feast.data_source import DataSource

from feast.feature_view import FeatureView
from feast.infra.offline_stores.file_source import SavedDatasetFileStorage
from feast.infra.offline_stores.offline_store import (
    OfflineStore,
    RetrievalJob,
    RetrievalMetadata,
)
from feast.infra.offline_stores.offline_utils import (
    DEFAULT_ENTITY_DF_EVENT_TIMESTAMP_COL,
)
from feast.infra.provider import (
    RetrievalJob,
)
from feast.registry import Registry
from feast.repo_config import FeastBaseModel, RepoConfig
from feast.saved_dataset import SavedDatasetStorage
from feast import FileSource
from feast.utils import _get_requested_feature_views_to_features_dict

EntitySchema = Dict[str, np.dtype]


class MsSqlServerOfflineStoreConfig(FeastBaseModel):
    """Offline store config for SQL Server"""

    type: Literal[
        "mssql"
    ] = "mssql"
    """ Offline store type selector"""

    connection_string: StrictStr = "mssql+pyodbc://sa:yourStrong(!)Password@localhost:1433/feast_test?driver=ODBC+Driver+17+for+SQL+Server"
    """Connection string containing the host, port, and configuration parameters for SQL Server
     format: SQLAlchemy connection string, e.g. mssql+pyodbc://sa:yourStrong(!)Password@localhost:1433/feast_test?driver=ODBC+Driver+17+for+SQL+Server"""


class MsSqlServerOfflineStore(OfflineStore):
    def __init__(self):
        self._engine = None

    def _make_engine(self, config: RepoConfig = None) -> Session:
        if self._engine is None:
            self._engine = create_engine(config.connection_string)
        return self._engine

    @staticmethod
    #@log_exceptions_and_usage(offline_store="mssql")
    def pull_latest_from_table_or_query(
        self,
        config: RepoConfig,
        data_source: DataSource,
        join_key_columns: List[str],
        feature_name_columns: List[str],
        event_timestamp_column: str,
        created_timestamp_column: Optional[str],
        start_date: datetime,
        end_date: datetime,
    ) -> RetrievalJob:
        assert type(data_source).__name__ == "MsSqlServerSource"
        from_expression = data_source.get_table_query_string().replace("`", "")

        partition_by_join_key_string = ", ".join(join_key_columns)
        if partition_by_join_key_string != "":
            partition_by_join_key_string = (
                "PARTITION BY " + partition_by_join_key_string
            )
        timestamps = [event_timestamp_column]
        if created_timestamp_column:
            timestamps.append(created_timestamp_column)
        timestamp_desc_string = " DESC, ".join(timestamps) + " DESC"
        field_string = ", ".join(join_key_columns + feature_name_columns + timestamps)

        query = f"""
            SELECT {field_string}
            FROM (
                SELECT {field_string},
                ROW_NUMBER() OVER({partition_by_join_key_string} ORDER BY {timestamp_desc_string}) AS _feast_row
                FROM {from_expression} inner_t
                WHERE {event_timestamp_column} BETWEEN CONVERT(DATETIMEOFFSET, '{start_date}', 120) AND CONVERT(DATETIMEOFFSET, '{end_date}', 120)
            ) outer_t
            WHERE outer_t._feast_row = 1
            """
        self._make_engine(config.offline_store)

        return MsSqlServerRetrievalJob(
            query=query,
            engine=self._engine,
            config=config,
            full_feature_names=False,
            on_demand_feature_views=None,
        )

    @staticmethod
    #@log_exceptions_and_usage(offline_store="mssql")
    def pull_all_from_table_or_query(
        self,
        config: RepoConfig,
        data_source: DataSource,
        join_key_columns: List[str],
        feature_name_columns: List[str],
        event_timestamp_column: str,
        start_date: datetime,
        end_date: datetime,
    ) -> RetrievalJob:
        assert type(data_source).__name__ == "MsSqlServerSource"
        from_expression = data_source.get_table_query_string().replace("`", "")
        timestamps = [event_timestamp_column]
        field_string = ", ".join(join_key_columns + feature_name_columns + timestamps)

        query = f"""
            SELECT {field_string}
            FROM (
                SELECT {field_string}
                FROM {from_expression}
                WHERE {event_timestamp_column} BETWEEN TIMESTAMP '{start_date}' AND TIMESTAMP '{end_date}'
            )
            """
        self._make_engine(config.offline_store)

        return MsSqlServerRetrievalJob(
            query=query,
            engine=self._engine,
            config=config,
            full_feature_names=False,
        )

    @staticmethod
    #@log_exceptions_and_usage(offline_store="mssql")
    def get_historical_features(
        self,
        config: RepoConfig,
        feature_views: List[FeatureView],
        feature_refs: List[str],
        entity_df: Union[pandas.DataFrame, str],
        registry: Registry,
        project: str,
        full_feature_names: bool = False,
    ) -> RetrievalJob:
        expected_join_keys = _get_join_keys(project, feature_views, registry)

        assert isinstance(config.offline_store, MsSqlServerOfflineStoreConfig)
        engine = self._make_engine(config.offline_store)

        (
            table_schema,
            table_name,
        ) = _upload_entity_df_into_sqlserver_and_get_entity_schema(
            engine, config, entity_df
        )

        entity_df_event_timestamp_col = (
            offline_utils.infer_event_timestamp_from_entity_df(table_schema)
        )
        _assert_expected_columns_in_sqlserver(
            expected_join_keys,
            entity_df_event_timestamp_col,
            table_schema,
        )

        # Build a query context containing all information required to template the SQL query
        query_context = get_feature_view_query_context(
            feature_refs, feature_views, registry, project
        )

        # TODO: Infer min_timestamp and max_timestamp from entity_df
        # Generate the SQL query from the query context
        query = build_point_in_time_query(
            query_context,
            min_timestamp=datetime.now() - timedelta(days=365),
            max_timestamp=datetime.now() + timedelta(days=1),
            left_table_query_string=table_name,
            entity_df_event_timestamp_col=entity_df_event_timestamp_col,
            full_feature_names=full_feature_names,
        )

        job = MsSqlServerRetrievalJob(
            query=query,
            engine=self._engine,
            config=config.offline_store,
            full_feature_names=full_feature_names,
            on_demand_feature_views=registry.list_on_demand_feature_views(project),
        )
        return job


def _assert_expected_columns_in_dataframe(
    join_keys: Set[str], entity_df_event_timestamp_col: str, entity_df: pandas.DataFrame
):
    entity_df_columns = set(entity_df.columns.values)
    expected_columns = join_keys.copy()
    expected_columns.add(entity_df_event_timestamp_col)

    missing_keys = expected_columns - entity_df_columns

    if len(missing_keys) != 0:
        raise errors.FeastEntityDFMissingColumnsError(expected_columns, missing_keys)


def _assert_expected_columns_in_sqlserver(
    join_keys: Set[str], entity_df_event_timestamp_col: str, table_schema: EntitySchema
):
    entity_columns = set(table_schema.keys())

    expected_columns = join_keys.copy()
    expected_columns.add(entity_df_event_timestamp_col)

    missing_keys = expected_columns - entity_columns

    if len(missing_keys) != 0:
        raise errors.FeastEntityDFMissingColumnsError(expected_columns, missing_keys)


def _get_join_keys(
    project: str, feature_views: List[FeatureView], registry: Registry
) -> Set[str]:
    join_keys = set()
    for feature_view in feature_views:
        entities = feature_view.entities
        for entity_name in entities:
            entity = registry.get_entity(entity_name, project)
            join_keys.add(entity.join_key)
    return join_keys


def _infer_event_timestamp_from_sqlserver_schema(table_schema) -> str:
    if any(
        schema_field["COLUMN_NAME"] == DEFAULT_ENTITY_DF_EVENT_TIMESTAMP_COL
        for schema_field in table_schema
    ):
        return DEFAULT_ENTITY_DF_EVENT_TIMESTAMP_COL
    else:
        datetime_columns = list(
            filter(
                lambda schema_field: schema_field["DATA_TYPE"] == "DATETIMEOFFSET",
                table_schema,
            )
        )
        if len(datetime_columns) == 1:
            print(
                f"Using {datetime_columns[0]['COLUMN_NAME']} as the event timestamp. To specify a column explicitly, please name it {DEFAULT_ENTITY_DF_EVENT_TIMESTAMP_COL}."
            )
            return datetime_columns[0].name
        else:
            raise ValueError(
                f"Please provide an entity_df with a column named {DEFAULT_ENTITY_DF_EVENT_TIMESTAMP_COL} representing the time of events."
            )


class MsSqlServerRetrievalJob(RetrievalJob):
    def __init__(
        self,
        query: str,
        engine: Engine,
        config: RepoConfig,
        full_feature_names: bool,
        on_demand_feature_views: Optional[List[OnDemandFeatureView]],
        metadata: Optional[RetrievalMetadata] = None,
        drop_columns: Optional[List[str]] = None,
    ):
        self.query = query
        self.engine = engine
        self._config = config
        self._full_feature_names = full_feature_names
        self._on_demand_feature_views = on_demand_feature_views
        self._drop_columns = drop_columns
        self._metadata = metadata

    @property
    def full_feature_names(self) -> bool:
        return self._full_feature_names

    @property
    def on_demand_feature_views(self) -> Optional[List[OnDemandFeatureView]]:
        return self._on_demand_feature_views

    def _to_df_internal(self) -> pandas.DataFrame:
        return pandas.read_sql(self.query, con=self.engine).fillna(value=np.nan)

    def _to_arrow_internal(self) -> pyarrow.Table:
        result = pandas.read_sql(self.query, con=self.engine).fillna(value=np.nan)
        return pyarrow.Table.from_pandas(result)

    ## Implements persist in Feast 0.18 - This persists to filestorage
    ## ToDo: Persist to Azure Storage
    def persist(self, storage: SavedDatasetStorage):
        assert isinstance(storage, SavedDatasetFileStorage)

        filesystem, path = FileSource.create_filesystem_and_path(
            storage.file_options.file_url, storage.file_options.s3_endpoint_override,
        )

        if path.endswith(".parquet"):
            pyarrow.parquet.write_table(
                self.to_arrow(), where=path, filesystem=filesystem
            )
        else:
            # otherwise assume destination is directory
            pyarrow.parquet.write_to_dataset(
                self.to_arrow(), root_path=path, filesystem=filesystem
            )

    @property
    def metadata(self) -> Optional[RetrievalMetadata]:
        return self._metadata


@dataclass(frozen=True)
class FeatureViewQueryContext:
    """Context object used to template a point-in-time SQL query"""

    name: str
    ttl: int
    entities: List[str]
    features: List[str]  # feature reference format
    table_ref: str
    event_timestamp_column: str
    created_timestamp_column: Optional[str]
    table_subquery: str
    entity_selections: List[str]


def _upload_entity_df_into_sqlserver_and_get_entity_schema(
    engine: sqlalchemy.engine.Engine,
    config: RepoConfig,
    entity_df: Union[pandas.DataFrame, str],
) -> EntitySchema:
    """
    Uploads a Pandas entity dataframe into a SQL Server table and constructs the
    schema from the original entity_df dataframe.
    """
    table_id = offline_utils.get_temp_entity_table_name()
    session = sessionmaker(bind=engine)()

    if type(entity_df) is str:
        # TODO: This should be a temporary table, right?
        session.execute(f"SELECT * INTO {table_id} FROM ({entity_df}) t")

        session.commit()

        limited_entity_df = MsSqlServerRetrievalJob(
            f"SELECT TOP 1 * FROM {table_id}",
            engine,
            config,
            full_feature_names=False,
            on_demand_feature_views=None,
        ).to_df()

        entity_schema = dict(zip(limited_entity_df.columns, limited_entity_df.dtypes)), table_id

    elif isinstance(entity_df, pandas.DataFrame):
        # Drop the index so that we don't have unnecessary columns
        entity_df.to_sql(
            name=table_id,
            con=engine,
            index=False
        )
        entity_schema = dict(zip(entity_df.columns, entity_df.dtypes)), table_id
    else:
        raise ValueError(
            f"The entity dataframe you have provided must be a SQL Server SQL query,"
            f" or a Pandas dataframe. But we found: {type(entity_df)} "
        )

    return entity_schema


def get_feature_view_query_context(
    feature_refs: List[str],
    feature_views: List[FeatureView],
    registry: Registry,
    project: str,
) -> List[FeatureViewQueryContext]:
    """Build a query context containing all information required to template a point-in-time SQL query"""

    (
        feature_views_to_feature_map,
        on_demand_feature_views_to_features,
    ) = _get_requested_feature_views_to_features_dict(
        feature_refs, feature_views, registry.list_on_demand_feature_views(project)
    )

    query_context = []
    for feature_view, features in feature_views_to_feature_map.items():
        join_keys = []
        entity_selections = []
        reverse_field_mapping = {
            v: k for k, v in feature_view.source.field_mapping.items()
        }
        for entity_name in feature_view.entities:
            entity = registry.get_entity(entity_name, project)
            join_keys.append(entity.join_key)
            join_key_column = reverse_field_mapping.get(
                entity.join_key, entity.join_key
            )
            entity_selections.append(f"{join_key_column} AS {entity.join_key}")

        if isinstance(feature_view.ttl, timedelta):
            ttl_seconds = int(feature_view.ttl.total_seconds())
        else:
            ttl_seconds = 0

        #assert isinstance(feature_view.source, MsSqlServerSource)

        event_timestamp_column = feature_view.source.event_timestamp_column
        created_timestamp_column = feature_view.source.created_timestamp_column

        context = FeatureViewQueryContext(
            name=feature_view.name,
            ttl=ttl_seconds,
            entities=join_keys,
            features=features,
            table_ref=feature_view.source.table_ref,
            event_timestamp_column=reverse_field_mapping.get(
                event_timestamp_column, event_timestamp_column
            ),
            created_timestamp_column=reverse_field_mapping.get(
                created_timestamp_column, created_timestamp_column
            ),
            # TODO: Make created column optional and not hardcoded
            table_subquery=feature_view.source.get_table_query_string().replace("`", ""),
            entity_selections=entity_selections,
        )
        query_context.append(context)
    return query_context


def build_point_in_time_query(
    feature_view_query_contexts: List[FeatureViewQueryContext],
    min_timestamp: datetime,
    max_timestamp: datetime,
    left_table_query_string: str,
    entity_df_event_timestamp_col: str,
    full_feature_names: bool = False,
):

    """Build point-in-time query between each feature view table and the entity dataframe"""
    template = Environment(loader=BaseLoader()).from_string(
        source=MULTIPLE_FEATURE_VIEW_POINT_IN_TIME_JOIN
    )

    # Add additional fields to dict
    template_context = {
        "min_timestamp": min_timestamp,
        "max_timestamp": max_timestamp,
        "left_table_query_string": left_table_query_string,
        "entity_df_event_timestamp_col": entity_df_event_timestamp_col,
        "unique_entity_keys": set(
            [entity for fv in feature_view_query_contexts for entity in fv.entities]
        ),
        "featureviews": [asdict(context) for context in feature_view_query_contexts],
        "full_feature_names": full_feature_names,
    }

    query = template.render(template_context)
    return query


# TODO: Optimizations
#   * Use NEWID() instead of ROW_NUMBER(), or join on entity columns directly
#   * Precompute ROW_NUMBER() so that it doesn't have to be recomputed for every query on entity_dataframe
#   * Create temporary tables instead of keeping all tables in memory

MULTIPLE_FEATURE_VIEW_POINT_IN_TIME_JOIN = """
/*
 Compute a deterministic hash for the `left_table_query_string` that will be used throughout
 all the logic as the field to GROUP BY the data
*/
WITH entity_dataframe AS (
    SELECT *,
        {{entity_df_event_timestamp_col}} AS entity_timestamp
        {% for featureview in featureviews %}
            ,CONCAT(
                {% for entity_key in unique_entity_keys %}
                    {{entity_key}},
                {% endfor %}
                {{entity_df_event_timestamp_col}}
            ) AS {{featureview.name}}__entity_row_unique_id
        {% endfor %}
    FROM {{ left_table_query_string }}
),
{% for featureview in featureviews %}
{{ featureview.name }}__entity_dataframe AS (
    SELECT
        {{ featureview.entities | join(', ')}},
        entity_timestamp,
        {{featureview.name}}__entity_row_unique_id
    FROM entity_dataframe
    GROUP BY {{ featureview.entities | join(', ')}}, entity_timestamp, {{featureview.name}}__entity_row_unique_id
),
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
        t.{{ featureview.event_timestamp_column }} as event_timestamp,
        {{ 't.' + featureview.created_timestamp_column ~ ' as created_timestamp,' if featureview.created_timestamp_column else '' }}
        t.{{ featureview.entity_selections | join(', ')}},
        {% for feature in featureview.features %}
            {{ feature }} as {% if full_feature_names %}{{ featureview.name }}__{{feature}}{% else %}{{ feature }}{% endif %}{% if loop.last %}{% else %}, {% endif %}
        {% endfor %}
    FROM {{ featureview.table_subquery }} t
    WHERE {{ featureview.event_timestamp_column }} <= (SELECT MAX(entity_timestamp) FROM entity_dataframe)
    {% if featureview.ttl == 0 %}{% else %}
    AND {{ featureview.ttl }} >= DATEDIFF(SECOND, {{ featureview.event_timestamp_column }}, (SELECT MIN(entity_timestamp) FROM entity_dataframe) )
    {% endif %}
),
{{ featureview.name }}__base AS (
    SELECT
        subquery.*,
        entity_dataframe.{{entity_df_event_timestamp_col}} AS entity_timestamp,
        entity_dataframe.{{featureview.name}}__entity_row_unique_id
    FROM {{ featureview.name }}__subquery AS subquery
    INNER JOIN entity_dataframe
        ON 1=1
        AND subquery.event_timestamp <= entity_dataframe.{{entity_df_event_timestamp_col}}
        {% if featureview.ttl == 0 %}{% else %}
        AND {{ featureview.ttl }} > = DATEDIFF(SECOND, subquery.event_timestamp, entity_dataframe.{{entity_df_event_timestamp_col}})
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
        {{ featureview.name }}__base.{{ featureview.name }}__entity_row_unique_id,
        MAX({{ featureview.name }}__base.event_timestamp) AS event_timestamp
        {% if featureview.created_timestamp_column %}
            ,MAX({{ featureview.name }}__base.created_timestamp) AS created_timestamp
        {% endif %}
    FROM {{ featureview.name }}__base
    {% if featureview.created_timestamp_column %}
        INNER JOIN {{ featureview.name }}__dedup
        ON {{ featureview.name }}__dedup.{{ featureview.name }}__entity_row_unique_id = {{ featureview.name }}__base.{{ featureview.name }}__entity_row_unique_id
        AND {{ featureview.name }}__dedup.event_timestamp = {{ featureview.name }}__base.event_timestamp
        AND {{ featureview.name }}__dedup.created_timestamp = {{ featureview.name }}__base.created_timestamp
    {% endif %}
    GROUP BY {{ featureview.name }}__base.{{ featureview.name }}__entity_row_unique_id
),
/*
 4. Once we know the latest value of each feature for a given timestamp,
 we can join again the data back to the original "base" dataset
*/
{{ featureview.name }}__cleaned AS (
    SELECT base.*
    FROM {{ featureview.name }}__base as base
    INNER JOIN {{ featureview.name }}__latest
    ON base.{{ featureview.name }}__entity_row_unique_id = {{ featureview.name }}__latest.{{ featureview.name }}__entity_row_unique_id
    AND base.event_timestamp = {{ featureview.name }}__latest.event_timestamp
        {% if featureview.created_timestamp_column %}
            AND base.created_timestamp = {{ featureview.name }}__latest.created_timestamp
        {% endif %}
){% if loop.last %}{% else %}, {% endif %}
{% endfor %}
/*
 Joins the outputs of multiple time travel joins to a single table.
 The entity_dataframe dataset being our source of truth here.
 */
SELECT entity_dataframe.*
{% for featureview in featureviews %}
    {% for feature in featureview.features %}
            ,{% if full_feature_names %}{{ featureview.name }}__{{feature}}{% else %}{{ feature }}{% endif %}
    {% endfor %}
{% endfor %}
FROM entity_dataframe
{% for featureview in featureviews %}
LEFT JOIN (
    SELECT
        {{featureview.name}}__entity_row_unique_id
        {% for feature in featureview.features %}
            ,{% if full_feature_names %}{{ featureview.name }}__{{feature}}{% else %}{{ feature }}{% endif %}
        {% endfor %}
    FROM {{ featureview.name }}__cleaned
) {{ featureview.name }}__cleaned
ON
{{ featureview.name }}__cleaned.{{ featureview.name }}__entity_row_unique_id = entity_dataframe.{{ featureview.name }}__entity_row_unique_id
{% endfor %}
"""