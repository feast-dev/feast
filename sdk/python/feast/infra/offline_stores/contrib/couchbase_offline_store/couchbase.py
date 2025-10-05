import contextlib
import warnings
from dataclasses import asdict
from datetime import datetime, timedelta
from typing import (
    Any,
    Callable,
    ContextManager,
    Dict,
    Iterator,
    KeysView,
    List,
    Literal,
    Optional,
    Tuple,
    Union,
    cast,
)

import numpy as np
import pandas as pd
import pyarrow as pa
from couchbase_columnar.cluster import Cluster
from couchbase_columnar.common.result import BlockingQueryResult
from couchbase_columnar.credential import Credential
from couchbase_columnar.options import ClusterOptions, QueryOptions, TimeoutOptions
from jinja2 import BaseLoader, Environment
from pydantic import StrictFloat, StrictStr

from feast.data_source import DataSource
from feast.errors import InvalidEntityType, ZeroRowsQueryResult
from feast.feature_view import DUMMY_ENTITY_ID, DUMMY_ENTITY_VAL, FeatureView
from feast.infra.offline_stores.offline_store import (
    OfflineStore,
    RetrievalJob,
    RetrievalMetadata,
)
from feast.infra.registry.base_registry import BaseRegistry
from feast.infra.utils.couchbase.couchbase_utils import normalize_timestamp
from feast.on_demand_feature_view import OnDemandFeatureView
from feast.repo_config import FeastConfigBaseModel, RepoConfig
from feast.saved_dataset import SavedDatasetStorage

from ... import offline_utils
from ...offline_utils import get_timestamp_filter_sql
from .couchbase_source import (
    CouchbaseColumnarSource,
    SavedDatasetCouchbaseColumnarStorage,
)

# Only prints out runtime warnings once.
warnings.simplefilter("once", RuntimeWarning)


class CouchbaseColumnarOfflineStoreConfig(FeastConfigBaseModel):
    """Offline store config for Couchbase Columnar"""

    type: Literal["couchbase.offline"] = "couchbase.offline"

    connection_string: Optional[StrictStr] = None
    user: Optional[StrictStr] = None
    password: Optional[StrictStr] = None
    timeout: StrictFloat = 120


class CouchbaseColumnarOfflineStore(OfflineStore):
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
        """
        Fetch the latest rows for each join key.
        """
        warnings.warn(
            "This offline store is an experimental feature in alpha development. "
            "Some functionality may still be unstable so functionality can change in the future.",
            RuntimeWarning,
        )
        assert isinstance(config.offline_store, CouchbaseColumnarOfflineStoreConfig)
        assert isinstance(data_source, CouchbaseColumnarSource)
        from_expression = data_source.get_table_query_string()

        partition_by_join_key_string = ", ".join(_append_alias(join_key_columns, "a"))
        if partition_by_join_key_string != "":
            partition_by_join_key_string = (
                "PARTITION BY " + partition_by_join_key_string
            )
        timestamps = [timestamp_field]
        if created_timestamp_column:
            timestamps.append(created_timestamp_column)
        timestamp_desc_string = " DESC, ".join(_append_alias(timestamps, "a")) + " DESC"
        a_field_string = ", ".join(
            _append_alias(join_key_columns + feature_name_columns + timestamps, "a")
        )
        b_field_string = ", ".join(
            _append_alias(join_key_columns + feature_name_columns + timestamps, "b")
        )

        start_date_normalized = normalize_timestamp(start_date)
        end_date_normalized = normalize_timestamp(end_date)

        query = f"""
            SELECT
                {b_field_string}
                {f", {repr(DUMMY_ENTITY_VAL)} AS {DUMMY_ENTITY_ID}" if not join_key_columns else ""}
            FROM (
                SELECT {a_field_string},
                ROW_NUMBER() OVER({partition_by_join_key_string} ORDER BY {timestamp_desc_string}) AS _feast_row
                FROM {from_expression} a
                WHERE a.{timestamp_field} BETWEEN '{start_date_normalized}' AND '{end_date_normalized}'
            ) b
            WHERE _feast_row = 1
            """

        return CouchbaseColumnarRetrievalJob(
            query=query,
            config=config,
            full_feature_names=False,
            on_demand_feature_views=None,
            timestamp_field=timestamp_field,
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
        """
        Retrieve historical features using point-in-time joins.
        """
        warnings.warn(
            "This offline store is an experimental feature in alpha development. "
            "Some functionality may still be unstable so functionality can change in the future.",
            RuntimeWarning,
        )
        assert isinstance(config.offline_store, CouchbaseColumnarOfflineStoreConfig)
        for fv in feature_views:
            assert isinstance(fv.batch_source, CouchbaseColumnarSource)

        entity_schema = _get_entity_schema(entity_df, config)

        entity_df_event_timestamp_col = (
            offline_utils.infer_event_timestamp_from_entity_df(entity_schema)
        )

        entity_df_event_timestamp_range = _get_entity_df_event_timestamp_range(
            entity_df, entity_df_event_timestamp_col, config
        )

        @contextlib.contextmanager
        def query_generator() -> Iterator[str]:
            source = cast(CouchbaseColumnarSource, feature_views[0].batch_source)
            database = source.database
            scope = source.scope

            table_name = (
                f"{database}.{scope}.{offline_utils.get_temp_entity_table_name()}"
            )

            _upload_entity_df(config, entity_df, table_name)

            expected_join_keys = offline_utils.get_expected_join_keys(
                project, feature_views, registry
            )

            offline_utils.assert_expected_columns_in_entity_df(
                entity_schema, expected_join_keys, entity_df_event_timestamp_col
            )

            query_context = offline_utils.get_feature_view_query_context(
                feature_refs,
                feature_views,
                registry,
                project,
                entity_df_event_timestamp_range,
            )

            query_context_dict = [asdict(context) for context in query_context]

            try:
                query = build_point_in_time_query(
                    query_context_dict,
                    left_table_query_string=table_name,
                    entity_df_event_timestamp_col=entity_df_event_timestamp_col,
                    entity_df_columns=entity_schema.keys(),
                    query_template=MULTIPLE_FEATURE_VIEW_POINT_IN_TIME_JOIN,
                    full_feature_names=full_feature_names,
                )
                yield query
            finally:
                if table_name:
                    _execute_query(
                        config.offline_store,
                        f"DROP COLLECTION {table_name} IF EXISTS",
                    )

        return CouchbaseColumnarRetrievalJob(
            query=query_generator,
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
            timestamp_field=entity_df_event_timestamp_col,
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
        """
        Fetch all rows from the specified table or query within the time range.
        """
        warnings.warn(
            "This offline store is an experimental feature in alpha development. "
            "Some functionality may still be unstable so functionality can change in the future.",
            RuntimeWarning,
        )
        assert isinstance(config.offline_store, CouchbaseColumnarOfflineStoreConfig)
        assert isinstance(data_source, CouchbaseColumnarSource)
        from_expression = data_source.get_table_query_string()

        timestamp_fields = [timestamp_field]
        if created_timestamp_column:
            timestamp_fields.append(created_timestamp_column)
        field_string = ", ".join(
            join_key_columns + feature_name_columns + timestamp_fields
        )
        start_date_normalized = (
            f"`{normalize_timestamp(start_date)}`" if start_date else None
        )
        end_date_normalized = f"`{normalize_timestamp(end_date)}`" if end_date else None
        timestamp_filter = get_timestamp_filter_sql(
            start_date_normalized,
            end_date_normalized,
            timestamp_field,
            cast_style="raw",
            quote_fields=False,
        )

        query = f"""
        SELECT {field_string}
        FROM {from_expression}
        WHERE {timestamp_filter}
        """

        return CouchbaseColumnarRetrievalJob(
            query=query,
            config=config,
            full_feature_names=False,
            on_demand_feature_views=None,
            timestamp_field=timestamp_field,
        )


class CouchbaseColumnarRetrievalJob(RetrievalJob):
    def __init__(
        self,
        query: Union[str, Callable[[], ContextManager[str]]],
        config: RepoConfig,
        full_feature_names: bool,
        timestamp_field: str,
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
        self._config = config
        self._full_feature_names = full_feature_names
        self._on_demand_feature_views = on_demand_feature_views or []
        self._metadata = metadata
        self._timestamp_field = timestamp_field

    @property
    def full_feature_names(self) -> bool:
        return self._full_feature_names

    @property
    def on_demand_feature_views(self) -> List[OnDemandFeatureView]:
        return self._on_demand_feature_views

    def _to_df_internal(self, timeout: Optional[int] = None) -> pd.DataFrame:
        # Use PyArrow to convert the result to a pandas DataFrame
        return self._to_arrow_internal(timeout).to_pandas()

    def to_sql(self) -> str:
        with self._query_generator() as query:
            return query

    def _to_arrow_internal(self, timeout: Optional[int] = None) -> pa.Table:
        with self._query_generator() as query:
            res = _execute_query(self._config.offline_store, query)
            rows = res.get_all_rows()

            processed_rows = []
            for row in rows:
                processed_row = {}
                for key, value in row.items():
                    if key == self._timestamp_field and value is not None:
                        # Parse and ensure timezone-aware datetime
                        processed_row[key] = pd.to_datetime(value, utc=True)
                    else:
                        processed_row[key] = np.nan if value is None else value
                processed_rows.append(processed_row)

            # Convert to PyArrow table
            table = pa.Table.from_pylist(processed_rows)
            return table

    @property
    def metadata(self) -> Optional[RetrievalMetadata]:
        return self._metadata

    def persist(
        self,
        storage: SavedDatasetStorage,
        allow_overwrite: Optional[bool] = False,
        timeout: Optional[int] = None,
    ):
        assert isinstance(storage, SavedDatasetCouchbaseColumnarStorage)
        table_name = f"{storage.couchbase_options._database}.{storage.couchbase_options._scope}.{offline_utils.get_temp_entity_table_name()}"
        df_to_columnar(self.to_df(), table_name, self._config.offline_store)


def _get_columnar_cluster(config: CouchbaseColumnarOfflineStoreConfig) -> Cluster:
    assert config.connection_string is not None
    assert config.user is not None
    assert config.password is not None

    cred = Credential.from_username_and_password(config.user, config.password)
    timeout_opts = TimeoutOptions(dispatch_timeout=timedelta(seconds=120))
    return Cluster.create_instance(
        config.connection_string, cred, ClusterOptions(timeout_options=timeout_opts)
    )


def _execute_query(
    config: CouchbaseColumnarOfflineStoreConfig,
    query: str,
    named_params: Optional[Dict[str, Any]] = None,
) -> BlockingQueryResult:
    cluster = _get_columnar_cluster(config)
    return cluster.execute_query(
        query,
        QueryOptions(
            named_parameters=named_params, timeout=timedelta(seconds=config.timeout)
        ),
    )


def df_to_columnar(
    df: pd.DataFrame,
    table_name: str,
    offline_store: CouchbaseColumnarOfflineStoreConfig,
):
    df_copy = df.copy()
    insert_values = df_copy.apply(
        lambda row: {
            col: (
                normalize_timestamp(row[col], "%Y-%m-%dT%H:%M:%S.%f+00:00")
                if isinstance(row[col], pd.Timestamp)
                else row[col]
            )
            for col in df_copy.columns
        },
        axis=1,
    ).tolist()

    create_collection_query = f"CREATE COLLECTION {table_name} IF NOT EXISTS PRIMARY KEY(pk: UUID) AUTOGENERATED;"
    insert_query = f"INSERT INTO {table_name} ({insert_values});"

    _execute_query(offline_store, create_collection_query)
    _execute_query(offline_store, insert_query)


def _upload_entity_df(
    config: RepoConfig, entity_df: Union[pd.DataFrame, str], table_name: str
):
    if isinstance(entity_df, pd.DataFrame):
        df_to_columnar(entity_df, table_name, config.offline_store)
    elif isinstance(entity_df, str):
        # If the entity_df is a string (SQL query), create a Columnar collection out of it
        create_collection_query = f"""
        CREATE COLLECTION {table_name} IF NOT EXISTS
        PRIMARY KEY(pk: UUID) AUTOGENERATED
        AS {entity_df}
        """
        _execute_query(config.offline_store, create_collection_query)
    else:
        raise InvalidEntityType(type(entity_df))


def _get_entity_df_event_timestamp_range(
    entity_df: Union[pd.DataFrame, str],
    entity_df_event_timestamp_col: str,
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
        query = f"""
            SELECT
                MIN({entity_df_event_timestamp_col}) AS min,
                MAX({entity_df_event_timestamp_col}) AS max
            FROM ({entity_df}) AS tmp_alias
            """

        res = _execute_query(config.offline_store, query)
        rows = res.get_all_rows()

        if not rows:
            raise ZeroRowsQueryResult(query)

        # Convert the string timestamps to datetime objects
        min_ts = pd.to_datetime(rows[0]["min"], utc=True).to_pydatetime()
        max_ts = pd.to_datetime(rows[0]["max"], utc=True).to_pydatetime()
        entity_df_event_timestamp_range = (min_ts, max_ts)
    else:
        raise InvalidEntityType(type(entity_df))
    return entity_df_event_timestamp_range


def _escape_column(column: str) -> str:
    """Wrap column names in backticks to handle reserved words."""
    return f"`{column}`"


def _append_alias(field_names: List[str], alias: str) -> List[str]:
    """Append alias to escaped column names."""
    return [f"{alias}.{_escape_column(field_name)}" for field_name in field_names]


def build_point_in_time_query(
    feature_view_query_contexts: List[dict],
    left_table_query_string: str,
    entity_df_event_timestamp_col: str,
    entity_df_columns: KeysView[str],
    query_template: str,
    full_feature_names: bool = False,
) -> str:
    """Build point-in-time query between each feature view table and the entity dataframe for Couchbase Columnar"""
    template = Environment(loader=BaseLoader()).from_string(source=query_template)
    final_output_feature_names = list(entity_df_columns)
    final_output_feature_names.extend(
        [
            (
                f"{fv['name']}__{fv['field_mapping'].get(feature, feature)}"
                if full_feature_names
                else fv["field_mapping"].get(feature, feature)
            )
            for fv in feature_view_query_contexts
            for feature in fv["features"]
        ]
    )

    # Add additional fields to dict
    template_context = {
        "left_table_query_string": left_table_query_string,
        "entity_df_event_timestamp_col": entity_df_event_timestamp_col,
        "unique_entity_keys": set(
            [entity for fv in feature_view_query_contexts for entity in fv["entities"]]
        ),
        "featureviews": feature_view_query_contexts,
        "full_feature_names": full_feature_names,
        "final_output_feature_names": final_output_feature_names,
    }

    query = template.render(template_context)
    return query


def get_couchbase_query_schema(config, entity_df: str) -> Dict[str, np.dtype]:
    df_query = f"({entity_df}) AS sub"
    res = _execute_query(config.offline_store, f"SELECT sub.* FROM {df_query} LIMIT 1")
    rows = res.get_all_rows()

    if rows and len(rows) > 0:
        # Get the first row
        first_row = rows[0]
        # Create dictionary mapping each column to dtype('O')
        return {key: np.dtype("O") for key in first_row.keys()}

    return {}


def _get_entity_schema(
    entity_df: Union[pd.DataFrame, str],
    config: RepoConfig,
) -> Dict[str, np.dtype]:
    if isinstance(entity_df, pd.DataFrame):
        return dict(zip(entity_df.columns, entity_df.dtypes))

    elif isinstance(entity_df, str):
        return get_couchbase_query_schema(config, entity_df)
    else:
        raise InvalidEntityType(type(entity_df))


MULTIPLE_FEATURE_VIEW_POINT_IN_TIME_JOIN = """
WITH entity_dataframe AS (
    SELECT e.*,
        e.`{{entity_df_event_timestamp_col}}` AS entity_timestamp
        {% for featureview in featureviews -%}
            {% if featureview.entities -%}
            ,CONCAT(
                {% for entity in featureview.entities -%}
                    TOSTRING(e.`{{entity}}`),
                {% endfor -%}
                TOSTRING(e.`{{entity_df_event_timestamp_col}}`)
            ) AS `{{featureview.name}}__entity_row_unique_id`
            {% else -%}
            ,TOSTRING(e.`{{entity_df_event_timestamp_col}}`) AS `{{featureview.name}}__entity_row_unique_id`
            {% endif -%}
        {% endfor %}
    FROM {{ left_table_query_string }} e
),

{% for featureview in featureviews %}

`{{ featureview.name }}__entity_dataframe` AS (
    SELECT
        {% if featureview.entities %}`{{ featureview.entities | join('`, `') }}`,{% endif %}
        entity_timestamp,
        `{{featureview.name}}__entity_row_unique_id`
    FROM entity_dataframe
    GROUP BY
        {% if featureview.entities %}`{{ featureview.entities | join('`, `')}}`,{% endif %}
        entity_timestamp,
        `{{featureview.name}}__entity_row_unique_id`
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
`{{ featureview.name }}__subquery` AS (
    LET max_ts = (SELECT RAW MAX(entity_timestamp) FROM entity_dataframe)[0]
    SELECT s.* FROM (
        LET min_ts = (SELECT RAW MIN(entity_timestamp) FROM entity_dataframe)[0]
        SELECT
            `{{ featureview.timestamp_field }}` as event_timestamp,
            {{ '`' ~ featureview.created_timestamp_column ~ '` as created_timestamp,' if featureview.created_timestamp_column else '' }}
            {{ featureview.entity_selections | join(', ')}}{% if featureview.entity_selections %},{% else %}{% endif %}
            {% for feature in featureview.features -%}
                `{{ feature }}` as {% if full_feature_names %}`{{ featureview.name }}__{{featureview.field_mapping.get(feature, feature)}}`{% else %}`{{ featureview.field_mapping.get(feature, feature) }}`{% endif %}{% if not loop.last %}, {% endif %}
            {%- endfor %}
        FROM {{ featureview.table_subquery }} AS sub
        WHERE `{{ featureview.timestamp_field }}` <= max_ts
        {% if featureview.ttl == 0 %}{% else %}
        AND date_diff_str(min_ts, `{{ featureview.timestamp_field }}`, "second") <= {{ featureview.ttl }}
        {% endif %}
    ) s
),

`{{ featureview.name }}__base` AS (
    SELECT
        subquery.*,
        entity_dataframe.entity_timestamp,
        entity_dataframe.`{{featureview.name}}__entity_row_unique_id`
    FROM `{{ featureview.name }}__subquery` AS subquery
    INNER JOIN `{{ featureview.name }}__entity_dataframe` AS entity_dataframe
    ON TRUE
        AND subquery.event_timestamp <= entity_dataframe.entity_timestamp
        {% if featureview.ttl == 0 %}{% else %}
        AND date_diff_str(entity_dataframe.entity_timestamp, subquery.event_timestamp, "second") <= {{ featureview.ttl }}
        {% endif %}
        {% for entity in featureview.entities %}
        AND subquery.`{{ entity }}` = entity_dataframe.`{{ entity }}`
        {% endfor %}
),

/*
 2. If the `created_timestamp_column` has been set, we need to
 deduplicate the data first. This is done by calculating the
 `MAX(created_at_timestamp)` for each event_timestamp.
 We then join the data on the next CTE
*/
{% if featureview.created_timestamp_column %}
`{{ featureview.name }}__dedup` AS (
    SELECT
        `{{featureview.name}}__entity_row_unique_id`,
        event_timestamp,
        MAX(created_timestamp) AS created_timestamp
    FROM `{{ featureview.name }}__base`
    GROUP BY `{{featureview.name}}__entity_row_unique_id`, event_timestamp
),
{% endif %}

/*
 3. The data has been filtered during the first CTE "*__base"
 Thus we only need to compute the latest timestamp of each feature.
*/
`{{ featureview.name }}__latest` AS (
    SELECT
        event_timestamp
        {% if featureview.created_timestamp_column %},created_timestamp{% endif %},
        `{{featureview.name}}__entity_row_unique_id`
    FROM (
        SELECT base.*,
            ROW_NUMBER() OVER(
                PARTITION BY base.`{{featureview.name}}__entity_row_unique_id`
                ORDER BY base.event_timestamp DESC
                {% if featureview.created_timestamp_column %}, base.created_timestamp DESC{% endif %}
            ) AS row_number
        FROM `{{ featureview.name }}__base` base
        {% if featureview.created_timestamp_column %}
        INNER JOIN `{{ featureview.name }}__dedup` dedup
        ON base.`{{featureview.name}}__entity_row_unique_id` = dedup.`{{featureview.name}}__entity_row_unique_id`
        AND base.event_timestamp = dedup.event_timestamp
        AND base.created_timestamp = dedup.created_timestamp
        {% endif %}
    ) AS sub
    WHERE sub.row_number = 1
),

/*
 4. Once we know the latest value of each feature for a given timestamp,
 we can join again the data back to the original "base" dataset
*/
`{{ featureview.name }}__cleaned` AS (
    SELECT base.*
    FROM `{{ featureview.name }}__base` AS base
    INNER JOIN `{{ featureview.name }}__latest` AS latest
    ON base.`{{featureview.name}}__entity_row_unique_id` = latest.`{{featureview.name}}__entity_row_unique_id`
    AND base.event_timestamp = latest.event_timestamp
    {% if featureview.created_timestamp_column %}
    AND base.created_timestamp = latest.created_timestamp
    {% endif %}
){% if not loop.last %},{% endif %}

{% endfor %}

/*
 Joins the outputs of multiple time travel joins to a single table.
 The entity_dataframe dataset being our source of truth here.
 */
SELECT DISTINCT
    {%- set fields = [] %}
    {%- for feature_name in final_output_feature_names %}
    {%- if '__' not in feature_name %}
        {%- set ns = namespace(found=false) %}
        {%- for fv in featureviews %}
            {%- for feature in fv.features %}
                {%- if feature == feature_name %}
                    {%- set ns.found = true %}
                    {%- if full_feature_names %}
                        {%- set _ = fields.append('IFMISSINGORNULL(`' ~ fv.name ~ '_final`.`' ~ fv.name ~ '__' ~ feature ~ '`, null) AS `' ~ fv.name ~ '__' ~ feature ~ '`') %}
                    {%- else %}
                        {%- set _ = fields.append('IFMISSINGORNULL(`' ~ fv.name ~ '_final`.`' ~ feature ~ '`, null) AS `' ~ feature ~ '`') %}
                    {%- endif %}
                {%- endif %}
            {%- endfor %}
        {%- endfor %}
        {%- if not ns.found %}
            {%- if feature_name == 'feature_name' %}
                {%- set _ = fields.append('IFMISSINGORNULL(`field_mapping_final`.`' ~ feature_name ~ '`, null) AS `' ~ feature_name ~ '`') %}
            {%- else %}
                {%- set _ = fields.append('main_entity.`' ~ feature_name ~ '`') %}
            {%- endif %}
        {%- endif %}
    {%- else %}
        {%- set feature_parts = feature_name.split('__') %}
        {%- set fv_name = feature_parts[0] %}
        {%- set feature = feature_parts[1] %}
        {%- if feature_name == 'field_mapping__feature_name' %}
            {%- set _ = fields.append('IFMISSINGORNULL(`field_mapping_final`.`field_mapping__feature_name`, null) AS `field_mapping__feature_name`') %}
        {%- else %}
            {%- set _ = fields.append('IFMISSINGORNULL(`' ~ fv_name ~ '_final`.`' ~ feature_name ~ '`, null) AS `' ~ feature_name ~ '`') %}
        {%- endif %}
    {%- endif %}
    {%- endfor %}
    {{ fields | reject('none') | join(',\n    ') }}
FROM entity_dataframe AS main_entity

{%- for featureview in featureviews %}
LEFT JOIN (
    SELECT
        `{{featureview.name}}__entity_row_unique_id`,
        {% for feature in featureview.features -%}
        IFMISSINGORNULL(`{% if full_feature_names %}{{ featureview.name }}__{{ featureview.field_mapping.get(feature, feature) }}{% else %}{{ featureview.field_mapping.get(feature, feature) }}{% endif %}`, null) AS `{% if full_feature_names %}{{ featureview.name }}__{{ featureview.field_mapping.get(feature, feature) }}{% else %}{{ featureview.field_mapping.get(feature, feature) }}{% endif %}`{% if not loop.last %},{% endif %}
        {% endfor %}
    FROM `{{ featureview.name }}__cleaned`
) AS `{{featureview.name}}_final`
ON main_entity.`{{featureview.name}}__entity_row_unique_id` = `{{featureview.name}}_final`.`{{featureview.name}}__entity_row_unique_id`
{% endfor %}
"""
