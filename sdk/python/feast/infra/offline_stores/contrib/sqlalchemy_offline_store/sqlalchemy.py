# Copyright 2024 The Feast Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
SQLAlchemy Offline Store implementation for Feast.

This module provides a SQLAlchemy-based offline store that can connect to any
database supported by SQLAlchemy, including PostgreSQL, MySQL, SQLite, Oracle,
MS SQL Server, and more.
"""

import contextlib
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
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
)

import numpy as np
import pandas as pd
import pyarrow as pa
from jinja2 import BaseLoader, Environment
from pydantic import StrictStr
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from feast.data_source import DataSource
from feast.errors import InvalidEntityType, ZeroRowsQueryResult
from feast.feature_view import DUMMY_ENTITY_ID, DUMMY_ENTITY_VAL, FeatureView
from feast.infra.offline_stores import offline_utils
from feast.infra.offline_stores.offline_store import (
    OfflineStore,
    RetrievalJob,
    RetrievalMetadata,
)
from feast.infra.registry.base_registry import BaseRegistry
from feast.on_demand_feature_view import OnDemandFeatureView
from feast.repo_config import FeastConfigBaseModel, RepoConfig
from feast.saved_dataset import SavedDatasetStorage
from feast.utils import _utc_now, make_tzaware

from .sqlalchemy_source import (
    SavedDatasetSQLAlchemyStorage,
    SQLAlchemySource,
)


class SQLAlchemyOfflineStoreConfig(FeastConfigBaseModel):
    """
    Configuration for the SQLAlchemy offline store.

    Example YAML configuration:

        offline_store:
            type: sqlalchemy
            connection_string: postgresql+psycopg://user:password@localhost:5432/feast_db
            db_schema: public

    Attributes:
        type: Must be "sqlalchemy"
        connection_string: SQLAlchemy connection string (e.g., "postgresql://user:pass@host/db")
        db_schema: Database schema to use (optional, defaults to database default)
        sqlalchemy_config_kwargs: Additional kwargs to pass to SQLAlchemy create_engine()
    """

    type: Literal["sqlalchemy"] = "sqlalchemy"

    connection_string: StrictStr
    """SQLAlchemy connection string (e.g., 'postgresql+psycopg://user:pass@host:port/dbname')"""

    db_schema: Optional[StrictStr] = None
    """Database schema name (optional). If not specified, uses the database default."""

    sqlalchemy_config_kwargs: Dict[str, Any] = {"echo": False, "pool_pre_ping": True}
    """Additional keyword arguments to pass to SQLAlchemy's create_engine()"""


class SQLAlchemyOfflineStore(OfflineStore):
    """
    SQLAlchemy-based offline store implementation.

    This offline store uses SQLAlchemy to connect to various databases,
    providing a unified interface for feature retrieval across different
    database backends.

    Supported databases include:
        - PostgreSQL (postgresql+psycopg, postgresql+psycopg2)
        - MySQL (mysql+pymysql, mysql+mysqlconnector)
        - SQLite (sqlite:///)
        - Oracle (oracle+cx_oracle)
        - MS SQL Server (mssql+pyodbc, mssql+pymssql)
        - And many more supported by SQLAlchemy
    """

    @staticmethod
    def _get_engine(config: RepoConfig) -> Engine:
        """Create and return a SQLAlchemy engine."""
        assert isinstance(config.offline_store, SQLAlchemyOfflineStoreConfig)
        return create_engine(
            config.offline_store.connection_string,
            **config.offline_store.sqlalchemy_config_kwargs,
        )

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
        assert isinstance(config.offline_store, SQLAlchemyOfflineStoreConfig)
        assert isinstance(data_source, SQLAlchemySource)

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

        # Detect dialect for timestamp formatting
        dialect = _get_dialect(config.offline_store.connection_string)
        start_date_str = _format_timestamp(start_date, dialect)
        end_date_str = _format_timestamp(end_date, dialect)

        query = f"""
            SELECT
                {b_field_string}
                {f", '{DUMMY_ENTITY_VAL}' AS {DUMMY_ENTITY_ID}" if not join_key_columns else ""}
            FROM (
                SELECT {a_field_string},
                ROW_NUMBER() OVER({partition_by_join_key_string} ORDER BY {timestamp_desc_string}) AS _feast_row
                FROM {from_expression} a
                WHERE a."{timestamp_field}" BETWEEN {start_date_str} AND {end_date_str}
            ) b
            WHERE _feast_row = 1
            """

        return SQLAlchemyRetrievalJob(
            query=query,
            config=config,
            full_feature_names=False,
            on_demand_feature_views=None,
        )

    @staticmethod
    def get_historical_features(
        config: RepoConfig,
        feature_views: List[FeatureView],
        feature_refs: List[str],
        entity_df: Optional[Union[pd.DataFrame, str]],
        registry: BaseRegistry,
        project: str,
        full_feature_names: bool = False,
        **kwargs,
    ) -> RetrievalJob:
        assert isinstance(config.offline_store, SQLAlchemyOfflineStoreConfig)
        for fv in feature_views:
            assert isinstance(fv.batch_source, SQLAlchemySource)

        start_date: Optional[datetime] = kwargs.get("start_date", None)
        end_date: Optional[datetime] = kwargs.get("end_date", None)

        # Handle non-entity retrieval mode
        if entity_df is None:
            if end_date is None:
                end_date = _utc_now()
            else:
                end_date = make_tzaware(end_date)

            if start_date is None:
                max_ttl_seconds = 0
                for fv in feature_views:
                    if fv.ttl and isinstance(fv.ttl, timedelta):
                        ttl_seconds = int(fv.ttl.total_seconds())
                        max_ttl_seconds = max(max_ttl_seconds, ttl_seconds)

                if max_ttl_seconds > 0:
                    start_date = end_date - timedelta(seconds=max_ttl_seconds)
                else:
                    start_date = end_date - timedelta(days=30)
            else:
                start_date = make_tzaware(start_date)

            entity_df = pd.DataFrame(
                {
                    "event_timestamp": pd.date_range(
                        start=start_date, end=end_date, freq="1s", tz=timezone.utc
                    )[:1]
                }
            )

        entity_schema = _get_entity_schema(entity_df, config)

        entity_df_event_timestamp_col = (
            offline_utils.infer_event_timestamp_from_entity_df(entity_schema)
        )

        entity_df_event_timestamp_range = _get_entity_df_event_timestamp_range(
            entity_df,
            entity_df_event_timestamp_col,
            config,
        )

        @contextlib.contextmanager
        def query_generator() -> Iterator[str]:
            table_name = offline_utils.get_temp_entity_table_name()

            # Determine if we should use CTE or temp table
            use_cte = isinstance(entity_df, str)

            if use_cte:
                left_table_query_string = entity_df
            else:
                left_table_query_string = table_name
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
            # Hack for query_context.entity_selections to support uppercase in columns
            for context in query_context_dict:
                context["entity_selections"] = [
                    f""""{entity_selection.replace(" AS ", '" AS "')}\""""
                    for entity_selection in context["entity_selections"]
                ]

            dialect = _get_dialect(config.offline_store.connection_string)

            try:
                yield build_point_in_time_query(
                    query_context_dict,
                    left_table_query_string=left_table_query_string,
                    entity_df_event_timestamp_col=entity_df_event_timestamp_col,
                    entity_df_columns=entity_schema.keys(),
                    query_template=MULTIPLE_FEATURE_VIEW_POINT_IN_TIME_JOIN,
                    full_feature_names=full_feature_names,
                    use_cte=use_cte,
                    start_date=start_date,
                    end_date=end_date,
                    dialect=dialect,
                )
            finally:
                # Clean up temp table if we created one
                if not use_cte:
                    engine = SQLAlchemyOfflineStore._get_engine(config)
                    with engine.connect() as conn:
                        conn.execute(text(f'DROP TABLE IF EXISTS "{table_name}"'))
                        conn.commit()

        return SQLAlchemyRetrievalJob(
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
        assert isinstance(config.offline_store, SQLAlchemyOfflineStoreConfig)
        assert isinstance(data_source, SQLAlchemySource)

        from_expression = data_source.get_table_query_string()

        timestamp_fields = [timestamp_field]
        if created_timestamp_column:
            timestamp_fields.append(created_timestamp_column)

        field_string = ", ".join(
            _append_alias(
                join_key_columns + feature_name_columns + timestamp_fields,
                "paftoq_alias",
            )
        )

        dialect = _get_dialect(config.offline_store.connection_string)
        timestamp_filter = _build_timestamp_filter(
            start_date, end_date, timestamp_field, dialect
        )

        query = f"""
            SELECT {field_string}
            FROM {from_expression} AS paftoq_alias
            WHERE {timestamp_filter}
        """

        return SQLAlchemyRetrievalJob(
            query=query,
            config=config,
            full_feature_names=False,
            on_demand_feature_views=None,
        )


class SQLAlchemyRetrievalJob(RetrievalJob):
    """Retrieval job implementation for SQLAlchemy offline store."""

    def __init__(
        self,
        query: Union[str, Callable[[], ContextManager[str]]],
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
        self.config = config
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
        return self._to_arrow_internal().to_pandas()

    def to_sql(self) -> str:
        with self._query_generator() as query:
            return query

    def _to_arrow_internal(self, timeout: Optional[int] = None) -> pa.Table:
        with self._query_generator() as query:
            engine = SQLAlchemyOfflineStore._get_engine(self.config)
            with engine.connect() as conn:
                # Use pandas to read SQL and convert to Arrow
                df = pd.read_sql(query, conn)
                return pa.Table.from_pandas(df)

    @property
    def metadata(self) -> Optional[RetrievalMetadata]:
        return self._metadata

    def persist(
        self,
        storage: SavedDatasetStorage,
        allow_overwrite: Optional[bool] = False,
        timeout: Optional[int] = None,
    ):
        assert isinstance(storage, SavedDatasetSQLAlchemyStorage)
        df = self.to_df()
        _df_to_table(self.config, df, storage.sqlalchemy_options._table)


def _get_dialect(connection_string: str) -> str:
    """Extract the dialect from a SQLAlchemy connection string."""
    # Connection string format: dialect+driver://...
    if "://" in connection_string:
        dialect_part = connection_string.split("://")[0]
        if "+" in dialect_part:
            return dialect_part.split("+")[0]
        return dialect_part
    return "generic"


def _format_timestamp(dt: datetime, dialect: str) -> str:
    """Format a datetime for use in SQL queries based on the dialect."""
    dt_str = dt.strftime("%Y-%m-%d %H:%M:%S")

    if dialect in ("postgresql", "postgres"):
        return f"'{dt_str}'::timestamptz"
    elif dialect in ("mysql", "mariadb"):
        return f"'{dt_str}'"
    elif dialect == "sqlite":
        return f"'{dt_str}'"
    elif dialect in ("mssql", "oracle"):
        return f"'{dt_str}'"
    else:
        return f"'{dt_str}'"


def _build_timestamp_filter(
    start_date: Optional[datetime],
    end_date: Optional[datetime],
    timestamp_field: str,
    dialect: str,
) -> str:
    """Build a SQL WHERE clause for timestamp filtering."""
    conditions = []

    if start_date:
        start_str = _format_timestamp(start_date, dialect)
        conditions.append(f'"{timestamp_field}" >= {start_str}')

    if end_date:
        end_str = _format_timestamp(end_date, dialect)
        conditions.append(f'"{timestamp_field}" <= {end_str}')

    if conditions:
        return " AND ".join(conditions)
    else:
        return "1=1"


def _append_alias(field_names: List[str], alias: str) -> List[str]:
    """Append table alias to field names."""
    return [f'{alias}."{field_name}"' for field_name in field_names]


def _get_entity_schema(
    entity_df: Union[pd.DataFrame, str],
    config: RepoConfig,
) -> Dict[str, np.dtype]:
    """Get the schema of an entity DataFrame."""
    if isinstance(entity_df, pd.DataFrame):
        return dict(zip(entity_df.columns, entity_df.dtypes))
    elif isinstance(entity_df, str):
        df_query = f"({entity_df}) AS sub"
        engine = SQLAlchemyOfflineStore._get_engine(config)
        with engine.connect() as conn:
            df = pd.read_sql(f"SELECT * FROM {df_query} LIMIT 0", conn)
            return dict(zip(df.columns, df.dtypes))
    else:
        raise InvalidEntityType(type(entity_df))


def _get_entity_df_event_timestamp_range(
    entity_df: Union[pd.DataFrame, str],
    entity_df_event_timestamp_col: str,
    config: RepoConfig,
) -> Tuple[datetime, datetime]:
    """Get the min/max timestamp range from the entity DataFrame."""
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
        engine = SQLAlchemyOfflineStore._get_engine(config)
        with engine.connect() as conn:
            query = f"""
                SELECT
                    MIN({entity_df_event_timestamp_col}) AS min_ts,
                    MAX({entity_df_event_timestamp_col}) AS max_ts
                FROM ({entity_df}) AS tmp_alias
                """
            result = conn.execute(text(query))
            row = result.fetchone()
            if not row:
                raise ZeroRowsQueryResult(query)
            entity_df_event_timestamp_range = (row[0], row[1])
    else:
        raise InvalidEntityType(type(entity_df))

    return entity_df_event_timestamp_range


def _upload_entity_df(
    config: RepoConfig, entity_df: Union[pd.DataFrame, str], table_name: str
):
    """Upload an entity DataFrame to a temporary table."""
    engine = SQLAlchemyOfflineStore._get_engine(config)

    if isinstance(entity_df, pd.DataFrame):
        _df_to_table(config, entity_df, table_name)
    elif isinstance(entity_df, str):
        with engine.connect() as conn:
            conn.execute(text(f'CREATE TABLE "{table_name}" AS ({entity_df})'))
            conn.commit()
    else:
        raise InvalidEntityType(type(entity_df))


def _df_to_table(config: RepoConfig, df: pd.DataFrame, table_name: str):
    """Create a table from a DataFrame and insert data."""
    engine = SQLAlchemyOfflineStore._get_engine(config)
    dialect = _get_dialect(config.offline_store.connection_string)

    # For Oracle, uppercase column names to match Oracle's default case handling
    if dialect == "oracle":
        df = df.copy()
        df.columns = [col.upper() for col in df.columns]

    # Use pandas to_sql for simplicity and cross-database compatibility
    df.to_sql(table_name, engine, if_exists="replace", index=False)


def build_point_in_time_query(
    feature_view_query_contexts: List[dict],
    left_table_query_string: str,
    entity_df_event_timestamp_col: str,
    entity_df_columns: KeysView[str],
    query_template: str,
    full_feature_names: bool = False,
    use_cte: bool = False,
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None,
    dialect: str = "generic",
) -> str:
    """Build point-in-time query between feature view tables and entity dataframe."""
    # For Oracle, uppercase column names to match Oracle's default identifier handling
    if dialect == "oracle":
        entity_df_event_timestamp_col = entity_df_event_timestamp_col.upper()
        entity_df_columns = [col.upper() for col in entity_df_columns]
        # Uppercase all column references in feature view contexts
        for fv_context in feature_view_query_contexts:
            fv_context["entities"] = [e.upper() for e in fv_context.get("entities", [])]
            # Uppercase timestamp and created timestamp columns
            if "timestamp_field" in fv_context:
                fv_context["timestamp_field"] = fv_context["timestamp_field"].upper()
            if (
                "created_timestamp_column" in fv_context
                and fv_context["created_timestamp_column"]
            ):
                fv_context["created_timestamp_column"] = fv_context[
                    "created_timestamp_column"
                ].upper()
            # Uppercase feature names
            if "features" in fv_context:
                fv_context["features"] = [f.upper() for f in fv_context["features"]]
            # Update entity_selections to use uppercase
            if "entity_selections" in fv_context:
                fv_context["entity_selections"] = [
                    sel.upper() if '"' not in sel else sel.replace('"', "").upper()
                    for sel in fv_context["entity_selections"]
                ]
            # Update field_mapping keys and values to uppercase
            if "field_mapping" in fv_context:
                fv_context["field_mapping"] = {
                    k.upper(): v.upper() for k, v in fv_context["field_mapping"].items()
                }

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

    template_context = {
        "left_table_query_string": left_table_query_string,
        "entity_df_event_timestamp_col": entity_df_event_timestamp_col,
        "unique_entity_keys": set(
            [entity for fv in feature_view_query_contexts for entity in fv["entities"]]
        ),
        "featureviews": feature_view_query_contexts,
        "full_feature_names": full_feature_names,
        "final_output_feature_names": final_output_feature_names,
        "use_cte": use_cte,
        "start_date": start_date,
        "end_date": end_date,
        "dialect": dialect,
    }

    query = template.render(template_context)
    return query


# Point-in-time join query template
# This template supports various SQL dialects and provides point-in-time correct joins
# Dialect-aware macros:
# - AS keyword: Some databases (Oracle) don't use AS for table aliases
# - INTERVAL syntax: Different databases have different interval syntax
# - ON TRUE: Not supported by all databases
MULTIPLE_FEATURE_VIEW_POINT_IN_TIME_JOIN = """
{# Dialect-aware helper macros #}
{#
   Supported dialects: postgresql, oracle, mysql, sqlite
   - postgresql: Standard SQL with some PostgreSQL extensions
   - oracle: Uppercase identifiers, NUMTODSINTERVAL, no AS for aliases, no USING
   - mysql: INTERVAL without quotes, backticks for identifiers (handled by SQLAlchemy)
   - sqlite: datetime() for interval arithmetic, no native boolean
#}

{% macro table_alias(alias) -%}
{% if dialect == 'oracle' %}{{ alias }}{% else %}AS {{ alias }}{% endif %}
{%- endmacro %}

{% macro interval_seconds(seconds) -%}
{% if dialect == 'oracle' %}NUMTODSINTERVAL({{ seconds }}, 'SECOND'){% elif dialect == 'mysql' %}INTERVAL {{ seconds }} SECOND{% else %}INTERVAL '{{ seconds }}' SECOND{% endif %}
{%- endmacro %}

{# For SQLite, we need a different approach for timestamp - interval #}
{% macro timestamp_minus_seconds(timestamp_col, seconds) -%}
{% if dialect == 'oracle' %}{{ timestamp_col }} - NUMTODSINTERVAL({{ seconds }}, 'SECOND'){% elif dialect == 'sqlite' %}datetime({{ timestamp_col }}, '-' || {{ seconds }} || ' seconds'){% elif dialect == 'mysql' %}{{ timestamp_col }} - INTERVAL {{ seconds }} SECOND{% else %}{{ timestamp_col }} - INTERVAL '{{ seconds }}' SECOND{% endif %}
{%- endmacro %}

{% macro on_true() -%}
{% if dialect in ['oracle', 'sqlite'] %}ON 1=1{% else %}ON TRUE{% endif %}
{%- endmacro %}

{% macro join_using(left_alias, right_table, columns) -%}
{% if dialect == 'oracle' %}ON {% for col in columns %}{{ left_alias }}."{{ col }}" = {{ right_table }}."{{ col }}"{% if not loop.last %} AND {% endif %}{% endfor %}{% else %}USING ({% for col in columns %}"{{ col }}"{% if not loop.last %}, {% endif %}{% endfor %}){% endif %}
{%- endmacro %}

{% macro cast_to_string(column) -%}
{% if dialect == 'oracle' %}TO_CHAR({{ column }}){% elif dialect == 'sqlite' %}CAST({{ column }} AS TEXT){% elif dialect == 'mysql' %}CAST({{ column }} AS CHAR){% else %}CAST({{ column }} AS VARCHAR){% endif %}
{%- endmacro %}

{# String concatenation - MySQL uses CONCAT(), others use || #}
{% macro concat_strings(str1, str2) -%}
{% if dialect == 'mysql' %}CONCAT({{ str1 }}, {{ str2 }}){% else %}{{ str1 }} || {{ str2 }}{% endif %}
{%- endmacro %}
{% if start_date and end_date %}
/*
 Non-entity timestamp range query - use JOINs to combine features into single rows
*/
{% if featureviews | length == 1 %}
SELECT
    "{{ featureviews[0].timestamp_field }}" AS event_timestamp,
    {% if featureviews[0].created_timestamp_column %}
    "{{ featureviews[0].created_timestamp_column }}" AS created_timestamp,
    {% endif %}
    {% for entity in featureviews[0].entities %}
    "{{ entity }}",
    {% endfor %}
    {% for feature in featureviews[0].features %}
    "{{ feature }}" AS {% if full_feature_names %}"{{ featureviews[0].name }}__{{ featureviews[0].field_mapping.get(feature, feature) }}"{% else %}"{{ featureviews[0].field_mapping.get(feature, feature) }}"{% endif %}{% if not loop.last %},{% endif %}
    {% endfor %}
    FROM {{ featureviews[0].table_subquery }} {{ table_alias('fv_alias') }}
WHERE "{{ featureviews[0].timestamp_field }}" BETWEEN '{{ start_date }}' AND '{{ end_date }}'
{% if featureviews[0].ttl != 0 and featureviews[0].min_event_timestamp %}
AND "{{ featureviews[0].timestamp_field }}" >= '{{ featureviews[0].min_event_timestamp }}'
{% endif %}
{% else %}
WITH
{% for featureview in featureviews %}
"{{ featureview.name }}__data" AS (
    SELECT
        "{{ featureview.timestamp_field }}" AS event_timestamp,
        {% if featureview.created_timestamp_column %}
        "{{ featureview.created_timestamp_column }}" AS created_timestamp,
        {% endif %}
        {% for entity in featureview.entities %}
        "{{ entity }}",
        {% endfor %}
        {% for feature in featureview.features %}
        "{{ feature }}" AS {% if full_feature_names %}"{{ featureview.name }}__{{ featureview.field_mapping.get(feature, feature) }}"{% else %}"{{ featureview.field_mapping.get(feature, feature) }}"{% endif %}{% if not loop.last %},{% endif %}
        {% endfor %}
    FROM {{ featureview.table_subquery }} {{ table_alias('sub') }}
    WHERE "{{ featureview.timestamp_field }}" BETWEEN '{{ start_date }}' AND '{{ end_date }}'
    {% if featureview.ttl != 0 and featureview.min_event_timestamp %}
    AND "{{ featureview.timestamp_field }}" >= '{{ featureview.min_event_timestamp }}'
    {% endif %}
),
{% endfor %}

-- Create a base query with all unique entity + timestamp combinations
base_entities AS (
    {% for featureview in featureviews %}
    SELECT DISTINCT
        event_timestamp,
        {% for entity in featureview.entities %}
        "{{ entity }}"{% if not loop.last %},{% endif %}
        {% endfor %}
    FROM "{{ featureview.name }}__data"
    {% if not loop.last %}
    UNION
    {% endif %}
    {% endfor %}
)

SELECT
    base.event_timestamp,
    {% set all_entities = [] %}
    {% for featureview in featureviews %}
        {% for entity in featureview.entities %}
            {% if entity not in all_entities %}
                {% set _ = all_entities.append(entity) %}
            {% endif %}
        {% endfor %}
    {% endfor %}
    {% for entity in all_entities %}
    base."{{ entity }}",
    {% endfor %}
    {% set total_features = featureviews|map(attribute='features')|map('length')|sum %}
    {% set feature_counter = namespace(count=0) %}
    {% for featureview in featureviews %}
        {% set outer_loop_index = loop.index0 %}
        {% for feature in featureview.features %}
        {% set feature_counter.count = feature_counter.count + 1 %}
        fv_{{ outer_loop_index }}."{% if full_feature_names %}{{ featureview.name }}__{{ featureview.field_mapping.get(feature, feature) }}{% else %}{{ featureview.field_mapping.get(feature, feature) }}{% endif %}"{% if feature_counter.count < total_features %},{% endif %}
        {% endfor %}
    {% endfor %}
FROM base_entities base
{% for featureview in featureviews %}
{% set outer_loop_index = loop.index0 %}
LEFT JOIN (
    {% if dialect == 'oracle' %}
    {# Oracle doesn't support DISTINCT ON, use ROW_NUMBER instead #}
    SELECT event_timestamp,
        {% for entity in featureview.entities %}
        "{{ entity }}",
        {% endfor %}
        {% for feature in featureview.features %}
        "{% if full_feature_names %}{{ featureview.name }}__{{ featureview.field_mapping.get(feature, feature) }}{% else %}{{ featureview.field_mapping.get(feature, feature) }}{% endif %}"{% if not loop.last %},{% endif %}
        {% endfor %}
    FROM (
        SELECT fv_sub_{{ outer_loop_index }}.*,
            ROW_NUMBER() OVER(
                PARTITION BY {% for entity in featureview.entities %}"{{ entity }}"{% if not loop.last %}, {% endif %}{% endfor %}
                ORDER BY event_timestamp DESC
            ) AS rn
        FROM "{{ featureview.name }}__data" fv_sub_{{ outer_loop_index }}
    ) ranked
    WHERE ranked.rn = 1
    {% else %}
    SELECT DISTINCT ON ({% for entity in featureview.entities %}"{{ entity }}"{% if not loop.last %}, {% endif %}{% endfor %})
        event_timestamp,
        {% for entity in featureview.entities %}
        "{{ entity }}",
        {% endfor %}
        {% for feature in featureview.features %}
        "{% if full_feature_names %}{{ featureview.name }}__{{ featureview.field_mapping.get(feature, feature) }}{% else %}{{ featureview.field_mapping.get(feature, feature) }}{% endif %}"{% if not loop.last %},{% endif %}
        {% endfor %}
    FROM "{{ featureview.name }}__data" fv_sub_{{ outer_loop_index }}
    WHERE fv_sub_{{ outer_loop_index }}.event_timestamp <= base.event_timestamp
    {% if featureview.ttl != 0 %}
    AND fv_sub_{{ outer_loop_index }}.event_timestamp >= {{ timestamp_minus_seconds('base.event_timestamp', featureview.ttl) }}
    {% endif %}
    {% for entity in featureview.entities %}
    AND fv_sub_{{ outer_loop_index }}."{{ entity }}" = base."{{ entity }}"
    {% endfor %}
    ORDER BY {% for entity in featureview.entities %}"{{ entity }}"{% if not loop.last %}, {% endif %}{% endfor %}, event_timestamp DESC
    {% endif %}
) {{ table_alias('fv_' ~ outer_loop_index) }} {{ on_true() }}
{% endfor %}
ORDER BY base.event_timestamp
{% endif %}
{% else %}
WITH
{% if use_cte %}
    entity_query AS ({{ left_table_query_string }}),
{% endif %}
/*
 Compute a deterministic hash for the `left_table_query_string` that will be used throughout
 all the logic as the field to GROUP BY the data
*/
entity_dataframe AS (
    {% if dialect == 'oracle' %}
    {# Oracle stores unquoted identifiers as uppercase - use unquoted names #}
    SELECT entity_src.*,
        entity_src.{{entity_df_event_timestamp_col | upper}} AS entity_timestamp
        {% for featureview in featureviews %}
            {% if featureview.entities %}
            ,(
                {% for entity in featureview.entities %}
                    {{ cast_to_string('entity_src.' ~ entity | upper) }} ||
                {% endfor %}
                {{ cast_to_string('entity_src.' ~ entity_df_event_timestamp_col | upper) }}
            ) AS "{{featureview.name}}__entity_row_unique_id"
            {% else %}
            ,{{ cast_to_string('entity_src.' ~ entity_df_event_timestamp_col | upper) }} AS "{{featureview.name}}__entity_row_unique_id"
            {% endif %}
        {% endfor %}
    FROM
        {% if use_cte %}
            entity_query entity_src
        {% else %}
            {{ left_table_query_string }} entity_src
        {% endif %}
    {% else %}
    SELECT *,
        {{entity_df_event_timestamp_col}} AS entity_timestamp
        {% for featureview in featureviews %}
            {% if featureview.entities %}
            {% if dialect == 'mysql' %}
            ,CONCAT(
                {% for entity in featureview.entities %}
                    {{ cast_to_string('"' ~ entity ~ '"') }},
                {% endfor %}
                {{ cast_to_string('"' ~ entity_df_event_timestamp_col ~ '"') }}
            ) AS "{{featureview.name}}__entity_row_unique_id"
            {% else %}
            ,(
                {% for entity in featureview.entities %}
                    {{ cast_to_string('"' ~ entity ~ '"') }} ||
                {% endfor %}
                {{ cast_to_string('"' ~ entity_df_event_timestamp_col ~ '"') }}
            ) AS "{{featureview.name}}__entity_row_unique_id"
            {% endif %}
            {% else %}
            ,{{ cast_to_string('"' ~ entity_df_event_timestamp_col ~ '"') }} AS "{{featureview.name}}__entity_row_unique_id"
            {% endif %}
        {% endfor %}
    FROM
        {% if use_cte %}
            entity_query
        {% else %}
            {{ left_table_query_string }}
        {% endif %}
    {% endif %}
)

{% if featureviews | length > 0 %}
,
{% endif %}

{% for featureview in featureviews %}

"{{ featureview.name }}__entity_dataframe" AS (
    SELECT
        {% if featureview.entities %}"{{ featureview.entities | join('", "') }}",{% endif %}
        entity_timestamp,
        "{{featureview.name}}__entity_row_unique_id"
    FROM entity_dataframe
    GROUP BY
        {% if featureview.entities %}"{{ featureview.entities | join('", "')}}",{% endif %}
        entity_timestamp,
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
        "{{ featureview.timestamp_field }}" as event_timestamp,
        {{ '"' ~ featureview.created_timestamp_column ~ '" as created_timestamp,' if featureview.created_timestamp_column else '' }}
        {{ featureview.entity_selections | join(', ')}}{% if featureview.entity_selections %},{% else %}{% endif %}
        {% for feature in featureview.features %}
            "{{ feature }}" as {% if full_feature_names %}"{{ featureview.name }}__{{featureview.field_mapping.get(feature, feature)}}"{% else %}"{{ featureview.field_mapping.get(feature, feature) }}"{% endif %}{% if loop.last %}{% else %}, {% endif %}
        {% endfor %}
    FROM {{ featureview.table_subquery }} {{ table_alias('sub') }}
    WHERE "{{ featureview.timestamp_field }}" <= (SELECT MAX(entity_timestamp) FROM entity_dataframe)
    {% if featureview.ttl == 0 %}{% else %}
    AND "{{ featureview.timestamp_field }}" >= {{ timestamp_minus_seconds('(SELECT MIN(entity_timestamp) FROM entity_dataframe)', featureview.ttl) }}
    {% endif %}
),

"{{ featureview.name }}__base" AS (
    SELECT
        subquery.*,
        entity_dataframe.entity_timestamp,
        entity_dataframe."{{featureview.name}}__entity_row_unique_id"
    FROM "{{ featureview.name }}__subquery" {{ table_alias('subquery') }}
    INNER JOIN "{{ featureview.name }}__entity_dataframe" {{ table_alias('entity_dataframe') }}
    {{ on_true() }}
        AND subquery.event_timestamp <= entity_dataframe.entity_timestamp

        {% if featureview.ttl == 0 %}{% else %}
        AND subquery.event_timestamp >= {{ timestamp_minus_seconds('entity_dataframe.entity_timestamp', featureview.ttl) }}
        {% endif %}

        {% for entity in featureview.entities %}
        AND subquery."{{ entity }}" = entity_dataframe."{{ entity }}"
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
        event_timestamp,
        MAX(created_timestamp) as created_timestamp
    FROM "{{ featureview.name }}__base"
    GROUP BY "{{featureview.name}}__entity_row_unique_id", event_timestamp
),
{% endif %}

/*
 3. The data has been filtered during the first CTE "*__base"
 Thus we only need to compute the latest timestamp of each feature.
*/
"{{ featureview.name }}__latest" AS (
    SELECT
        event_timestamp,
        {% if featureview.created_timestamp_column %}created_timestamp,{% endif %}
        "{{featureview.name}}__entity_row_unique_id"
    FROM
    (
        {% if dialect == 'oracle' %}
        {# Oracle doesn't support SELECT *, additional_col - must use table alias #}
        SELECT base_inner.event_timestamp, {% if featureview.created_timestamp_column %}base_inner.created_timestamp, {% endif %}base_inner."{{featureview.name}}__entity_row_unique_id",
            ROW_NUMBER() OVER(
                PARTITION BY base_inner."{{featureview.name}}__entity_row_unique_id"
                ORDER BY base_inner.event_timestamp DESC{% if featureview.created_timestamp_column %}, base_inner.created_timestamp DESC{% endif %}
            ) AS rn
        FROM "{{ featureview.name }}__base" base_inner
        {% if featureview.created_timestamp_column %}
            INNER JOIN "{{ featureview.name }}__dedup" dedup
            ON base_inner."{{featureview.name}}__entity_row_unique_id" = dedup."{{featureview.name}}__entity_row_unique_id" AND base_inner.event_timestamp = dedup.event_timestamp AND base_inner.created_timestamp = dedup.created_timestamp
        {% endif %}
        {% else %}
        SELECT *,
            ROW_NUMBER() OVER(
                PARTITION BY "{{featureview.name}}__entity_row_unique_id"
                ORDER BY event_timestamp DESC{% if featureview.created_timestamp_column %},created_timestamp DESC{% endif %}
            ) AS rn
        FROM "{{ featureview.name }}__base" {{ table_alias('base_inner') }}
        {% if featureview.created_timestamp_column %}
            INNER JOIN "{{ featureview.name }}__dedup" {{ table_alias('dedup') }}
            USING ("{{featureview.name}}__entity_row_unique_id", event_timestamp, created_timestamp)
        {% endif %}
        {% endif %}
    ) {{ table_alias('sub') }}
    WHERE rn = 1
),

/*
 4. Once we know the latest value of each feature for a given timestamp,
 we can join again the data back to the original "base" dataset
*/
"{{ featureview.name }}__cleaned" AS (
    SELECT base.*
    FROM "{{ featureview.name }}__base" {{ table_alias('base') }}
    INNER JOIN "{{ featureview.name }}__latest" {{ table_alias('latest') }}
    {% if dialect == 'oracle' %}ON base."{{featureview.name}}__entity_row_unique_id" = latest."{{featureview.name}}__entity_row_unique_id" AND base.event_timestamp = latest.event_timestamp{% if featureview.created_timestamp_column %} AND base.created_timestamp = latest.created_timestamp{% endif %}{% else %}USING(
        "{{featureview.name}}__entity_row_unique_id",
        event_timestamp
        {% if featureview.created_timestamp_column %}
            ,created_timestamp
        {% endif %}
    ){% endif %}
){% if loop.last %}{% else %}, {% endif %}


{% endfor %}
/*
 Joins the outputs of multiple time travel joins to a single table.
 The entity_dataframe dataset being our source of truth here.
 */

SELECT "{{ final_output_feature_names | join('", "')}}"
FROM entity_dataframe
{% for featureview in featureviews %}
LEFT JOIN (
    SELECT
        "{{featureview.name}}__entity_row_unique_id"
        {% for feature in featureview.features %}
            ,"{% if full_feature_names %}{{ featureview.name }}__{{featureview.field_mapping.get(feature, feature)}}{% else %}{{ featureview.field_mapping.get(feature, feature) }}{% endif %}"
        {% endfor %}
    FROM "{{ featureview.name }}__cleaned"
) {{ table_alias('"' ~ featureview.name ~ '"') }} {% if dialect == 'oracle' %}ON entity_dataframe."{{featureview.name}}__entity_row_unique_id" = "{{ featureview.name }}"."{{featureview.name}}__entity_row_unique_id"{% else %}USING ("{{featureview.name}}__entity_row_unique_id"){% endif %}
{% endfor %}
{% endif %}
"""
