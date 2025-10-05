import os
import tempfile
import uuid
import warnings
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    List,
    Optional,
    Tuple,
    Union,
    cast,
)

if TYPE_CHECKING:
    from feast.saved_dataset import ValidationReference

import numpy as np
import pandas
import pandas as pd
import pyarrow
import pyarrow.parquet as pq
import pyspark
from pydantic import StrictStr
from pyspark import SparkConf
from pyspark.sql import SparkSession

from feast import FeatureView, OnDemandFeatureView
from feast.data_source import DataSource
from feast.dataframe import DataFrameEngine, FeastDataFrame
from feast.errors import EntitySQLEmptyResults, InvalidEntityType
from feast.feature_view import DUMMY_ENTITY_ID, DUMMY_ENTITY_VAL
from feast.infra.offline_stores import offline_utils
from feast.infra.offline_stores.contrib.spark_offline_store.spark_source import (
    SavedDatasetSparkStorage,
    SparkSource,
)
from feast.infra.offline_stores.offline_store import (
    OfflineStore,
    RetrievalJob,
    RetrievalMetadata,
)
from feast.infra.offline_stores.offline_utils import get_timestamp_filter_sql
from feast.infra.registry.base_registry import BaseRegistry
from feast.repo_config import FeastConfigBaseModel, RepoConfig
from feast.saved_dataset import SavedDatasetStorage
from feast.type_map import spark_schema_to_np_dtypes
from feast.utils import _get_fields_with_aliases

# Make sure spark warning doesn't raise more than once.
warnings.simplefilter("once", RuntimeWarning)


class SparkOfflineStoreConfig(FeastConfigBaseModel):
    type: StrictStr = "spark"
    """ Offline store type selector"""

    spark_conf: Optional[Dict[str, str]] = None
    """ Configuration overlay for the spark session """

    staging_location: Optional[StrictStr] = None
    """ Remote path for batch materialization jobs"""

    region: Optional[StrictStr] = None
    """ AWS Region if applicable for s3-based staging locations"""


@dataclass(frozen=True)
class SparkFeatureViewQueryContext(offline_utils.FeatureViewQueryContext):
    min_date_partition: Optional[str]
    max_date_partition: str


class SparkOfflineStore(OfflineStore):
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
        spark_session = get_spark_session_or_start_new_with_repoconfig(
            config.offline_store
        )
        assert isinstance(config.offline_store, SparkOfflineStoreConfig)
        assert isinstance(data_source, SparkSource)

        warnings.warn(
            "The spark offline store is an experimental feature in alpha development. "
            "Some functionality may still be unstable so functionality can change in the future.",
            RuntimeWarning,
        )

        print("Pulling latest features from spark offline store")

        from_expression = data_source.get_table_query_string()

        partition_by_join_key_string = ", ".join(join_key_columns)
        if partition_by_join_key_string != "":
            partition_by_join_key_string = (
                "PARTITION BY " + partition_by_join_key_string
            )
        timestamps = [timestamp_field]
        if created_timestamp_column:
            timestamps.append(created_timestamp_column)
        timestamp_desc_string = " DESC, ".join(timestamps) + " DESC"
        (fields_with_aliases, aliases) = _get_fields_with_aliases(
            fields=join_key_columns + feature_name_columns + timestamps,
            field_mappings=data_source.field_mapping,
        )

        fields_as_string = ", ".join(fields_with_aliases)
        aliases_as_string = ", ".join(aliases)

        date_partition_column = data_source.date_partition_column
        date_partition_column_format = data_source.date_partition_column_format

        start_date_str = _format_datetime(start_date)
        end_date_str = _format_datetime(end_date)
        query = f"""
                SELECT
                    {aliases_as_string}
                    {f", {repr(DUMMY_ENTITY_VAL)} AS {DUMMY_ENTITY_ID}" if not join_key_columns else ""}
                FROM (
                    SELECT {fields_as_string},
                    ROW_NUMBER() OVER({partition_by_join_key_string} ORDER BY {timestamp_desc_string}) AS feast_row_
                    FROM {from_expression} t1
                    WHERE {timestamp_field} BETWEEN TIMESTAMP('{start_date_str}') AND TIMESTAMP('{end_date_str}'){" AND " + date_partition_column + " >= '" + start_date.strftime(date_partition_column_format) + "' AND " + date_partition_column + " <= '" + end_date.strftime(date_partition_column_format) + "' " if date_partition_column != "" and date_partition_column is not None else ""}
                ) t2
                WHERE feast_row_ = 1
                """

        return SparkRetrievalJob(
            spark_session=spark_session,
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
        entity_df: Union[pandas.DataFrame, str, pyspark.sql.DataFrame],
        registry: BaseRegistry,
        project: str,
        full_feature_names: bool = False,
    ) -> RetrievalJob:
        assert isinstance(config.offline_store, SparkOfflineStoreConfig)
        date_partition_column_formats = []
        for fv in feature_views:
            assert isinstance(fv.batch_source, SparkSource)
            date_partition_column_formats.append(
                fv.batch_source.date_partition_column_format
            )

        warnings.warn(
            "The spark offline store is an experimental feature in alpha development. "
            "Some functionality may still be unstable so functionality can change in the future.",
            RuntimeWarning,
        )

        spark_session = get_spark_session_or_start_new_with_repoconfig(
            store_config=config.offline_store
        )
        tmp_entity_df_table_name = offline_utils.get_temp_entity_table_name()

        entity_schema = _get_entity_schema(
            spark_session=spark_session,
            entity_df=entity_df,
        )
        event_timestamp_col = offline_utils.infer_event_timestamp_from_entity_df(
            entity_schema=entity_schema,
        )
        entity_df_event_timestamp_range = _get_entity_df_event_timestamp_range(
            entity_df,
            event_timestamp_col,
            spark_session,
        )
        _upload_entity_df(
            spark_session=spark_session,
            table_name=tmp_entity_df_table_name,
            entity_df=entity_df,
            event_timestamp_col=event_timestamp_col,
        )

        expected_join_keys = offline_utils.get_expected_join_keys(
            project=project, feature_views=feature_views, registry=registry
        )
        offline_utils.assert_expected_columns_in_entity_df(
            entity_schema=entity_schema,
            join_keys=expected_join_keys,
            entity_df_event_timestamp_col=event_timestamp_col,
        )

        query_context = offline_utils.get_feature_view_query_context(
            feature_refs,
            feature_views,
            registry,
            project,
            entity_df_event_timestamp_range,
        )

        spark_query_context = [
            SparkFeatureViewQueryContext(
                **asdict(context),
                min_date_partition=datetime.fromisoformat(
                    context.min_event_timestamp
                ).strftime(date_format)
                if context.min_event_timestamp is not None
                else None,
                max_date_partition=datetime.fromisoformat(
                    context.max_event_timestamp
                ).strftime(date_format),
            )
            for date_format, context in zip(
                date_partition_column_formats, query_context
            )
        ]

        query = offline_utils.build_point_in_time_query(
            feature_view_query_contexts=cast(
                List[offline_utils.FeatureViewQueryContext], spark_query_context
            ),
            left_table_query_string=tmp_entity_df_table_name,
            entity_df_event_timestamp_col=event_timestamp_col,
            entity_df_columns=entity_schema.keys(),
            query_template=MULTIPLE_FEATURE_VIEW_POINT_IN_TIME_JOIN,
            full_feature_names=full_feature_names,
        )

        return SparkRetrievalJob(
            spark_session=spark_session,
            query=query,
            full_feature_names=full_feature_names,
            on_demand_feature_views=OnDemandFeatureView.get_requested_odfvs(
                feature_refs, project, registry
            ),
            metadata=RetrievalMetadata(
                features=feature_refs,
                keys=list(set(entity_schema.keys()) - {event_timestamp_col}),
                min_event_timestamp=entity_df_event_timestamp_range[0],
                max_event_timestamp=entity_df_event_timestamp_range[1],
            ),
            config=config,
        )

    @staticmethod
    def offline_write_batch(
        config: RepoConfig,
        feature_view: FeatureView,
        table: pyarrow.Table,
        progress: Optional[Callable[[int], Any]],
    ):
        assert isinstance(config.offline_store, SparkOfflineStoreConfig)
        assert isinstance(feature_view.batch_source, SparkSource)

        pa_schema, column_names = offline_utils.get_pyarrow_schema_from_batch_source(
            config, feature_view.batch_source
        )
        if column_names != table.column_names:
            raise ValueError(
                f"The input pyarrow table has schema {table.schema} with the incorrect columns {table.column_names}. "
                f"The schema is expected to be {pa_schema} with the columns (in this exact order) to be {column_names}."
            )

        spark_session = get_spark_session_or_start_new_with_repoconfig(
            store_config=config.offline_store
        )

        if feature_view.batch_source.path:
            # write data to disk so that it can be loaded into spark (for preserving column types)
            with tempfile.NamedTemporaryFile(suffix=".parquet") as tmp_file:
                print(tmp_file.name)
                pq.write_table(table, tmp_file.name)

                # load data
                df_batch = spark_session.read.parquet(tmp_file.name)

                # load existing data to get spark table schema
                df_existing = spark_session.read.format(
                    feature_view.batch_source.file_format
                ).load(feature_view.batch_source.path)

                # cast columns if applicable
                df_batch = _cast_data_frame(df_batch, df_existing)

                df_batch.write.format(feature_view.batch_source.file_format).mode(
                    "append"
                ).save(feature_view.batch_source.path)
        elif feature_view.batch_source.query:
            raise NotImplementedError(
                "offline_write_batch not implemented for batch sources specified by query"
            )
        else:
            raise NotImplementedError(
                "offline_write_batch not implemented for batch sources specified by a table"
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
        Note that join_key_columns, feature_name_columns, timestamp_field, and
        created_timestamp_column have all already been mapped to column names of the
        source table and those column names are the values passed into this function.
        """
        assert isinstance(config.offline_store, SparkOfflineStoreConfig)
        assert isinstance(data_source, SparkSource)
        warnings.warn(
            "The spark offline store is an experimental feature in alpha development. "
            "This API is unstable and it could and most probably will be changed in the future.",
            RuntimeWarning,
        )

        spark_session = get_spark_session_or_start_new_with_repoconfig(
            store_config=config.offline_store
        )

        timestamp_fields = [timestamp_field]
        if created_timestamp_column:
            timestamp_fields.append(created_timestamp_column)
        (fields_with_aliases, aliases) = _get_fields_with_aliases(
            fields=join_key_columns + feature_name_columns + timestamp_fields,
            field_mappings=data_source.field_mapping,
        )

        fields_with_alias_string = ", ".join(fields_with_aliases)

        from_expression = data_source.get_table_query_string()
        timestamp_filter = get_timestamp_filter_sql(
            start_date, end_date, timestamp_field, tz=timezone.utc, quote_fields=False
        )

        query = f"""
            SELECT {fields_with_alias_string}
            FROM {from_expression}
            WHERE {timestamp_filter}
        """

        return SparkRetrievalJob(
            spark_session=spark_session,
            query=query,
            full_feature_names=False,
            config=config,
        )


class SparkRetrievalJob(RetrievalJob):
    def __init__(
        self,
        spark_session: SparkSession,
        query: str,
        full_feature_names: bool,
        config: RepoConfig,
        on_demand_feature_views: Optional[List[OnDemandFeatureView]] = None,
        metadata: Optional[RetrievalMetadata] = None,
    ):
        super().__init__()
        self.spark_session = spark_session
        self.query = query
        self._full_feature_names = full_feature_names
        self._on_demand_feature_views = on_demand_feature_views or []
        self._metadata = metadata
        self._config = config

    @property
    def full_feature_names(self) -> bool:
        return self._full_feature_names

    @property
    def on_demand_feature_views(self) -> List[OnDemandFeatureView]:
        return self._on_demand_feature_views

    def to_spark_df(self) -> pyspark.sql.DataFrame:
        statements = self.query.split("---EOS---")
        *_, last = map(self.spark_session.sql, statements)
        return last

    def _to_df_internal(self, timeout: Optional[int] = None) -> pd.DataFrame:
        """Return dataset as Pandas DataFrame synchronously"""
        return self.to_spark_df().toPandas()

    def _to_arrow_internal(self, timeout: Optional[int] = None) -> pyarrow.Table:
        """Return dataset as pyarrow Table synchronously"""
        return pyarrow.Table.from_pandas(self._to_df_internal(timeout=timeout))

    def to_feast_df(
        self,
        validation_reference: Optional["ValidationReference"] = None,
        timeout: Optional[int] = None,
    ) -> FeastDataFrame:
        """
        Return the result as a FeastDataFrame with Spark engine.

        This preserves Spark's lazy execution by wrapping the Spark DataFrame directly.
        """
        # Get the Spark DataFrame directly (maintains lazy execution)
        spark_df = self.to_spark_df()
        return FeastDataFrame(
            data=spark_df,
            engine=DataFrameEngine.SPARK,
        )

    def persist(
        self,
        storage: SavedDatasetStorage,
        allow_overwrite: Optional[bool] = False,
        timeout: Optional[int] = None,
    ):
        """
        Run the retrieval and persist the results in the same offline store used for read.
        Please note the persisting is done only within the scope of the spark session for local warehouse directory.
        """
        assert isinstance(storage, SavedDatasetSparkStorage)
        table_name = storage.spark_options.table
        if not table_name:
            raise ValueError("Cannot persist, table_name is not defined")
        if self._has_remote_warehouse_in_config():
            file_format = storage.spark_options.file_format
            if not file_format:
                self.to_spark_df().write.saveAsTable(table_name)
            else:
                self.to_spark_df().write.format(file_format).saveAsTable(table_name)
        else:
            self.to_spark_df().createOrReplaceTempView(table_name)

    def _has_remote_warehouse_in_config(self) -> bool:
        """
        Check if Spark Session config has info about hive metastore uri
        or warehouse directory is not a local path
        """
        self.spark_session.sparkContext.getConf().getAll()
        try:
            self.spark_session.conf.get("hive.metastore.uris")
            return True
        except Exception:
            warehouse_dir = self.spark_session.conf.get("spark.sql.warehouse.dir")
            if warehouse_dir and warehouse_dir.startswith("file:"):
                return False
            else:
                return True

    def supports_remote_storage_export(self) -> bool:
        return self._config.offline_store.staging_location is not None

    def to_remote_storage(self) -> List[str]:
        """Currently only works for local and s3-based staging locations"""
        if self.supports_remote_storage_export():
            sdf: pyspark.sql.DataFrame = self.to_spark_df()

            if self._config.offline_store.staging_location.startswith("/"):
                local_file_staging_location = os.path.abspath(
                    self._config.offline_store.staging_location
                )

                # write to staging location
                output_uri = os.path.join(
                    str(local_file_staging_location), str(uuid.uuid4())
                )
                sdf.write.parquet(output_uri)

                return _list_files_in_folder(output_uri)
            elif self._config.offline_store.staging_location.startswith("s3://"):
                from feast.infra.utils import aws_utils

                spark_compatible_s3_staging_location = (
                    self._config.offline_store.staging_location.replace(
                        "s3://", "s3a://"
                    )
                )

                # write to staging location
                output_uri = os.path.join(
                    str(spark_compatible_s3_staging_location), str(uuid.uuid4())
                )
                sdf.write.parquet(output_uri)

                return aws_utils.list_s3_files(
                    self._config.offline_store.region, output_uri
                )
            elif self._config.offline_store.staging_location.startswith("hdfs://"):
                output_uri = os.path.join(
                    self._config.offline_store.staging_location, str(uuid.uuid4())
                )
                sdf.write.parquet(output_uri)
                spark_session = get_spark_session_or_start_new_with_repoconfig(
                    store_config=self._config.offline_store
                )
                return _list_hdfs_files(spark_session, output_uri)
            else:
                raise NotImplementedError(
                    "to_remote_storage is only implemented for file://, s3:// and hdfs:// uri schemes"
                )

        else:
            raise NotImplementedError()

    @property
    def metadata(self) -> Optional[RetrievalMetadata]:
        """
        Return metadata information about retrieval.
        Should be available even before materializing the dataset itself.
        """
        return self._metadata


def get_spark_session_or_start_new_with_repoconfig(
    store_config: SparkOfflineStoreConfig,
) -> SparkSession:
    spark_session = SparkSession.getActiveSession()
    if not spark_session:
        spark_builder = SparkSession.builder
        spark_conf = store_config.spark_conf
        if spark_conf:
            spark_builder = spark_builder.config(
                conf=SparkConf().setAll([(k, v) for k, v in spark_conf.items()])
            )

        spark_session = spark_builder.getOrCreate()
    spark_session.conf.set("spark.sql.parser.quotedRegexColumnNames", "true")
    return spark_session


def _get_entity_df_event_timestamp_range(
    entity_df: Union[pd.DataFrame, str],
    entity_df_event_timestamp_col: str,
    spark_session: SparkSession,
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
    elif isinstance(entity_df, str) or isinstance(entity_df, pyspark.sql.DataFrame):
        # If the entity_df is a string (SQL query), determine range
        # from table
        if isinstance(entity_df, str):
            df = spark_session.sql(entity_df).select(entity_df_event_timestamp_col)
            # Checks if executing entity sql resulted in any data
            if df.rdd.isEmpty():
                raise EntitySQLEmptyResults(entity_df)
        else:
            df = entity_df
        # TODO(kzhang132): need utc conversion here.

        entity_df_event_timestamp_range = (
            df.agg({entity_df_event_timestamp_col: "min"}).collect()[0][0],
            df.agg({entity_df_event_timestamp_col: "max"}).collect()[0][0],
        )
    else:
        raise InvalidEntityType(type(entity_df))

    return entity_df_event_timestamp_range


def _get_entity_schema(
    spark_session: SparkSession, entity_df: Union[pandas.DataFrame, str]
) -> Dict[str, np.dtype]:
    if isinstance(entity_df, pd.DataFrame):
        return dict(zip(entity_df.columns, entity_df.dtypes))
    elif isinstance(entity_df, str) or isinstance(entity_df, pyspark.sql.DataFrame):
        if isinstance(entity_df, str):
            entity_spark_df = spark_session.sql(entity_df)
        else:
            entity_spark_df = entity_df
        return dict(
            zip(
                entity_spark_df.columns,
                spark_schema_to_np_dtypes(entity_spark_df.dtypes),
            )
        )
    else:
        raise InvalidEntityType(type(entity_df))


def _upload_entity_df(
    spark_session: SparkSession,
    table_name: str,
    entity_df: Union[pandas.DataFrame, str],
    event_timestamp_col: str,
) -> None:
    if isinstance(entity_df, pd.DataFrame):
        entity_df[event_timestamp_col] = pd.to_datetime(
            entity_df[event_timestamp_col], utc=True
        )
        spark_session.createDataFrame(entity_df).createOrReplaceTempView(table_name)
    elif isinstance(entity_df, str):
        spark_session.sql(entity_df).createOrReplaceTempView(table_name)
    elif isinstance(entity_df, pyspark.sql.DataFrame):
        entity_df.createOrReplaceTempView(table_name)
    else:
        raise InvalidEntityType(type(entity_df))


def _format_datetime(t: datetime) -> str:
    # Since Hive does not support timezone, need to transform to utc.
    if t.tzinfo:
        t = t.astimezone(tz=timezone.utc)
    dt = t.strftime("%Y-%m-%d %H:%M:%S.%f")
    return dt


def _list_files_in_folder(folder):
    """List full filenames in a folder"""
    files = []
    for file in os.listdir(folder):
        filename = os.path.join(folder, file)
        if os.path.isfile(filename):
            files.append(filename)

    return files


def _list_hdfs_files(spark_session: SparkSession, uri: str) -> List[str]:
    jvm = spark_session._jvm
    jsc = spark_session._jsc
    if jvm is None or jsc is None:
        raise RuntimeError("Spark JVM or JavaSparkContext is not available")
    conf = jsc.hadoopConfiguration()
    path = jvm.org.apache.hadoop.fs.Path(uri)
    fs = jvm.org.apache.hadoop.fs.FileSystem.get(path.toUri(), conf)
    statuses = fs.listStatus(path)
    files = []
    for f in statuses:
        if f.isFile():
            files.append(f.getPath().toString())
    return files


def _cast_data_frame(
    df_new: pyspark.sql.DataFrame, df_existing: pyspark.sql.DataFrame
) -> pyspark.sql.DataFrame:
    """Convert new dataframe's columns to the same types as existing dataframe while preserving the order of columns"""
    existing_dtypes = {k: v for k, v in df_existing.dtypes}
    new_dtypes = {k: v for k, v in df_new.dtypes}

    select_expression = []
    for col, new_type in new_dtypes.items():
        existing_type = existing_dtypes[col]
        if new_type != existing_type:
            select_expression.append(f"cast({col} as {existing_type}) as {col}")
        else:
            select_expression.append(col)

    return df_new.selectExpr(*select_expression)


MULTIPLE_FEATURE_VIEW_POINT_IN_TIME_JOIN = """
/*
 Compute a deterministic hash for the `left_table_query_string` that will be used throughout
 all the logic as the field to GROUP BY the data
*/
CREATE OR REPLACE TEMPORARY VIEW entity_dataframe AS (
    SELECT *,
        {{entity_df_event_timestamp_col}} AS entity_timestamp
        {% for featureview in featureviews %}
            ,CONCAT(
                {% for entity in featureview.entities %}
                    CAST({{entity}} AS STRING),
                {% endfor %}
                CAST({{entity_df_event_timestamp_col}} AS STRING)
            ) AS {{featureview.name}}__entity_row_unique_id
        {% endfor %}
    FROM {{ left_table_query_string }}
);

---EOS---

{% for featureview in featureviews %}

CREATE OR REPLACE TEMPORARY VIEW {{ featureview.name }}__cleaned AS (

    WITH {{ featureview.name }}__entity_dataframe AS (
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
        {% if featureview.date_partition_column != "" and featureview.date_partition_column is not none %}
        AND {{ featureview.date_partition_column }} <= '{{ featureview.max_date_partition }}'
        {% endif %}

        {% if featureview.ttl == 0 %}{% else %}
        AND {{ featureview.timestamp_field }} >= '{{ featureview.min_event_timestamp }}'
        {% if featureview.date_partition_column != "" and featureview.date_partition_column is not none %}
          AND {{ featureview.date_partition_column }} >= '{{ featureview.min_date_partition }}'
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
    )

    /*
     4. Once we know the latest value of each feature for a given timestamp,
     we can join again the data back to the original "base" dataset
    */
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
)

---EOS---

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
