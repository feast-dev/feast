from types import MethodType
from typing import List, Optional, Set, Union, no_type_check

import pandas as pd
import pyarrow
from pyspark import SparkContext
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.column import Column, _to_java_column
from pyspark.sql.functions import col, from_json
from pyspark.sql.streaming import StreamingQuery

from feast import FeatureView
from feast.data_format import AvroFormat, ConfluentAvroFormat, JsonFormat, StreamFormat
from feast.data_source import KafkaSource, PushMode
from feast.feature_store import FeatureStore
from feast.infra.contrib.stream_processor import (
    ProcessorConfig,
    StreamProcessor,
    StreamTable,
)
from feast.infra.materialization.contrib.spark.spark_materialization_engine import (
    _SparkSerializedArtifacts,
)
from feast.stream_feature_view import StreamFeatureView
from feast.utils import _convert_arrow_to_proto, _run_pyarrow_field_mapping


class SparkProcessorConfig(ProcessorConfig):
    """spark_kafka_options, schema_registry_config and checkpoint_location are only used for ConfluentAvroFormat"""

    spark_session: SparkSession
    processing_time: str
    query_timeout: Optional[int]
    spark_kafka_options: Optional[dict]
    schema_registry_config: Optional[dict]
    checkpoint_location: Optional[str]


def _from_confluent_avro(column: Column, abris_config) -> Column:
    jvm_gateway = SparkContext._active_spark_context._gateway.jvm  # type: ignore
    abris_avro = jvm_gateway.za.co.absa.abris.avro

    return Column(abris_avro.functions.from_avro(_to_java_column(column), abris_config))


def _to_abris_config(
    schema_registry_config: dict,
    record_name: str,
    record_namespace: str,
):
    """:return: za.co.absa.abris.config.FromAvroConfig"""
    topic = schema_registry_config["schema.registry.topic"]

    jvm_gateway = SparkContext._active_spark_context._gateway.jvm  # type: ignore
    scala_map = jvm_gateway.PythonUtils.toScalaMap(schema_registry_config)

    return (
        jvm_gateway.za.co.absa.abris.config.AbrisConfig.fromConfluentAvro()
        .downloadReaderSchemaByLatestVersion()
        .andTopicRecordNameStrategy(topic, record_name, record_namespace)
        .usingSchemaRegistry(scala_map)
    )


class SparkKafkaProcessor(StreamProcessor):
    spark: SparkSession
    format: StreamFormat
    preprocess_fn: Optional[MethodType]
    join_keys: List[str]

    def __init__(
        self,
        *,
        fs: FeatureStore,
        sfv: Union[StreamFeatureView, FeatureView],
        config: ProcessorConfig,
        preprocess_fn: Optional[MethodType] = None,
    ):
        if not isinstance(sfv.stream_source, KafkaSource):
            raise ValueError("data source is not kafka source")

        if type(sfv.stream_source.kafka_options.message_format) not in {
            AvroFormat,
            JsonFormat,
            ConfluentAvroFormat,
        }:
            raise ValueError(
                "Spark Streaming's Kafka source format must be one of {AvroFormat, JsonFormat, ConfluentAvroFormat}"
            )

        self.format = sfv.stream_source.kafka_options.message_format

        if not isinstance(config, SparkProcessorConfig):
            raise ValueError("config is not spark processor config")

        self.spark = config.spark_session
        self.preprocess_fn = preprocess_fn
        self.processing_time = config.processing_time
        self.query_timeout = config.query_timeout
        self.spark_kafka_options = config.spark_kafka_options
        self.schema_registry_config = config.schema_registry_config
        self.checkpoint_location = (
            config.checkpoint_location
            if config.checkpoint_location is not None
            else "/tmp/checkpoint/"
        )
        self.join_keys = [fs.get_entity(entity).join_key for entity in sfv.entities]
        self.spark_serialized_artifacts = _SparkSerializedArtifacts.serialize(
            feature_view=sfv, repo_config=fs.config
        )
        super().__init__(fs=fs, sfv=sfv, data_source=sfv.stream_source)

        # Type hinting for data_source type.
        # data_source type has been checked to be an instance of KafkaSource.
        self.data_source: KafkaSource = self.data_source  # type: ignore

    def ingest_stream_feature_view(
        self, to: PushMode = PushMode.ONLINE
    ) -> StreamingQuery:
        ingested_stream_df = self._ingest_stream_data()
        transformed_df = self._construct_transformation_plan(ingested_stream_df)
        if self.fs.config.provider == "expedia":
            online_store_query = self._write_stream_data_expedia(transformed_df, to)
        else:
            online_store_query = self._write_stream_data(transformed_df, to)
        return online_store_query

    # In the line 116 of __init__(), the "data_source" is assigned a stream_source (and has to be KafkaSource as in line 80).
    @no_type_check
    def _ingest_stream_data(self) -> StreamTable:
        """Only supports json and avro formats currently."""
        if isinstance(self.format, JsonFormat):
            stream_df = (
                self.spark.readStream.format("kafka")
                .option(
                    "kafka.bootstrap.servers",
                    self.data_source.kafka_options.kafka_bootstrap_servers,
                )
                .option("subscribe", self.data_source.kafka_options.topic)
                .option("startingOffsets", "latest")  # Query start
                .load()
                .selectExpr("CAST(value AS STRING)")
                .select(
                    from_json(
                        col("value"),
                        self.data_source.kafka_options.message_format.schema_json,
                    ).alias("table")
                )
                .select("table.*")
            )
        elif isinstance(self.format, ConfluentAvroFormat):
            # Need Abris jar dependency to read Confluent Avro format along with schema registry integration
            if self.schema_registry_config is None:
                raise ValueError(
                    "schema_registry_config is required for ConfluentAvroFormat"
                )
            spark_kafka_options = self.spark_kafka_options or {
                "kafka.bootstrap.servers": self.data_source.kafka_options.kafka_bootstrap_servers,
                "subscribe": self.data_source.kafka_options.topic,
                "startingOffsets": "latest",
            }
            stream_df = (
                self.spark.readStream.format("kafka")
                .options(**spark_kafka_options)
                .load()
                .select(
                    _from_confluent_avro(
                        col("value"),
                        _to_abris_config(
                            self.schema_registry_config,
                            self.data_source.kafka_options.message_format.record_name,
                            self.data_source.kafka_options.message_format.record_namespace,
                        ),
                    ).alias("table")
                )
                .select("table.*")
            )
        else:  # AvroFormat
            stream_df = (
                self.spark.readStream.format("kafka")
                .option(
                    "kafka.bootstrap.servers",
                    self.data_source.kafka_options.kafka_bootstrap_servers,
                )
                .option("subscribe", self.data_source.kafka_options.topic)
                .option("startingOffsets", "latest")  # Query start
                .load()
                .selectExpr("CAST(value AS STRING)")
                .select(
                    from_avro(
                        col("value"),
                        self.data_source.kafka_options.message_format.schema_json,
                    ).alias("table")
                )
                .select("table.*")
            )
        return stream_df

    def _construct_transformation_plan(self, df: StreamTable) -> StreamTable:
        if isinstance(self.sfv, FeatureView):
            return df
        elif isinstance(self.sfv, StreamFeatureView):
            return self.sfv.udf.__call__(df) if self.sfv.udf else df

    def _write_stream_data_expedia(self, df: StreamTable, to: PushMode):
        """
        Ensures materialization logic in sync with stream ingestion.
        Support only write to online store. No support for preprocess_fn also.
        In Spark 3.2.2, toPandas() is throwing error when the dataframe has Boolean columns.
        To fix this error, we need spark 3.4.0 or numpy < 1.20.0 but feast needs numpy >= 1.22.
        Switching to use mapInPandas to solve the problem for boolean columns and
        toPandas() also load all data into driver's memory.
        Error Message:
            AttributeError: module 'numpy' has no attribute 'bool'.
            `np.bool` was a deprecated alias for the builtin `bool`.
            To avoid this error in existing code, use `bool` by itself.
            Doing this will not modify any behavior and is safe.
            If you specifically wanted the numpy scalar type, use `np.bool_` here.
        """

        # TODO: Support writing to offline store and preprocess_fn. Remove _write_stream_data method

        # Validation occurs at the fs.write_to_online_store() phase against the stream feature view schema.
        def batch_write_pandas_df(iterator, spark_serialized_artifacts, join_keys):
            for pdf in iterator:
                (
                    feature_view,
                    online_store,
                    repo_config,
                ) = spark_serialized_artifacts.unserialize()

                if isinstance(feature_view, StreamFeatureView):
                    ts_field = feature_view.timestamp_field
                else:
                    ts_field = feature_view.stream_source.timestamp_field

                # Extract the latest feature values for each unique entity row (i.e. the join keys).
                pdf = (
                    pdf.sort_values(by=[*join_keys, ts_field], ascending=False)
                    .groupby(join_keys)
                    .nth(0)
                )

                table = pyarrow.Table.from_pandas(pdf)
                if feature_view.batch_source.field_mapping is not None:
                    table = _run_pyarrow_field_mapping(
                        table, feature_view.batch_source.field_mapping
                    )

                join_key_to_value_type = {
                    entity.name: entity.dtype.to_value_type()
                    for entity in feature_view.entity_columns
                }
                rows_to_write = _convert_arrow_to_proto(
                    table, feature_view, join_key_to_value_type
                )
                online_store.online_write_batch(
                    repo_config,
                    feature_view,
                    rows_to_write,
                    lambda x: None,
                )

            yield pd.DataFrame([pd.Series(range(1, 2))])  # dummy result

        def batch_write(
            sdf: DataFrame,
            batch_id: int,
            spark_serialized_artifacts,
            join_keys,
            feature_view,
        ):
            drop_list: List[str] = []
            fv_schema: Set[str] = set(
                map(lambda field: field.name, feature_view.schema)
            )
            # Add timestamp field to the schema so we don't delete from dataframe
            if isinstance(feature_view, StreamFeatureView):
                fv_schema.add(feature_view.timestamp_field)
                if feature_view.source.created_timestamp_column:
                    fv_schema.add(feature_view.source.created_timestamp_column)

            if isinstance(feature_view, FeatureView):
                if feature_view.stream_source is not None:
                    fv_schema.add(feature_view.stream_source.timestamp_field)
                    if feature_view.stream_source.created_timestamp_column:
                        fv_schema.add(
                            feature_view.stream_source.created_timestamp_column
                        )
                else:
                    fv_schema.add(feature_view.batch_source.timestamp_field)
                    if feature_view.batch_source.created_timestamp_column:
                        fv_schema.add(
                            feature_view.batch_source.created_timestamp_column
                        )

            for column in df.columns:
                if column not in fv_schema:
                    drop_list.append(column)

            if len(drop_list) > 0:
                print(
                    f"INFO!!! Dropping extra columns in the dataframe: {drop_list}. Avoid unnecessary columns in the dataframe."
                )

            sdf.drop(*drop_list).mapInPandas(
                lambda x: batch_write_pandas_df(
                    x, spark_serialized_artifacts, join_keys
                ),
                "status int",
            ).count()  # dummy action to force evaluation

        query = (
            df.writeStream.outputMode("update")
            .option("checkpointLocation", self.checkpoint_location)
            .trigger(processingTime=self.processing_time)
            .foreachBatch(
                lambda df, batch_id: batch_write(
                    df,
                    batch_id,
                    self.spark_serialized_artifacts,
                    self.join_keys,
                    self.sfv,
                )
            )
            .start()
        )

        query.awaitTermination(timeout=self.query_timeout)
        return query

    def _write_stream_data(self, df: StreamTable, to: PushMode) -> StreamingQuery:
        # Validation occurs at the fs.write_to_online_store() phase against the stream feature view schema.
        def batch_write(row: DataFrame, batch_id: int):
            rows: pd.DataFrame = row.toPandas()

            # Extract the latest feature values for each unique entity row (i.e. the join keys).
            # Also add a 'created' column.
            if isinstance(self.sfv, StreamFeatureView):
                ts_field = self.sfv.timestamp_field
            else:
                ts_field = self.sfv.stream_source.timestamp_field
            rows = (
                rows.sort_values(by=[*self.join_keys, ts_field], ascending=False)
                .groupby(self.join_keys)
                .nth(0)
            )
            # Created column is not used anywhere in the code, but it is added to the dataframe.
            # Expedia provider drops the unused columns from dataframe
            # Commenting this out as it is not used anywhere in the code
            # rows["created"] = pd.to_datetime("now", utc=True)

            # Reset indices to ensure the dataframe has all the required columns.
            rows = rows.reset_index()

            # Optionally execute preprocessor before writing to the online store.
            if self.preprocess_fn:
                rows = self.preprocess_fn(rows)

            # Finally persist the data to the online store and/or offline store.
            if rows.size > 0:
                if to == PushMode.ONLINE or to == PushMode.ONLINE_AND_OFFLINE:
                    self.fs.write_to_online_store(self.sfv.name, rows)
                if to == PushMode.OFFLINE or to == PushMode.ONLINE_AND_OFFLINE:
                    self.fs.write_to_offline_store(self.sfv.name, rows)

        query = (
            df.writeStream.outputMode("update")
            .option("checkpointLocation", self.checkpoint_location)
            .trigger(processingTime=self.processing_time)
            .foreachBatch(batch_write)
            .start()
        )

        query.awaitTermination(timeout=self.query_timeout)
        return query
