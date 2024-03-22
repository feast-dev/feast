from types import MethodType
from typing import List, Optional, Union

import pandas as pd
from pyspark import SparkContext
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.column import Column, _to_java_column
from pyspark.sql.functions import col, from_json

from feast import FeatureView
from feast.data_format import AvroFormat, ConfluentAvroFormat, JsonFormat, StreamFormat
from feast.data_source import KafkaSource, PushMode
from feast.feature_store import FeatureStore
from feast.infra.contrib.stream_processor import (
    ProcessorConfig,
    StreamProcessor,
    StreamTable,
)
from feast.stream_feature_view import StreamFeatureView


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
        super().__init__(fs=fs, sfv=sfv, data_source=sfv.stream_source)

    def ingest_stream_feature_view(self, to: PushMode = PushMode.ONLINE) -> None:
        ingested_stream_df = self._ingest_stream_data()
        transformed_df = self._construct_transformation_plan(ingested_stream_df)
        online_store_query = self._write_stream_data(transformed_df, to)
        return online_store_query

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

    def _write_stream_data(self, df: StreamTable, to: PushMode):
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
