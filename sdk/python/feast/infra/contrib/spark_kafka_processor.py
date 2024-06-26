from types import MethodType
from typing import List, Optional, no_type_check

import pandas as pd
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.functions import col, from_json
from pyspark.sql.streaming import StreamingQuery

from feast.data_format import AvroFormat, JsonFormat
from feast.data_source import KafkaSource, PushMode
from feast.feature_store import FeatureStore
from feast.infra.contrib.stream_processor import (
    ProcessorConfig,
    StreamProcessor,
    StreamTable,
)
from feast.stream_feature_view import StreamFeatureView


class SparkProcessorConfig(ProcessorConfig):
    spark_session: SparkSession
    processing_time: str
    query_timeout: Optional[int] = None


class SparkKafkaProcessor(StreamProcessor):
    spark: SparkSession
    format: str
    preprocess_fn: Optional[MethodType]
    join_keys: List[str]

    def __init__(
        self,
        *,
        fs: FeatureStore,
        sfv: StreamFeatureView,
        config: ProcessorConfig,
        preprocess_fn: Optional[MethodType] = None,
    ):
        if not isinstance(sfv.stream_source, KafkaSource):
            raise ValueError("data source is not kafka source")
        if not isinstance(
            sfv.stream_source.kafka_options.message_format, AvroFormat
        ) and not isinstance(
            sfv.stream_source.kafka_options.message_format, JsonFormat
        ):
            raise ValueError(
                "spark streaming currently only supports json or avro format for kafka source schema"
            )

        self.format = (
            "json"
            if isinstance(sfv.stream_source.kafka_options.message_format, JsonFormat)
            else "avro"
        )

        if not isinstance(config, SparkProcessorConfig):
            raise ValueError("config is not spark processor config")
        self.spark = config.spark_session
        self.preprocess_fn = preprocess_fn
        self.processing_time = config.processing_time
        self.query_timeout = config.query_timeout
        self.join_keys = [fs.get_entity(entity).join_key for entity in sfv.entities]
        super().__init__(fs=fs, sfv=sfv, data_source=sfv.stream_source)

        # Type hinting for data_source type.
        # data_source type has been checked to be an instance of KafkaSource.
        self.data_source: KafkaSource = self.data_source  # type: ignore

    def ingest_stream_feature_view(
        self, to: PushMode = PushMode.ONLINE
    ) -> StreamingQuery:
        ingested_stream_df = self._ingest_stream_data()
        transformed_df = self._construct_transformation_plan(ingested_stream_df)
        online_store_query = self._write_stream_data(transformed_df, to)
        return online_store_query

    # In the line 64 of __init__(), the "data_source" is assigned a stream_source (and has to be KafkaSource as in line 40).
    @no_type_check
    def _ingest_stream_data(self) -> StreamTable:
        """Only supports json and avro formats currently."""
        if self.format == "json":
            if not isinstance(
                self.data_source.kafka_options.message_format, JsonFormat
            ):
                raise ValueError("kafka source message format is not jsonformat")
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
        else:
            if not isinstance(
                self.data_source.kafka_options.message_format, AvroFormat
            ):
                raise ValueError("kafka source message format is not avro format")
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
        return self.sfv.udf.__call__(df) if self.sfv.udf else df

    def _write_stream_data(self, df: StreamTable, to: PushMode) -> StreamingQuery:
        # Validation occurs at the fs.write_to_online_store() phase against the stream feature view schema.
        def batch_write(row: DataFrame, batch_id: int):
            rows: pd.DataFrame = row.toPandas()

            # Extract the latest feature values for each unique entity row (i.e. the join keys).
            # Also add a 'created' column.
            rows = (
                rows.sort_values(
                    by=[*self.join_keys, self.sfv.timestamp_field], ascending=False
                )
                .groupby(self.join_keys)
                .nth(0)
            )
            rows["created"] = pd.to_datetime("now", utc=True)

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
            .option("checkpointLocation", "/tmp/checkpoint/")
            .trigger(processingTime=self.processing_time)
            .foreachBatch(batch_write)
            .start()
        )

        query.awaitTermination(timeout=self.query_timeout)
        return query
