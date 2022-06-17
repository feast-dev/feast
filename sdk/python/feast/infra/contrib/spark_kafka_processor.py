from types import MethodType
from typing import List

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.functions import col, from_json

from feast.data_format import AvroFormat, JsonFormat
from feast.data_source import KafkaSource
from feast.infra.contrib.stream_processor import (
    ProcessorConfig,
    StreamProcessor,
    StreamTable,
)
from feast.stream_feature_view import StreamFeatureView


class SparkProcessorConfig(ProcessorConfig):
    spark_session: SparkSession
    processing_time: str
    query_timeout: int


class SparkKafkaProcessor(StreamProcessor):
    spark: SparkSession
    format: str
    write_function: MethodType
    join_keys: List[str]

    def __init__(
        self,
        sfv: StreamFeatureView,
        config: ProcessorConfig,
        write_function: MethodType,
        processing_time: str = "30 seconds",
        query_timeout: int = 15,
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
        self.write_function = write_function
        self.processing_time = processing_time
        self.query_timeout = query_timeout
        super().__init__(sfv=sfv, data_source=sfv.stream_source)

    def ingest_stream_feature_view(self) -> None:
        ingested_stream_df = self._ingest_stream_data()
        transformed_df = self._construct_transformation_plan(ingested_stream_df)
        online_store_query = self._write_to_online_store(transformed_df)
        return online_store_query

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

    def _write_to_online_store(self, df: StreamTable):
        # Validation occurs at the fs.write_to_online_store() phase against the stream feature view schema.
        def batch_write(row: DataFrame, batch_id: int):
            pd_row = row.toPandas()
            self.write_function(
                pd_row, input_timestamp="event_timestamp", output_timestamp=""
            )

        query = (
            df.writeStream.outputMode("update")
            .option("checkpointLocation", "/tmp/checkpoint/")
            .trigger(processingTime=self.processing_time)
            .foreachBatch(batch_write)
            .start()
        )

        query.awaitTermination(timeout=self.query_timeout)
        return query
