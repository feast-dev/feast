import abc
from types import MethodType
from typing import List

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.functions import col, from_json

from feast.data_format import AvroFormat, JsonFormat
from feast.data_source import DataSource, KafkaSource
from feast.repo_config import FeastConfigBaseModel
from feast.stream_feature_view import StreamFeatureView

StreamTable = DataFrame  # Can add more to this later(change to union).


class ProcessorConfig(FeastConfigBaseModel):
    # Processor mode(spark, etc)
    mode: str
    # Ingestion source(kafka, kinesis, etc)
    source: str


class SparkProcessorConfig(ProcessorConfig):
    spark_session: SparkSession


class StreamProcessor(abc.ABC):
    data_source: DataSource
    sfv: StreamFeatureView

    def __init__(self, sfv: StreamFeatureView, data_source: DataSource):
        self.sfv = sfv
        self.data_source = data_source

    def _ingest_stream_data(self) -> StreamTable:
        """
        Ingests data into StreamTable depending on what type of data it is
        """
        pass

    def _construct_transformation_plan(self, table: StreamTable) -> StreamTable:
        """
        Applies transformations on top of StreamTable object. Since stream engines use lazy
        evaluation, the StreamTable will not be materialized until it is actually evaluated.
        For example: df.collect() in spark or tbl.execute() in Flink.
        """
        pass

    def _write_to_online_store(self, table: StreamTable):
        """
        Returns query for writing stream.
        """
        pass

    def transform_stream_data(self) -> StreamTable:
        pass

    def ingest_stream_feature_view(self):
        pass

    def transform_and_write(self, table: StreamTable):
        pass


class SparkStreamKafkaProcessor(StreamProcessor):
    # TODO: wrap spark data in some kind of config
    # includes session, format, checkpoint location etc.
    spark: SparkSession
    format: str
    write_function: MethodType
    join_keys: List[str]

    def __init__(
        self,
        sfv: StreamFeatureView,
        spark_session: SparkSession,
        write_function: MethodType,
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
        # if not sfv.mode == "spark":
        #     raise ValueError(f"stream feature view mode is {sfv.mode}, but only supports spark")
        self.format = (
            "json"
            if isinstance(sfv.stream_source.kafka_options.message_format, JsonFormat)
            else "avro"
        )
        self.spark = spark_session
        self.write_function = write_function
        super().__init__(sfv=sfv, data_source=sfv.stream_source)

    def _ingest_stream_data(self) -> StreamTable:
        """
        Ingests data into StreamTable depending on what type of data format it is in.
        Only supports json and avro formats currently.
        """
        if not isinstance(self.data_source, KafkaSource):
            raise ValueError(
                f"data source {type(self.data_source)} is not kafka source"
            )

        if self.format == "json":
            if not isinstance(
                self.data_source.kafka_options.message_format, JsonFormat
            ):
                raise ValueError("kafka source message format is not jsonformat")
            streamingDF = (
                self.spark.readStream.format("kafka")
                .option(
                    "kafka.bootstrap.servers",
                    self.data_source.kafka_options.bootstrap_servers,
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
            streamingDF = (
                self.spark.readStream.format("kafka")
                .option(
                    "kafka.bootstrap.servers",
                    self.data_source.kafka_options.bootstrap_servers,
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
        return streamingDF

    def _construct_transformation_plan(self, df: StreamTable) -> StreamTable:
        """
        Applies transformations on top of StreamTable object. Since stream engines use lazy
        evaluation, the StreamTable will not be materialized until it is actually evaluated.
        For example: df.collect() in spark or tbl.execute() in Flink.
        """
        if not self.sfv.udf:
            return df
        else:
            return self.sfv.udf(df)

    def _write_to_online_store(self, df: StreamTable):
        """
        Returns query for writing stream.
        """
        # Validation occurs at the fs.write_to_online_store() phase against the stream feature view schema.
        def batch_write(row: DataFrame, batch_id: int):
            pd_row = row.toPandas()
            self.write_function(
                pd_row, input_timestamp="event_timestamp", output_timestamp=""
            )

        query = (
            df.writeStream.outputMode("update")
            .option("checkpointLocation", "/tmp/checkpoint/")
            .trigger(processingTime="30 seconds")
            .foreachBatch(batch_write)
            .start()
        )
        query.awaitTermination(timeout=30)
        return query

    def transform_stream_data(self) -> StreamTable:
        df = self._ingest_stream_data()
        return self._construct_transformation_plan(df)

    def ingest_stream_feature_view(self):
        ingested_stream_df = self._ingest_stream_data()
        transformed_df = self._construct_transformation_plan(ingested_stream_df)
        online_store_query = self._write_to_online_store(transformed_df)
        return online_store_query

    def transform_and_write(self, table: StreamTable):
        pass


def get_stream_processor_object(
    config: ProcessorConfig, sfv: StreamFeatureView, write_function
):
    """
    Returns a stream processor object based on the config mode and stream source type. The write function is a
    function that wraps the feature store "write_to_online_store" capability.
    """
    if config.mode == "spark" and config.source == "kafka":
        if not isinstance(config, SparkProcessorConfig):
            raise ValueError(f"spark processor config required, got {type(config)}")
        return SparkStreamKafkaProcessor(
            sfv=sfv, spark_session=config.spark_session, write_function=write_function,
        )
    else:
        raise ValueError("other processors besides spark-kafka not supported")
