import abc
from datetime import timedelta
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Iterable,
    List,
    Mapping,
    Optional,
    Sequence,
    Set,
    Tuple,
    Union,
    cast,
)

import pandas as pd
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import DoubleType, IntegerType, StructType, TimestampType

from feast.data_format import AvroFormat, JsonFormat
from feast.data_source import DataSource, KafkaSource
from feast.stream_feature_view import StreamFeatureView

StreamTable = DataFrame  # Can add more to this later(change to union).


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
    write_function: Callable
    join_keys: List[str]

    def __init__(
        self,
        sfv: StreamFeatureView,
        spark_session: SparkSession,
        write_function: Callable,
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
        if self.format == "json":
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
        # if self.sfv.udf == None:
        #     return table
        # else:
        #     return None
        return df

    def _write_to_online_store(self, df: StreamTable):
        """
        Returns query for writing stream.
        """
        # Validation occurs at the fs.write_to_online_store() phase against the stream feature view schema.
        query = (
            df.writeStream.outputMode("update")
            .option("checkpointLocation", "/tmp/checkpoint/")
            .trigger(processingTime="30 seconds")
            .foreachBatch(
                lambda row, batch_id: self.write_function(
                    row, input_timestamp="event_timestamp", output_timestamp=""
                )
            )
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
