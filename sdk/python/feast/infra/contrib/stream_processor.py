from abc import ABC
from typing import Callable

import pandas as pd
from pyspark.sql import DataFrame

from feast.data_source import DataSource
from feast.importer import import_class
from feast.repo_config import FeastConfigBaseModel
from feast.stream_feature_view import StreamFeatureView

STREAM_PROCESSOR_CLASS_FOR_TYPE = {
    ("spark", "kafka"): "feast.infra.contrib.spark_kafka_processor.SparkKafkaProcessor",
}

# TODO: support more types other than just Spark.
StreamTable = DataFrame


class ProcessorConfig(FeastConfigBaseModel):
    # Processor mode (spark, etc)
    mode: str
    # Ingestion source (kafka, kinesis, etc)
    source: str


class StreamProcessor(ABC):
    """
    A StreamProcessor can ingest and transform data for a specific stream feature view,
    and persist that data to the online store.

    Attributes:
        sfv: The stream feature view on which the stream processor operates.
        data_source: The stream data source from which data will be ingested.
    """

    sfv: StreamFeatureView
    data_source: DataSource

    def __init__(self, sfv: StreamFeatureView, data_source: DataSource):
        self.sfv = sfv
        self.data_source = data_source

    def ingest_stream_feature_view(self) -> None:
        """
        Ingests data from the stream source attached to the stream feature view; transforms the data
        and then persists it to the online store.
        """
        pass

    def _ingest_stream_data(self) -> StreamTable:
        """
        Ingests data into a StreamTable.
        """
        pass

    def _construct_transformation_plan(self, table: StreamTable) -> StreamTable:
        """
        Applies transformations on top of StreamTable object. Since stream engines use lazy
        evaluation, the StreamTable will not be materialized until it is actually evaluated.
        For example: df.collect() in spark or tbl.execute() in Flink.
        """
        pass

    def _write_to_online_store(self, table: StreamTable) -> None:
        """
        Returns query for persisting data to the online store.
        """
        pass


def get_stream_processor_object(
    config: ProcessorConfig,
    sfv: StreamFeatureView,
    write_function: Callable[[pd.DataFrame, str, str], None],
):
    """
    Returns a stream processor object based on the config mode and stream source type. The write function is a
    function that wraps the feature store "write_to_online_store" capability.
    """
    if config.mode == "spark" and config.source == "kafka":
        stream_processor = STREAM_PROCESSOR_CLASS_FOR_TYPE[("spark", "kafka")]
        module_name, class_name = stream_processor.rsplit(".", 1)
        cls = import_class(module_name, class_name, "StreamProcessor")
        return cls(sfv=sfv, config=config, write_function=write_function,)
    else:
        raise ValueError("other processors besides spark-kafka not supported")
