from abc import ABC, abstractmethod
from types import MethodType
from typing import TYPE_CHECKING, Any, Optional

from pyspark.sql import DataFrame
from typing_extensions import TypeAlias

from feast.data_source import DataSource, PushMode
from feast.importer import import_class
from feast.repo_config import FeastConfigBaseModel
from feast.stream_feature_view import StreamFeatureView

if TYPE_CHECKING:
    from feast.feature_store import FeatureStore

STREAM_PROCESSOR_CLASS_FOR_TYPE = {
    ("spark", "kafka"): "feast.infra.contrib.spark_kafka_processor.SparkKafkaProcessor",
}

# TODO: support more types other than just Spark.
StreamTable: TypeAlias = DataFrame


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
        fs: The feature store where data should be persisted.
        sfv: The stream feature view on which the stream processor operates.
        data_source: The stream data source from which data will be ingested.
    """

    fs: "FeatureStore"
    sfv: StreamFeatureView
    data_source: DataSource

    def __init__(
        self, fs: "FeatureStore", sfv: StreamFeatureView, data_source: DataSource
    ):
        self.fs = fs
        self.sfv = sfv
        self.data_source = data_source

    @abstractmethod
    def ingest_stream_feature_view(
        self, to: PushMode = PushMode.ONLINE
    ) -> Optional[Any]:
        """
        Ingests data from the stream source attached to the stream feature view; transforms the data
        and then persists it to the online store and/or offline store, depending on the 'to' parameter.
        """
        raise NotImplementedError

    @abstractmethod
    def _ingest_stream_data(self) -> StreamTable:
        """
        Ingests data into a StreamTable.
        """
        raise NotImplementedError

    @abstractmethod
    def _construct_transformation_plan(self, table: StreamTable) -> StreamTable:
        """
        Applies transformations on top of StreamTable object. Since stream engines use lazy
        evaluation, the StreamTable will not be materialized until it is actually evaluated.
        For example: df.collect() in spark or tbl.execute() in Flink.
        """
        raise NotImplementedError

    @abstractmethod
    def _write_stream_data(self, table: StreamTable, to: PushMode) -> Optional[Any]:
        """
        Launches a job to persist stream data to the online store and/or offline store, depending
        on the 'to' parameter, and returns a handle for the job.
        """
        raise NotImplementedError


def get_stream_processor_object(
    config: ProcessorConfig,
    fs: "FeatureStore",
    sfv: StreamFeatureView,
    preprocess_fn: Optional[MethodType] = None,
):
    """
    Returns a stream processor object based on the config.

    The returned object will be capable of launching an ingestion job that reads data from the
    given stream feature view's stream source, transforms it if the stream feature view has a
    transformation, and then writes it to the online store. It will also preprocess the data
    if a preprocessor method is defined.
    """
    if config.mode == "spark" and config.source == "kafka":
        stream_processor = STREAM_PROCESSOR_CLASS_FOR_TYPE[("spark", "kafka")]
        module_name, class_name = stream_processor.rsplit(".", 1)
        cls = import_class(module_name, class_name, "StreamProcessor")
        return cls(fs=fs, sfv=sfv, config=config, preprocess_fn=preprocess_fn)
    else:
        raise ValueError("other processors besides spark-kafka not supported")
