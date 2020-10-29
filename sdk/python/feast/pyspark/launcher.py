import shutil
import tempfile
from datetime import datetime
from typing import TYPE_CHECKING, List, Union
from urllib.parse import urlparse

from feast.config import Config
from feast.constants import (
    CONFIG_REDIS_HOST,
    CONFIG_REDIS_PORT,
    CONFIG_REDIS_SSL,
    CONFIG_SPARK_DATAPROC_CLUSTER_NAME,
    CONFIG_SPARK_DATAPROC_PROJECT,
    CONFIG_SPARK_DATAPROC_REGION,
    CONFIG_SPARK_EMR_CLUSTER_ID,
    CONFIG_SPARK_EMR_CLUSTER_TEMPLATE_PATH,
    CONFIG_SPARK_EMR_LOG_LOCATION,
    CONFIG_SPARK_EMR_REGION,
    CONFIG_SPARK_EXTRA_OPTIONS,
    CONFIG_SPARK_HOME,
    CONFIG_SPARK_INGESTION_JOB_JAR,
    CONFIG_SPARK_LAUNCHER,
    CONFIG_SPARK_STAGING_LOCATION,
    CONFIG_SPARK_STANDALONE_MASTER,
)
from feast.data_source import BigQuerySource, DataSource, FileSource, KafkaSource
from feast.feature_table import FeatureTable
from feast.pyspark.abc import (
    BatchIngestionJob,
    BatchIngestionJobParameters,
    JobLauncher,
    RetrievalJob,
    RetrievalJobParameters,
    SparkJob,
    StreamIngestionJob,
    StreamIngestionJobParameters,
)
from feast.staging.storage_client import get_staging_client
from feast.value_type import ValueType

if TYPE_CHECKING:
    from feast.client import Client


def _standalone_launcher(config: Config) -> JobLauncher:
    from feast.pyspark.launchers import standalone

    return standalone.StandaloneClusterLauncher(
        config.get(CONFIG_SPARK_STANDALONE_MASTER), config.get(CONFIG_SPARK_HOME)
    )


def _dataproc_launcher(config: Config) -> JobLauncher:
    from feast.pyspark.launchers import gcloud

    return gcloud.DataprocClusterLauncher(
        config.get(CONFIG_SPARK_DATAPROC_CLUSTER_NAME),
        config.get(CONFIG_SPARK_STAGING_LOCATION),
        config.get(CONFIG_SPARK_DATAPROC_REGION),
        config.get(CONFIG_SPARK_DATAPROC_PROJECT),
    )


def _emr_launcher(config: Config) -> JobLauncher:
    from feast.pyspark.launchers import aws

    def _get_optional(option):
        if config.exists(option):
            return config.get(option)

    return aws.EmrClusterLauncher(
        region=config.get(CONFIG_SPARK_EMR_REGION),
        existing_cluster_id=_get_optional(CONFIG_SPARK_EMR_CLUSTER_ID),
        new_cluster_template_path=_get_optional(CONFIG_SPARK_EMR_CLUSTER_TEMPLATE_PATH),
        staging_location=config.get(CONFIG_SPARK_STAGING_LOCATION),
        emr_log_location=config.get(CONFIG_SPARK_EMR_LOG_LOCATION),
    )


_launchers = {
    "standalone": _standalone_launcher,
    "dataproc": _dataproc_launcher,
    "emr": _emr_launcher,
}


def resolve_launcher(config: Config) -> JobLauncher:
    return _launchers[config.get(CONFIG_SPARK_LAUNCHER)](config)


def _source_to_argument(source: DataSource):
    common_properties = {
        "field_mapping": dict(source.field_mapping),
        "event_timestamp_column": source.event_timestamp_column,
        "created_timestamp_column": source.created_timestamp_column,
        "date_partition_column": source.date_partition_column,
    }

    properties = {**common_properties}

    if isinstance(source, FileSource):
        properties["path"] = source.file_options.file_url
        properties["format"] = dict(
            json_class=source.file_options.file_format.__class__.__name__
        )
        return {"file": properties}

    if isinstance(source, BigQuerySource):
        project, dataset_and_table = source.bigquery_options.table_ref.split(":")
        dataset, table = dataset_and_table.split(".")
        properties["project"] = project
        properties["dataset"] = dataset
        properties["table"] = table
        return {"bq": properties}

    if isinstance(source, KafkaSource):
        properties["bootstrap_servers"] = source.kafka_options.bootstrap_servers
        properties["topic"] = source.kafka_options.topic
        properties["format"] = {
            **source.kafka_options.message_format.__dict__,
            "json_class": source.kafka_options.message_format.__class__.__name__,
        }
        return {"kafka": properties}

    raise NotImplementedError(f"Unsupported Datasource: {type(source)}")


def _feature_table_to_argument(client: "Client", feature_table: FeatureTable):
    return {
        "features": [
            {"name": f.name, "type": ValueType(f.dtype).name}
            for f in feature_table.features
        ],
        "project": "default",
        "name": feature_table.name,
        "entities": [
            {"name": n, "type": client.get_entity(n).value_type}
            for n in feature_table.entities
        ],
        "max_age": feature_table.max_age.ToSeconds() if feature_table.max_age else None,
    }


def start_historical_feature_retrieval_spark_session(
    client: "Client",
    entity_source: Union[FileSource, BigQuerySource],
    feature_tables: List[FeatureTable],
):
    from pyspark.sql import SparkSession

    from feast.pyspark.historical_feature_retrieval_job import (
        retrieve_historical_features,
    )

    spark_session = SparkSession.builder.getOrCreate()
    return retrieve_historical_features(
        spark=spark_session,
        entity_source_conf=_source_to_argument(entity_source),
        feature_tables_sources_conf=[
            _source_to_argument(feature_table.batch_source)
            for feature_table in feature_tables
        ],
        feature_tables_conf=[
            _feature_table_to_argument(client, feature_table)
            for feature_table in feature_tables
        ],
    )


def start_historical_feature_retrieval_job(
    client: "Client",
    entity_source: Union[FileSource, BigQuerySource],
    feature_tables: List[FeatureTable],
    feature_tables_sources: List[DataSource],
    output_format: str,
    output_path: str,
) -> RetrievalJob:
    launcher = resolve_launcher(client._config)
    return launcher.historical_feature_retrieval(
        RetrievalJobParameters(
            entity_source=_source_to_argument(entity_source),
            feature_tables_sources=[
                _source_to_argument(source) for source in feature_tables_sources
            ],
            feature_tables=[
                _feature_table_to_argument(client, feature_table)
                for feature_table in feature_tables
            ],
            destination={"format": output_format, "path": output_path},
            extra_options=client._config.get(CONFIG_SPARK_EXTRA_OPTIONS),
        )
    )


def _download_jar(remote_jar: str) -> str:
    remote_jar_parts = urlparse(remote_jar)

    local_temp_jar = tempfile.NamedTemporaryFile(suffix=".jar", delete=False)
    with local_temp_jar:
        shutil.copyfileobj(
            get_staging_client(remote_jar_parts.scheme).download_file(remote_jar_parts),
            local_temp_jar,
        )

    return local_temp_jar.name


def start_offline_to_online_ingestion(
    feature_table: FeatureTable, start: datetime, end: datetime, client: "Client"
) -> BatchIngestionJob:

    launcher = resolve_launcher(client._config)
    local_jar_path = _download_jar(client._config.get(CONFIG_SPARK_INGESTION_JOB_JAR))

    return launcher.offline_to_online_ingestion(
        BatchIngestionJobParameters(
            jar=local_jar_path,
            source=_source_to_argument(feature_table.batch_source),
            feature_table=_feature_table_to_argument(client, feature_table),
            start=start,
            end=end,
            redis_host=client._config.get(CONFIG_REDIS_HOST),
            redis_port=client._config.getint(CONFIG_REDIS_PORT),
            redis_ssl=client._config.getboolean(CONFIG_REDIS_SSL),
            extra_options=client._config.get(CONFIG_SPARK_EXTRA_OPTIONS),
        )
    )


def start_stream_to_online_ingestion(
    feature_table: FeatureTable, extra_jars: List[str], client: "Client"
) -> StreamIngestionJob:

    launcher = resolve_launcher(client._config)
    local_jar_path = _download_jar(client._config.get(CONFIG_SPARK_INGESTION_JOB_JAR))

    return launcher.start_stream_to_online_ingestion(
        StreamIngestionJobParameters(
            jar=local_jar_path,
            extra_jars=extra_jars,
            source=_source_to_argument(feature_table.stream_source),
            feature_table=_feature_table_to_argument(client, feature_table),
            redis_host=client._config.get(CONFIG_REDIS_HOST),
            redis_port=client._config.getint(CONFIG_REDIS_PORT),
            redis_ssl=client._config.getboolean(CONFIG_REDIS_SSL),
            extra_options=client._config.get(CONFIG_SPARK_EXTRA_OPTIONS),
        )
    )


def list_jobs(include_terminated: bool, client: "Client") -> List[SparkJob]:
    launcher = resolve_launcher(client._config)
    return launcher.list_jobs(include_terminated=include_terminated)


def get_job_by_id(job_id: str, client: "Client") -> SparkJob:
    launcher = resolve_launcher(client._config)
    return launcher.get_job_by_id(job_id)


def stage_dataframe(df, event_timestamp_column: str, client: "Client") -> FileSource:
    launcher = resolve_launcher(client._config)
    return launcher.stage_dataframe(df, event_timestamp_column)
