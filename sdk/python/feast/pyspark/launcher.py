import os
import tempfile
from datetime import datetime
from typing import TYPE_CHECKING, List, Union
from urllib.parse import urlparse, urlunparse

from feast.config import Config
from feast.constants import ConfigOptions as opt
from feast.data_format import ParquetFormat
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
from feast.staging.entities import create_bq_view_of_joined_features_and_entities
from feast.staging.storage_client import get_staging_client
from feast.value_type import ValueType

if TYPE_CHECKING:
    from feast.client import Client


def _standalone_launcher(config: Config) -> JobLauncher:
    from feast.pyspark.launchers import standalone

    return standalone.StandaloneClusterLauncher(
        config.get(opt.SPARK_STANDALONE_MASTER), config.get(opt.SPARK_HOME),
    )


def _dataproc_launcher(config: Config) -> JobLauncher:
    from feast.pyspark.launchers import gcloud

    return gcloud.DataprocClusterLauncher(
        cluster_name=config.get(opt.DATAPROC_CLUSTER_NAME),
        staging_location=config.get(opt.SPARK_STAGING_LOCATION),
        region=config.get(opt.DATAPROC_REGION),
        project_id=config.get(opt.DATAPROC_PROJECT),
        executor_instances=config.get(opt.DATAPROC_EXECUTOR_INSTANCES),
        executor_cores=config.get(opt.DATAPROC_EXECUTOR_CORES),
        executor_memory=config.get(opt.DATAPROC_EXECUTOR_MEMORY),
    )


def _emr_launcher(config: Config) -> JobLauncher:
    from feast.pyspark.launchers import aws

    def _get_optional(option):
        if config.exists(option):
            return config.get(option)

    return aws.EmrClusterLauncher(
        region=config.get(opt.EMR_REGION),
        existing_cluster_id=_get_optional(opt.EMR_CLUSTER_ID),
        new_cluster_template_path=_get_optional(opt.EMR_CLUSTER_TEMPLATE_PATH),
        staging_location=config.get(opt.SPARK_STAGING_LOCATION),
        emr_log_location=config.get(opt.EMR_LOG_LOCATION),
    )


def _k8s_launcher(config: Config) -> JobLauncher:
    from feast.pyspark.launchers import k8s

    staging_location = config.get(opt.SPARK_STAGING_LOCATION)
    staging_uri = urlparse(staging_location)

    return k8s.KubernetesJobLauncher(
        namespace=config.get(opt.SPARK_K8S_NAMESPACE),
        resource_template_path=config.get(opt.SPARK_K8S_JOB_TEMPLATE_PATH, None),
        staging_location=staging_location,
        incluster=config.getboolean(opt.SPARK_K8S_USE_INCLUSTER_CONFIG),
        staging_client=get_staging_client(staging_uri.scheme, config),
        # azure-related arguments are None if not using Azure blob storage
        azure_account_name=config.get(opt.AZURE_BLOB_ACCOUNT_NAME),
        azure_account_key=config.get(opt.AZURE_BLOB_ACCOUNT_ACCESS_KEY),
    )


_launchers = {
    "standalone": _standalone_launcher,
    "dataproc": _dataproc_launcher,
    "emr": _emr_launcher,
    "k8s": _k8s_launcher,
}


def resolve_launcher(config: Config) -> JobLauncher:
    return _launchers[config.get(opt.SPARK_LAUNCHER)](config)


def _source_to_argument(source: DataSource, config: Config):
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
        if config.exists(opt.SPARK_BQ_MATERIALIZATION_PROJECT) and config.exists(
            opt.SPARK_BQ_MATERIALIZATION_DATASET
        ):
            properties["materialization"] = dict(
                project=config.get(opt.SPARK_BQ_MATERIALIZATION_PROJECT),
                dataset=config.get(opt.SPARK_BQ_MATERIALIZATION_DATASET),
            )

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


def _feature_table_to_argument(
    client: "Client", project: str, feature_table: FeatureTable
):
    return {
        "features": [
            {"name": f.name, "type": ValueType(f.dtype).name}
            for f in feature_table.features
        ],
        "project": project,
        "name": feature_table.name,
        "entities": [
            {"name": n, "type": client.get_entity(n, project=project).value_type}
            for n in feature_table.entities
        ],
        "max_age": feature_table.max_age.ToSeconds() if feature_table.max_age else None,
        "labels": dict(feature_table.labels),
    }


def start_historical_feature_retrieval_spark_session(
    client: "Client",
    project: str,
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
        entity_source_conf=_source_to_argument(entity_source, client._config),
        feature_tables_sources_conf=[
            _source_to_argument(feature_table.batch_source, client._config)
            for feature_table in feature_tables
        ],
        feature_tables_conf=[
            _feature_table_to_argument(client, project, feature_table)
            for feature_table in feature_tables
        ],
    )


def start_historical_feature_retrieval_job(
    client: "Client",
    project: str,
    entity_source: Union[FileSource, BigQuerySource],
    feature_tables: List[FeatureTable],
    output_format: str,
    output_path: str,
) -> RetrievalJob:
    launcher = resolve_launcher(client._config)
    feature_sources = [
        _source_to_argument(
            replace_bq_table_with_joined_view(feature_table, entity_source),
            client._config,
        )
        for feature_table in feature_tables
    ]

    extra_packages = []
    if output_format == "tfrecord":
        extra_packages.append("com.linkedin.sparktfrecord:spark-tfrecord_2.12:0.3.0")

    return launcher.historical_feature_retrieval(
        RetrievalJobParameters(
            entity_source=_source_to_argument(entity_source, client._config),
            feature_tables_sources=feature_sources,
            feature_tables=[
                _feature_table_to_argument(client, project, feature_table)
                for feature_table in feature_tables
            ],
            destination={"format": output_format, "path": output_path},
            extra_packages=extra_packages,
        )
    )


def replace_bq_table_with_joined_view(
    feature_table: FeatureTable, entity_source: Union[FileSource, BigQuerySource],
) -> Union[FileSource, BigQuerySource]:
    """
    Applies optimization to historical retrieval. Instead of pulling all data from Batch Source,
    with this optimization we join feature values & entities on Data Warehouse side (improving data locality).
    Several conditions should be met to enable this optimization:
    * entities are staged to BigQuery
    * feature values are in in BigQuery
    * Entity columns are not mapped (ToDo: fix this limitation)
    :return: replacement for feature source
    """
    if not isinstance(feature_table.batch_source, BigQuerySource):
        return feature_table.batch_source

    if not isinstance(entity_source, BigQuerySource):
        return feature_table.batch_source

    if any(
        entity in feature_table.batch_source.field_mapping
        for entity in feature_table.entities
    ):
        return feature_table.batch_source

    return create_bq_view_of_joined_features_and_entities(
        feature_table.batch_source, entity_source, feature_table.entities,
    )


def start_offline_to_online_ingestion(
    client: "Client",
    project: str,
    feature_table: FeatureTable,
    start: datetime,
    end: datetime,
) -> BatchIngestionJob:

    launcher = resolve_launcher(client._config)

    return launcher.offline_to_online_ingestion(
        BatchIngestionJobParameters(
            jar=client._config.get(opt.SPARK_INGESTION_JAR),
            source=_source_to_argument(feature_table.batch_source, client._config),
            feature_table=_feature_table_to_argument(client, project, feature_table),
            start=start,
            end=end,
            redis_host=client._config.get(opt.REDIS_HOST),
            redis_port=client._config.getint(opt.REDIS_PORT),
            redis_ssl=client._config.getboolean(opt.REDIS_SSL),
            statsd_host=(
                client._config.getboolean(opt.STATSD_ENABLED)
                and client._config.get(opt.STATSD_HOST)
            ),
            statsd_port=(
                client._config.getboolean(opt.STATSD_ENABLED)
                and client._config.getint(opt.STATSD_PORT)
            ),
            deadletter_path=client._config.get(opt.DEADLETTER_PATH),
            stencil_url=client._config.get(opt.STENCIL_URL),
        )
    )


def get_stream_to_online_ingestion_params(
    client: "Client", project: str, feature_table: FeatureTable, extra_jars: List[str]
) -> StreamIngestionJobParameters:
    return StreamIngestionJobParameters(
        jar=client._config.get(opt.SPARK_INGESTION_JAR),
        extra_jars=extra_jars,
        source=_source_to_argument(feature_table.stream_source, client._config),
        feature_table=_feature_table_to_argument(client, project, feature_table),
        redis_host=client._config.get(opt.REDIS_HOST),
        redis_port=client._config.getint(opt.REDIS_PORT),
        redis_ssl=client._config.getboolean(opt.REDIS_SSL),
        statsd_host=client._config.getboolean(opt.STATSD_ENABLED)
        and client._config.get(opt.STATSD_HOST),
        statsd_port=client._config.getboolean(opt.STATSD_ENABLED)
        and client._config.getint(opt.STATSD_PORT),
        deadletter_path=client._config.get(opt.DEADLETTER_PATH),
        stencil_url=client._config.get(opt.STENCIL_URL),
        drop_invalid_rows=client._config.get(opt.INGESTION_DROP_INVALID_ROWS),
    )


def start_stream_to_online_ingestion(
    client: "Client", project: str, feature_table: FeatureTable, extra_jars: List[str]
) -> StreamIngestionJob:

    launcher = resolve_launcher(client._config)

    return launcher.start_stream_to_online_ingestion(
        get_stream_to_online_ingestion_params(
            client, project, feature_table, extra_jars
        )
    )


def list_jobs(include_terminated: bool, client: "Client") -> List[SparkJob]:
    launcher = resolve_launcher(client._config)
    return launcher.list_jobs(include_terminated=include_terminated)


def get_job_by_id(job_id: str, client: "Client") -> SparkJob:
    launcher = resolve_launcher(client._config)
    return launcher.get_job_by_id(job_id)


def stage_dataframe(df, event_timestamp_column: str, config: Config) -> FileSource:
    """
    Helper function to upload a pandas dataframe in parquet format to a temporary location (under
    SPARK_STAGING_LOCATION) and return it wrapped in a FileSource.

    Args:
        event_timestamp_column(str): the name of the timestamp column in the dataframe.
        config(Config): feast config.
    """
    staging_location = config.get(opt.SPARK_STAGING_LOCATION)
    staging_uri = urlparse(staging_location)

    with tempfile.NamedTemporaryFile() as f:
        df.to_parquet(f)

        file_url = urlunparse(
            get_staging_client(staging_uri.scheme, config).upload_fileobj(
                f,
                f.name,
                remote_path_prefix=os.path.join(staging_location, "dataframes"),
                remote_path_suffix=".parquet",
            )
        )

    return FileSource(
        event_timestamp_column=event_timestamp_column,
        file_format=ParquetFormat(),
        file_url=file_url,
    )
