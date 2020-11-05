from datetime import datetime
from typing import TYPE_CHECKING, List, Union

from feast.config import Config
from feast.constants import (
    CONFIG_DEADLETTER_PATH,
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
    CONFIG_SPARK_HOME,
    CONFIG_SPARK_INGESTION_JOB_JAR,
    CONFIG_SPARK_LAUNCHER,
    CONFIG_SPARK_STAGING_LOCATION,
    CONFIG_SPARK_STANDALONE_MASTER,
    CONFIG_STATSD_ENABLED,
    CONFIG_STATSD_HOST,
    CONFIG_STATSD_PORT,
    CONFIG_STENCIL_URL,
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
from feast.staging.entities import create_bq_view_of_joined_features_and_entities
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
        entity_source_conf=_source_to_argument(entity_source),
        feature_tables_sources_conf=[
            _source_to_argument(feature_table.batch_source)
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
            replace_bq_table_with_joined_view(feature_table, entity_source)
        )
        for feature_table in feature_tables
    ]

    return launcher.historical_feature_retrieval(
        RetrievalJobParameters(
            entity_source=_source_to_argument(entity_source),
            feature_tables_sources=feature_sources,
            feature_tables=[
                _feature_table_to_argument(client, project, feature_table)
                for feature_table in feature_tables
            ],
            destination={"format": output_format, "path": output_path},
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
            jar=client._config.get(CONFIG_SPARK_INGESTION_JOB_JAR),
            source=_source_to_argument(feature_table.batch_source),
            feature_table=_feature_table_to_argument(client, project, feature_table),
            start=start,
            end=end,
            redis_host=client._config.get(CONFIG_REDIS_HOST),
            redis_port=client._config.getint(CONFIG_REDIS_PORT),
            redis_ssl=client._config.getboolean(CONFIG_REDIS_SSL),
            statsd_host=(
                client._config.getboolean(CONFIG_STATSD_ENABLED)
                and client._config.get(CONFIG_STATSD_HOST)
            ),
            statsd_port=(
                client._config.getboolean(CONFIG_STATSD_ENABLED)
                and client._config.getint(CONFIG_STATSD_PORT)
            ),
            deadletter_path=client._config.get(CONFIG_DEADLETTER_PATH),
            stencil_url=client._config.get(CONFIG_STENCIL_URL),
        )
    )


def start_stream_to_online_ingestion(
    client: "Client", project: str, feature_table: FeatureTable, extra_jars: List[str]
) -> StreamIngestionJob:

    launcher = resolve_launcher(client._config)

    return launcher.start_stream_to_online_ingestion(
        StreamIngestionJobParameters(
            jar=client._config.get(CONFIG_SPARK_INGESTION_JOB_JAR),
            extra_jars=extra_jars,
            source=_source_to_argument(feature_table.stream_source),
            feature_table=_feature_table_to_argument(client, project, feature_table),
            redis_host=client._config.get(CONFIG_REDIS_HOST),
            redis_port=client._config.getint(CONFIG_REDIS_PORT),
            redis_ssl=client._config.getboolean(CONFIG_REDIS_SSL),
            statsd_host=client._config.getboolean(CONFIG_STATSD_ENABLED)
            and client._config.get(CONFIG_STATSD_HOST),
            statsd_port=client._config.getboolean(CONFIG_STATSD_ENABLED)
            and client._config.getint(CONFIG_STATSD_PORT),
            deadletter_path=client._config.get(CONFIG_DEADLETTER_PATH),
            stencil_url=client._config.get(CONFIG_STENCIL_URL),
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
