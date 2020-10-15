import shutil
import tempfile
from datetime import datetime
from typing import TYPE_CHECKING, List, Union
from urllib.parse import urlparse

from feast.config import Config
from feast.constants import (
    CONFIG_SPARK_DATAPROC_CLUSTER_NAME,
    CONFIG_SPARK_DATAPROC_PROJECT,
    CONFIG_SPARK_DATAPROC_REGION,
    CONFIG_SPARK_DATAPROC_STAGING_LOCATION,
    CONFIG_SPARK_HOME,
    CONFIG_SPARK_INGESTION_JOB_JAR,
    CONFIG_SPARK_LAUNCHER,
    CONFIG_SPARK_STANDALONE_MASTER,
)
from feast.data_source import BigQuerySource, DataSource, FileSource
from feast.feature_table import FeatureTable
from feast.pyspark.abc import (
    IngestionJob,
    IngestionJobParameters,
    JobLauncher,
    RetrievalJob,
    RetrievalJobParameters,
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
        config.get(CONFIG_SPARK_DATAPROC_STAGING_LOCATION),
        config.get(CONFIG_SPARK_DATAPROC_REGION),
        config.get(CONFIG_SPARK_DATAPROC_PROJECT),
    )


_launchers = {"standalone": _standalone_launcher, "dataproc": _dataproc_launcher}


def resolve_launcher(config: Config) -> JobLauncher:
    return _launchers[config.get(CONFIG_SPARK_LAUNCHER)](config)


_SOURCES = {
    FileSource: ("file", "file_options", {"path": "file_url", "format": "file_format"}),
    BigQuerySource: ("bq", "bigquery_options", {"table_ref": "table_ref"}),
}


def _source_to_argument(source: DataSource):
    common_properties = {
        "field_mapping": dict(source.field_mapping),
        "event_timestamp_column": source.event_timestamp_column,
        "created_timestamp_column": source.created_timestamp_column,
        "date_partition_column": source.date_partition_column,
    }

    kind, option_field, extra_properties = _SOURCES[type(source)]

    properties = {
        **common_properties,
        **{
            k: getattr(getattr(source, option_field), ref)
            for k, ref in extra_properties.items()
        },
    }

    return {kind: properties}


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
    output_format: str,
    output_path: str,
) -> RetrievalJob:
    launcher = resolve_launcher(client._config)
    return launcher.historical_feature_retrieval(
        RetrievalJobParameters(
            entity_source=_source_to_argument(entity_source),
            feature_tables_sources=[
                _source_to_argument(feature_table.batch_source)
                for feature_table in feature_tables
            ],
            feature_tables=[
                _feature_table_to_argument(client, feature_table)
                for feature_table in feature_tables
            ],
            destination={"format": output_format, "path": output_path},
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
) -> IngestionJob:

    launcher = resolve_launcher(client._config)
    local_jar_path = _download_jar(client._config.get(CONFIG_SPARK_INGESTION_JOB_JAR))

    return launcher.offline_to_online_ingestion(
        IngestionJobParameters(
            jar=local_jar_path,
            source=_source_to_argument(feature_table.batch_source),
            feature_table=_feature_table_to_argument(client, feature_table),
            start=start,
            end=end,
        )
    )
