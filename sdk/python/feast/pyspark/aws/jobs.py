import hashlib
import json
import logging
import os
import random
import string
import tempfile
import time
from collections import defaultdict
from io import BytesIO
from pathlib import Path
from typing import IO, Any, Dict, List, NamedTuple, Optional, Tuple

import boto3
import botocore
import pandas
import yaml

from feast.client import Client
from feast.feature_table import FeatureTable
from feast.value_type import ValueType

log = logging.getLogger("aws")

# Config example:
#
# aws:
#   logS3Prefix: "..a prefix for logs.."
#   artifactS3Prefix: "..a prefix for jars.."
#   dataframeS3Prefix: "..a prefix for dataframes.."
#   existingClusterId: "..."                  # You need to set either existingClusterId
#   runJobFlowTemplate:                       # or runJobFlowTemplate
#     Name: "feast-ingestion-test"
#     ReleaseLabel: emr-6.0.0
#     Instances:
#         InstanceFleets:
#             - InstanceFleetType: MASTER
#               TargetOnDemandCapacity: 0
#               TargetSpotCapacity: 1
#               LaunchSpecifications:
#                   SpotSpecification:
#                       TimeoutDurationMinutes: 60
#                       TimeoutAction: TERMINATE_CLUSTER
#               InstanceTypeConfigs:
#                   - WeightedCapacity: 1
#                     EbsConfiguration:
#                         EbsBlockDeviceConfigs:
#                             - VolumeSpecification:
#                                 SizeInGB: 32
#                                 VolumeType: gp2
#                               VolumesPerInstance: 2
#                     BidPriceAsPercentageOfOnDemandPrice: 100
#                     InstanceType: m4.xlarge
#             - InstanceFleetType: CORE
#               TargetOnDemandCapacity: 0
#               TargetSpotCapacity: 2
#               LaunchSpecifications:
#                   SpotSpecification:
#                       TimeoutDurationMinutes: 60
#                       TimeoutAction: TERMINATE_CLUSTER
#               InstanceTypeConfigs:
#                   - WeightedCapacity: 1
#                     EbsConfiguration:
#                         EbsBlockDeviceConfigs:
#                             - VolumeSpecification:
#                                 SizeInGB: 32
#                                 VolumeType: gp2
#                               VolumesPerInstance: 2
#                     BidPriceAsPercentageOfOnDemandPrice: 100
#                     InstanceType: m4.xlarge
#         Ec2SubnetIds:
#             - "..a subnet id within a VPC with a route to redis..."
#         AdditionalMasterSecurityGroups:
#             - "..a security group that allows access to redis..."
#         AdditionalSlaveSecurityGroups:
#             - "..a security group that allows access to redis..."
#         KeepJobFlowAliveWhenNoSteps: false
#     BootstrapActions:
#         - Name: "s3://aws-bigdata-blog/artifacts/resize_storage/resize_storage.sh"
#           ScriptBootstrapAction:
#             Path: "s3://aws-bigdata-blog/artifacts/resize_storage/resize_storage.sh"
#             Args:
#                 - "--scaling-factor"
#                 - "1.5"
#     Applications:
#         - Name: Hadoop
#         - Name: Hive
#         - Name: Spark
#         - Name: Livy
#     JobFlowRole: my-spark-node
#     ServiceRole: my-worker-node
#     ScaleDownBehavior: TERMINATE_AT_TASK_COMPLETION
# redisConfig:
#   host: my.redis.com
#   port: 6379
#   ssl: true

SUPPORTED_EMR_VERSION = "emr-6.0.0"
STREAM_TO_ONLINE_JOB_TYPE = "STREAM_TO_ONLINE_JOB"
OFFLINE_TO_ONLINE_JOB_TYPE = "OFFLINE_TO_ONLINE_JOB"
HISTORICAL_RETRIEVAL_JOB_TYPE = "HISTORICAL_RETRIEVAL_JOB"


# EMR Step states considered "active", i.e. not terminated
ACTIVE_STEP_STATES = ["PENDING", "CANCEL_PENDING", "RUNNING"]
TERMINAL_STEP_STATES = ["COMPLETED", "CANCELLED", "FAILED", "INTERRUPTED"]


def _sanity_check_config(config, config_path: str):
    """
    Sanity check the config. We don't really have to do this here but if the spark job fails
    you'll only find out much later and this is annoying. Those are not exhaustive, just
    some checks to help debugging common configuration issues.
    """
    aws_config = config.get("aws", {})

    if ("runJobFlowTemplate" not in aws_config) and (
        "existingClusterId" not in aws_config
    ):
        log.error("{config_path}: either clusterId or runJobFlowTemplate should be set")
    elif "runJobFlowTemplate" in aws_config:
        runJobFlowTemplate = aws_config["runJobFlowTemplate"]
        releaseLabel = runJobFlowTemplate.get("ReleaseLabel")
        if releaseLabel != SUPPORTED_EMR_VERSION:
            log.warn(
                f"{config_path}: ReleaseLabel is set to {releaseLabel}. Recommended: {SUPPORTED_EMR_VERSION}"
            )

    if "redisConfig" not in config:
        log.error("{config_path}: redisConfig is not set")


def _get_config_path() -> str:
    return os.environ["JOB_SERVICE_CONFIG_PATH"]


def _load_job_service_config(config_path: str):
    with open(config_path) as f:
        config = yaml.safe_load(f)
        _sanity_check_config(config, config_path)
        return config


def _random_string(length) -> str:
    return "".join(random.choice(string.ascii_lowercase) for _ in range(length))


def _batch_source_to_json(batch_source):
    return {
        "file": {
            "path": batch_source.file_options.file_url,
            "format": batch_source.file_options.file_format,
            "field_mapping": dict(batch_source.field_mapping),
            "event_timestamp_column": batch_source.event_timestamp_column,
            "created_timestamp_column": batch_source.created_timestamp_column,
            "date_partition_column": batch_source.date_partition_column,
        }
    }


def _stream_source_to_json(stream_source):
    return {
        "kafka": {
            "bootstrapServers": stream_source.kafka_options.bootstrap_servers,
            "mapping": dict(stream_source.field_mapping),
            "topic": stream_source.kafka_options.topic,
            "timestampColumn": stream_source.timestamp_column,
            "classpath": stream_source.kafka_options.class_path,
        }
    }


def _feature_table_to_json(client: Client, feature_table, features=Optional[List[str]]):
    """
    Convert feature_table info to a dict format to be serialized as JSON.

    Features is an optional list of features to include. If not specified, we include all
    features in the result.
    """
    return {
        "features": [
            {"name": f.name, "type": ValueType(f.dtype).name}
            for f in (feature_table.features if not features else features)
        ],
        "project": "default",
        "name": feature_table.name,
        "entities": [
            {"name": n, "type": client.get_entity(n).value_type}
            for n in feature_table.entities
        ],
    }


def _s3_split_path(path: str) -> Tuple[str, str]:
    """ Convert s3:// url to (bucket, key) """
    assert path.startswith("s3://")
    _, _, bucket, key = path.split("/", 3)
    return bucket, key


def _hash_fileobj(fileobj: IO[bytes]) -> str:
    """ Compute sha256 hash of a file. File pointer will be reset to 0 on return. """
    fileobj.seek(0)
    h = hashlib.sha256()
    for block in iter(lambda: fileobj.read(2 ** 20), b""):
        h.update(block)
    fileobj.seek(0)
    return h.hexdigest()


def _s3_upload(
    fileobj: IO[bytes],
    local_path: str,
    *,
    remote_path: Optional[str] = None,
    remote_path_prefix: Optional[str] = None,
    remote_path_suffix: Optional[str] = None,
) -> str:
    """
    Upload a local file to S3. We store the file sha256 sum in S3 metadata and skip the upload
    if the file hasn't changed.

    You can either specify remote_path or remote_path_prefix+remote_path_suffix. In the latter case,
    the remote path will be computed as $remote_path_prefix/$sha256$remote_path_suffix
    """

    assert (remote_path is not None) or (
        remote_path_prefix is not None and remote_path_suffix is not None
    )

    sha256sum = _hash_fileobj(fileobj)

    if remote_path is None:
        assert remote_path_prefix is not None
        remote_path = os.path.join(
            remote_path_prefix, f"{sha256sum}{remote_path_suffix}"
        )

    bucket, key = _s3_split_path(remote_path)
    client = boto3.client("s3")

    try:
        head_response = client.head_object(Bucket=bucket, Key=key)
        if head_response["Metadata"]["sha256sum"] == sha256sum:
            # File already exists
            return remote_path
        else:
            log.info("Uploading {local_path} to {remote_path}")
            client.upload_fileobj(
                fileobj, bucket, key, ExtraArgs={"Metadata": {"sha256sum": sha256sum}},
            )
            return remote_path
    except botocore.exceptions.ClientError as e:
        if e.response["Error"]["Code"] == "404":
            log.info("Uploading {local_path} to {remote_path}")
            client.upload_fileobj(
                fileobj, bucket, key, ExtraArgs={"Metadata": {"sha256sum": sha256sum}},
            )
            return remote_path
        else:
            raise


def _upload_jar(jar_s3_prefix: str, local_path: str) -> str:
    with open(local_path, "rb") as f:
        return _s3_upload(
            f,
            local_path,
            remote_path=os.path.join(jar_s3_prefix, os.path.basename(local_path)),
        )


def _get_ingestion_jar_s3_path(config) -> str:
    """
    Extract job jar path from the configuration, upload it to S3 if necessary and return S3 path.
    """
    jar_path = os.environ.get("INGESTION_JOB_JAR_PATH")
    if jar_path is None:
        raise ValueError("INGESTION_JOB_JAR_PATH not set")
    elif jar_path.startswith("s3://"):
        return jar_path
    else:
        artifactS3Prefix = config.get("aws").get("artifactS3Prefix")
        if artifactS3Prefix:
            return _upload_jar(artifactS3Prefix, jar_path)
        else:
            raise ValueError("artifactS3Prefix must be set")


def _sync_offline_to_online_step(
    client: Client, config, feature_table, start_ts: str, end_ts: str
) -> Dict[str, Any]:
    feature_table_json = _feature_table_to_json(client, feature_table)
    source_json = _batch_source_to_json(feature_table.batch_source)

    return {
        "Name": "Feast Ingestion",
        "HadoopJarStep": {
            # TODO: generate those from proto
            "Properties": [
                {
                    "Key": "feast.step_metadata.job_type",
                    "Value": OFFLINE_TO_ONLINE_JOB_TYPE,
                },
                {
                    "Key": "feast.step_metadata.offline_to_online.table_name",
                    "Value": feature_table.name,
                },
                {
                    "Key": "feast.step_metadata.offline_to_online.start_ts",
                    "Value": start_ts,
                },
                {
                    "Key": "feast.step_metadata.offline_to_online.end_ts",
                    "Value": end_ts,
                },
            ],
            "Args": [
                "spark-submit",
                "--class",
                "feast.ingestion.IngestionJob",
                "--packages",
                "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.17.2",
                _get_ingestion_jar_s3_path(config),
                "--mode",
                "offline",
                "--feature-table",
                json.dumps(feature_table_json),
                "--source",
                json.dumps(source_json),
                "--redis",
                json.dumps(config["redisConfig"]),
                "--start",
                start_ts,
                "--end",
                end_ts,
            ],
            "Jar": "command-runner.jar",
        },
    }


def _submit_emr_job(
    step: Dict[str, Any], config: Dict[str, Any]
) -> Tuple[str, Optional[str]]:
    """
    Submit EMR job using a new or existing cluster.

    Returns a tuple of JobId, StepId
    """
    aws_config = config.get("aws", {})

    emr = boto3.client("emr", region_name=aws_config.get("region"))

    if "existingClusterId" in aws_config:
        step["ActionOnFailure"] = "CONTINUE"
        step_ids = emr.add_job_flow_steps(
            JobFlowId=aws_config["existingClusterId"], Steps=[step],
        )
        return (aws_config["existingClusterId"], step_ids["StepIds"][0])
    else:
        jobTemplate = aws_config["runJobFlowTemplate"]
        step["ActionOnFailure"] = "TERMINATE_CLUSTER"

        jobTemplate["Steps"] = [step]

        if aws_config.get("logS3Prefix"):
            jobTemplate["LogUri"] = os.path.join(
                aws_config["logS3Prefix"], _random_string(5)
            )

        job = emr.run_job_flow(**jobTemplate)
        return (job["JobFlowId"], None)


def sync_offline_to_online(
    client: Client, feature_table: FeatureTable, start_ts: str, end_ts: str
):
    config = _load_job_service_config(_get_config_path())
    step = _sync_offline_to_online_step(client, config, feature_table, start_ts, end_ts)
    _submit_emr_job(step, config)


def _stream_ingestion_step(
    client: Client, config, feature_table, jars: List[str]
) -> Dict[str, Any]:
    feature_table_json = _feature_table_to_json(client, feature_table)
    source_json = _stream_source_to_json(feature_table.stream_source)

    if jars:
        jars_args = ["--jars", ",".join(jars)]
    else:
        jars_args = []

    return {
        "Name": "Feast Streaming Ingestion",
        "HadoopJarStep": {
            "Properties": [
                {
                    "Key": "feast.step_metadata.job_type",
                    "Value": STREAM_TO_ONLINE_JOB_TYPE,
                },
                {
                    "Key": "feast.step_metadata.stream_to_online.table_name",
                    "Value": feature_table.name,
                },
            ],
            "Args": ["spark-submit", "--class", "feast.ingestion.IngestionJob"]
            + jars_args
            + [
                "--packages",
                "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.17.2",
                _get_ingestion_jar_s3_path(config),
                "--mode",
                "online",
                "--feature-table",
                json.dumps(feature_table_json),
                "--source",
                json.dumps(source_json),
                "--redis",
                json.dumps(config["redisConfig"]),
            ],
            "Jar": "command-runner.jar",
        },
    }


def start_stream_to_online(
    client: Client, feature_table: FeatureTable, jars: List[str]
):
    if _get_stream_to_online_job(client, feature_table):
        raise Exception("Job already running")

    config = _load_job_service_config(_get_config_path())
    step = _stream_ingestion_step(client, config, feature_table, jars)
    _submit_emr_job(step, config)


class JobInfo(NamedTuple):
    job_type: str
    cluster_id: str
    step_id: str
    table_name: str
    state: str


def list_jobs(
    job_type: Optional[str], table_name: Optional[str], active_only=True
) -> List[JobInfo]:
    """
    List Feast EMR jobs.

    Args:
        job_type: optional filter by job type
        table_name: optional filter by table name
        active_only: filter only for "active" jobs, that is the ones that are running or pending, not terminated

    Returns:
        A list of jobs.
    """
    config = _load_job_service_config(_get_config_path())
    aws_config = config.get("aws", {})
    emr = boto3.client("emr", region_name=aws_config.get("region"))
    paginator = emr.get_paginator("list_clusters")
    res: List[JobInfo] = []
    for page in paginator.paginate(
        ClusterStates=["STARTING", "BOOTSTRAPPING", "RUNNING", "WAITING", "TERMINATING"]
    ):
        for cluster in page["Clusters"]:
            cluster_id = cluster["Id"]
            step_paginator = emr.get_paginator("list_steps")

            list_steps_params = dict(ClusterId=cluster_id)
            if active_only:
                list_steps_params["StepStates"] = ACTIVE_STEP_STATES

            for step_page in step_paginator.paginate(**list_steps_params):
                for step in step_page["Steps"]:
                    props = step["Config"]["Properties"]
                    if "feast.step_metadata.job_type" not in props:
                        continue

                    step_table_name = props.get(
                        "feast.step_metadata.stream_to_online.table_name"
                    ) or props.get("feast.step_metadata.offline_to_online.table_name")
                    step_job_type = props["feast.step_metadata.job_type"]

                    if table_name and step_table_name != table_name:
                        continue

                    if job_type and step_job_type != job_type:
                        continue

                    res.append(
                        JobInfo(
                            job_type=step_job_type,
                            cluster_id=cluster_id,
                            step_id=step["Id"],
                            state=step["Status"]["State"],
                            table_name=step_table_name,
                        )
                    )
    return res


def _get_stream_to_online_job(
    client: Client, feature_table: FeatureTable
) -> List[JobInfo]:
    return list_jobs(
        job_type=STREAM_TO_ONLINE_JOB_TYPE,
        table_name=feature_table.name,
        active_only=True,
    )


def _wait_for_job_state(
    emr_client, job: JobInfo, desired_states: List[str], timeout_seconds=90
):
    """
    Wait up to timeout seconds for job to go into one of the desired states.
    """
    start_time = time.time()
    while time.time() - start_time < timeout_seconds:
        response = emr_client.describe_step(
            ClusterId=job.cluster_id, StepId=job.step_id
        )
        state = response["Step"]["Status"]["State"]
        if state in desired_states:
            return
        else:
            time.sleep(0.5)
    else:
        raise TimeoutError(
            f'Timeout waiting for job state to become {"|".join(desired_states)}'
        )


def _cancel_job(job_type, table_name):
    """
    Cancel a EMR job.
    """
    jobs = list_jobs(job_type=job_type, table_name=table_name, active_only=True)
    config = _load_job_service_config(_get_config_path())
    aws_config = config.get("aws", {})

    emr = boto3.client("emr", region_name=aws_config.get("region"))
    for job in jobs:
        emr.cancel_steps(ClusterId=job.cluster_id, StepIds=[job.step_id])

    for job in jobs:
        _wait_for_job_state(emr, job, TERMINAL_STEP_STATES)


def stop_stream_to_online(table_name: str):
    """
    Stop offline-to-online ingestion job for the table.
    """
    _cancel_job(STREAM_TO_ONLINE_JOB_TYPE, table_name)


def _upload_dataframe(s3prefix: str, df: pandas.DataFrame) -> str:
    with tempfile.NamedTemporaryFile() as f:
        df.to_parquet(f)
        return _s3_upload(
            f, f.name, remote_path_prefix=s3prefix, remote_path_suffix=".parquet"
        )


def _historical_retrieval_step(
    pyspark_script_path: str,
    entity_source_conf: Dict,
    feature_tables_sources_conf: List[Dict],
    feature_tables_conf: List[Dict],
    destination_conf: Dict,
) -> Dict[str, Any]:

    return {
        "Name": "Feast Historical Retrieval",
        "HadoopJarStep": {
            "Properties": [
                {
                    "Key": "feast.step_metadata.job_type",
                    "Value": HISTORICAL_RETRIEVAL_JOB_TYPE,
                },
            ],
            "Args": [
                "spark-submit",
                pyspark_script_path,
                "--feature-tables",
                json.dumps(feature_tables_conf),
                "--feature-tables-sources",
                json.dumps(feature_tables_sources_conf),
                "--entity-source",
                json.dumps(entity_source_conf),
                "--destination",
                json.dumps(destination_conf),
            ],
            "Jar": "command-runner.jar",
        },
    }


def _historical_feature_retrieval(
    pyspark_script: str,
    entity_source_conf: Dict,
    feature_tables_sources_conf: List[Dict],
    feature_tables_conf: List[Dict],
    destination_conf: Dict,
    config,
):
    aws_config = config.get("aws", {})
    pyspark_script_path = _s3_upload(
        BytesIO(pyspark_script.encode("utf8")),
        local_path="historical_retrieval.py",
        remote_path_prefix=aws_config["artifactS3Prefix"],
        remote_path_suffix=".py",
    )
    step = _historical_retrieval_step(
        pyspark_script_path,
        entity_source_conf,
        feature_tables_sources_conf,
        feature_tables_conf,
        destination_conf,
    )
    _submit_emr_job(step, config)


def _parse_feature_refs(
    client, feature_refs: List[str]
) -> Tuple[List[FeatureTable], List[Dict[str, Any]]]:
    """
    Return feature table objects and feature tables conf containing the subset of the features.
    """

    table_features = defaultdict(list)

    for ref in feature_refs:
        parts = ref.split(":", 1)
        table_name = parts[0]

        if len(parts) == 2:
            table_features[table_name].append(parts[1])
        else:
            table_features[table_name].append("*")

    res_dicts = []
    res_tables = []
    for table_name in table_features:
        table = client.get_feature_table(table_name)
        res_tables.append(table)
        feature_filter = set(table_features)
        features = [
            f.name
            for f in table.features
            if f.name in feature_filter or "*" in feature_filter
        ]

        res_dicts.append(_feature_table_to_json(client, table, features))
    return res_tables, res_dicts


def historical_feature_retrieval(
    client: Client,
    feature_refs: List[str],
    entity_df: pandas.DataFrame,
    destination: Optional[str] = None,
) -> str:
    """
    Run the historical retrieval job.

    Args:
        feature_refs (List[str]): feature names in table:feature format.
        entity_df: entity dataframe, must contain an event_timestamp column
        destination: s3 location to write results to. If not provided, we'll generate a random path
            under dataframeS3Prefix.

    Returns:
        str: path to the result in parquet format.
    """
    config = _load_job_service_config(_get_config_path())
    aws_config = config.get("aws", {})

    with open(Path(__file__).parent / "../historical_feature_retrieval_job.py") as f:
        pyspark_script = f.read()

    entity_df_path = _upload_dataframe(aws_config["dataframeS3Prefix"], entity_df)

    entity_source_conf = {
        "file": {
            "path": entity_df_path,
            "event_timestamp_column": "event_timestamp",
            "format": "parquet",
        }
    }

    feature_tables, feature_tables_conf = _parse_feature_refs(client, feature_refs)

    if destination:
        result_path = destination
    else:
        result_path = os.path.join(
            aws_config["dataframeS3Prefix"], _random_string(8) + ".parquet"
        )
    _historical_feature_retrieval(
        pyspark_script=pyspark_script,
        entity_source_conf=entity_source_conf,
        feature_tables_sources_conf=[
            _batch_source_to_json(ft.batch_source) for ft in feature_tables
        ],
        feature_tables_conf=feature_tables_conf,
        destination_conf={"format": "parquet", "path": result_path},
        config=config,
    )
    return result_path
