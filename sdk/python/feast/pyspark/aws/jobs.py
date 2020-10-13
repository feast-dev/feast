import hashlib
import json
import logging
import os
import random
import string
from typing import Any, Dict, Tuple

import boto3
import botocore
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
        config = yaml.load(f)
        _sanity_check_config(config, config_path)
        return config


def _random_string(length) -> str:
    return "".join(random.choice(string.ascii_letters) for _ in range(length))


def _batch_source_to_json(batch_source):
    return {
        "file": {
            "path": batch_source.file_options.file_url,
            "field_mapping": dict(batch_source.field_mapping),
            "event_timestamp_column": batch_source.event_timestamp_column,
            "created_timestamp_column": batch_source.created_timestamp_column,
            "date_partition_column": batch_source.date_partition_column,
        }
    }


def _feature_table_to_json(client: Client, feature_table):
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
    }


def _s3_split_path(path: str) -> Tuple[str, str]:
    """ Convert s3:// url to (bucket, key) """
    assert path.startswith("s3://")
    _, _, bucket, key = path.split("/", 3)
    return bucket, key


def _hash_file(local_path: str) -> str:
    """ Compute sha256 hash of a file """
    h = hashlib.sha256()
    with open(local_path, "rb") as f:
        for block in iter(lambda: f.read(2 ** 20), b""):
            h.update(block)
    return h.hexdigest()


def _s3_upload(local_path: str, remote_path: str) -> str:
    """
    Upload a local file to S3. We store the file sha256 sum in S3 metadata and skip the upload
    if the file hasn't changed.
    """
    bucket, key = _s3_split_path(remote_path)
    client = boto3.client("s3")

    sha256sum = _hash_file(local_path)

    try:
        head_response = client.head_object(Bucket=bucket, Key=key)
        if head_response["Metadata"]["sha256sum"] == sha256sum:
            # File already exists
            return remote_path
        else:
            log.info("Uploading {local_path} to {remote_path}")
            client.upload_file(
                local_path,
                bucket,
                key,
                ExtraArgs={"Metadata": {"sha256sum": sha256sum}},
            )
            return remote_path
    except botocore.exceptions.ClientError as e:
        if e.response["Error"]["Code"] == "404":
            log.info("Uploading {local_path} to {remote_path}")
            client.upload_file(
                local_path,
                bucket,
                key,
                ExtraArgs={"Metadata": {"sha256sum": sha256sum}},
            )
            return remote_path
        else:
            raise


def _upload_jar(jar_s3_prefix: str, local_path: str) -> str:
    return _s3_upload(
        local_path, os.path.join(jar_s3_prefix, os.path.basename(local_path))
    )


def _get_jar_s3_path(config) -> str:
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
                    "Value": "OFFLINE_TO_ONLINE_JOB",
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
                _get_jar_s3_path(config),
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


def _submit_emr_job(step: Dict[str, Any], config: Dict[str, Any]):
    aws_config = config.get("aws", {})

    emr = boto3.client("emr", region_name=aws_config.get("region"))

    if "existingClusterId" in aws_config:
        step["ActionOnFailure"] = "CONTINUE"
        step_ids = emr.add_job_flow_steps(
            JobFlowId=aws_config["existingClusterId"], Steps=[step],
        )
        print(step_ids)
    else:
        jobTemplate = aws_config["runJobFlowTemplate"]
        step["ActionOnFailure"] = "TERMINATE_CLUSTER"

        jobTemplate["Steps"] = [step]

        if aws_config.get("logS3Prefix"):
            jobTemplate["LogUri"] = os.path.join(
                aws_config["logS3Prefix"], _random_string(5)
            )

        job = emr.run_job_flow(**jobTemplate)
        print(job)


def sync_offline_to_online(
    client: Client, feature_table: FeatureTable, start_ts: str, end_ts: str
):
    config = _load_job_service_config(_get_config_path())
    step = _sync_offline_to_online_step(client, config, feature_table, start_ts, end_ts)
    _submit_emr_job(step, config)
