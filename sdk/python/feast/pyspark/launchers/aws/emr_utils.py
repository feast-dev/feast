import hashlib
import logging
import os
import random
import string
import tempfile
import time
from typing import IO, Any, Dict, List, NamedTuple, Optional, Tuple

import boto3
import botocore
import pandas
import yaml

log = logging.getLogger("aws")

SUPPORTED_EMR_VERSION = "emr-6.0.0"
STREAM_TO_ONLINE_JOB_TYPE = "STREAM_TO_ONLINE_JOB"
OFFLINE_TO_ONLINE_JOB_TYPE = "OFFLINE_TO_ONLINE_JOB"
HISTORICAL_RETRIEVAL_JOB_TYPE = "HISTORICAL_RETRIEVAL_JOB"


# Mapping of EMR states to "active" vs terminated for whatever reason
ACTIVE_STEP_STATES = ["PENDING", "CANCEL_PENDING", "RUNNING"]
TERMINAL_STEP_STATES = ["COMPLETED", "CANCELLED", "FAILED", "INTERRUPTED"]

# Mapping of EMR states to generic states
IN_PROGRESS_STEP_STATES = ["PENDING", "CANCEL_PENDING", "RUNNING"]
SUCCEEDED_STEP_STATES = ["COMPLETED"]
FAILED_STEP_STATES = ["CANCELLED", "FAILED", "INTERRUPTED"]


def _sanity_check_cluster_template(template: Dict[str, Any], template_path: str):
    """
    Sanity check the run job flow template. We don't really have to do this here but if the spark
    job fails you'll only find out much later and this is annoying. Those are not exhaustive, just
    some checks to help debugging common configuration issues.
    """

    releaseLabel = template.get("ReleaseLabel")
    if releaseLabel != SUPPORTED_EMR_VERSION:
        log.warn(
            f"{template_path}: ReleaseLabel is set to {releaseLabel}. Recommended: {SUPPORTED_EMR_VERSION}"
        )


def _load_new_cluster_template(path: str) -> Dict[str, Any]:
    with open(path) as f:
        template = yaml.safe_load(f)
        _sanity_check_cluster_template(template, path)
        return template


def _random_string(length) -> str:
    return "".join(random.choice(string.ascii_lowercase) for _ in range(length))


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


def _sync_offline_to_online_step(
    jar_path: str, feature_table_name: str, args: List[str],
) -> Dict[str, Any]:

    return {
        "Name": "Feast Ingestion",
        "HadoopJarStep": {
            "Properties": [
                {
                    "Key": "feast.step_metadata.job_type",
                    "Value": OFFLINE_TO_ONLINE_JOB_TYPE,
                },
                {
                    "Key": "feast.step_metadata.offline_to_online.table_name",
                    "Value": feature_table_name,
                },
            ],
            "Args": [
                "spark-submit",
                "--class",
                "feast.ingestion.IngestionJob",
                "--packages",
                "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.17.2",
                jar_path,
            ]
            + args,
            "Jar": "command-runner.jar",
        },
    }


class EmrJobRef(NamedTuple):
    """ EMR job reference. step_id can be None when using on-demand clusters, in that case each
    cluster has only one step """

    cluster_id: str
    step_id: Optional[str]


def _job_ref_to_str(job_ref: EmrJobRef) -> str:
    return ":".join(["emr", job_ref.cluster_id, job_ref.step_id or ""])


class JobInfo(NamedTuple):
    job_ref: EmrJobRef
    job_type: str
    state: str
    table_name: Optional[str]
    output_file_uri: Optional[str]


def _list_jobs(
    emr_client, job_type: Optional[str], table_name: Optional[str], active_only=True
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

    paginator = emr_client.get_paginator("list_clusters")
    res: List[JobInfo] = []
    for page in paginator.paginate(
        ClusterStates=["STARTING", "BOOTSTRAPPING", "RUNNING", "WAITING", "TERMINATING"]
    ):
        for cluster in page["Clusters"]:
            cluster_id = cluster["Id"]
            step_paginator = emr_client.get_paginator("list_steps")

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

                    output_file_uri = props.get(
                        "feast.step_metadata.historical_retrieval.output_file_uri"
                    )

                    if table_name and step_table_name != table_name:
                        continue

                    if job_type and step_job_type != job_type:
                        continue

                    res.append(
                        JobInfo(
                            job_type=step_job_type,
                            job_ref=EmrJobRef(cluster_id, step["Id"]),
                            state=step["Status"]["State"],
                            table_name=step_table_name,
                            output_file_uri=output_file_uri,
                        )
                    )
    return res


def _get_stream_to_online_job(emr_client, table_name: str) -> List[JobInfo]:
    return _list_jobs(
        emr_client,
        job_type=STREAM_TO_ONLINE_JOB_TYPE,
        table_name=table_name,
        active_only=True,
    )


def _get_first_step_id(emr_client, cluster_id: str) -> str:
    response = emr_client.list_steps(ClusterId=cluster_id,)
    assert len(response["Steps"]) == 1
    return response["Steps"][0]["Id"]


def _wait_for_job_state(
    emr_client,
    job: EmrJobRef,
    desired_states: List[str],
    timeout_seconds: Optional[int],
) -> str:
    if job.step_id is None:
        step_id = _get_first_step_id(emr_client, job.cluster_id)
    else:
        step_id = job.step_id

    return _wait_for_step_state(
        emr_client, job.cluster_id, step_id, desired_states, timeout_seconds
    )


def _get_job_state(emr_client, job: EmrJobRef):
    if job.step_id is None:
        step_id = _get_first_step_id(emr_client, job.cluster_id)
    else:
        step_id = job.step_id

    return _get_step_state(emr_client, job.cluster_id, step_id)


def _get_step_state(emr_client, cluster_id: str, step_id: str) -> str:
    response = emr_client.describe_step(ClusterId=cluster_id, StepId=step_id)
    state = response["Step"]["Status"]["State"]
    return state


def _wait_for_step_state(
    emr_client,
    cluster_id: str,
    step_id: str,
    desired_states: List[str],
    timeout_seconds: Optional[int],
) -> str:
    """
    Wait up to timeout seconds for job to go into one of the desired states.
    """
    start_time = time.time()
    while (timeout_seconds is None) or (time.time() - start_time < timeout_seconds):
        state = _get_step_state(emr_client, cluster_id, step_id)
        if state in desired_states:
            return state
        else:
            time.sleep(1)
    else:
        raise TimeoutError(
            f'Timeout waiting for job state to become {"|".join(desired_states)}'
        )


def _cancel_job(emr_client, job: EmrJobRef):
    if job.step_id is None:
        step_id = _get_first_step_id(emr_client, job.cluster_id)
    else:
        step_id = job.step_id

    emr_client.cancel_steps(
        ClusterId=job.cluster_id,
        StepIds=[step_id],
        StepCancellationOption="TERMINATE_PROCESS",
    )

    _wait_for_job_state(
        emr_client, EmrJobRef(job.cluster_id, step_id), TERMINAL_STEP_STATES, 180
    )


def _upload_dataframe(s3prefix: str, df: pandas.DataFrame) -> str:
    with tempfile.NamedTemporaryFile() as f:
        df.to_parquet(f)
        return _s3_upload(
            f, f.name, remote_path_prefix=s3prefix, remote_path_suffix=".parquet"
        )


def _historical_retrieval_step(
    pyspark_script_path: str, args: List[str], output_file_uri: str,
) -> Dict[str, Any]:

    return {
        "Name": "Feast Historical Retrieval",
        "HadoopJarStep": {
            "Properties": [
                {
                    "Key": "feast.step_metadata.job_type",
                    "Value": HISTORICAL_RETRIEVAL_JOB_TYPE,
                },
                {
                    "Key": "feast.step_metadata.historical_retrieval.output_file_uri",
                    "Value": output_file_uri,
                },
            ],
            "Args": ["spark-submit", pyspark_script_path] + args,
            "Jar": "command-runner.jar",
        },
    }


def _stream_ingestion_step(
    jar_path: str, extra_jar_paths: List[str], feature_table_name: str, args: List[str],
) -> Dict[str, Any]:

    if extra_jar_paths:
        jars_args = ["--jars", ",".join(extra_jar_paths)]
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
                    "Value": feature_table_name,
                },
            ],
            "Args": ["spark-submit", "--class", "feast.ingestion.IngestionJob"]
            + jars_args
            + [
                "--packages",
                "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.17.2",
                jar_path,
            ]
            + args,
            "Jar": "command-runner.jar",
        },
    }
