import logging
import os
import random
import string
import time
from datetime import datetime
from typing import Any, Dict, List, NamedTuple, Optional
from urllib.parse import urlparse, urlunparse

import pytz
import yaml

from feast.pyspark.abc import BQ_SPARK_PACKAGE

__all__ = [
    "FAILED_STEP_STATES",
    "HISTORICAL_RETRIEVAL_JOB_TYPE",
    "IN_PROGRESS_STEP_STATES",
    "OFFLINE_TO_ONLINE_JOB_TYPE",
    "STREAM_TO_ONLINE_JOB_TYPE",
    "SUCCEEDED_STEP_STATES",
    "TERMINAL_STEP_STATES",
    "EmrJobRef",
    "JobInfo",
    "_cancel_job",
    "_get_job_creation_time",
    "_get_job_state",
    "_historical_retrieval_step",
    "_job_ref_to_str",
    "_list_jobs",
    "_load_new_cluster_template",
    "_random_string",
    "_stream_ingestion_step",
    "_sync_offline_to_online_step",
    "_upload_jar",
    "_wait_for_job_state",
]
from feast.staging.storage_client import get_staging_client

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


def _upload_jar(jar_s3_prefix: str, local_path: str) -> str:
    with open(local_path, "rb") as f:
        uri = urlparse(os.path.join(jar_s3_prefix, os.path.basename(local_path)))
        return urlunparse(
            get_staging_client(uri.scheme).upload_fileobj(
                f, local_path, remote_uri=uri,
            )
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
                BQ_SPARK_PACKAGE,
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
    job_hash: Optional[str]


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

                    job_hash = props.get("feast.step_metadata.job_hash")

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
                            job_hash=job_hash,
                        )
                    )
    return res


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


def _get_job_creation_time(emr_client, job: EmrJobRef) -> datetime:
    if job.step_id is None:
        step_id = _get_first_step_id(emr_client, job.cluster_id)
    else:
        step_id = job.step_id

    return _get_step_creation_time(emr_client, job.cluster_id, step_id)


def _get_step_creation_time(emr_client, cluster_id: str, step_id: str) -> datetime:
    response = emr_client.describe_step(ClusterId=cluster_id, StepId=step_id)
    step_creation_time = response["Step"]["Status"]["Timeline"]["CreationDateTime"]
    return step_creation_time.astimezone(pytz.utc).replace(tzinfo=None)


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


def _historical_retrieval_step(
    pyspark_script_path: str,
    args: List[str],
    output_file_uri: str,
    packages: List[str] = None,
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
            "Args": ["spark-submit"]
            + (["--packages", ",".join(packages)] if packages else [])
            + [pyspark_script_path]
            + args,
            "Jar": "command-runner.jar",
        },
    }


def _stream_ingestion_step(
    jar_path: str,
    extra_jar_paths: List[str],
    feature_table_name: str,
    args: List[str],
    job_hash: str,
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
                {"Key": "feast.step_metadata.job_hash", "Value": job_hash},
            ],
            "Args": ["spark-submit", "--class", "feast.ingestion.IngestionJob"]
            + jars_args
            + ["--conf", "spark.yarn.isPython=true"]
            + ["--packages", BQ_SPARK_PACKAGE, jar_path]
            + args,
            "Jar": "command-runner.jar",
        },
    }
