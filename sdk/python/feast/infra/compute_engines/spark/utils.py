import logging
import os
from typing import Dict, Iterable, Literal, Optional

import pandas as pd
import pyarrow
import pyarrow as pa
from pyspark import SparkConf
from pyspark.sql import SparkSession

from feast.infra.common.serde import SerializedArtifacts
from feast.utils import _convert_arrow_to_proto, _run_pyarrow_field_mapping

try:
    import boto3
    from botocore.client import Config as BotoConfig
except ImportError:
    boto3 = None  # type: ignore[assignment]
    BotoConfig = None  # type: ignore[assignment,misc]

logger = logging.getLogger(__name__)


def _ensure_s3a_event_log_dir(spark_config: Dict[str, str]) -> None:
    """Pre-create the S3A event log prefix before SparkContext initialisation.

    Spark's EventLogFileWriter.requireLogBaseDirAsDirectory() is called inside
    SparkContext.__init__ and crashes if the S3A path doesn't exist yet (S3 has no
    real directories, so an empty prefix returns a 404). This function writes a
    zero-byte placeholder so the prefix exists before SparkContext is built.

    This is only attempted when:
      - spark.eventLog.enabled == "true"
      - spark.eventLog.dir starts with "s3a://"
    Failures are non-fatal: Spark will surface its own error if the dir is still missing.
    """
    if spark_config.get("spark.eventLog.enabled", "false").lower() != "true":
        return
    event_dir = spark_config.get("spark.eventLog.dir", "")
    if not event_dir.startswith("s3a://"):
        return

    path = event_dir[len("s3a://") :]
    bucket, _, prefix = path.partition("/")
    prefix = prefix.rstrip("/")
    prefix = (prefix + "/") if prefix else prefix
    placeholder_key = prefix + ".keep"

    endpoint = spark_config.get(
        "spark.hadoop.fs.s3a.endpoint",
        os.environ.get("AWS_ENDPOINT_URL", ""),
    )
    access_key = spark_config.get(
        "spark.hadoop.fs.s3a.access.key",
        os.environ.get("AWS_ACCESS_KEY_ID", ""),
    )
    secret_key = spark_config.get(
        "spark.hadoop.fs.s3a.secret.key",
        os.environ.get("AWS_SECRET_ACCESS_KEY", ""),
    )
    session_token = (
        spark_config.get(
            "spark.hadoop.fs.s3a.session.token",
            os.environ.get("AWS_SESSION_TOKEN", ""),
        )
        or None
    )

    try:
        if boto3 is None:
            raise ImportError("boto3 is not installed")

        addressing_style = (
            "path"
            if spark_config.get(
                "spark.hadoop.fs.s3a.path.style.access", "false"
            ).lower()
            == "true"
            else "auto"
        )

        s3 = boto3.client(
            "s3",
            endpoint_url=endpoint if endpoint else None,
            aws_access_key_id=access_key or None,
            aws_secret_access_key=secret_key or None,
            aws_session_token=session_token,
            config=BotoConfig(
                signature_version="s3v4",
                s3={"addressing_style": addressing_style},
            ),
        )
        resp = s3.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=1)
        if resp.get("KeyCount", 0) == 0:
            s3.put_object(Bucket=bucket, Key=placeholder_key, Body=b"")
            logger.debug(
                "Created S3A event log dir placeholder: s3a://%s/%s",
                bucket,
                placeholder_key,
            )
    except Exception as exc:
        logger.warning(
            "Could not pre-create S3A event log dir s3a://%s/%s — "
            "SparkContext may fail if the path still doesn't exist: %s",
            bucket,
            prefix,
            exc,
        )


def get_or_create_new_spark_session(
    spark_config: Optional[Dict[str, str]] = None,
) -> SparkSession:
    spark_session = SparkSession.getActiveSession()
    if not spark_session:
        spark_builder = SparkSession.builder
        if spark_config:
            _ensure_s3a_event_log_dir(spark_config)
            spark_builder = spark_builder.config(
                conf=SparkConf().setAll([(k, v) for k, v in spark_config.items()])
            )

        spark_session = spark_builder.getOrCreate()
    spark_session.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
    return spark_session


def map_in_arrow(
    iterator: Iterable[pa.RecordBatch],
    serialized_artifacts: "SerializedArtifacts",
    mode: Literal["online", "offline"] = "online",
):
    for batch in iterator:
        table = pa.Table.from_batches([batch])

        (
            feature_view,
            online_store,
            offline_store,
            repo_config,
        ) = serialized_artifacts.unserialize()

        if mode == "online":
            join_key_to_value_type = {
                entity.name: entity.dtype.to_value_type()
                for entity in feature_view.entity_columns
            }

            batch_size = repo_config.materialization_config.online_write_batch_size
            # Single batch if None (backward compatible), otherwise use configured batch_size
            sub_batches = (
                [table]
                if batch_size is None
                else table.to_batches(max_chunksize=batch_size)
            )
            for sub_batch in sub_batches:
                rows_to_write = _convert_arrow_to_proto(
                    sub_batch, feature_view, join_key_to_value_type
                )

                online_store.online_write_batch(
                    config=repo_config,
                    table=feature_view,
                    data=rows_to_write,
                    progress=lambda x: None,
                )
        if mode == "offline":
            offline_store.offline_write_batch(
                config=repo_config,
                feature_view=feature_view,
                table=table,
                progress=lambda x: None,
            )

        yield batch


def map_in_pandas(iterator, serialized_artifacts: SerializedArtifacts):
    for pdf in iterator:
        if pdf.shape[0] == 0:
            print("Skipping")
            return

        table = pyarrow.Table.from_pandas(pdf)

        (
            feature_view,
            online_store,
            _,
            repo_config,
        ) = serialized_artifacts.unserialize()

        if feature_view.batch_source.field_mapping is not None:
            # Spark offline store does the field mapping in pull_latest_from_table_or_query() call
            # This may be needed in future if this materialization engine supports other offline stores
            table = _run_pyarrow_field_mapping(
                table, feature_view.batch_source.field_mapping
            )

        join_key_to_value_type = {
            entity.name: entity.dtype.to_value_type()
            for entity in feature_view.entity_columns
        }

        batch_size = repo_config.materialization_config.online_write_batch_size
        # Single batch if None (backward compatible), otherwise use configured batch_size
        sub_batches = (
            [table]
            if batch_size is None
            else table.to_batches(max_chunksize=batch_size)
        )
        for sub_batch in sub_batches:
            rows_to_write = _convert_arrow_to_proto(
                sub_batch, feature_view, join_key_to_value_type
            )
            online_store.online_write_batch(
                repo_config,
                feature_view,
                rows_to_write,
                lambda x: None,
            )

    yield pd.DataFrame(
        [pd.Series(range(1, 2))]
    )  # dummy result because mapInPandas needs to return something
