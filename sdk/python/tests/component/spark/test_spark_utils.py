from unittest.mock import MagicMock, patch

from feast.infra.compute_engines.spark.utils import _ensure_s3a_event_log_dir

BOTO3_PATH = "feast.infra.compute_engines.spark.utils.boto3"
BOTOCONFIG_PATH = "feast.infra.compute_engines.spark.utils.BotoConfig"


def _base_conf(event_log_dir: str) -> dict:
    return {
        "spark.eventLog.enabled": "true",
        "spark.eventLog.dir": event_log_dir,
        "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
    }


@patch(BOTOCONFIG_PATH, MagicMock())
@patch(BOTO3_PATH)
def test_ensure_s3a_event_log_dir_creates_placeholder_when_empty(mock_boto3):
    """S3A prefix doesn't exist -> placeholder object is written."""
    s3 = MagicMock()
    mock_boto3.client.return_value = s3
    s3.list_objects_v2.return_value = {"KeyCount": 0}

    _ensure_s3a_event_log_dir(_base_conf("s3a://my-bucket/spark-events/"))

    s3.list_objects_v2.assert_called_once_with(
        Bucket="my-bucket", Prefix="spark-events/", MaxKeys=1
    )
    s3.put_object.assert_called_once_with(
        Bucket="my-bucket", Key="spark-events/.keep", Body=b""
    )


@patch(BOTOCONFIG_PATH, MagicMock())
@patch(BOTO3_PATH)
def test_ensure_s3a_event_log_dir_skips_when_prefix_exists(mock_boto3):
    """S3A prefix already has objects -> no placeholder written."""
    s3 = MagicMock()
    mock_boto3.client.return_value = s3
    s3.list_objects_v2.return_value = {"KeyCount": 3}

    _ensure_s3a_event_log_dir(_base_conf("s3a://my-bucket/spark-events/"))

    s3.put_object.assert_not_called()


@patch(BOTOCONFIG_PATH, MagicMock())
@patch(BOTO3_PATH)
def test_ensure_s3a_event_log_dir_noop_when_event_log_disabled(mock_boto3):
    """spark.eventLog.enabled != true -> boto3 never called."""
    _ensure_s3a_event_log_dir(
        {"spark.eventLog.enabled": "false", "spark.eventLog.dir": "s3a://b/p/"}
    )
    mock_boto3.client.assert_not_called()


@patch(BOTOCONFIG_PATH, MagicMock())
@patch(BOTO3_PATH)
def test_ensure_s3a_event_log_dir_noop_for_non_s3a_path(mock_boto3):
    """Non-S3A paths (hdfs://, file://, etc.) are left untouched."""
    _ensure_s3a_event_log_dir(
        {"spark.eventLog.enabled": "true", "spark.eventLog.dir": "hdfs:///spark-logs"}
    )
    mock_boto3.client.assert_not_called()


@patch(BOTOCONFIG_PATH, MagicMock())
@patch(BOTO3_PATH)
def test_ensure_s3a_event_log_dir_non_fatal_on_s3_error(mock_boto3):
    """boto3 errors are swallowed -> SparkContext will surface its own error."""
    s3 = MagicMock()
    mock_boto3.client.return_value = s3
    s3.list_objects_v2.side_effect = Exception("connection refused")

    _ensure_s3a_event_log_dir(_base_conf("s3a://my-bucket/spark-events/"))


# ---------------------------------------------------------------------------
# Bucket-root edge cases (s3a://bucket, s3a://bucket/)
# ---------------------------------------------------------------------------


@patch(BOTOCONFIG_PATH, MagicMock())
@patch(BOTO3_PATH)
def test_ensure_s3a_event_log_dir_bucket_root_no_trailing_slash(mock_boto3):
    """s3a://bucket (no path) -> .keep at bucket root, not /.keep."""
    s3 = MagicMock()
    mock_boto3.client.return_value = s3
    s3.list_objects_v2.return_value = {"KeyCount": 0}

    _ensure_s3a_event_log_dir(_base_conf("s3a://my-bucket"))

    s3.list_objects_v2.assert_called_once_with(Bucket="my-bucket", Prefix="", MaxKeys=1)
    s3.put_object.assert_called_once_with(Bucket="my-bucket", Key=".keep", Body=b"")


@patch(BOTOCONFIG_PATH, MagicMock())
@patch(BOTO3_PATH)
def test_ensure_s3a_event_log_dir_bucket_root_trailing_slash(mock_boto3):
    """s3a://bucket/ (trailing slash, empty prefix) -> .keep at bucket root."""
    s3 = MagicMock()
    mock_boto3.client.return_value = s3
    s3.list_objects_v2.return_value = {"KeyCount": 0}

    _ensure_s3a_event_log_dir(_base_conf("s3a://my-bucket/"))

    s3.list_objects_v2.assert_called_once_with(Bucket="my-bucket", Prefix="", MaxKeys=1)
    s3.put_object.assert_called_once_with(Bucket="my-bucket", Key=".keep", Body=b"")


# ---------------------------------------------------------------------------
# Credentials from spark config / env var fallback
# ---------------------------------------------------------------------------


@patch.dict(
    "os.environ",
    {
        "AWS_ACCESS_KEY_ID": "env-ak",
        "AWS_SECRET_ACCESS_KEY": "env-sk",  # pragma: allowlist secret
        "AWS_SESSION_TOKEN": "env-st",
    },
)
@patch(BOTOCONFIG_PATH, MagicMock())
@patch(BOTO3_PATH)
def test_ensure_s3a_event_log_dir_uses_spark_config_credentials(mock_boto3):
    """Credentials in spark config take precedence over env vars."""
    s3 = MagicMock()
    mock_boto3.client.return_value = s3
    s3.list_objects_v2.return_value = {"KeyCount": 1}

    conf = {
        **_base_conf("s3a://my-bucket/logs/"),
        "spark.hadoop.fs.s3a.access.key": "spark-ak",
        "spark.hadoop.fs.s3a.secret.key": "spark-sk",  # pragma: allowlist secret
        "spark.hadoop.fs.s3a.session.token": "spark-st",
    }
    _ensure_s3a_event_log_dir(conf)

    mock_boto3.client.assert_called_once()
    kwargs = mock_boto3.client.call_args
    assert kwargs.kwargs["aws_access_key_id"] == "spark-ak"
    assert kwargs.kwargs["aws_secret_access_key"] == "spark-sk"  # pragma: allowlist secret
    assert kwargs.kwargs["aws_session_token"] == "spark-st"


@patch.dict(
    "os.environ",
    {
        "AWS_ACCESS_KEY_ID": "env-ak",
        "AWS_SECRET_ACCESS_KEY": "env-sk",  # pragma: allowlist secret
        "AWS_SESSION_TOKEN": "env-st",
    },
)
@patch(BOTOCONFIG_PATH, MagicMock())
@patch(BOTO3_PATH)
def test_ensure_s3a_event_log_dir_falls_back_to_env_credentials(mock_boto3):
    """Without spark config keys, env vars are used."""
    s3 = MagicMock()
    mock_boto3.client.return_value = s3
    s3.list_objects_v2.return_value = {"KeyCount": 1}

    _ensure_s3a_event_log_dir(_base_conf("s3a://my-bucket/logs/"))

    mock_boto3.client.assert_called_once()
    kwargs = mock_boto3.client.call_args
    assert kwargs.kwargs["aws_access_key_id"] == "env-ak"
    assert kwargs.kwargs["aws_secret_access_key"] == "env-sk"  # pragma: allowlist secret
    assert kwargs.kwargs["aws_session_token"] == "env-st"


@patch.dict("os.environ", {}, clear=True)
@patch(BOTOCONFIG_PATH, MagicMock())
@patch(BOTO3_PATH)
def test_ensure_s3a_event_log_dir_no_credentials_passes_none(mock_boto3):
    """No credentials anywhere -> None passed to boto3 (anonymous / instance role)."""
    s3 = MagicMock()
    mock_boto3.client.return_value = s3
    s3.list_objects_v2.return_value = {"KeyCount": 1}

    conf = {
        "spark.eventLog.enabled": "true",
        "spark.eventLog.dir": "s3a://my-bucket/logs/",
    }
    _ensure_s3a_event_log_dir(conf)

    mock_boto3.client.assert_called_once()
    kwargs = mock_boto3.client.call_args
    assert kwargs.kwargs["aws_access_key_id"] is None
    assert kwargs.kwargs["aws_secret_access_key"] is None
    assert kwargs.kwargs["aws_session_token"] is None


# ---------------------------------------------------------------------------
# Path-style addressing (MinIO / S3-compatible)
# ---------------------------------------------------------------------------


@patch(BOTOCONFIG_PATH)
@patch(BOTO3_PATH)
def test_ensure_s3a_event_log_dir_path_style_when_enabled(mock_boto3, mock_config_cls):
    """spark.hadoop.fs.s3a.path.style.access=true -> addressing_style='path'."""
    s3 = MagicMock()
    mock_boto3.client.return_value = s3
    s3.list_objects_v2.return_value = {"KeyCount": 1}

    conf = {
        **_base_conf("s3a://my-bucket/logs/"),
        "spark.hadoop.fs.s3a.path.style.access": "true",
    }
    _ensure_s3a_event_log_dir(conf)

    mock_config_cls.assert_called_once()
    config_kwargs = mock_config_cls.call_args
    assert config_kwargs.kwargs["s3"] == {"addressing_style": "path"}


@patch(BOTOCONFIG_PATH)
@patch(BOTO3_PATH)
def test_ensure_s3a_event_log_dir_virtual_hosted_style_by_default(
    mock_boto3, mock_config_cls
):
    """No path.style.access config -> addressing_style='auto'."""
    s3 = MagicMock()
    mock_boto3.client.return_value = s3
    s3.list_objects_v2.return_value = {"KeyCount": 1}

    _ensure_s3a_event_log_dir(_base_conf("s3a://my-bucket/logs/"))

    mock_config_cls.assert_called_once()
    config_kwargs = mock_config_cls.call_args
    assert config_kwargs.kwargs["s3"] == {"addressing_style": "auto"}


# ---------------------------------------------------------------------------
# Endpoint env var fallback (AWS_ENDPOINT_URL)
# ---------------------------------------------------------------------------


@patch.dict("os.environ", {"AWS_ENDPOINT_URL": "http://localhost:9000"}, clear=True)
@patch(BOTOCONFIG_PATH, MagicMock())
@patch(BOTO3_PATH)
def test_ensure_s3a_event_log_dir_endpoint_from_env(mock_boto3):
    """AWS_ENDPOINT_URL env var is used when spark config has no endpoint."""
    s3 = MagicMock()
    mock_boto3.client.return_value = s3
    s3.list_objects_v2.return_value = {"KeyCount": 1}

    conf = {
        "spark.eventLog.enabled": "true",
        "spark.eventLog.dir": "s3a://my-bucket/logs/",
    }
    _ensure_s3a_event_log_dir(conf)

    mock_boto3.client.assert_called_once()
    kwargs = mock_boto3.client.call_args
    assert kwargs.kwargs["endpoint_url"] == "http://localhost:9000"


@patch.dict("os.environ", {"AWS_ENDPOINT_URL": "http://env-endpoint:9000"}, clear=True)
@patch(BOTOCONFIG_PATH, MagicMock())
@patch(BOTO3_PATH)
def test_ensure_s3a_event_log_dir_spark_endpoint_over_env(mock_boto3):
    """spark.hadoop.fs.s3a.endpoint takes precedence over AWS_ENDPOINT_URL."""
    s3 = MagicMock()
    mock_boto3.client.return_value = s3
    s3.list_objects_v2.return_value = {"KeyCount": 1}

    _ensure_s3a_event_log_dir(_base_conf("s3a://my-bucket/logs/"))

    mock_boto3.client.assert_called_once()
    kwargs = mock_boto3.client.call_args
    assert kwargs.kwargs["endpoint_url"] == "http://minio:9000"
