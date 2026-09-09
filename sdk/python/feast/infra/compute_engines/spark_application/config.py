import warnings
from typing import Dict, List, Literal, Optional

from pydantic import StrictStr, model_validator

from feast.repo_config import FeastConfigBaseModel


class SparkApplicationComputeEngineConfig(FeastConfigBaseModel):
    """Batch Compute Engine config for SparkApplication CRDs via Kubeflow Spark Operator."""

    type: Literal["spark_application"] = "spark_application"

    image: StrictStr
    image_pull_secrets: List[str] = []

    namespace: StrictStr = "default"
    service_account: StrictStr = ""

    driver_cores: int = 1
    driver_memory: StrictStr = "1g"
    executor_instances: int = 1
    executor_cores: int = 1
    executor_memory: StrictStr = "1g"

    spark_conf: Optional[Dict[str, str]] = None
    hadoop_conf: Optional[Dict[str, str]] = None
    spark_version: StrictStr = "4.0.1"
    staging_location: Optional[str] = None

    env: List[dict] = []
    env_from: List[dict] = []

    queue_name: Optional[str] = None

    job_timeout_seconds: int = 3600
    poll_interval_seconds: int = 10
    ttl_seconds_after_finished: int = 3600
    restart_policy: StrictStr = "Never"
    max_retries: int = 3

    concurrency: int = 1

    labels: Dict[str, str] = {}

    volumes: List[dict] = []
    volume_mounts: List[dict] = []
    py_files: List[str] = []
    node_selector: Optional[Dict[str, str]] = None
    tolerations: List[dict] = []

    @staticmethod
    def _validate_env_entry(index: int, entry: object) -> None:
        """Validate a single K8s EnvVar dict (must have name + value or valueFrom)."""
        if not isinstance(entry, dict):
            raise ValueError(
                f"env[{index}] must be a dict, got {type(entry).__name__}: {entry}"
            )
        if "name" not in entry:
            raise ValueError(
                f"env[{index}] is missing required 'name'. "
                f"Each env entry must be a K8s EnvVar dict. Got: {entry}"
            )
        if "value" not in entry and "valueFrom" not in entry:
            raise ValueError(
                f"env[{index}] must set 'value' or 'valueFrom' "
                f"(K8s EnvVar spec). Got: {entry}"
            )

    @model_validator(mode="after")
    def _validate_config(self) -> "SparkApplicationComputeEngineConfig":
        if self.staging_location:
            warnings.warn(
                "staging_location is configured but only used for "
                "get_historical_features (not yet supported by this engine). "
                "It will be ignored for materialize operations.",
                stacklevel=2,
            )
        for i, entry in enumerate(self.env):
            self._validate_env_entry(i, entry)
        return self
