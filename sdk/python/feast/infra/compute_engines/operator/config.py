import warnings
from typing import Dict, List, Optional

from pydantic import StrictStr, model_validator

from feast.repo_config import FeastConfigBaseModel


class OperatorComputeEngineConfig(FeastConfigBaseModel):
    """Shared config for Kubernetes operator-backed batch compute engines.

    Engines that launch a Feast materialization run inside a cluster via a
    Kubernetes operator custom resource share these fields. Engine-specific
    fields (driver/executor sizing, runtime versions, etc.) live on subclasses.

    Subclasses must declare the ``type`` Literal that selects the engine in
    ``feature_store.yaml``.
    """

    image: StrictStr
    """ The container image that runs the in-cluster materialization job. """

    image_pull_secrets: List[str] = []
    """ (optional) Names of image pull secrets for the job image. """

    namespace: StrictStr = "default"
    """ (optional) Namespace used for the custom resource and ConfigMap. """

    service_account: StrictStr = ""
    """ (optional) Service account the job pods run as. """

    env: List[dict] = []
    """ (optional) Environment variables (K8s EnvVar dicts) for the job pods. """

    env_from: List[dict] = []
    """ (optional) envFrom sources (K8s EnvFromSource dicts) for the job pods. """

    queue_name: Optional[str] = None
    """ (optional) Kueue local queue name, applied as a label on the resource. """

    job_timeout_seconds: int = 3600
    """ (optional) Max seconds to wait for the job to complete before erroring. """

    poll_interval_seconds: int = 10
    """ (optional) Seconds between status polls of the custom resource. """

    ttl_seconds_after_finished: int = 3600
    """ (optional) Operator-managed TTL for the finished custom resource. """

    max_retries: int = 3
    """ (optional) Max in-cluster retries the operator should attempt. """

    concurrency: int = 1
    """ (optional) Number of feature views materialized in parallel in-pod. """

    labels: Dict[str, str] = {}
    """ (optional) Additional labels applied to created Kubernetes objects. """

    volumes: List[dict] = []
    """ (optional) Extra volumes mounted into the job pods. """

    volume_mounts: List[dict] = []
    """ (optional) Extra volume mounts for the job pods. """

    node_selector: Optional[Dict[str, str]] = None
    """ (optional) Node selector for the job pods. """

    tolerations: List[dict] = []
    """ (optional) Tolerations for the job pods. """

    staging_location: Optional[str] = None
    """ (optional) Staging location for get_historical_features (not yet used). """

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
    def _validate_operator_config(self) -> "OperatorComputeEngineConfig":
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
