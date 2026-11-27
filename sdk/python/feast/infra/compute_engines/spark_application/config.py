from typing import Dict, List, Literal, Optional

from pydantic import StrictStr

from feast.infra.compute_engines.operator.config import OperatorComputeEngineConfig


class SparkApplicationComputeEngineConfig(OperatorComputeEngineConfig):
    """Batch Compute Engine config for SparkApplication CRDs via Kubeflow Spark Operator.

    Shared operator fields (image, namespace, env, timeouts, volumes, ...) are
    inherited from :class:`OperatorComputeEngineConfig`; the fields below are
    Spark-specific.
    """

    type: Literal["spark_application"] = "spark_application"

    driver_cores: int = 1
    driver_memory: StrictStr = "1g"
    executor_instances: int = 1
    executor_cores: int = 1
    executor_memory: StrictStr = "1g"

    spark_conf: Optional[Dict[str, str]] = None
    hadoop_conf: Optional[Dict[str, str]] = None
    spark_version: StrictStr = "4.0.1"

    restart_policy: StrictStr = "Never"
    py_files: List[str] = []
