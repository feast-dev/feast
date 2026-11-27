import logging
from typing import Any, Dict, Optional

from kubernetes.client.exceptions import ApiException

from feast.infra.common.materialization_job import MaterializationJob
from feast.infra.compute_engines.operator.compute import OperatorComputeEngine
from feast.infra.compute_engines.operator.job import OperatorMaterializationJob

from .config import (
    SparkApplicationComputeEngineConfig,  # noqa: F401 — required for Feast config resolution
)
from .job import (
    CompletedMaterializationJob,
    SparkApplicationMaterializationJob,
)

logger = logging.getLogger(__name__)


class SparkApplicationComputeEngine(OperatorComputeEngine):
    """Batch compute engine that runs materialization as a ``SparkApplication``
    custom resource via the Kubeflow Spark Operator.

    The generic operator-launcher lifecycle (ConfigMap, submit, poll, cleanup,
    per-FV resolution) lives in :class:`OperatorComputeEngine`; this subclass
    supplies the Spark-specific custom resource and job wiring.
    """

    _cr_group = "sparkoperator.k8s.io"
    _cr_version = "v1beta2"
    _cr_plural = "sparkapplications"
    _cr_kind = "SparkApplication"
    _resource_prefix = "feast-sa"
    _pod_batch_engine_type = "spark.engine"

    # ── Operator hooks ──────────────────────────────────────────────
    def _make_job(
        self, job_id: str, error: Optional[BaseException] = None
    ) -> OperatorMaterializationJob:
        return SparkApplicationMaterializationJob(
            job_id, self.config.namespace, self.custom_api, error=error
        )

    def _make_completed_job(self, job_id: str) -> MaterializationJob:
        return CompletedMaterializationJob(job_id)

    def _build_cr(self, job_id: str, **kwargs) -> dict:
        from feast.openlineage.identity import coerce_lineage_parent

        lineage_parent = coerce_lineage_parent(
            kwargs.get("lineage_parent") or kwargs.get("openlineage_parent")
        )
        return self._build_spark_application_cr(job_id, lineage_parent=lineage_parent)

    # ── Spark-specific custom resource ──────────────────────────────
    def _build_spark_application_cr(
        self, job_id: str, lineage_parent: Optional[Any] = None
    ) -> dict:
        driver_env_conf = {
            "spark.kubernetes.driverEnv.FEAST_CONFIGMAP_NAME": f"feast-sa-{job_id}",
            "spark.kubernetes.driverEnv.FEAST_CONFIGMAP_NAMESPACE": self.config.namespace,
        }
        for entry in self.config.env:
            name = entry.get("name", "")
            if name and "value" in entry and entry["value"] is not None:
                driver_env_conf[f"spark.kubernetes.driverEnv.{name}"] = str(
                    entry["value"]
                )

        from feast.openlineage.identity import (
            coerce_lineage_parent,
            spark_compute_job_name,
        )

        # K8s names stay unique per run; OL job identity is stable.
        k8s_app_name = f"feast-sa-{job_id}"
        project = getattr(self.repo_config, "project", None) or "feast"
        parent_conf: Dict[str, str] = {
            "spark.app.name": k8s_app_name,
            "spark.openlineage.appName": spark_compute_job_name(project),
        }
        parent = coerce_lineage_parent(lineage_parent)
        if parent:
            parent_conf.update(parent.to_spark_openlineage_conf())

        spec = {
            "type": "Python",
            "mode": "cluster",
            "pythonVersion": "3",
            "image": self.config.image,
            "imagePullPolicy": "IfNotPresent",
            "mainApplicationFile": "local:///opt/feast/main.py",
            "sparkVersion": self.config.spark_version,
            "sparkConf": {
                "spark.scheduler.mode": "FAIR",
                **(self.config.spark_conf or {}),
                **parent_conf,
                **driver_env_conf,
            },
            "restartPolicy": {
                "type": self.config.restart_policy,
                "onFailureRetries": self.config.max_retries,
                "onFailureRetryInterval": 30,
            },
            "timeToLiveSeconds": self.config.ttl_seconds_after_finished,
            "volumes": [
                {
                    "name": "feast-config",
                    "configMap": {"name": f"feast-sa-{job_id}"},
                },
                *self.config.volumes,
            ],
            "driver": {
                "cores": self.config.driver_cores,
                "memory": self.config.driver_memory,
                "serviceAccount": self.config.service_account,
                "volumeMounts": [
                    {"name": "feast-config", "mountPath": "/var/feast/"},
                    *self.config.volume_mounts,
                ],
            },
            "executor": {
                "instances": max(self.config.executor_instances, 1),
                "cores": self.config.executor_cores,
                "memory": self.config.executor_memory,
            },
        }

        if self.config.image_pull_secrets:
            spec["imagePullSecrets"] = self.config.image_pull_secrets
        if self.config.hadoop_conf:
            spec["hadoopConf"] = self.config.hadoop_conf
        if self.config.py_files:
            spec["deps"] = {"pyFiles": self.config.py_files}
        if self.config.env:
            spec["driver"]["env"] = self.config.env
            spec["executor"]["env"] = self.config.env
        if self.config.env_from:
            spec["driver"]["envFrom"] = self.config.env_from
            spec["executor"]["envFrom"] = self.config.env_from
        if self.config.node_selector:
            spec["driver"]["nodeSelector"] = self.config.node_selector
            spec["executor"]["nodeSelector"] = self.config.node_selector
        if self.config.tolerations:
            spec["driver"]["tolerations"] = self.config.tolerations
            spec["executor"]["tolerations"] = self.config.tolerations
        if self.config.volume_mounts:
            spec["executor"]["volumeMounts"] = self.config.volume_mounts

        return {
            "apiVersion": "sparkoperator.k8s.io/v1beta2",
            "kind": "SparkApplication",
            "metadata": {
                "name": f"feast-sa-{job_id}",
                "namespace": self.config.namespace,
                "labels": {
                    "feast-materializer": "sparkapplication",
                    "feast-job-id": job_id,
                    "feast-server-id": self._server_id,
                    **self._kueue_labels(),
                    **self.config.labels,
                },
            },
            "spec": spec,
        }

    def _kueue_labels(self) -> dict:
        if self.config.queue_name:
            return {"kueue.x-k8s.io/queue-name": self.config.queue_name}
        return {}

    def _get_driver_logs(self, job_id: str, tail_lines: int = 50) -> Optional[str]:
        """Fetch last N lines of driver pod logs for error diagnostics."""
        try:
            pods = self.core_v1.list_namespaced_pod(
                namespace=self.config.namespace,
                label_selector=f"spark-role=driver,sparkoperator.k8s.io/app-name=feast-sa-{job_id}",
            )
            if pods.items:
                return self.core_v1.read_namespaced_pod_log(
                    name=pods.items[0].metadata.name,
                    namespace=self.config.namespace,
                    tail_lines=tail_lines,
                )
        except ApiException:
            logger.warning(f"Could not retrieve driver logs for feast-sa-{job_id}")
        return None
