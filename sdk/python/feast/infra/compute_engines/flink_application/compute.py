import logging
from typing import Optional

from kubernetes.client.exceptions import ApiException

from feast.infra.common.materialization_job import MaterializationJob
from feast.infra.compute_engines.operator.compute import OperatorComputeEngine
from feast.infra.compute_engines.operator.job import OperatorMaterializationJob

from .config import (
    FlinkApplicationComputeEngineConfig,  # noqa: F401 — required for Feast config resolution
)
from .job import (
    CompletedMaterializationJob,
    FlinkDeploymentMaterializationJob,
)

logger = logging.getLogger(__name__)


class FlinkApplicationComputeEngine(OperatorComputeEngine):
    """Batch compute engine that runs materialization as a ``FlinkDeployment``
    custom resource via the Apache Flink Kubernetes Operator.

    The generic operator-launcher lifecycle (ConfigMap, submit, poll, cleanup,
    per-FV resolution) lives in :class:`OperatorComputeEngine`; this subclass
    supplies the Flink-specific custom resource and job wiring. The pod runs
    Feast's in-process ``flink.engine`` via the operator's PythonDriver.
    """

    _cr_group = "flink.apache.org"
    _cr_version = "v1beta1"
    _cr_plural = "flinkdeployments"
    _cr_kind = "FlinkDeployment"
    _resource_prefix = "feast-flink"
    _pod_batch_engine_type = "flink.engine"

    # ── Operator hooks ──────────────────────────────────────────────
    def _make_job(
        self, job_id: str, error: Optional[BaseException] = None
    ) -> OperatorMaterializationJob:
        return FlinkDeploymentMaterializationJob(
            job_id, self.config.namespace, self.custom_api, error=error
        )

    def _make_completed_job(self, job_id: str) -> MaterializationJob:
        return CompletedMaterializationJob(job_id)

    def _build_cr(self, job_id: str, **kwargs) -> dict:
        return self._build_flink_deployment_cr(job_id)

    # ── Flink-specific custom resource ──────────────────────────────
    def _build_flink_deployment_cr(self, job_id: str) -> dict:
        name = f"feast-flink-{job_id}"

        flink_configuration = {
            "taskmanager.numberOfTaskSlots": str(self.config.task_slots),
            "execution.runtime-mode": "BATCH",
            **self.config.flink_configuration,
        }

        # The pod loads the repo config from the ConfigMap (via env) or from the
        # mounted file fallback at /var/feast/.
        container_env = [
            {"name": "FEAST_CONFIGMAP_NAME", "value": name},
            {"name": "FEAST_CONFIGMAP_NAMESPACE", "value": self.config.namespace},
            *self.config.env,
        ]

        pod_spec: dict = {
            "containers": [
                {
                    "name": "flink-main-container",
                    "env": container_env,
                    "volumeMounts": [
                        {"name": "feast-config", "mountPath": "/var/feast/"},
                        *self.config.volume_mounts,
                    ],
                }
            ],
            "volumes": [
                {"name": "feast-config", "configMap": {"name": name}},
                *self.config.volumes,
            ],
        }
        if self.config.service_account:
            pod_spec["serviceAccountName"] = self.config.service_account
        if self.config.image_pull_secrets:
            pod_spec["imagePullSecrets"] = [
                {"name": s} for s in self.config.image_pull_secrets
            ]
        if self.config.node_selector:
            pod_spec["nodeSelector"] = self.config.node_selector
        if self.config.tolerations:
            pod_spec["tolerations"] = self.config.tolerations
        if self.config.env_from:
            pod_spec["containers"][0]["envFrom"] = self.config.env_from

        pod_template = {
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {"name": "pod-template"},
            "spec": pod_spec,
        }

        spec: dict = {
            "image": self.config.image,
            "imagePullPolicy": "IfNotPresent",
            "flinkVersion": self.config.flink_version,
            "serviceAccount": self.config.service_account or "default",
            "flinkConfiguration": flink_configuration,
            "podTemplate": pod_template,
            "jobManager": {
                "resource": {
                    "cpu": self.config.jobmanager_cpu,
                    "memory": self.config.jobmanager_memory,
                }
            },
            "taskManager": {
                "resource": {
                    "cpu": self.config.taskmanager_cpu,
                    "memory": self.config.taskmanager_memory,
                }
            },
            "job": {
                "jarURI": self.config.python_jar_uri,
                "entryClass": "org.apache.flink.client.python.PythonDriver",
                "args": [
                    "-pyclientexec",
                    self.config.python_executable,
                    "-py",
                    self.config.main_application_file,
                ],
                "parallelism": self.config.parallelism,
                "upgradeMode": self.config.upgrade_mode,
            },
        }

        return {
            "apiVersion": "flink.apache.org/v1beta1",
            "kind": "FlinkDeployment",
            "metadata": {
                "name": name,
                "namespace": self.config.namespace,
                "labels": {
                    "feast-materializer": "flinkdeployment",
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
        """Fetch last N lines of the JobManager pod logs for error diagnostics."""
        try:
            pods = self.core_v1.list_namespaced_pod(
                namespace=self.config.namespace,
                label_selector=f"component=jobmanager,app=feast-flink-{job_id}",
            )
            if pods.items:
                return self.core_v1.read_namespaced_pod_log(
                    name=pods.items[0].metadata.name,
                    namespace=self.config.namespace,
                    tail_lines=tail_lines,
                )
        except ApiException:
            logger.warning(
                f"Could not retrieve JobManager logs for feast-flink-{job_id}"
            )
        return None
