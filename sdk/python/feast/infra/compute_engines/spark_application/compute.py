import logging
import time
import uuid
from typing import List, Optional, Sequence, Union

import pyarrow as pa
import yaml
from kubernetes import client
from kubernetes import config as k8s_config
from kubernetes.client.exceptions import ApiException

from feast import RepoConfig
from feast.batch_feature_view import BatchFeatureView
from feast.entity import Entity
from feast.feature_view import FeatureView
from feast.infra.common.materialization_job import (
    MaterializationJob,
    MaterializationJobStatus,
    MaterializationTask,
)
from feast.infra.common.retrieval_task import HistoricalRetrievalTask
from feast.infra.compute_engines.base import ComputeEngine
from feast.infra.offline_stores.offline_store import OfflineStore
from feast.infra.online_stores.online_store import OnlineStore
from feast.infra.registry.base_registry import BaseRegistry
from feast.on_demand_feature_view import OnDemandFeatureView
from feast.stream_feature_view import StreamFeatureView

from .config import SparkApplicationComputeEngineConfig  # noqa: F401 — required for Feast config resolution
from .job import SparkApplicationMaterializationJob

logger = logging.getLogger(__name__)


class SparkApplicationComputeEngine(ComputeEngine):
    def __init__(
        self,
        *,
        repo_config: RepoConfig,
        offline_store: OfflineStore,
        online_store: OnlineStore,
        **kwargs,
    ):
        super().__init__(
            repo_config=repo_config,
            offline_store=offline_store,
            online_store=online_store,
            **kwargs,
        )
        self.config = repo_config.batch_engine

        # EC-3: SQLite online store — data written inside pod is lost on termination
        online_type = getattr(repo_config.online_store, "type", "")
        if online_type == "sqlite":
            raise ValueError(
                "spark_application engine cannot use SQLite online store. "
                "SQLite is file-based — data written inside the "
                "SparkApplication pod is lost when the pod terminates. "
                "Use a network-accessible store: redis, postgres, etc."
            )

        # EC-2: Registry access — pod must be able to reach registry
        if not self.config.registry_address:
            registry = repo_config.registry
            if hasattr(registry, "path") and registry.path:
                path = registry.path
                is_remote = any(
                    path.startswith(s)
                    for s in ("s3://", "gs://", "hdfs://", "http://", "https://", "postgresql", "mysql")
                )
                if not is_remote:
                    raise ValueError(
                        f"Registry path '{path}' is a local file. "
                        f"SparkApplication pods cannot access the Feast "
                        f"server's filesystem. Either:\n"
                        f"  1. Set registry_address to the Feast registry "
                        f"gRPC endpoint (recommended), or\n"
                        f"  2. Use a remote registry path (s3://, gs://), or\n"
                        f"  3. Switch to registry_type: 'sql' or 'remote'."
                    )

        k8s_config.load_config()
        self.k8s_client = client.ApiClient()
        self.core_v1 = client.CoreV1Api(self.k8s_client)
        self.custom_api = client.CustomObjectsApi(self.k8s_client)
        self._server_id = uuid.uuid4().hex[:8]

    def update(
        self,
        project: str,
        views_to_delete: Sequence[Union[BatchFeatureView, StreamFeatureView, FeatureView]],
        views_to_keep: Sequence[Union[BatchFeatureView, StreamFeatureView, FeatureView, OnDemandFeatureView]],
        entities_to_delete: Sequence[Entity],
        entities_to_keep: Sequence[Entity],
    ):
        pass

    def teardown_infra(
        self,
        project: str,
        fvs: Sequence[Union[BatchFeatureView, StreamFeatureView, FeatureView]],
        entities: Sequence[Entity],
    ):
        pass

    def get_historical_features(
        self, registry: BaseRegistry, task: HistoricalRetrievalTask
    ) -> pa.Table:
        raise NotImplementedError(
            "SparkApplicationComputeEngine does not yet support get_historical_features(). "
            "This is planned for Phase 2."
        )

    def materialize(
        self,
        registry: BaseRegistry,
        tasks: Union[MaterializationTask, List[MaterializationTask]],
        **kwargs,
    ) -> List[MaterializationJob]:
        """Batch all materialization tasks into a single SparkApplication."""
        if isinstance(tasks, MaterializationTask):
            tasks = [tasks]

        job_id = uuid.uuid4().hex[:8]

        try:
            self._create_secret(job_id, tasks)
        except ApiException as e:
            job = SparkApplicationMaterializationJob(
                job_id, self.config.namespace, self.custom_api,
                error=Exception(f"Secret creation failed: {e.reason}"),
            )
            return [job for _ in tasks]

        try:
            cr = self._build_spark_application_cr(job_id)
            self.custom_api.create_namespaced_custom_object(
                group="sparkoperator.k8s.io",
                version="v1beta2",
                namespace=self.config.namespace,
                plural="sparkapplications",
                body=cr,
            )
        except ApiException as e:
            self._cleanup(job_id)
            job = SparkApplicationMaterializationJob(
                job_id, self.config.namespace, self.custom_api,
                error=Exception(f"SparkApplication creation failed: {e.reason}"),
            )
            return [job for _ in tasks]

        job = SparkApplicationMaterializationJob(job_id, self.config.namespace, self.custom_api)
        self._wait_for_completion(job)
        return [job for _ in tasks]

    def _build_driver_repo_config(self) -> dict:
        """Build feature_store.yaml for the SparkApplication driver pod.

        Two rewrites:
        1. batch_engine → spark.engine: Pod uses SparkComputeEngine with the
           active SparkSession (from spark-submit). Enables distributed reads
           via SparkReadNode and distributed writes via mapInArrow across executors.
           This is NOT recursive — SparkComputeEngine uses the local session,
           it does not create CRDs.
        2. registry → remote (if registry_address set): Pod can't access
           server's local filesystem. Routes registry ops via gRPC.

        offline_store is NOT rewritten — respects user's configured data sources.
        User should configure offline_store: spark for full distributed performance.
        """
        config_dict = self.repo_config.model_dump(by_alias=True)

        config_dict["batch_engine"] = {"type": "spark.engine"}
        if self.config.spark_conf:
            config_dict["batch_engine"]["spark_conf"] = self.config.spark_conf

        if self.config.registry_address:
            config_dict["registry"] = {
                "registry_type": "remote",
                "path": self.config.registry_address,
            }

        return config_dict

    def _create_secret(self, job_id: str, tasks: List[MaterializationTask]):
        feast_config_yaml = yaml.dump(self._build_driver_repo_config())
        mat_config = {
            "operation": "materialize",
            "tasks": [
                {
                    "feature_view": task.feature_view.name,
                    "start_time": task.start_time.isoformat(),
                    "end_time": task.end_time.isoformat(),
                }
                for task in tasks
            ],
        }
        if self.config.concurrency > 1:
            mat_config["concurrency"] = self.config.concurrency
        mat_config_yaml = yaml.dump(mat_config)
        manifest = {
            "apiVersion": "v1",
            "kind": "Secret",
            "metadata": {
                "name": f"feast-sa-{job_id}",
                "namespace": self.config.namespace,
                "labels": {"feast-materializer": "secret", **self.config.labels},
            },
            "stringData": {
                "feature_store.yaml": feast_config_yaml,
                "materialization_config.yaml": mat_config_yaml,
            },
        }
        self.core_v1.create_namespaced_secret(
            namespace=self.config.namespace, body=manifest
        )

    def _build_spark_application_cr(self, job_id: str) -> dict:
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
                "spark.kubernetes.driverEnv.FEAST_SECRET_NAME": f"feast-sa-{job_id}",
                "spark.kubernetes.driverEnv.FEAST_SECRET_NAMESPACE": self.config.namespace,
            },
            "restartPolicy": {
                "type": self.config.restart_policy,
                "onFailureRetries": self.config.max_retries,
                "onFailureRetryInterval": 30,
            },
            "timeToLiveSeconds": self.config.ttl_seconds_after_finished,
            "volumes": [
                {"name": "feast-config", "secret": {"secretName": f"feast-sa-{job_id}"}},
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

    def _wait_for_completion(self, job: SparkApplicationMaterializationJob):
        start = time.monotonic()
        deadline = start + self.config.job_timeout_seconds
        while time.monotonic() < deadline:
            status = job.status()
            elapsed = time.monotonic() - start
            logger.info(
                f"SparkApplication {job.job_id()} status={status.name} elapsed={elapsed:.0f}s"
            )
            if status == MaterializationJobStatus.ERROR:
                logs = self._get_driver_logs(job._job_id)
                if logs:
                    logger.error(f"Driver logs (last 50 lines):\n{logs}")
                return
            if status == MaterializationJobStatus.SUCCEEDED:
                return
            time.sleep(self.config.poll_interval_seconds)
        self._cleanup(job._job_id)
        job._error = Exception(
            f"SparkApplication {job.job_id()} did not complete "
            f"within {self.config.job_timeout_seconds}s"
        )

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

    def _cleanup(self, job_id: str):
        for fn in [
            lambda: self.custom_api.delete_namespaced_custom_object(
                "sparkoperator.k8s.io", "v1beta2", self.config.namespace,
                "sparkapplications", f"feast-sa-{job_id}",
            ),
            lambda: self.core_v1.delete_namespaced_secret(
                f"feast-sa-{job_id}", self.config.namespace,
            ),
        ]:
            try:
                fn()
            except ApiException as e:
                if e.status != 404:
                    logger.warning(f"Cleanup failed: {e.reason}")
