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
from feast.feature_view import FeatureView, FeatureViewState
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

from .config import (
    SparkApplicationComputeEngineConfig,  # noqa: F401 — required for Feast config resolution
)
from .job import (
    _MAX_RETRIES,
    _RETRY_BACKOFF_BASE,
    CompletedMaterializationJob,
    SparkApplicationMaterializationJob,
    _is_retryable,
    _rbac_hint,
)

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

        _FILE_BASED_ONLINE = {"sqlite", "faiss"}
        _FILE_BASED_OFFLINE = {"dask", "file", "duckdb"}
        _FILE_BASED_REGISTRY = {"file"}

        online_type = getattr(repo_config.online_store, "type", "")
        if online_type in _FILE_BASED_ONLINE:
            raise ValueError(
                f"spark_application engine cannot use '{online_type}' online store. "
                f"File-based stores ({', '.join(sorted(_FILE_BASED_ONLINE))}) write "
                "data inside the SparkApplication pod, which is lost when the pod "
                "terminates. Use a network-accessible store: redis, postgres, etc."
            )

        offline_type = getattr(repo_config.offline_store, "type", "")
        if offline_type in _FILE_BASED_OFFLINE:
            raise ValueError(
                f"spark_application engine cannot use '{offline_type}' offline store. "
                f"File-based stores ({', '.join(sorted(_FILE_BASED_OFFLINE))}) read "
                "from the local filesystem, which is inaccessible from the "
                "SparkApplication pod. Use a network-accessible store: spark, "
                "bigquery, snowflake, redshift, etc."
            )

        registry_type = getattr(repo_config.registry, "registry_type", "")
        if registry_type in _FILE_BASED_REGISTRY:
            raise ValueError(
                f"spark_application engine cannot use '{registry_type}' registry. "
                f"File-based registries ({', '.join(sorted(_FILE_BASED_REGISTRY))}) "
                "store data on the local filesystem, which is inaccessible from the "
                "SparkApplication pod. Use a network-accessible registry: sql, "
                "snowflake.registry, etc."
            )

        # Defer kubeconfig load until materialize/cleanup — feast apply only
        # constructs the engine and calls update() (a no-op), so it must not
        # require a cluster.
        self._k8s_client = None
        self._core_v1 = None
        self._custom_api = None
        self._server_id = uuid.uuid4().hex[:8]

    def _ensure_k8s(self) -> None:
        """Load kubeconfig and create API clients on first K8s use."""
        if self._custom_api is not None:
            return
        k8s_config.load_config()
        self._k8s_client = client.ApiClient()
        self._core_v1 = client.CoreV1Api(self._k8s_client)
        self._custom_api = client.CustomObjectsApi(self._k8s_client)

    @property
    def core_v1(self):
        self._ensure_k8s()
        return self._core_v1

    @property
    def custom_api(self):
        self._ensure_k8s()
        return self._custom_api

    @property
    def supports_batch(self) -> bool:
        return True

    @property
    def applies_materialization(self) -> bool:
        return True

    def update(
        self,
        project: str,
        views_to_delete: Sequence[
            Union[BatchFeatureView, StreamFeatureView, FeatureView]
        ],
        views_to_keep: Sequence[
            Union[BatchFeatureView, StreamFeatureView, FeatureView, OnDemandFeatureView]
        ],
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
        """Batch all materialization tasks into a single SparkApplication.

        The pod calls apply_materialization (via gRPC → Feast server → registry)
        for each FV it successfully materializes. After the pod finishes, we read
        each FV's state from the registry: AVAILABLE_ONLINE = succeeded,
        still MATERIALIZING = failed.
        """
        if isinstance(tasks, MaterializationTask):
            tasks = [tasks]

        job_id = uuid.uuid4().hex[:8]

        try:
            self._create_with_retry(
                lambda: self._create_configmap(job_id, tasks),
                "ConfigMap",
                job_id,
            )
        except ApiException as e:
            job = SparkApplicationMaterializationJob(
                job_id,
                self.config.namespace,
                self.custom_api,
                error=Exception(
                    f"ConfigMap creation failed: HTTP {e.status} {e.reason}."
                    f"{_rbac_hint(e.status)}"
                ),
            )
            return [job for _ in tasks]

        try:
            cr = self._build_spark_application_cr(job_id)
            self._create_with_retry(
                lambda: self.custom_api.create_namespaced_custom_object(
                    group="sparkoperator.k8s.io",
                    version="v1beta2",
                    namespace=self.config.namespace,
                    plural="sparkapplications",
                    body=cr,
                ),
                "SparkApplication",
                job_id,
            )
        except ApiException as e:
            self._cleanup(job_id)
            job = SparkApplicationMaterializationJob(
                job_id,
                self.config.namespace,
                self.custom_api,
                error=Exception(
                    f"SparkApplication creation failed: HTTP {e.status} {e.reason}."
                    f"{_rbac_hint(e.status)}"
                ),
            )
            return [job for _ in tasks]

        job = SparkApplicationMaterializationJob(
            job_id, self.config.namespace, self.custom_api
        )
        try:
            self._wait_for_completion(job)
            return self._build_per_fv_jobs(registry, tasks, job_id, job)
        finally:
            self._cleanup(job_id)

    def _build_driver_repo_config(self) -> dict:
        """Build feature_store.yaml for the SparkApplication driver pod.

        One rewrite: batch_engine → spark.engine. Pod uses SparkComputeEngine
        with the active SparkSession (from spark-submit). Enables distributed
        reads via SparkReadNode and distributed writes via mapInArrow across
        executors. This is NOT recursive — SparkComputeEngine uses the local
        session, it does not create CRDs.

        offline_store and registry are NOT rewritten — the pod inherits the
        server's config. File-based registries are rejected at __init__(), so
        the registry is always network-accessible (SQL, Snowflake, etc.) and
        the pod can write apply_materialization() directly.
        """
        config_dict = self.repo_config.model_dump(by_alias=True, mode="json")

        config_dict["batch_engine"] = {"type": "spark.engine"}

        return config_dict

    def _build_per_fv_jobs(
        self,
        registry: BaseRegistry,
        tasks: List[MaterializationTask],
        job_id: str,
        job: SparkApplicationMaterializationJob,
    ) -> List[MaterializationJob]:
        """Build one independent job object per FV from registry state.

        The driver pod calls ``apply_materialization`` for each FV it
        successfully materializes, setting state to ``AVAILABLE_ONLINE``.
        FVs still in ``MATERIALIZING`` were not processed.

        Each returned job is an independent object so that a failed
        SparkApplication does not pollute the status of succeeded FVs.
        """
        if len(tasks) <= 1:
            return [job for _ in tasks]

        jobs: List[MaterializationJob] = []
        for task in tasks:
            fv = registry.get_feature_view(task.feature_view.name, task.project)
            if getattr(fv, "state", None) == FeatureViewState.AVAILABLE_ONLINE:
                jobs.append(CompletedMaterializationJob(job_id))
            else:
                jobs.append(
                    SparkApplicationMaterializationJob(
                        job_id,
                        self.config.namespace,
                        self.custom_api,
                        error=Exception(
                            f"Feature view '{task.feature_view.name}' was not "
                            f"materialized by SparkApplication feast-sa-{job_id}"
                        ),
                    )
                )
        return jobs

    def _create_configmap(self, job_id: str, tasks: List[MaterializationTask]):
        feast_config_yaml = yaml.dump(
            self._build_driver_repo_config(), default_flow_style=False
        )
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
            "kind": "ConfigMap",
            "metadata": {
                "name": f"feast-sa-{job_id}",
                "namespace": self.config.namespace,
                "labels": {"feast-materializer": "configmap", **self.config.labels},
            },
            "data": {
                "feature_store.yaml": feast_config_yaml,
                "materialization_config.yaml": mat_config_yaml,
            },
        }
        self.core_v1.create_namespaced_config_map(
            namespace=self.config.namespace, body=manifest
        )

    def _build_spark_application_cr(self, job_id: str) -> dict:
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

    @staticmethod
    def _create_with_retry(fn, resource_kind: str, job_id: str):
        """Call *fn* with exponential backoff on transient K8s API errors."""
        last_exc = None
        for attempt in range(_MAX_RETRIES):
            try:
                return fn()
            except ApiException as e:
                if not _is_retryable(e):
                    raise
                last_exc = e
                wait = _RETRY_BACKOFF_BASE**attempt
                logger.warning(
                    f"{resource_kind} feast-sa-{job_id}: create attempt "
                    f"{attempt + 1}/{_MAX_RETRIES} failed (HTTP {e.status}), "
                    f"retrying in {wait}s"
                )
                time.sleep(wait)
        raise last_exc  # type: ignore[misc]

    def _cleanup(self, job_id: str):
        """Best-effort delete of SparkApplication + ConfigMap.

        The SparkApplication CR is also garbage-collected by the Spark Operator
        after ``spec.timeToLiveSeconds`` (default 1h), so a failed delete here
        is not a resource leak — just delayed cleanup.  The ConfigMap is ours
        and not covered by operator TTL.
        """
        resources = [
            (
                "SparkApplication",
                lambda: self.custom_api.delete_namespaced_custom_object(
                    "sparkoperator.k8s.io",
                    "v1beta2",
                    self.config.namespace,
                    "sparkapplications",
                    f"feast-sa-{job_id}",
                ),
            ),
            (
                "ConfigMap",
                lambda: self.core_v1.delete_namespaced_config_map(
                    f"feast-sa-{job_id}",
                    self.config.namespace,
                ),
            ),
        ]
        for kind, fn in resources:
            try:
                fn()
            except ApiException as e:
                if e.status != 404:
                    logger.warning(
                        f"Cleanup of {kind} feast-sa-{job_id} failed "
                        f"(HTTP {e.status}): {e.reason}. "
                        f"Manual cleanup: kubectl delete {kind.lower()} "
                        f"feast-sa-{job_id} -n {self.config.namespace}"
                    )
