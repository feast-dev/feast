import logging
import time
import uuid
from typing import Callable, List, Optional, Sequence, Union

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
from feast.infra.compute_engines.operator.job import (
    _MAX_RETRIES,
    _RETRY_BACKOFF_BASE,
    OperatorMaterializationJob,
    _is_retryable,
    _rbac_hint,
)
from feast.infra.offline_stores.offline_store import OfflineStore
from feast.infra.online_stores.online_store import OnlineStore
from feast.infra.registry.base_registry import BaseRegistry
from feast.on_demand_feature_view import OnDemandFeatureView
from feast.stream_feature_view import StreamFeatureView

logger = logging.getLogger(__name__)

_FILE_BASED_ONLINE = {"sqlite", "faiss"}
_FILE_BASED_OFFLINE = {"dask", "file", "duckdb"}
_FILE_BASED_REGISTRY = {"file"}


class OperatorComputeEngine(ComputeEngine):
    """Base class for batch compute engines backed by a Kubernetes operator.

    The engine is a thin launcher: it writes a ConfigMap holding the repo config
    (with ``batch_engine`` rewritten to the in-pod engine) plus the task list,
    submits a single custom resource (e.g. ``SparkApplication``) whose pod runs
    Feast's in-process engine in-cluster, polls the custom resource to
    completion, and resolves per-feature-view outcomes from registry state. Both
    the online/offline/registry stores must be network-accessible, since
    file-based backends live only inside the pod.

    Subclasses provide the CR coordinates (``_cr_group``/``_cr_version``/
    ``_cr_plural``/``_cr_kind``), the resource-name prefix, the in-pod engine
    type, the CR manifest (``_build_cr``), the job factory (``_make_job`` /
    ``_make_completed_job``), and — optionally — driver-log retrieval.
    """

    # ── Variant surface (set/overridden by subclasses) ──────────────
    _cr_group: str
    _cr_version: str
    _cr_plural: str
    _cr_kind: str
    _resource_prefix: str
    _pod_batch_engine_type: str

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
        self._validate_stores(repo_config)

        # Defer kubeconfig load until materialize/cleanup — feast apply only
        # constructs the engine and calls update() (a no-op), so it must not
        # require a cluster.
        self._k8s_client: Optional[client.ApiClient] = None
        self._core_v1: Optional[client.CoreV1Api] = None
        self._custom_api: Optional[client.CustomObjectsApi] = None
        self._server_id = uuid.uuid4().hex[:8]

    def _validate_stores(self, repo_config: RepoConfig) -> None:
        """Reject file-based stores that are inaccessible from the job pod."""
        engine = self.config.type
        online_type = getattr(repo_config.online_store, "type", "")
        if online_type in _FILE_BASED_ONLINE:
            raise ValueError(
                f"{engine} engine cannot use '{online_type}' online store. "
                f"File-based stores ({', '.join(sorted(_FILE_BASED_ONLINE))}) write "
                "data inside the pod, which is lost when the pod terminates. "
                "Use a network-accessible store: redis, postgres, etc."
            )

        offline_type = getattr(repo_config.offline_store, "type", "")
        if offline_type in _FILE_BASED_OFFLINE:
            raise ValueError(
                f"{engine} engine cannot use '{offline_type}' offline store. "
                f"File-based stores ({', '.join(sorted(_FILE_BASED_OFFLINE))}) read "
                "from the local filesystem, which is inaccessible from the pod. "
                "Use a network-accessible store: spark, bigquery, snowflake, "
                "redshift, etc."
            )

        registry_type = getattr(repo_config.registry, "registry_type", "")
        if registry_type in _FILE_BASED_REGISTRY:
            raise ValueError(
                f"{engine} engine cannot use '{registry_type}' registry. "
                f"File-based registries ({', '.join(sorted(_FILE_BASED_REGISTRY))}) "
                "store data on the local filesystem, which is inaccessible from the "
                "pod. Use a network-accessible registry: sql, snowflake.registry, "
                "etc."
            )

    # ── Kubernetes client (lazily created) ──────────────────────────
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
            f"{type(self).__name__} does not yet support get_historical_features(). "
            "This is planned for Phase 2."
        )

    # ── Variant hooks (implemented by subclasses) ───────────────────
    def _build_cr(self, job_id: str, **kwargs) -> dict:
        """Build the operator custom resource manifest for this job."""
        raise NotImplementedError

    def _make_job(
        self, job_id: str, error: Optional[BaseException] = None
    ) -> OperatorMaterializationJob:
        """Create the engine-specific MaterializationJob for this job id."""
        raise NotImplementedError

    def _make_completed_job(self, job_id: str) -> MaterializationJob:
        """Create a job object that reports SUCCEEDED without any K8s call."""
        raise NotImplementedError

    def _get_driver_logs(self, job_id: str, tail_lines: int = 50) -> Optional[str]:
        """Fetch last N lines of driver pod logs for error diagnostics."""
        return None

    # ── Shared materialization flow ─────────────────────────────────
    def materialize(
        self,
        registry: BaseRegistry,
        tasks: Union[MaterializationTask, List[MaterializationTask]],
        **kwargs,
    ) -> List[MaterializationJob]:
        """Batch all materialization tasks into a single operator custom resource.

        The pod calls ``apply_materialization`` (via the registry) for each FV it
        successfully materializes. After the pod finishes, each FV's outcome is
        read from registry state: ``AVAILABLE_ONLINE`` = succeeded, still
        ``MATERIALIZING`` = failed.
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
            job = self._make_job(
                job_id,
                error=Exception(
                    f"ConfigMap creation failed: HTTP {e.status} {e.reason}."
                    f"{_rbac_hint(e.status)}"
                ),
            )
            return [job for _ in tasks]

        try:
            cr = self._build_cr(job_id, **kwargs)
            self._create_with_retry(
                lambda: self._submit_cr(cr),
                self._cr_kind,
                job_id,
            )
        except ApiException as e:
            self._cleanup(job_id)
            job = self._make_job(
                job_id,
                error=Exception(
                    f"{self._cr_kind} creation failed: HTTP {e.status} {e.reason}."
                    f"{_rbac_hint(e.status)}"
                ),
            )
            return [job for _ in tasks]

        job = self._make_job(job_id)
        try:
            self._wait_for_completion(job)
            # Freeze success before finally-cleanup deletes the CR; otherwise
            # FeatureStore's later job.status() poll gets 404 → false FAIL.
            if (
                job.error() is None
                and job.status() == MaterializationJobStatus.SUCCEEDED
            ):
                job.mark_succeeded()
            return self._build_per_fv_jobs(registry, tasks, job_id, job)
        finally:
            self._cleanup(job_id)

    def _resource_name(self, job_id: str) -> str:
        return f"{self._resource_prefix}-{job_id}"

    def _submit_cr(self, cr: dict):
        return self.custom_api.create_namespaced_custom_object(
            group=self._cr_group,
            version=self._cr_version,
            namespace=self.config.namespace,
            plural=self._cr_plural,
            body=cr,
        )

    def _build_driver_repo_config(self) -> dict:
        """Build feature_store.yaml for the in-cluster job pod.

        One rewrite: ``batch_engine`` → the in-pod engine type. The pod runs
        Feast's in-process engine with the cluster runtime. This is NOT recursive
        — the in-pod engine executes directly, it does not create custom
        resources. offline_store and registry are inherited unchanged; file-based
        backends are rejected at ``__init__`` so they are always
        network-accessible from the pod.
        """
        config_dict = self.repo_config.model_dump(by_alias=True, mode="json")
        config_dict["batch_engine"] = {"type": self._pod_batch_engine_type}
        return config_dict

    def _build_per_fv_jobs(
        self,
        registry: BaseRegistry,
        tasks: List[MaterializationTask],
        job_id: str,
        job: OperatorMaterializationJob,
    ) -> List[MaterializationJob]:
        """Build one independent job object per FV from registry state.

        The driver pod calls ``apply_materialization`` for each FV it
        successfully materializes, setting state to ``AVAILABLE_ONLINE``; FVs
        still in ``MATERIALIZING`` were not processed. Each returned job is an
        independent object so a failed run does not pollute the status of
        succeeded FVs. The per-FV outcome is resolved from registry state — never
        from the live polling job — because ``materialize()`` deletes the CR in
        its ``finally`` immediately after this returns, so a job that re-queried
        the now-deleted CR would 404 and report a false failure (#6673).
        """
        if len(tasks) <= 1:
            if (
                job.error() is None
                and job.status() == MaterializationJobStatus.SUCCEEDED
            ):
                return [self._make_completed_job(job_id)]
            return [job]

        jobs: List[MaterializationJob] = []
        for task in tasks:
            fv = registry.get_feature_view(task.feature_view.name, task.project)
            if getattr(fv, "state", None) == FeatureViewState.AVAILABLE_ONLINE:
                jobs.append(self._make_completed_job(job_id))
            else:
                jobs.append(
                    self._make_job(
                        job_id,
                        error=Exception(
                            f"Feature view '{task.feature_view.name}' was not "
                            f"materialized by {self._cr_kind} "
                            f"{self._resource_name(job_id)}"
                        ),
                    )
                )
        return jobs

    def _create_configmap(self, job_id: str, tasks: List[MaterializationTask]):
        feast_config_yaml = yaml.dump(
            self._build_driver_repo_config(), default_flow_style=False
        )
        mat_config: dict = {
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
                "name": self._resource_name(job_id),
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

    def _wait_for_completion(self, job: OperatorMaterializationJob):
        start = time.monotonic()
        deadline = start + self.config.job_timeout_seconds
        while time.monotonic() < deadline:
            status = job.status()
            elapsed = time.monotonic() - start
            logger.info(
                f"{self._cr_kind} {job.job_id()} status={status.name} "
                f"elapsed={elapsed:.0f}s"
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
            f"{self._cr_kind} {job.job_id()} did not complete "
            f"within {self.config.job_timeout_seconds}s"
        )

    def _create_with_retry(self, fn: Callable, resource_kind: str, job_id: str):
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
                    f"{resource_kind} {self._resource_name(job_id)}: create attempt "
                    f"{attempt + 1}/{_MAX_RETRIES} failed (HTTP {e.status}), "
                    f"retrying in {wait}s"
                )
                time.sleep(wait)
        raise last_exc  # type: ignore[misc]

    def _cleanup(self, job_id: str):
        """Best-effort delete of the custom resource + ConfigMap.

        The custom resource is also garbage-collected by the operator after
        ``ttl_seconds_after_finished``, so a failed delete here is delayed
        cleanup, not a leak. The ConfigMap is ours and not covered by that TTL.
        """
        name = self._resource_name(job_id)
        resources = [
            (
                self._cr_kind,
                lambda: self.custom_api.delete_namespaced_custom_object(
                    self._cr_group,
                    self._cr_version,
                    self.config.namespace,
                    self._cr_plural,
                    name,
                ),
            ),
            (
                "ConfigMap",
                lambda: self.core_v1.delete_namespaced_config_map(
                    name,
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
                        f"Cleanup of {kind} {name} failed "
                        f"(HTTP {e.status}): {e.reason}. "
                        f"Manual cleanup: kubectl delete {kind.lower()} "
                        f"{name} -n {self.config.namespace}"
                    )
