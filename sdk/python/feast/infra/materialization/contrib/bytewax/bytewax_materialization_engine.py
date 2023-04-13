import uuid
from datetime import datetime
from typing import Callable, List, Literal, Sequence, Union

import yaml
from kubernetes import client
from kubernetes import config as k8s_config
from kubernetes import utils
from kubernetes.utils import FailToCreateError
from pydantic import StrictStr
from tqdm import tqdm

from feast import FeatureView, RepoConfig
from feast.batch_feature_view import BatchFeatureView
from feast.entity import Entity
from feast.infra.materialization.batch_materialization_engine import (
    BatchMaterializationEngine,
    MaterializationJob,
    MaterializationTask,
)
from feast.infra.offline_stores.offline_store import OfflineStore
from feast.infra.online_stores.online_store import OnlineStore
from feast.infra.registry.base_registry import BaseRegistry
from feast.repo_config import FeastConfigBaseModel
from feast.stream_feature_view import StreamFeatureView
from feast.utils import _get_column_names, get_default_yaml_file_path

from .bytewax_materialization_job import BytewaxMaterializationJob


class BytewaxMaterializationEngineConfig(FeastConfigBaseModel):
    """Batch Materialization Engine config for Bytewax"""

    type: Literal["bytewax"] = "bytewax"
    """ Materialization type selector"""

    namespace: StrictStr = "default"
    """ (optional) The namespace in Kubernetes to use when creating services, configuration maps and jobs.
    """

    image: StrictStr = "bytewax/bytewax-feast:latest"
    """ (optional) The container image to use when running the materialization job."""

    env: List[dict] = []
    """ (optional) A list of environment variables to set in the created Kubernetes pods.
    These environment variables can be used to reference Kubernetes secrets.
    """

    image_pull_secrets: List[dict] = []
    """ (optional) The secrets to use when pulling the image to run for the materialization job """

    resources: dict = {}
    """ (optional) The resource requests and limits for the materialization containers """

    service_account_name: StrictStr = ""
    """ (optional) The service account name to use when running the job """

    annotations: dict = {}
    """ (optional) Annotations to apply to the job container. Useful for linking the service account to IAM roles, operational metadata, etc  """


class BytewaxMaterializationEngine(BatchMaterializationEngine):
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
        self.repo_config = repo_config
        self.offline_store = offline_store
        self.online_store = online_store

        # TODO: Configure k8s here
        k8s_config.load_kube_config()

        self.k8s_client = client.api_client.ApiClient()
        self.v1 = client.CoreV1Api(self.k8s_client)
        self.batch_v1 = client.BatchV1Api(self.k8s_client)
        self.batch_engine_config = repo_config.batch_engine
        self.namespace = self.batch_engine_config.namespace

    def update(
        self,
        project: str,
        views_to_delete: Sequence[
            Union[BatchFeatureView, StreamFeatureView, FeatureView]
        ],
        views_to_keep: Sequence[
            Union[BatchFeatureView, StreamFeatureView, FeatureView]
        ],
        entities_to_delete: Sequence[Entity],
        entities_to_keep: Sequence[Entity],
    ):
        """This method ensures that any necessary infrastructure or resources needed by the
        engine are set up ahead of materialization."""
        pass

    def teardown_infra(
        self,
        project: str,
        fvs: Sequence[Union[BatchFeatureView, StreamFeatureView, FeatureView]],
        entities: Sequence[Entity],
    ):
        """This method ensures that any infrastructure or resources set up by ``update()``are torn down."""
        pass

    def materialize(
        self,
        registry: BaseRegistry,
        tasks: List[MaterializationTask],
    ) -> List[MaterializationJob]:
        return [
            self._materialize_one(
                registry,
                task.feature_view,
                task.start_time,
                task.end_time,
                task.project,
                task.tqdm_builder,
            )
            for task in tasks
        ]

    def _materialize_one(
        self,
        registry: BaseRegistry,
        feature_view: Union[BatchFeatureView, StreamFeatureView, FeatureView],
        start_date: datetime,
        end_date: datetime,
        project: str,
        tqdm_builder: Callable[[int], tqdm],
    ):
        entities = []
        for entity_name in feature_view.entities:
            entities.append(registry.get_entity(entity_name, project))

        (
            join_key_columns,
            feature_name_columns,
            timestamp_field,
            created_timestamp_column,
        ) = _get_column_names(feature_view, entities)

        offline_job = self.offline_store.pull_latest_from_table_or_query(
            config=self.repo_config,
            data_source=feature_view.batch_source,
            join_key_columns=join_key_columns,
            feature_name_columns=feature_name_columns,
            timestamp_field=timestamp_field,
            created_timestamp_column=created_timestamp_column,
            start_date=start_date,
            end_date=end_date,
        )

        paths = offline_job.to_remote_storage()
        job_id = str(uuid.uuid4())
        return self._create_kubernetes_job(job_id, paths, feature_view)

    def _create_kubernetes_job(self, job_id, paths, feature_view):
        try:
            # Create a k8s configmap with information needed by bytewax
            self._create_configuration_map(job_id, paths, feature_view, self.namespace)

            # Create the k8s job definition
            self._create_job_definition(
                job_id,
                self.namespace,
                len(paths),  # Create a pod for each parquet file
                self.batch_engine_config.env,
            )
        except FailToCreateError as failures:
            return BytewaxMaterializationJob(job_id, self.namespace, error=failures)

        return BytewaxMaterializationJob(job_id, self.namespace)

    def _create_configuration_map(self, job_id, paths, feature_view, namespace):
        """Create a Kubernetes configmap for this job"""

        repo_path = self.repo_config.repo_path
        assert repo_path
        feature_store_path = get_default_yaml_file_path(repo_path)
        feature_store_configuration = feature_store_path.read_text()

        materialization_config = yaml.dump(
            {"paths": paths, "feature_view": feature_view.name}
        )

        configmap_manifest = {
            "kind": "ConfigMap",
            "apiVersion": "v1",
            "metadata": {
                "name": f"feast-{job_id}",
            },
            "data": {
                "feature_store.yaml": feature_store_configuration,
                "bytewax_materialization_config.yaml": materialization_config,
            },
        }
        self.v1.create_namespaced_config_map(
            namespace=namespace,
            body=configmap_manifest,
        )

    def _create_job_definition(self, job_id, namespace, pods, env):
        """Create a kubernetes job definition."""
        job_env = [
            {"name": "RUST_BACKTRACE", "value": "full"},
            {
                "name": "BYTEWAX_PYTHON_FILE_PATH",
                "value": "/bytewax/dataflow.py",
            },
            {"name": "BYTEWAX_WORKDIR", "value": "/bytewax"},
            {
                "name": "BYTEWAX_WORKERS_PER_PROCESS",
                "value": "1",
            },
            {
                "name": "BYTEWAX_POD_NAME",
                "valueFrom": {
                    "fieldRef": {
                        "apiVersion": "v1",
                        "fieldPath": "metadata.annotations['batch.kubernetes.io/job-completion-index']",
                    }
                },
            },
            {
                "name": "BYTEWAX_REPLICAS",
                "value": f"{pods}",
            },
            {
                "name": "BYTEWAX_KEEP_CONTAINER_ALIVE",
                "value": "false",
            },
            {
                "name": "BYTEWAX_STATEFULSET_NAME",
                "value": f"dataflow-{job_id}",
            },
        ]
        # Add any Feast configured environment variables
        job_env.extend(env)

        job_definition = {
            "apiVersion": "batch/v1",
            "kind": "Job",
            "metadata": {
                "name": f"dataflow-{job_id}",
                "namespace": namespace,
            },
            "spec": {
                "ttlSecondsAfterFinished": 3600,
                "completions": pods,
                "parallelism": pods,
                "completionMode": "Indexed",
                "template": {
                    "metadata": {
                        "annotations": self.batch_engine_config.annotations,
                    },
                    "spec": {
                        "restartPolicy": "Never",
                        "subdomain": f"dataflow-{job_id}",
                        "imagePullSecrets": self.batch_engine_config.image_pull_secrets,
                        "serviceAccountName": self.batch_engine_config.service_account_name,
                        "initContainers": [
                            {
                                "env": [
                                    {
                                        "name": "BYTEWAX_REPLICAS",
                                        "value": f"{pods}",
                                    }
                                ],
                                "image": "busybox",
                                "imagePullPolicy": "Always",
                                "name": "init-hostfile",
                                "resources": {},
                                "securityContext": {
                                    "allowPrivilegeEscalation": False,
                                    "capabilities": {
                                        "add": ["NET_BIND_SERVICE"],
                                        "drop": ["ALL"],
                                    },
                                    "readOnlyRootFilesystem": True,
                                },
                                "terminationMessagePath": "/dev/termination-log",
                                "terminationMessagePolicy": "File",
                                "volumeMounts": [
                                    {"mountPath": "/etc/bytewax", "name": "hostfile"},
                                    {
                                        "mountPath": "/tmp/bytewax/",
                                        "name": "python-files",
                                    },
                                    {
                                        "mountPath": "/var/feast/",
                                        "name": f"feast-{job_id}",
                                    },
                                ],
                            }
                        ],
                        "containers": [
                            {
                                "command": ["sh", "-c", "sh ./entrypoint.sh"],
                                "env": job_env,
                                "image": self.batch_engine_config.image,
                                "imagePullPolicy": "Always",
                                "name": "process",
                                "ports": [
                                    {
                                        "containerPort": 9999,
                                        "name": "process",
                                        "protocol": "TCP",
                                    }
                                ],
                                "resources": self.batch_engine_config.resources,
                                "securityContext": {
                                    "allowPrivilegeEscalation": False,
                                    "capabilities": {
                                        "add": ["NET_BIND_SERVICE"],
                                        "drop": ["ALL"],
                                    },
                                    "readOnlyRootFilesystem": False,
                                },
                                "terminationMessagePath": "/dev/termination-log",
                                "terminationMessagePolicy": "File",
                                "volumeMounts": [
                                    {"mountPath": "/etc/bytewax", "name": "hostfile"},
                                    {
                                        "mountPath": "/var/feast/",
                                        "name": f"feast-{job_id}",
                                    },
                                ],
                            }
                        ],
                        "volumes": [
                            {"emptyDir": {}, "name": "hostfile"},
                            {
                                "configMap": {
                                    "defaultMode": 420,
                                    "name": f"feast-{job_id}",
                                },
                                "name": "python-files",
                            },
                            {
                                "configMap": {"name": f"feast-{job_id}"},
                                "name": f"feast-{job_id}",
                            },
                        ],
                    },
                },
            },
        }
        utils.create_from_dict(self.k8s_client, job_definition)
