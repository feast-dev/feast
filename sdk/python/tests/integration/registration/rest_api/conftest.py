import os
import time
from pathlib import Path

import pytest
import requests
from kubernetes import client, config

from tests.integration.registration.rest_api.support import (
    applyFeastProject,
    create_feast_project,
    create_namespace,
    create_route,
    delete_namespace,
    deploy_and_validate_pod,
    execPodCommand,
    get_pod_name_by_prefix,
    run_kubectl_apply_with_sed,
    run_kubectl_command,
    validate_feature_store_cr_status,
)


class FeastRestClient:
    def __init__(self, base_url):
        self.base_url = base_url.rstrip("/")
        self.api_prefix = "/api/v1"

    def _build_url(self, endpoint):
        if not endpoint.startswith("/"):
            endpoint = "/" + endpoint
        return f"{self.base_url}{self.api_prefix}{endpoint}"

    def get(self, endpoint, params=None):
        params = params or {}
        params.setdefault("allow_cache", "false")
        url = self._build_url(endpoint)
        return requests.get(url, params=params, verify=False)


def _wait_for_http_ready(route_url: str, timeout: int = 180, interval: int = 5) -> None:
    """
    Poll the HTTP endpoint until it returns a non-502 response.

    After Pod/CR readiness is confirmed, the backend behind the ingress may
    still be initializing.  This helper avoids the race condition where tests
    start before the Feast server is ready, causing all requests to return 502.
    """
    health_url = f"{route_url}/api/v1/projects"
    deadline = time.time() + timeout
    last_status = None

    print(
        f"\n Waiting for HTTP endpoint to become ready (timeout={timeout}s): {health_url}"
    )

    while time.time() < deadline:
        try:
            resp = requests.get(health_url, timeout=10, verify=False)
            last_status = resp.status_code
            if resp.status_code != 502:
                print(f" HTTP endpoint is ready (status={resp.status_code})")
                return
            print(
                f" HTTP endpoint returned {resp.status_code}, retrying in {interval}s..."
            )
        except requests.exceptions.RequestException as exc:
            last_status = str(exc)
            print(f" HTTP request failed ({exc}), retrying in {interval}s...")

        time.sleep(interval)

    raise RuntimeError(
        f"HTTP endpoint {health_url} did not become ready within {timeout}s "
        f"(last status: {last_status})"
    )


@pytest.fixture(scope="session")
def feast_rest_client():
    # Load kubeconfig and initialize Kubernetes client
    config.load_kube_config()
    api_instance = client.CoreV1Api()

    # Get the directory containing this conftest.py file
    test_dir = Path(__file__).parent
    resource_dir = test_dir / "resource"

    # Constants and environment values
    namespace = "test-ns-feast-rest"
    credit_scoring = "credit-scoring"
    driver_ranking = "driver-ranking"
    service_name = "feast-test-s3-registry-rest"
    run_on_openshift = os.getenv("RUN_ON_OPENSHIFT_CI", "false").lower() == "true"

    # Create test namespace
    create_namespace(api_instance, namespace)

    try:
        if not run_on_openshift:
            # Deploy dependencies
            deploy_and_validate_pod(
                namespace, str(resource_dir / "redis.yaml"), "app=redis"
            )
            deploy_and_validate_pod(
                namespace, str(resource_dir / "postgres.yaml"), "app=postgres"
            )

            # Create and validate FeatureStore CRs
            create_feast_project(
                str(resource_dir / "feast_config_credit_scoring.yaml"),
                namespace,
                credit_scoring,
            )
            validate_feature_store_cr_status(namespace, credit_scoring)

            create_feast_project(
                str(resource_dir / "feast_config_driver_ranking.yaml"),
                namespace,
                driver_ranking,
            )
            validate_feature_store_cr_status(namespace, driver_ranking)

            # Deploy ingress and get route URL
            run_kubectl_command(
                [
                    "apply",
                    "-f",
                    str(resource_dir / "feast-registry-nginx.yaml"),
                    "-n",
                    namespace,
                ]
            )
            ingress_host = run_kubectl_command(
                [
                    "get",
                    "ingress",
                    "feast-registry-ingress",
                    "-n",
                    namespace,
                    "-o",
                    "jsonpath={.spec.rules[0].host}",
                ]
            )
            route_url = f"http://{ingress_host}"

            # Apply feast projects

            applyFeastProject(namespace, credit_scoring)

            applyFeastProject(namespace, driver_ranking)

            # Create Saved Datasets and Permissions
            pod_name = get_pod_name_by_prefix(namespace, credit_scoring)

            # Apply datasets
            execPodCommand(
                namespace, pod_name, ["python", "create_ui_visible_datasets.py"]
            )

            # Apply permissions
            execPodCommand(namespace, pod_name, ["python", "permissions_apply.py"])

        else:
            # OpenShift cluster setup using S3-based registry
            aws_access_key = os.getenv("AWS_ACCESS_KEY")
            aws_secret_key = os.getenv("AWS_SECRET_KEY")
            aws_bucket = os.getenv("AWS_BUCKET_NAME")
            registry_path = os.getenv("AWS_REGISTRY_FILE_PATH")

            run_kubectl_apply_with_sed(
                aws_access_key,
                aws_secret_key,
                aws_bucket,
                registry_path,
                str(resource_dir / "feast_config_rhoai.yaml"),
                namespace,
            )
            validate_feature_store_cr_status(namespace, "test-s3")
            route_url = create_route(namespace, credit_scoring, service_name)
        if not route_url:
            raise RuntimeError("Route URL could not be fetched.")

        # Wait for the HTTP endpoint to become ready before running tests.
        # Pod/CR readiness does not guarantee the backend is serving traffic;
        # the ingress may return 502 while the Feast server is still starting.
        _wait_for_http_ready(route_url)

        print(f"\n Connected to Feast REST at: {route_url}")
        yield FeastRestClient(route_url)

    finally:
        print(f"\n Deleting namespace: {namespace}")
        delete_namespace(api_instance, namespace)
