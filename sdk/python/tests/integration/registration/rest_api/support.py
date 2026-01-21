import shlex
import subprocess
import time

from kubernetes import client


def run_command(cmd, cwd=None, check=True):
    print(f"Running command: {' '.join(cmd)}")
    result = subprocess.run(cmd, cwd=cwd, capture_output=True, text=True, check=True)
    if check and result.returncode != 0:
        print(result.stdout)
        print(result.stderr)
        raise RuntimeError(f"Command failed: {' '.join(cmd)}")
    return result.stdout.strip()


def create_namespace(api_instance, namespace_name):
    try:
        namespace_body = client.V1Namespace(
            metadata=client.V1ObjectMeta(name=namespace_name)
        )
        api_instance.create_namespace(namespace_body)
    except Exception as e:
        print(f"Kubernetes API error during namespace creation: {e}")
        return None


def delete_namespace(api_instance, namespace_name):
    api_instance.delete_namespace(namespace_name)


def run_kubectl_command(args):
    try:
        result = subprocess.run(
            ["kubectl"] + args, capture_output=True, text=True, check=True
        )
        return result.stdout.strip()
    except subprocess.CalledProcessError as e:
        print(f"Error executing 'kubectl {' '.join(args)}': {e}")
        return None


def run_oc_command(args):
    try:
        result = subprocess.run(
            ["oc"] + args,
            capture_output=True,
            text=True,
            check=True,
        )
        return result.stdout.strip()
    except subprocess.CalledProcessError as e:
        print(f"Error running oc command: {' '.join(args)}")
        print(e.stderr)
        return None


def validate_feature_store_cr_status(
    namespace, cr_name, timeout_seconds=300, interval_seconds=5
):
    print(
        f"Waiting for FeatureStore CR {namespace}/{cr_name} to reach 'Ready' status..."
    )
    start_time = time.time()

    while time.time() - start_time < timeout_seconds:
        status = run_kubectl_command(
            ["get", "feast", cr_name, "-n", namespace, "-o", "jsonpath={.status.phase}"]
        )
        if status == "Ready":
            print(f"Feature Store CR {namespace}/{cr_name} is in Ready state")
            return True
        time.sleep(interval_seconds)

    raise TimeoutError(
        f" Feature Store CR {namespace}/{cr_name} did not reach 'Ready' state within {timeout_seconds} seconds."
    )


def create_feast_project(project_path, namespace, feast_instance_name):
    print(f"Applying Feast project from {project_path} in namespace {namespace}...")
    apply_result = run_kubectl_command(["apply", "-f", project_path, "-n", namespace])

    if apply_result is None:
        raise RuntimeError(f" Failed to apply Feast project: {project_path}")

    print("Feast CR applied successfully. Validating status...")
    return validate_feature_store_cr_status(namespace, feast_instance_name)


def wait_for_pods_ready(
    namespace, label_selector, timeout_seconds=180, interval_seconds=5
):
    print(
        f"Waiting for pods with label '{label_selector}' in namespace '{namespace}' to be Running..."
    )
    start_time = time.time()

    while time.time() - start_time < timeout_seconds:
        pods_output = run_kubectl_command(
            [
                "get",
                "pods",
                "-n",
                namespace,
                "-l",
                label_selector,
                "-o",
                "jsonpath={.items[*].status.phase}",
            ]
        )

        if pods_output:
            statuses = pods_output.split()
            if all(status == "Running" for status in statuses):
                print(
                    f"Pods with label '{label_selector}' are Running in namespace '{namespace}'."
                )
                return True
            else:
                print(f"Current pod statuses: {statuses}")

        time.sleep(interval_seconds)
    raise TimeoutError(
        f" Pods did not reach Running state in {timeout_seconds} seconds."
    )


def deploy_and_validate_pod(namespace, deployment_yaml_path, label_selector):
    print(f"Applying deployment: {deployment_yaml_path} in namespace: {namespace}")
    apply_result = run_kubectl_command(
        ["apply", "-f", deployment_yaml_path, "-n", namespace]
    )

    if apply_result is None:
        raise RuntimeError(f"Failed to apply deployment YAML: {deployment_yaml_path}")

    return wait_for_pods_ready(namespace, label_selector)


def create_route(namespace, feast_project, service_name):
    create_args = [
        "create",
        "route",
        "passthrough",
        feast_project + "-registry-rest",
        "--service=" + service_name,
        "--port=https",
        "-n",
        namespace,
    ]
    create_output = run_oc_command(create_args)
    if create_output is None:
        return None
    print(f"Created route:\n{create_output}")

    get_args = [
        "get",
        "route",
        "-n",
        namespace,
        "-o",
        f"jsonpath={{.items[?(@.spec.to.name=='{service_name}')].spec.host}}",
    ]
    host = run_oc_command(get_args)
    if not host:
        print("Failed to get route host.")
        return None

    route_url = f"https://{host}"
    print(f"Route URL: {route_url}")
    return route_url


def run_kubectl_apply_with_sed(
    aws_access_key,
    aws_secret_key,
    aws_bucket_name,
    aws_feast_registry_path,
    template_path,
    namespace,
):
    if not aws_access_key or not aws_secret_key or not namespace:
        raise ValueError("Missing required values.")

    # Escape values to avoid shell issues
    escaped_access_key = shlex.quote(aws_access_key)
    escaped_secret_key = shlex.quote(aws_secret_key)
    escaped_namespace = shlex.quote(namespace)
    escaped_aws_bucket_name = shlex.quote(aws_bucket_name)
    escaped_aws_feast_registry_path = shlex.quote(aws_feast_registry_path)
    # Build sed command with pipeline
    cmd = (
        f"sed 's|AWS_ACCESS_KEY_PLACEHOLDER|{escaped_access_key}|g' {template_path} "
        f"| sed 's|AWS_SECRET_KEY_PLACEHOLDER|{escaped_secret_key}|g' "
        f"| sed 's|NAMESPACE_PLACEHOLDER|{escaped_namespace}|g' "
        f"| sed 's|AWS_BUCKET_NAME|{escaped_aws_bucket_name}|g' "
        f"| sed 's|REGISTRY_FILE_PATH|{escaped_aws_feast_registry_path}|g' "
        f"| kubectl apply -f -"
    )
    print("Running shell command:\n", cmd)
    try:
        result = subprocess.run(
            cmd, shell=True, check=True, capture_output=True, text=True
        )
        print(result.stdout.strip())
        return result.stdout.strip()
    except subprocess.CalledProcessError as e:
        print("Error applying manifest:")
        print(e.stderr)
        raise


def get_pod_name_by_prefix(namespace, prefix):
    args = [
        "get",
        "pods",
        "-n",
        namespace,
        "--no-headers",
        "-o",
        "custom-columns=:metadata.name",
    ]
    pods = run_kubectl_command(args)
    if pods:
        for line in pods.splitlines():
            if prefix in line:
                return line.strip()
    return None


def applyFeastProject(namespace, feast_project):
    pod_name = get_pod_name_by_prefix(namespace, feast_project)
    apply_output = execPodCommand(namespace, pod_name, ["feast", "apply"])
    return apply_output


def execPodCommand(namespace, podName, command_args):
    apply_args = ["exec", podName, "-n", namespace, "--"] + command_args
    apply_output = run_kubectl_command(apply_args)
    print("Output of args apply:\n", apply_output)
    return apply_output
