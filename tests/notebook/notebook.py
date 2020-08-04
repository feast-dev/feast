import json
import docker
from google.cloud import storage


def run_notebook(notebook_path: str, output_notebook_uri: str):
    # Get Jupyter container
    docker_client = docker.from_env()
    jupyter_container = docker_client.containers.get("feast_jupyter_1")

    # Configure command
    cmd = (f"papermill -k python3 %s %s" % (notebook_path, output_notebook_uri))

    # Print configuration
    print(f"Output notebook: %s" % output_notebook_uri)
    print(f"Exec command: %s" % cmd)

    # Run notebook
    jupyter_container.exec_run(user="root", cmd="pip install papermill[gcs] jupyter_client -U")
    exec_log = jupyter_container.exec_run(user="root", cmd=cmd)

    # Handle output logs and exception
    output_log = exec_log[1].decode("unicode_escape")
    exit_code = exec_log[0]

    print(output_log)
    if exit_code > 0:
        raise Exception("Notebook execution failed")


def get_notebook_from_gcs(output_bucket_name: str, notebook_blob: str):
    # Retrieve notebook back from GCS
    storage_client = storage.Client()
    bucket = storage_client.bucket(output_bucket_name)
    blob = bucket.blob(notebook_blob)
    notebook_json_str = blob.download_as_string()
    return json.loads(notebook_json_str)
