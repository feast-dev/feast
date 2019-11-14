import shutil
import tempfile
from typing import Optional
from urllib.parse import urlparse
import uuid
import yaml
import pandas as pd
from datetime import datetime
from google.cloud import storage
from pandavro import to_avro


def yaml_loader(yml, load_single=False):
    """
    Loads one or more Feast resources from a YAML path or string. Multiple resources
    can be divided by three hyphens '---'
    :param yml: A path ending in .yaml or .yml, or a YAML string
    :param load_single: Expect only a single YAML resource, fail otherwise
    :return: Either a single YAML dictionary or a list of YAML dictionaries
    """
    if (
        isinstance(yml, str)
        and yml.count("\n") == 0
        and (".yaml" in yml.lower() or ".yml" in yml.lower())
    ):
        with open(yml, "r") as f:
            yml_content = f.read()

    elif isinstance(yml, str) and "kind" in yml.lower():
        yml_content = yml
    else:
        raise Exception(
            f"Invalid YAML provided. Please provide either a file path or YAML string: ${yml}"
        )

    yaml_strings = yml_content.strip("---").split("---")

    # Return a single resource dict
    if load_single:
        if len(yaml_strings) > 1:
            raise Exception(
                f"More than one YAML file is being loaded when only a single file is supported: ${yaml_strings}"
            )
        return yaml_to_dict(yaml_strings[0])

    # Return a list of resource dicts
    resources = []
    for yaml_string in yaml_strings:
        resources.append(yaml_to_dict(yaml_string))
    return resources


def yaml_to_dict(yaml_string):
    yaml_dict = yaml.safe_load(yaml_string)
    if not isinstance(yaml_dict, dict) or not "kind" in yaml_dict:
        raise Exception(f"Could not detect YAML kind from resource: ${yaml_string}")
    return yaml_dict


def export_dataframe_to_staging_location(
    df: pd.DataFrame, staging_location_uri: str
) -> str:
    """
    Uploads a dataframe to a remote staging location
    :param df: Pandas dataframe
    :param staging_location_uri: Remote staging location where dataframe should be written
        Examples: gs://bucket/path/
                  file:///data/subfolder/
    :return: Returns the full path to the file in the remote staging location
    """
    # Validate staging location
    uri = urlparse(staging_location_uri)
    if uri.scheme == "gs":
        dir_path, file_name, source_path = export_dataframe_to_local(df)
        upload_file_to_gcs(
            source_path, uri.hostname, str(uri.path).strip("/") + "/" + file_name
        )
        if len(str(dir_path)) < 5:
            raise Exception(f"Export location {dir_path} dangerous. Stopping.")
        shutil.rmtree(dir_path)
    elif uri.scheme == "file":
        dir_path, file_name, source_path = export_dataframe_to_local(df, uri.path)
    else:
        raise Exception(
            f"Staging location {staging_location_uri} does not have a valid URI. Only gs:// and file:// are supported"
        )

    return staging_location_uri.rstrip("/") + "/" + file_name


def upload_file_to_gcs(local_path: str, bucket: str, remote_path: str):
    """
    Upload a file from the local file system to Google Cloud Storage (GCS)
    :param local_path: Local filesystem path of file to upload
    :param bucket: GCS bucket to upload to
    :param remote_path: Path within GCS bucket to upload file to, includes file name
    """
    storage_client = storage.Client(project=None)
    bucket = storage_client.get_bucket(bucket)
    blob = bucket.blob(remote_path)
    blob.upload_from_filename(local_path)


def export_dataframe_to_local(df: pd.DataFrame, dir_path: Optional[str] = None):
    """
    Exports a pandas dataframe to the local filesystem
    :param df: Pandas dataframe to save
    :param dir_path: (optional) Absolute directory path '/data/project/subfolder/'
    :return:
    """
    # Create local staging location if not provided
    if dir_path is None:
        dir_path = tempfile.mkdtemp()

    file_name = f'{datetime.now().strftime("%d-%m-%Y_%I-%M-%S_%p")}_{str(uuid.uuid4())[:8]}.avro'
    dest_path = f"{dir_path}/{file_name}"

    # Export dataset to file in local path
    to_avro(df=df, file_path_or_buffer=dest_path)

    return dir_path, file_name, dest_path
