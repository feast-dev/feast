# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

import json
import os
import uuid
from pathlib import Path
from tempfile import TemporaryFile
from typing import Optional
from urllib.parse import urlparse

from feast.infra.registry.registry import RegistryConfig
from feast.infra.registry.registry_store import RegistryStore
from feast.protos.feast.core.Registry_pb2 import Registry as RegistryProto
from feast.utils import _utc_now

REGISTRY_SCHEMA_VERSION = "1"


class AzBlobRegistryStore(RegistryStore):
    def __init__(self, registry_config: RegistryConfig, repo_path: Path):
        try:
            import logging

            from azure.identity import DefaultAzureCredential
            from azure.storage.blob import BlobServiceClient
        except ImportError as e:
            from feast.errors import FeastExtrasDependencyImportError

            raise FeastExtrasDependencyImportError("az", str(e))

        self._uri = urlparse(registry_config.path)
        self._account_url = self._uri.scheme + "://" + self._uri.netloc
        container_path = self._uri.path.lstrip("/").split("/")
        self._container = container_path.pop(0)
        self._path = "/".join(container_path)

        try:
            # turn the verbosity of the blob client to warning and above (this reduces verbosity)
            logger = logging.getLogger("azure")
            logger.setLevel(logging.ERROR)

            # Attempt to use shared account key to login first
            if "REGISTRY_BLOB_KEY" in os.environ:
                client = BlobServiceClient(
                    account_url=self._account_url,
                    credential=os.environ["REGISTRY_BLOB_KEY"],
                )
                self.blob = client.get_blob_client(
                    container=self._container, blob=self._path
                )
                return

            default_credential = DefaultAzureCredential(
                exclude_shared_token_cache_credential=True
            )

            client = BlobServiceClient(
                account_url=self._account_url, credential=default_credential
            )
            self.blob = client.get_blob_client(
                container=self._container, blob=self._path
            )
        except Exception as e:
            print(
                f"Could not connect to blob. Check the following\nIs the URL specified correctly?\nIs you IAM role set to Storage Blob Data Contributor? \n Errored out with exception {e}"
            )

        return

    def get_registry_proto(self):
        file_obj = TemporaryFile()
        registry_proto = RegistryProto()

        if self.blob.exists():
            download_stream = self.blob.download_blob()
            file_obj.write(download_stream.readall())

            file_obj.seek(0)
            registry_proto.ParseFromString(file_obj.read())
            return registry_proto
        raise FileNotFoundError(
            f'Registry not found at path "{self._uri.geturl()}". Have you run "feast apply"?'
        )

    def update_registry_proto(self, registry_proto: RegistryProto):
        self._write_registry(registry_proto)

    def teardown(self):
        self.blob.delete_blob()

    def _write_registry(self, registry_proto: RegistryProto):
        registry_proto.version_id = str(uuid.uuid4())
        registry_proto.last_updated.FromDatetime(_utc_now())

        file_obj = TemporaryFile()
        file_obj.write(registry_proto.SerializeToString())
        file_obj.seek(0)
        self.blob.upload_blob(file_obj, overwrite=True)  # type: ignore
        return

    def set_project_metadata(self, project: str, key: str, value: str):
        registry_proto = self.get_registry_proto()
        found = False
        for pm in registry_proto.project_metadata:
            if pm.project == project:
                try:
                    meta = json.loads(pm.project_uuid) if pm.project_uuid else {}
                except Exception:
                    meta = {}
                if not isinstance(meta, dict):
                    meta = {}
                meta[key] = value
                pm.project_uuid = json.dumps(meta)
                found = True
                break
        if not found:
            from feast.project_metadata import ProjectMetadata

            pm = ProjectMetadata(project_name=project)
            pm.project_uuid = json.dumps({key: value})
            registry_proto.project_metadata.append(pm.to_proto())
        self.update_registry_proto(registry_proto)

    def get_project_metadata(self, project: str, key: str) -> Optional[str]:
        registry_proto = self.get_registry_proto()
        for pm in registry_proto.project_metadata:
            if pm.project == project:
                try:
                    meta = json.loads(pm.project_uuid) if pm.project_uuid else {}
                except Exception:
                    meta = {}
                if not isinstance(meta, dict):
                    return None
                return meta.get(key, None)
        return None
