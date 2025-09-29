import json
import os
import uuid
from pathlib import Path
from tempfile import TemporaryFile
from typing import Optional
from urllib.parse import urlparse

from feast.errors import S3RegistryBucketForbiddenAccess, S3RegistryBucketNotExist
from feast.infra.registry.registry_store import RegistryStore
from feast.protos.feast.core.Registry_pb2 import Registry as RegistryProto
from feast.repo_config import RegistryConfig
from feast.utils import _utc_now

try:
    import boto3
except ImportError as e:
    from feast.errors import FeastExtrasDependencyImportError

    raise FeastExtrasDependencyImportError("aws", str(e))


class S3RegistryStore(RegistryStore):
    def __init__(self, registry_config: RegistryConfig, repo_path: Path):
        uri = registry_config.path
        self._uri = urlparse(uri)
        self._bucket = self._uri.hostname
        self._key = self._uri.path.lstrip("/")
        self._boto_extra_args = registry_config.s3_additional_kwargs or {}

        self.s3_client = boto3.resource(
            "s3", endpoint_url=os.environ.get("FEAST_S3_ENDPOINT_URL")
        )

    def get_registry_proto(self):
        file_obj = TemporaryFile()
        registry_proto = RegistryProto()
        try:
            from botocore.exceptions import ClientError
        except ImportError as e:
            from feast.errors import FeastExtrasDependencyImportError

            raise FeastExtrasDependencyImportError("aws", str(e))
        try:
            bucket = self.s3_client.Bucket(self._bucket)
            self.s3_client.meta.client.head_bucket(Bucket=bucket.name)
        except ClientError as e:
            # If a client error is thrown, then check that it was a 404 error.
            # If it was a 404 error, then the bucket does not exist.
            error_code = int(e.response["Error"]["Code"])
            if error_code == 404:
                raise S3RegistryBucketNotExist(self._bucket)
            else:
                raise S3RegistryBucketForbiddenAccess(self._bucket) from e

        try:
            obj = bucket.Object(self._key)
            obj.download_fileobj(file_obj)
            file_obj.seek(0)
            registry_proto.ParseFromString(file_obj.read())
            return registry_proto
        except ClientError as e:
            raise FileNotFoundError(
                f"Error while trying to locate Registry at path {self._uri.geturl()}"
            ) from e

    def update_registry_proto(self, registry_proto: RegistryProto):
        self._write_registry(registry_proto)

    def teardown(self):
        self.s3_client.Object(self._bucket, self._key).delete()

    def _write_registry(self, registry_proto: RegistryProto):
        registry_proto.version_id = str(uuid.uuid4())
        registry_proto.last_updated.FromDatetime(_utc_now())
        # we have already checked the bucket exists so no need to do it again
        file_obj = TemporaryFile()
        file_obj.write(registry_proto.SerializeToString())
        file_obj.seek(0)
        self.s3_client.Bucket(self._bucket).put_object(
            Body=file_obj, Key=self._key, **self._boto_extra_args
        )

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
