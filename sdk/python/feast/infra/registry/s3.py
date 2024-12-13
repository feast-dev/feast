import importlib.util
import uuid
from pathlib import Path
from tempfile import TemporaryFile
from urllib.parse import urlparse

from mypy_boto3_s3 import S3ServiceResource
from pydantic import StrictStr

from feast.errors import (
    FeastExtrasDependencyImportError,
    S3RegistryBucketForbiddenAccess,
    S3RegistryBucketNotExist,
    S3RegistryPathInvalid,
)
from feast.infra.registry.registry_store import RegistryStore
from feast.protos.feast.core.Registry_pb2 import Registry as RegistryProto
from feast.repo_config import RegistryConfig
from feast.utils import _utc_now

if importlib.util.find_spec("boto3") is None:
    raise FeastExtrasDependencyImportError(
        "aws", "boto3 is required to use S3 registry store"
    )


class S3RegistryConfig(RegistryConfig):
    registry_type: StrictStr = "s3"
    s3_client: S3ServiceResource


class S3RegistryStore(RegistryStore):
    def __init__(self, registry_config: S3RegistryConfig, repo_path: Path):
        uri = registry_config.path
        self._uri = urlparse(uri)
        self._bucket = self._uri.hostname or ""
        self._key = self._uri.path.lstrip("/")
        self._boto_extra_args = registry_config.s3_additional_kwargs or {}

        if self._bucket == "" or self._key == "":
            raise S3RegistryPathInvalid(uri)

        self.s3_client = registry_config.s3_client

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
            Body=file_obj,
            Key=self._key,
            **self._boto_extra_args,  # type: ignore
        )
