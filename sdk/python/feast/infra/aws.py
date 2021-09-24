import os
import uuid
from datetime import datetime
from pathlib import Path
from tempfile import TemporaryFile
from urllib.parse import urlparse

from colorama import Fore, Style

import feast
from feast.constants import AWS_LAMBDA_FEATURE_SERVER_IMAGE
from feast.errors import S3RegistryBucketForbiddenAccess, S3RegistryBucketNotExist
from feast.infra.passthrough_provider import PassthroughProvider
from feast.protos.feast.core.Registry_pb2 import Registry as RegistryProto
from feast.registry_store import RegistryStore
from feast.repo_config import RegistryConfig


class AwsProvider(PassthroughProvider):
    def _upload_docker_image(self) -> None:
        import base64

        try:
            import boto3
        except ImportError as e:
            from feast.errors import FeastExtrasDependencyImportError

            raise FeastExtrasDependencyImportError("aws", str(e))

        try:
            import docker
            from docker.errors import APIError
        except ImportError as e:
            from feast.errors import FeastExtrasDependencyImportError

            raise FeastExtrasDependencyImportError("docker", str(e))

        try:
            docker_client = docker.from_env()
        except APIError:
            from feast.errors import DockerDaemonNotRunning

            raise DockerDaemonNotRunning()

        print(
            f"Pulling remote image {Style.BRIGHT + Fore.GREEN}{AWS_LAMBDA_FEATURE_SERVER_IMAGE}{Style.RESET_ALL}:"
        )
        docker_client.images.pull(AWS_LAMBDA_FEATURE_SERVER_IMAGE)

        version = ".".join(feast.__version__.split(".")[:3])
        repository_name = f"feast-python-server-{version}"
        ecr_client = boto3.client("ecr")
        try:
            print(
                f"Creating remote ECR repository {Style.BRIGHT + Fore.GREEN}{repository_name}{Style.RESET_ALL}:"
            )
            response = ecr_client.create_repository(repositoryName=repository_name)
            repository_uri = response["repository"]["repositoryUri"]
        except ecr_client.exceptions.RepositoryAlreadyExistsException:
            response = ecr_client.describe_repositories(
                repositoryNames=[repository_name]
            )
            repository_uri = response["repositories"][0]["repositoryUri"]

        auth_token = ecr_client.get_authorization_token()["authorizationData"][0][
            "authorizationToken"
        ]
        username, password = base64.b64decode(auth_token).decode("utf-8").split(":")

        ecr_address = repository_uri.split("/")[0]
        docker_client.login(username=username, password=password, registry=ecr_address)

        image = docker_client.images.get(AWS_LAMBDA_FEATURE_SERVER_IMAGE)
        image_remote_name = f"{repository_uri}:{version}"
        print(
            f"Pushing local image to remote {Style.BRIGHT + Fore.GREEN}{image_remote_name}{Style.RESET_ALL}:"
        )
        image.tag(image_remote_name)
        docker_client.api.push(repository_uri, tag=version)


class S3RegistryStore(RegistryStore):
    def __init__(self, registry_config: RegistryConfig, repo_path: Path):
        uri = registry_config.path
        try:
            import boto3
        except ImportError as e:
            from feast.errors import FeastExtrasDependencyImportError

            raise FeastExtrasDependencyImportError("aws", str(e))
        self._uri = urlparse(uri)
        self._bucket = self._uri.hostname
        self._key = self._uri.path.lstrip("/")

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
        registry_proto.last_updated.FromDatetime(datetime.utcnow())
        # we have already checked the bucket exists so no need to do it again
        file_obj = TemporaryFile()
        file_obj.write(registry_proto.SerializeToString())
        file_obj.seek(0)
        self.s3_client.Bucket(self._bucket).put_object(Body=file_obj, Key=self._key)
