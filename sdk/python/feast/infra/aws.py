import base64
import hashlib
import logging
import os
import uuid
from datetime import datetime
from pathlib import Path
from tempfile import TemporaryFile
from typing import Optional, Sequence
from urllib.parse import urlparse

from colorama import Fore, Style

from feast.constants import (
    AWS_LAMBDA_FEATURE_SERVER_IMAGE,
    AWS_LAMBDA_FEATURE_SERVER_REPOSITORY,
    DOCKER_IMAGE_TAG_ENV_NAME,
    FEAST_USAGE,
    FEATURE_STORE_YAML_ENV_NAME,
)
from feast.entity import Entity
from feast.errors import (
    AwsAPIGatewayDoesNotExist,
    AwsLambdaDoesNotExist,
    ExperimentalFeatureNotEnabled,
    IncompatibleRegistryStoreClass,
    RepoConfigPathDoesNotExist,
    S3RegistryBucketForbiddenAccess,
    S3RegistryBucketNotExist,
)
from feast.feature_view import FeatureView
from feast.flags import FLAG_AWS_LAMBDA_FEATURE_SERVER_NAME
from feast.flags_helper import enable_aws_lambda_feature_server
from feast.infra.feature_servers.aws_lambda.config import AwsLambdaFeatureServerConfig
from feast.infra.passthrough_provider import PassthroughProvider
from feast.infra.utils import aws_utils
from feast.protos.feast.core.Registry_pb2 import Registry as RegistryProto
from feast.registry import get_registry_store_class_from_scheme
from feast.registry_store import RegistryStore
from feast.repo_config import RegistryConfig
from feast.usage import log_exceptions_and_usage
from feast.version import get_version

try:
    import boto3
except ImportError as e:
    from feast.errors import FeastExtrasDependencyImportError

    raise FeastExtrasDependencyImportError("aws", str(e))

_logger = logging.getLogger(__name__)


class AwsProvider(PassthroughProvider):
    @log_exceptions_and_usage(provider="AwsProvider")
    def update_infra(
        self,
        project: str,
        tables_to_delete: Sequence[FeatureView],
        tables_to_keep: Sequence[FeatureView],
        entities_to_delete: Sequence[Entity],
        entities_to_keep: Sequence[Entity],
        partial: bool,
    ):
        # Call update only if there is an online store
        if self.online_store:
            self.online_store.update(
                config=self.repo_config,
                tables_to_delete=tables_to_delete,
                tables_to_keep=tables_to_keep,
                entities_to_keep=entities_to_keep,
                entities_to_delete=entities_to_delete,
                partial=partial,
            )

        if self.repo_config.feature_server and self.repo_config.feature_server.enabled:
            if not enable_aws_lambda_feature_server(self.repo_config):
                raise ExperimentalFeatureNotEnabled(FLAG_AWS_LAMBDA_FEATURE_SERVER_NAME)

            # Since the AWS Lambda feature server will attempt to load the registry, we
            # only allow the registry to be in S3.
            registry_path = (
                self.repo_config.registry
                if isinstance(self.repo_config.registry, str)
                else self.repo_config.registry.path
            )
            registry_store_class = get_registry_store_class_from_scheme(registry_path)
            if registry_store_class != S3RegistryStore:
                raise IncompatibleRegistryStoreClass(
                    registry_store_class.__name__, S3RegistryStore.__name__
                )

            ecr_client = boto3.client("ecr")
            docker_image_version = _get_docker_image_version()
            repository_uri = self._create_or_get_repository_uri(ecr_client)
            # Only download & upload the docker image if it doesn't already exist in ECR
            if not ecr_client.batch_get_image(
                repositoryName=AWS_LAMBDA_FEATURE_SERVER_REPOSITORY,
                imageIds=[{"imageTag": docker_image_version}],
            ).get("images"):
                image_uri = self._upload_docker_image(
                    ecr_client, repository_uri, docker_image_version
                )
            else:
                image_uri = f"{repository_uri}:{docker_image_version}"

            self._deploy_feature_server(project, image_uri)

    def _deploy_feature_server(self, project: str, image_uri: str):
        _logger.info("Deploying feature server...")

        if not self.repo_config.repo_path:
            raise RepoConfigPathDoesNotExist()
        with open(self.repo_config.repo_path / "feature_store.yaml", "rb") as f:
            config_bytes = f.read()
            config_base64 = base64.b64encode(config_bytes).decode()

        resource_name = _get_lambda_name(project)
        lambda_client = boto3.client("lambda")
        api_gateway_client = boto3.client("apigatewayv2")
        function = aws_utils.get_lambda_function(lambda_client, resource_name)

        if function is None:
            # If the Lambda function does not exist, create it.
            _logger.info("  Creating AWS Lambda...")
            assert isinstance(
                self.repo_config.feature_server, AwsLambdaFeatureServerConfig
            )
            lambda_client.create_function(
                FunctionName=resource_name,
                Role=self.repo_config.feature_server.execution_role_name,
                Code={"ImageUri": image_uri},
                PackageType="Image",
                MemorySize=1769,
                Environment={
                    "Variables": {
                        FEATURE_STORE_YAML_ENV_NAME: config_base64,
                        FEAST_USAGE: "False",
                    }
                },
                Tags={
                    "feast-owned": "True",
                    "project": project,
                    "feast-sdk-version": get_version(),
                },
            )
            function = aws_utils.get_lambda_function(lambda_client, resource_name)
            if not function:
                raise AwsLambdaDoesNotExist(resource_name)
        else:
            # If the feature_store.yaml has changed, need to update the environment variable.
            env = function.get("Environment", {}).get("Variables", {})
            if env.get(FEATURE_STORE_YAML_ENV_NAME) != config_base64:
                # Note, that this does not update Lambda gracefully (e.g. no rolling deployment).
                # It's expected that feature_store.yaml is not regularly updated while the lambda
                # is serving production traffic. However, the update in registry (e.g. modifying
                # feature views, feature services, and other definitions does not update lambda).
                _logger.info("  Updating AWS Lambda...")

                aws_utils.update_lambda_function_environment(
                    lambda_client,
                    resource_name,
                    {"Variables": {FEATURE_STORE_YAML_ENV_NAME: config_base64}},
                )

        api = aws_utils.get_first_api_gateway(api_gateway_client, resource_name)
        if not api:
            # If the API Gateway doesn't exist, create it
            _logger.info("  Creating AWS API Gateway...")
            api = api_gateway_client.create_api(
                Name=resource_name,
                ProtocolType="HTTP",
                Target=function["FunctionArn"],
                RouteKey="POST /get-online-features",
                Tags={
                    "feast-owned": "True",
                    "project": project,
                    "feast-sdk-version": get_version(),
                },
            )
            if not api:
                raise AwsAPIGatewayDoesNotExist(resource_name)
            # Make sure to give AWS Lambda a permission to be invoked by the newly created API Gateway
            api_id = api["ApiId"]
            region = lambda_client.meta.region_name
            account_id = aws_utils.get_account_id()
            lambda_client.add_permission(
                FunctionName=function["FunctionArn"],
                StatementId=str(uuid.uuid4()),
                Action="lambda:InvokeFunction",
                Principal="apigateway.amazonaws.com",
                SourceArn=f"arn:aws:execute-api:{region}:{account_id}:{api_id}/*/*/get-online-features",
            )

    @log_exceptions_and_usage(provider="AwsProvider")
    def teardown_infra(
        self, project: str, tables: Sequence[FeatureView], entities: Sequence[Entity],
    ) -> None:
        if self.online_store:
            self.online_store.teardown(self.repo_config, tables, entities)

        if (
            self.repo_config.feature_server is not None
            and self.repo_config.feature_server.enabled
        ):
            _logger.info("Tearing down feature server...")
            resource_name = _get_lambda_name(project)
            lambda_client = boto3.client("lambda")
            api_gateway_client = boto3.client("apigatewayv2")

            function = aws_utils.get_lambda_function(lambda_client, resource_name)

            if function is not None:
                _logger.info("  Tearing down AWS Lambda...")
                aws_utils.delete_lambda_function(lambda_client, resource_name)

            api = aws_utils.get_first_api_gateway(api_gateway_client, resource_name)
            if api is not None:
                _logger.info("  Tearing down AWS API Gateway...")
                aws_utils.delete_api_gateway(api_gateway_client, api["ApiId"])

    @log_exceptions_and_usage(provider="AwsProvider")
    def get_feature_server_endpoint(self) -> Optional[str]:
        project = self.repo_config.project
        resource_name = _get_lambda_name(project)
        api_gateway_client = boto3.client("apigatewayv2")
        api = aws_utils.get_first_api_gateway(api_gateway_client, resource_name)

        if not api:
            return None

        api_id = api["ApiId"]
        lambda_client = boto3.client("lambda")
        region = lambda_client.meta.region_name
        return f"https://{api_id}.execute-api.{region}.amazonaws.com"

    def _upload_docker_image(
        self, ecr_client, repository_uri: str, docker_image_version: str
    ) -> str:
        """
        Pulls the AWS Lambda docker image from Dockerhub and uploads it to AWS ECR.

        Returns:
            The URI of the uploaded docker image.
        """
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

        dockerhub_image = f"{AWS_LAMBDA_FEATURE_SERVER_IMAGE}:{docker_image_version}"
        _logger.info(
            f"Pulling remote image {Style.BRIGHT + Fore.GREEN}{dockerhub_image}{Style.RESET_ALL}"
        )
        for line in docker_client.api.pull(dockerhub_image, stream=True, decode=True):
            _logger.debug(f"  {line}")

        auth_token = ecr_client.get_authorization_token()["authorizationData"][0][
            "authorizationToken"
        ]
        username, password = base64.b64decode(auth_token).decode("utf-8").split(":")

        ecr_address = repository_uri.split("/")[0]
        _logger.info(
            f"Logging in Docker client to {Style.BRIGHT + Fore.GREEN}{ecr_address}{Style.RESET_ALL}"
        )
        login_status = docker_client.login(
            username=username, password=password, registry=ecr_address
        )
        _logger.debug(f"  {login_status}")

        image = docker_client.images.get(dockerhub_image)
        image_remote_name = f"{repository_uri}:{docker_image_version}"
        _logger.info(
            f"Pushing local image to remote {Style.BRIGHT + Fore.GREEN}{image_remote_name}{Style.RESET_ALL}"
        )
        image.tag(image_remote_name)
        for line in docker_client.api.push(
            repository_uri, tag=docker_image_version, stream=True, decode=True
        ):
            _logger.debug(f"  {line}")

        return image_remote_name

    def _create_or_get_repository_uri(self, ecr_client):
        try:
            return ecr_client.describe_repositories(
                repositoryNames=[AWS_LAMBDA_FEATURE_SERVER_REPOSITORY]
            )["repositories"][0]["repositoryUri"]
        except ecr_client.exceptions.RepositoryNotFoundException:
            _logger.info(
                f"Creating remote ECR repository {Style.BRIGHT + Fore.GREEN}{AWS_LAMBDA_FEATURE_SERVER_REPOSITORY}{Style.RESET_ALL}"
            )
            response = ecr_client.create_repository(
                repositoryName=AWS_LAMBDA_FEATURE_SERVER_REPOSITORY
            )
            return response["repository"]["repositoryUri"]


def _get_lambda_name(project: str):
    lambda_prefix = AWS_LAMBDA_FEATURE_SERVER_REPOSITORY
    lambda_suffix = f"{project}-{_get_docker_image_version()}"
    # AWS Lambda name can't have the length greater than 64 bytes.
    # This usually occurs during integration tests where feast version is long
    if len(lambda_prefix) + len(lambda_suffix) >= 63:
        lambda_suffix = hashlib.md5(lambda_suffix.encode()).hexdigest()
    return f"{lambda_prefix}-{lambda_suffix}"


def _get_docker_image_version() -> str:
    """Returns a version for the feature server Docker image.

    If the feast.constants.DOCKER_IMAGE_TAG_ENV_NAME environment variable is set,
    we return that (mostly used for integration tests, but can be used for local testing too).

    For public Feast releases this equals to the Feast SDK version modified by replacing "." with "_".
    For example, Feast SDK version "0.14.1" would correspond to Docker image version "0_14_1".

    During development (when Feast is installed in editable mode) this equals to the Feast SDK version
    modified by removing the "dev..." suffix and replacing "." with "_". For example, Feast SDK version
    "0.14.1.dev41+g1cbfa225.d20211103" would correspond to Docker image version "0_14_1". This way,
    Feast SDK will use an already existing Docker image built during the previous public release.

    """
    tag = os.environ.get(DOCKER_IMAGE_TAG_ENV_NAME)
    if tag is not None:
        return tag
    else:
        version = get_version()
        if "dev" in version:
            version = version[: version.find("dev") - 1].replace(".", "_")
            _logger.warning(
                "You are trying to use AWS Lambda feature server while Feast is in a development mode. "
                f"Feast will use a docker image version {version} derived from Feast SDK "
                f"version {get_version()}. If you want to update the Feast SDK version, make "
                "sure to first fetch all new release tags from Github and then reinstall the library:\n"
                "> git fetch --all --tags\n"
                "> pip install -e sdk/python"
            )
        else:
            version = version.replace(".", "_")
        return version


class S3RegistryStore(RegistryStore):
    def __init__(self, registry_config: RegistryConfig, repo_path: Path):
        uri = registry_config.path
        self._uri = urlparse(uri)
        self._bucket = self._uri.hostname
        self._key = self._uri.path.lstrip("/")

        self.s3_client = boto3.resource(
            "s3", endpoint_url=os.environ.get("FEAST_S3_ENDPOINT_URL")
        )

    @log_exceptions_and_usage(registry="s3")
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

    @log_exceptions_and_usage(registry="s3")
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
