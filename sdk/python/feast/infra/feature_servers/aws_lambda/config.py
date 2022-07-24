from pydantic import StrictBool, StrictStr
from pydantic.typing import Literal

from feast.infra.feature_servers.base_config import BaseFeatureServerConfig


class AwsLambdaFeatureServerConfig(BaseFeatureServerConfig):
    """Feature server config for AWS Lambda."""

    type: Literal["aws_lambda"] = "aws_lambda"
    """Feature server type selector."""

    public: StrictBool = True
    """Whether the endpoint should be publicly accessible."""

    auth: Literal["none", "api-key"] = "none"
    """Authentication method for the endpoint."""

    execution_role_name: StrictStr
    """The execution role for the AWS Lambda function."""
