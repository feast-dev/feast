from pydantic import StrictBool, StrictStr
from pydantic.typing import Literal

from feast.repo_config import FeastConfigBaseModel


class AwsLambdaFeatureServerConfig(FeastConfigBaseModel):
    """Feature server config for AWS Lambda."""

    type: Literal["aws_lambda"] = "aws_lambda"
    """Feature server type selector."""

    enabled: StrictBool = False
    """Whether the feature server should be launched."""

    public: StrictBool = True
    """Whether the endpoint should be publicly accessible."""

    auth: Literal["none", "api-key"] = "none"
    """Authentication method for the endpoint."""

    execution_role_name: StrictStr
    """The execution role for the AWS Lambda function."""
