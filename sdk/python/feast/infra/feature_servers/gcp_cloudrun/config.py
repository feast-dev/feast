from pydantic import StrictBool
from pydantic.typing import Literal

from feast.repo_config import FeastConfigBaseModel


class GcpCloudRunFeatureServerConfig(FeastConfigBaseModel):
    """Feature server config for GCP CloudRun."""

    type: Literal["gcp_cloudrun"] = "gcp_cloudrun"
    """Feature server type selector."""

    enabled: StrictBool = False
    """Whether the feature server should be launched."""

    public: StrictBool = True
    """Whether the endpoint should be publicly accessible."""

    auth: Literal["none", "api-key"] = "none"
    """Authentication method for the endpoint."""
