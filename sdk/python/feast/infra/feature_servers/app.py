import base64
import os
import tempfile
from pathlib import Path

import yaml
from mangum import Mangum

from feast import FeatureStore
from feast.feature_server import get_app

# Load RepoConfig
config_base64 = os.environ["FEAST_CONFIG_BASE64"]
config_bytes = base64.b64decode(config_base64)

# Override the registry path
config_yaml = yaml.safe_load(config_bytes)
config_yaml["registry"] = "registry.db"
config_bytes = yaml.safe_dump(config_yaml).encode()

# Load Registry
registry_base64 = os.environ["FEAST_REGISTRY_BASE64"]
registry_bytes = base64.b64decode(registry_base64)

# Create a new unique directory for writing feature_store.yaml and registry.db files
repo_path = Path(tempfile.mkdtemp())

with open(repo_path / "feature_store.yaml", "wb") as f:
    f.write(config_bytes)

with open(repo_path / "registry.db", "wb") as f:
    f.write(registry_bytes)

# Initialize the feature store
store = FeatureStore(repo_path=str(repo_path.resolve()))

# Create the FastAPI app and AWS Lambda handler
app = get_app(store)
handler = Mangum(app)
