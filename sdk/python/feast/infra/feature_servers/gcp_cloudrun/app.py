import base64
import os
import tempfile
from pathlib import Path

from feast import FeatureStore
from feast.constants import FEATURE_STORE_YAML_ENV_NAME
from feast.feature_server import get_app

# Load RepoConfig
config_base64 = os.environ[FEATURE_STORE_YAML_ENV_NAME]
config_bytes = base64.b64decode(config_base64)

# Create a new unique directory for writing feature_store.yaml
repo_path = Path(tempfile.mkdtemp())

with open(repo_path / "feature_store.yaml", "wb") as f:
    f.write(config_bytes)

# Initialize the feature store
store = FeatureStore(repo_path=str(repo_path.resolve()))

# Create the FastAPI app
app = get_app(store)
