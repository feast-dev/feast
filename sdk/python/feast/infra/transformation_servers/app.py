import base64
import os
import tempfile
import threading
from pathlib import Path

import yaml

from feast import FeatureStore
from feast.constants import (
    DEFAULT_FEATURE_TRANSFORMATION_SERVER_PORT,
    FEATURE_STORE_YAML_ENV_NAME,
    FEATURE_TRANSFORMATION_SERVER_PORT_ENV_NAME,
    REGISTRY_ENV_NAME,
)
from feast.infra.registry.file import FileRegistryStore
from feast.infra.registry.registry import get_registry_store_class_from_scheme

# Load RepoConfig
config_base64 = os.environ[FEATURE_STORE_YAML_ENV_NAME]
config_bytes = base64.b64decode(config_base64)

# Create a new unique directory for writing feature_store.yaml
repo_path = Path(tempfile.mkdtemp())

with open(repo_path / "feature_store.yaml", "wb") as f:
    f.write(config_bytes)

# Write registry contents for local registries
config_string = config_bytes.decode("utf-8")
raw_config = yaml.safe_load(config_string)
registry = raw_config["registry"]
registry_path = registry["path"] if isinstance(registry, dict) else registry
registry_store_class = get_registry_store_class_from_scheme(registry_path)
if registry_store_class == FileRegistryStore and not os.path.exists(registry_path):
    registry_base64 = os.environ[REGISTRY_ENV_NAME]
    registry_bytes = base64.b64decode(registry_base64)
    registry_dir = os.path.dirname(registry_path)
    if not os.path.exists(repo_path / registry_dir):
        os.makedirs(repo_path / registry_dir)
    with open(repo_path / registry_path, "wb") as f:
        f.write(registry_bytes)

# Initialize the feature store
store = FeatureStore(repo_path=str(repo_path.resolve()))

if isinstance(registry, dict) and registry.get("cache_ttl_seconds", 0) > 0:
    # disable synchronous refresh
    store.config.registry.cache_ttl_seconds = 0

    # enable asynchronous refresh
    def async_refresh():
        store.refresh_registry()
        threading.Timer(registry["cache_ttl_seconds"], async_refresh).start()

    async_refresh()

# Start the feature transformation server
port = int(
    os.environ.get(
        FEATURE_TRANSFORMATION_SERVER_PORT_ENV_NAME,
        DEFAULT_FEATURE_TRANSFORMATION_SERVER_PORT,
    )
)
store.serve_transformations(port)
