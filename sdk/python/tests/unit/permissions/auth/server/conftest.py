import os
import tempfile
from textwrap import dedent

import pytest
import yaml

from feast import FeatureStore
from tests.unit.permissions.auth.server.mock_utils import PROJECT_NAME
from tests.utils.cli_repo_creator import CliRunner
from tests.utils.http_server import free_port  # noqa: E402


@pytest.fixture(
    scope="module",
    params=[
        dedent("""
          auth:
            type: no_auth
          """),
        dedent("""
          auth:
            type: kubernetes
        """),
        dedent("""
          auth:
            type: oidc
            client_id: client_id
            client_secret: client_secret
            username: username
            password: password
            realm: realm
            auth_server_url: http://localhost:8080
            auth_discovery_url: http://localhost:8080/realms/master/.well-known/openid-configuration
        """),
    ],
)
def auth_config(request):
    return request.param


@pytest.fixture
def temp_dir():
    with tempfile.TemporaryDirectory() as temp_dir:
        print(f"Created {temp_dir}")
        yield temp_dir


@pytest.fixture
def feature_store(temp_dir, auth_config):
    print(f"Creating store at {temp_dir}")
    return _default_store(str(temp_dir), auth_config)


@pytest.fixture
def server_port():
    return free_port()


def _include_auth_config(file_path, auth_config: str):
    with open(file_path, "r") as file:
        existing_content = yaml.safe_load(file)
    new_section = yaml.safe_load(auth_config)
    if isinstance(existing_content, dict) and isinstance(new_section, dict):
        existing_content.update(new_section)
    else:
        raise ValueError("Both existing content and new section must be dictionaries.")
    with open(file_path, "w") as file:
        yaml.safe_dump(existing_content, file, default_flow_style=False)
    print(f"Updated auth section at {file_path}")


def _default_store(temp_dir, auth_config: str):
    runner = CliRunner()
    result = runner.run(["init", PROJECT_NAME], cwd=temp_dir)
    repo_path = os.path.join(temp_dir, PROJECT_NAME, "feature_repo")
    assert result.returncode == 0

    _include_auth_config(
        file_path=f"{repo_path}/feature_store.yaml", auth_config=auth_config
    )

    result = runner.run(["--chdir", repo_path, "apply"], cwd=temp_dir)
    assert result.returncode == 0

    fs = FeatureStore(repo_path=repo_path)
    return fs
