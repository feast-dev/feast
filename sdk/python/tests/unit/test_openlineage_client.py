# Copyright 2026 The Feast Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from pathlib import Path

import pytest

# ---------------------------------------------------------------------------
# Guard: skip entire module if openlineage-python is not installed
# ---------------------------------------------------------------------------
ol = pytest.importorskip(
    "openlineage.client", reason="openlineage-python not installed"
)

from openlineage.client.transport.console import ConsoleTransport  # noqa: E402
from openlineage.client.transport.transform import TransformTransport  # noqa: E402

from feast.openlineage.client import FeastOpenLineageClient  # noqa: E402
from feast.openlineage.config import OpenLineageConfig  # noqa: E402

_TRANSFORM_YML = """\
transport:
  type: transform
  transformer_class: openlineage.client.transport.transform.JobNamespaceReplaceTransformer
  transformer_properties:
    new_job_namespace: new_value
  transport:
    type: console
"""


def _write_openlineage_yml(tmp_path: Path, content: str = _TRANSFORM_YML) -> str:
    """Write an openlineage.yml file and return its path."""
    yml = tmp_path / "openlineage.yml"
    yml.write_text(content)
    return str(yml)


class TestDefaultConsoleTransport:
    """When transport_type is None and there is no openlineage.yml,
    the OpenLineage SDK should fall back to ConsoleTransport."""

    def test_default_config_uses_console_transport(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # Ensure no openlineage.yml is found by changing cwd to an empty dir
        monkeypatch.chdir(tmp_path)
        monkeypatch.delenv("OPENLINEAGE_CONFIG", raising=False)
        monkeypatch.delenv("OPENLINEAGE_URL", raising=False)
        monkeypatch.delenv("OPENLINEAGE_DISABLED", raising=False)

        config = OpenLineageConfig(enabled=True)  # transport_type defaults to None
        client = FeastOpenLineageClient(config=config)

        assert client.is_enabled
        assert isinstance(client._client.transport, ConsoleTransport)

    def test_default_from_dict_uses_console_transport(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.chdir(tmp_path)
        monkeypatch.delenv("OPENLINEAGE_CONFIG", raising=False)
        monkeypatch.delenv("OPENLINEAGE_URL", raising=False)
        monkeypatch.delenv("OPENLINEAGE_DISABLED", raising=False)

        config = OpenLineageConfig.from_dict({"enabled": True})
        client = FeastOpenLineageClient(config=config)

        assert client.is_enabled
        assert isinstance(client._client.transport, ConsoleTransport)


class TestTransformTransportFromYml:
    def test_transform_yml_is_respected(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        yml_path = _write_openlineage_yml(tmp_path)
        monkeypatch.setenv("OPENLINEAGE_CONFIG", yml_path)
        monkeypatch.delenv("OPENLINEAGE_URL", raising=False)
        monkeypatch.delenv("OPENLINEAGE_DISABLED", raising=False)

        config = OpenLineageConfig(enabled=True)  # transport_type=None
        client = FeastOpenLineageClient(config=config)

        assert client.is_enabled
        assert isinstance(client._client.transport, TransformTransport)
        # The inner transport should be console
        assert isinstance(client._client.transport.transport, ConsoleTransport)

    def test_transform_yml_in_cwd_is_respected(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.delenv("OPENLINEAGE_CONFIG", raising=False)
        monkeypatch.delenv("OPENLINEAGE_URL", raising=False)
        monkeypatch.delenv("OPENLINEAGE_DISABLED", raising=False)

        # Write openlineage.yml in the dir we'll chdir to
        _write_openlineage_yml(tmp_path)
        monkeypatch.chdir(tmp_path)

        config = OpenLineageConfig(enabled=True)  # transport_type=None
        client = FeastOpenLineageClient(config=config)

        assert client.is_enabled
        assert isinstance(client._client.transport, TransformTransport)


class TestExplicitConfigOverridesYml:
    def test_explicit_console_ignores_yml_env(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        yml_path = _write_openlineage_yml(tmp_path)
        monkeypatch.setenv("OPENLINEAGE_CONFIG", yml_path)
        monkeypatch.delenv("OPENLINEAGE_URL", raising=False)
        monkeypatch.delenv("OPENLINEAGE_DISABLED", raising=False)

        config = OpenLineageConfig(enabled=True, transport_type="console")
        client = FeastOpenLineageClient(config=config)

        assert client.is_enabled
        # Must be plain ConsoleTransport, NOT TransformTransport
        assert isinstance(client._client.transport, ConsoleTransport)
        assert not isinstance(client._client.transport, TransformTransport)

    def test_explicit_console_ignores_yml_in_cwd(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.delenv("OPENLINEAGE_CONFIG", raising=False)
        monkeypatch.delenv("OPENLINEAGE_URL", raising=False)
        monkeypatch.delenv("OPENLINEAGE_DISABLED", raising=False)

        _write_openlineage_yml(tmp_path)
        monkeypatch.chdir(tmp_path)

        config = OpenLineageConfig(enabled=True, transport_type="console")
        client = FeastOpenLineageClient(config=config)

        assert client.is_enabled
        assert isinstance(client._client.transport, ConsoleTransport)
        assert not isinstance(client._client.transport, TransformTransport)
