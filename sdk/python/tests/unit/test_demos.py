"""Unit tests for feast.demos — demo notebook generation."""

import json
import pathlib
import textwrap

import pytest

from feast.demos import (
    _extract_store_info,
    _find_feature_store_yamls,
    _is_operator_client,
    copy_demo_notebooks,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_LOCAL_YAML = textwrap.dedent("""\
    project: local_proj
    provider: local
    registry:
      path: data/registry.db
      registry_type: file
    offline_store:
      type: file
    online_store:
      type: sqlite
      path: data/online_store.db
    entity_key_serialization_version: 3
""")

_OPERATOR_YAML = textwrap.dedent("""\
    project: remote_proj
    provider: local
    offline_store:
      host: feast-offline.svc.cluster.local
      port: 80
      type: remote
    online_store:
      path: http://feast-online.svc.cluster.local:80
      type: remote
    registry:
      path: feast-registry.svc.cluster.local:80
      registry_type: remote
    auth:
      type: oidc
    entity_key_serialization_version: 3
""")

_VECTOR_YAML = textwrap.dedent("""\
    project: vec_proj
    provider: local
    registry:
      path: data/registry.db
      registry_type: file
    offline_store:
      type: file
    online_store:
      type: pgvector
      vector_enabled: true
      embedding_dim: 512
    entity_key_serialization_version: 3
""")


def _write(tmp_path: pathlib.Path, rel: str, content: str) -> pathlib.Path:
    p = tmp_path / rel
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(content)
    return p


def _sections(nb: dict) -> list[str]:
    """Return the first line of every markdown cell that starts with #."""
    return [
        "".join(cell["source"]).splitlines()[0]
        for cell in nb["cells"]
        if cell["cell_type"] == "markdown" and "".join(cell["source"]).startswith("#")
    ]


# ---------------------------------------------------------------------------
# _extract_store_info
# ---------------------------------------------------------------------------


class TestExtractStoreInfo:
    def test_local_defaults(self):
        info = _extract_store_info({})
        assert info["project"] == "my_feast_project"
        assert info["provider"] == "local"
        assert info["online_store_type"] == "sqlite"
        assert info["offline_store_type"] == "file"
        assert info["registry_type"] == "file"
        assert info["auth_type"] == "no_auth"
        assert info["vector_enabled"] is False
        assert info["embedding_dim"] is None

    def test_operator_client_yaml(self):
        config = {
            "project": "sample",
            "provider": "local",
            "offline_store": {"type": "remote", "host": "h", "port": 80},
            "online_store": {"type": "remote", "path": "http://h:80"},
            "registry": {"registry_type": "remote", "path": "h:80"},
            "auth": {"type": "oidc"},
        }
        info = _extract_store_info(config)
        assert info["registry_type"] == "remote"
        assert info["online_store_type"] == "remote"
        assert info["offline_store_type"] == "remote"
        assert info["auth_type"] == "oidc"

    def test_registry_type_key_takes_priority_over_type(self):
        config = {"registry": {"registry_type": "remote", "type": "file"}}
        info = _extract_store_info(config)
        assert info["registry_type"] == "remote"

    def test_registry_type_fallback_to_type(self):
        config = {"registry": {"type": "snowflake"}}
        info = _extract_store_info(config)
        assert info["registry_type"] == "snowflake"

    def test_string_registry_path_stays_file(self):
        info = _extract_store_info({"registry": "data/registry.db"})
        assert info["registry_type"] == "file"

    def test_vector_enabled(self):
        config = {
            "online_store": {
                "type": "pgvector",
                "vector_enabled": True,
                "embedding_dim": 512,
            }
        }
        info = _extract_store_info(config)
        assert info["vector_enabled"] is True
        assert info["embedding_dim"] == 512

    def test_online_store_as_string(self):
        info = _extract_store_info({"online_store": "Redis"})
        assert info["online_store_type"] == "redis"

    def test_offline_store_as_string(self):
        info = _extract_store_info({"offline_store": "BigQuery"})
        assert info["offline_store_type"] == "bigquery"


# ---------------------------------------------------------------------------
# _is_operator_client
# ---------------------------------------------------------------------------


class TestIsOperatorClient:
    def _info(self, registry="remote", online="remote", offline="remote"):
        return {
            "registry_type": registry,
            "online_store_type": online,
            "offline_store_type": offline,
        }

    def test_all_remote_is_operator(self):
        assert _is_operator_client(self._info()) is True

    def test_local_registry_not_operator(self):
        assert _is_operator_client(self._info(registry="file")) is False

    def test_local_online_not_operator(self):
        assert _is_operator_client(self._info(online="sqlite")) is False

    def test_local_offline_not_operator(self):
        assert _is_operator_client(self._info(offline="file")) is False


# ---------------------------------------------------------------------------
# _find_feature_store_yamls
# ---------------------------------------------------------------------------


class TestFindFeatureStoreYamls:
    def test_direct(self, tmp_path):
        _write(tmp_path, "feature_store.yaml", "project: p")
        found = _find_feature_store_yamls(tmp_path)
        assert len(found) == 1
        assert found[0].name == "feature_store.yaml"

    def test_feast_config_root(self, tmp_path):
        _write(tmp_path, "feast-config/feature_store.yaml", "project: p")
        found = _find_feature_store_yamls(tmp_path)
        assert len(found) == 1

    def test_feast_config_multiple_files(self, tmp_path):
        _write(tmp_path, "feast-config/rag.yaml", "project: rag")
        _write(tmp_path, "feast-config/rec.yml", "project: rec")
        found = _find_feature_store_yamls(tmp_path)
        assert len(found) == 2

    def test_feast_config_any_extension(self, tmp_path):
        _write(tmp_path, "feast-config/project_a.yaml", "project: a")
        _write(tmp_path, "feast-config/project_b", "project: b")
        found = _find_feature_store_yamls(tmp_path)
        assert len(found) == 2

    def test_feast_config_ignores_directories(self, tmp_path):
        _write(tmp_path, "feast-config/valid.yaml", "project: p")
        (tmp_path / "feast-config" / "subdir").mkdir()
        found = _find_feature_store_yamls(tmp_path)
        assert len(found) == 1

    def test_multiple_sources(self, tmp_path):
        _write(tmp_path, "feature_store.yaml", "project: root")
        _write(tmp_path, "feast-config/a.yaml", "project: a")
        _write(tmp_path, "feast-config/b", "project: b")
        found = _find_feature_store_yamls(tmp_path)
        assert len(found) == 3

    def test_no_yaml_returns_empty(self, tmp_path):
        assert _find_feature_store_yamls(tmp_path) == []


# ---------------------------------------------------------------------------
# copy_demo_notebooks — file generation
# ---------------------------------------------------------------------------


class TestCopyDemoNotebooks:
    def test_generates_notebooks(self, tmp_path):
        _write(tmp_path, "feature_store.yaml", _LOCAL_YAML)
        out = tmp_path / "out"
        copy_demo_notebooks(output_dir=str(out), repo_path=str(tmp_path))

        assert (out / "local_proj" / "01_feature_store_overview.ipynb").exists()
        assert (out / "local_proj" / "02_historical_features_training.ipynb").exists()
        assert (out / "local_proj" / "03_online_features_serving.ipynb").exists()

    def test_valid_notebook_json(self, tmp_path):
        _write(tmp_path, "feature_store.yaml", _LOCAL_YAML)
        out = tmp_path / "out"
        copy_demo_notebooks(output_dir=str(out), repo_path=str(tmp_path))

        nb = json.loads(
            (out / "local_proj" / "01_feature_store_overview.ipynb").read_text()
        )
        assert nb["nbformat"] == 4
        assert isinstance(nb["cells"], list)

    def test_raises_if_output_exists(self, tmp_path):
        _write(tmp_path, "feature_store.yaml", _LOCAL_YAML)
        out = tmp_path / "out"
        copy_demo_notebooks(output_dir=str(out), repo_path=str(tmp_path))

        with pytest.raises(FileExistsError):
            copy_demo_notebooks(output_dir=str(out), repo_path=str(tmp_path))

    def test_overwrite_flag(self, tmp_path):
        _write(tmp_path, "feature_store.yaml", _LOCAL_YAML)
        out = tmp_path / "out"
        copy_demo_notebooks(output_dir=str(out), repo_path=str(tmp_path))
        copy_demo_notebooks(
            output_dir=str(out), repo_path=str(tmp_path), overwrite=True
        )

    def test_no_yaml_returns_without_creating_output(self, tmp_path):
        out = tmp_path / "out"
        copy_demo_notebooks(output_dir=str(out), repo_path=str(tmp_path))
        assert not out.exists()

    def test_multiple_projects(self, tmp_path):
        _write(
            tmp_path,
            "feast-config/proj_a.yaml",
            "project: proj_a\nprovider: local\n",
        )
        _write(
            tmp_path,
            "feast-config/proj_b.yaml",
            "project: proj_b\nprovider: local\n",
        )
        out = tmp_path / "out"
        copy_demo_notebooks(output_dir=str(out), repo_path=str(tmp_path))
        assert (out / "proj_a").is_dir()
        assert (out / "proj_b").is_dir()


# ---------------------------------------------------------------------------
# Notebook content — section headings
# ---------------------------------------------------------------------------


class TestNotebookContent:
    def _notebooks(self, tmp_path, yaml_content):
        _write(tmp_path, "feature_store.yaml", yaml_content)
        out = tmp_path / "out"
        copy_demo_notebooks(output_dir=str(out), repo_path=str(tmp_path))
        project = _extract_store_info(__import__("yaml").safe_load(yaml_content))[
            "project"
        ]
        return {
            name: json.loads((out / project / name).read_text())
            for name in [
                "01_feature_store_overview.ipynb",
                "02_historical_features_training.ipynb",
                "03_online_features_serving.ipynb",
            ]
        }

    def test_local_overview_has_apply_section(self, tmp_path):
        nbs = self._notebooks(tmp_path, _LOCAL_YAML)
        sections = _sections(nbs["01_feature_store_overview.ipynb"])
        assert any("Apply Feature Definitions" in s for s in sections)

    def test_remote_overview_has_registry_sync(self, tmp_path):
        nbs = self._notebooks(tmp_path, _OPERATOR_YAML)
        sections = _sections(nbs["01_feature_store_overview.ipynb"])
        assert any("Registry Sync" in s for s in sections)

    def test_historical_no_apply_section(self, tmp_path):
        nbs = self._notebooks(tmp_path, _LOCAL_YAML)
        sections = _sections(nbs["02_historical_features_training.ipynb"])
        assert not any("Apply" in s for s in sections)

    def test_online_no_apply_section(self, tmp_path):
        nbs = self._notebooks(tmp_path, _LOCAL_YAML)
        sections = _sections(nbs["03_online_features_serving.ipynb"])
        assert not any("Apply" in s for s in sections)

    def test_vector_notebook_has_vector_section(self, tmp_path):
        nbs = self._notebooks(tmp_path, _VECTOR_YAML)
        sections = _sections(nbs["03_online_features_serving.ipynb"])
        assert any("Vector" in s for s in sections)

    def test_non_vector_notebook_no_vector_section(self, tmp_path):
        nbs = self._notebooks(tmp_path, _LOCAL_YAML)
        sections = _sections(nbs["03_online_features_serving.ipynb"])
        assert not any("Vector" in s for s in sections)

    def test_auth_section_present_for_oidc(self, tmp_path):
        nbs = self._notebooks(tmp_path, _OPERATOR_YAML)
        sections = _sections(nbs["03_online_features_serving.ipynb"])
        assert any("Authentication" in s for s in sections)

    def test_auth_section_absent_for_no_auth(self, tmp_path):
        nbs = self._notebooks(tmp_path, _LOCAL_YAML)
        sections = _sections(nbs["03_online_features_serving.ipynb"])
        assert not any("Authentication" in s for s in sections)

    def test_path_setup_cell_contains_yaml_path(self, tmp_path):
        _write(tmp_path, "feature_store.yaml", _LOCAL_YAML)
        out = tmp_path / "out"
        copy_demo_notebooks(output_dir=str(out), repo_path=str(tmp_path))
        nb = json.loads(
            (out / "local_proj" / "01_feature_store_overview.ipynb").read_text()
        )
        code_sources = [
            "".join(c["source"]) for c in nb["cells"] if c["cell_type"] == "code"
        ]
        yaml_path = str((tmp_path / "feature_store.yaml").resolve())
        assert any(yaml_path in src for src in code_sources)
