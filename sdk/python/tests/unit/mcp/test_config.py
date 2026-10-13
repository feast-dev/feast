"""Config resolution for ``feast mcp``.

Priority is CLI args > env vars > YAML file > defaults, and every option has
to honour it -- a stray default leaking into ``cli_args`` silently outranks
the user's ``feast_mcp.yaml``, which is exactly the kind of bug that only
shows up in someone else's deployment.
"""

from __future__ import annotations

import textwrap

import pytest

from feast.mcp.config import Config, load_config

ENV_VARS = [
    "FEAST_MCP_TRANSPORT",
    "FEAST_MCP_WORKERS",
    "FEAST_MCP_FEATURE_SERVER_URL",
    "FEAST_MCP_REGISTRY_URL",
    "FEAST_MCP_AUTH_MODE",
    "FEAST_MCP_OIDC_DISCOVERY_URL",
    "FEAST_MCP_OIDC_CLIENT_ID",
    "FEAST_MCP_OIDC_CLIENT_SECRET",
    "FEAST_MCP_OIDC_AUDIENCE",
    "FEAST_MCP_BASE_URL",
    "FEAST_MCP_TIMEOUT",
]


@pytest.fixture(autouse=True)
def isolated_env(monkeypatch, tmp_path):
    """Neutralise the ambient environment and CWD.

    ``load_config`` auto-discovers ``feast_mcp.yaml`` from the working
    directory, so tests must not run wherever pytest happens to start.
    """
    for name in ENV_VARS:
        monkeypatch.delenv(name, raising=False)
    monkeypatch.chdir(tmp_path)
    return tmp_path


def write_yaml(tmp_path, body: str) -> str:
    path = tmp_path / "feast_mcp.yaml"
    path.write_text(textwrap.dedent(body))
    return str(path)


class TestDefaults:
    def test_defaults_when_nothing_is_configured(self):
        cfg = load_config()
        assert cfg == Config()
        assert cfg.server.transport == "stdio"
        assert cfg.server.host == "0.0.0.0"
        assert cfg.server.port == 8000
        assert cfg.server.workers is None
        assert cfg.auth.mode == "passthrough"
        assert cfg.timeout == 30.0


class TestYamlFile:
    def test_explicit_config_path_is_loaded(self, isolated_env):
        path = write_yaml(
            isolated_env,
            """
            server:
              transport: http
              host: 127.0.0.1
              port: 9000
              workers: 4
            features:
              url: http://features:6566
            registry:
              url: http://registry:8080
            timeout: 12.5
            """,
        )
        cfg = load_config(config_path=path)
        assert cfg.server.transport == "http"
        assert cfg.server.host == "127.0.0.1"
        assert cfg.server.port == 9000
        assert cfg.server.workers == 4
        assert cfg.features.url == "http://features:6566"
        assert cfg.registry.url == "http://registry:8080"
        assert cfg.timeout == 12.5

    def test_feast_mcp_yaml_is_discovered_in_the_cwd(self, isolated_env):
        write_yaml(isolated_env, "features:\n  url: http://discovered:6566\n")
        assert load_config().features.url == "http://discovered:6566"

    def test_empty_yaml_falls_back_to_defaults(self, isolated_env):
        path = write_yaml(isolated_env, "")
        assert load_config(config_path=path) == Config()

    def test_non_mapping_section_is_ignored_rather_than_crashing(self, isolated_env):
        path = write_yaml(isolated_env, "server: not-a-mapping\n")
        assert load_config(config_path=path).server.transport == "stdio"


class TestPrecedence:
    """CLI > env > YAML > default, checked one rung at a time."""

    def test_env_overrides_yaml(self, isolated_env, monkeypatch):
        path = write_yaml(isolated_env, "features:\n  url: http://from-yaml\n")
        monkeypatch.setenv("FEAST_MCP_FEATURE_SERVER_URL", "http://from-env")
        assert load_config(config_path=path).features.url == "http://from-env"

    def test_cli_overrides_env(self, isolated_env, monkeypatch):
        monkeypatch.setenv("FEAST_MCP_FEATURE_SERVER_URL", "http://from-env")
        cfg = load_config(cli_args={"feast_url": "http://from-cli"})
        assert cfg.features.url == "http://from-cli"

    def test_cli_overrides_yaml(self, isolated_env):
        path = write_yaml(isolated_env, "features:\n  url: http://from-yaml\n")
        cfg = load_config(config_path=path, cli_args={"feast_url": "http://from-cli"})
        assert cfg.features.url == "http://from-cli"

    def test_yaml_wins_when_no_cli_arg_is_supplied(self, isolated_env):
        """Absent CLI options must not be present as ``None`` in cli_args."""
        path = write_yaml(isolated_env, "server:\n  transport: sse\n")
        assert load_config(config_path=path, cli_args={}).server.transport == "sse"

    @pytest.mark.parametrize(
        "env_var,cli_key,attr_path",
        [
            ("FEAST_MCP_TRANSPORT", "transport", ("server", "transport")),
            ("FEAST_MCP_REGISTRY_URL", "registry_url", ("registry", "url")),
            ("FEAST_MCP_AUTH_MODE", "auth_mode", ("auth", "mode")),
            ("FEAST_MCP_OIDC_CLIENT_ID", "oidc_client_id", ("auth", "client_id")),
            ("FEAST_MCP_BASE_URL", "base_url", ("auth", "base_url")),
        ],
    )
    def test_every_option_honours_cli_over_env(
        self, isolated_env, monkeypatch, env_var, cli_key, attr_path
    ):
        monkeypatch.setenv(env_var, "from-env")
        cfg = load_config(cli_args={cli_key: "from-cli"})
        section, field = attr_path
        assert getattr(getattr(cfg, section), field) == "from-cli"


class TestCoercion:
    def test_port_from_yaml_string_becomes_an_int(self, isolated_env):
        path = write_yaml(isolated_env, 'server:\n  port: "9100"\n')
        assert load_config(config_path=path).server.port == 9100

    def test_workers_from_env_string_becomes_an_int(self, isolated_env, monkeypatch):
        monkeypatch.setenv("FEAST_MCP_WORKERS", "8")
        assert load_config().server.workers == 8

    def test_timeout_from_env_string_becomes_a_float(self, isolated_env, monkeypatch):
        monkeypatch.setenv("FEAST_MCP_TIMEOUT", "45")
        assert load_config().timeout == 45.0

    def test_workers_stays_none_when_unset(self, isolated_env):
        assert load_config().server.workers is None


class TestMissingYaml:
    def test_missing_explicit_config_path_raises(self, isolated_env):
        with pytest.raises(FileNotFoundError):
            load_config(config_path=str(isolated_env / "nope.yaml"))
