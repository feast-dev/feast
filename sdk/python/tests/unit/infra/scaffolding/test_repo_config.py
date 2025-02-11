import tempfile
from pathlib import Path
from textwrap import dedent
from typing import Optional

from feast.infra.materialization.contrib.spark.spark_materialization_engine import (
    SparkMaterializationEngineConfig,
)
from feast.infra.offline_stores.contrib.spark_offline_store.spark import (
    SparkOfflineStoreConfig,
)
from feast.infra.online_stores.redis import RedisOnlineStoreConfig
from feast.infra.online_stores.sqlite import SqliteOnlineStoreConfig
from feast.permissions.auth.auth_type import AuthType
from feast.permissions.auth_model import (
    KubernetesAuthConfig,
    NoAuthConfig,
    OidcAuthConfig,
    OidcClientAuthConfig,
)
from feast.repo_config import FeastConfigError, load_repo_config


def _test_config(config_text, expect_error: Optional[str]):
    """
    Try loading a repo config and check raised error against a regex.
    """
    with tempfile.TemporaryDirectory() as repo_dir_name:
        repo_path = Path(repo_dir_name)

        repo_config = repo_path / "feature_store.yaml"

        repo_config.write_text(config_text)
        error = None
        rc = None
        try:
            rc = load_repo_config(repo_path, repo_config)
        except FeastConfigError as e:
            error = e

        if expect_error is not None:
            assert expect_error in str(error)
        else:
            print(f"error: {error}")
            assert error is None

        return rc


def test_nullable_online_store_local():
    _test_config(
        dedent(
            """
        project: foo
        registry: "registry.db"
        provider: local
        online_store: null
        entity_key_serialization_version: 2
        """
        ),
        expect_error=None,
    )


def test_local_config():
    _test_config(
        dedent(
            """
        project: foo
        registry: "registry.db"
        provider: local
        entity_key_serialization_version: 2
        """
        ),
        expect_error=None,
    )


def test_local_config_with_full_online_class():
    c = _test_config(
        dedent(
            """
        project: foo
        registry: "registry.db"
        provider: local
        online_store:
            type: feast.infra.online_stores.sqlite.SqliteOnlineStore
        entity_key_serialization_version: 2
        """
        ),
        expect_error=None,
    )
    assert isinstance(c.online_store, SqliteOnlineStoreConfig)


def test_local_config_with_full_online_class_directly():
    c = _test_config(
        dedent(
            """
        project: foo
        registry: "registry.db"
        provider: local
        online_store: feast.infra.online_stores.sqlite.SqliteOnlineStore
        entity_key_serialization_version: 2
        """
        ),
        expect_error=None,
    )
    assert isinstance(c.online_store, SqliteOnlineStoreConfig)


def test_extra_field():
    _test_config(
        dedent(
            """
        project: foo
        registry: "registry.db"
        provider: local
        online_store:
            type: sqlite
            that_field_should_not_be_here: yes
            path: "online_store.db"
        """
        ),
        expect_error="1 validation error for RepoConfig\nthat_field_should_not_be_here\n  Extra inputs are not permitted",
    )


def test_no_online_store_type():
    _test_config(
        dedent(
            """
        project: foo
        registry: "registry.db"
        provider: local
        online_store:
            path: "blah"
        entity_key_serialization_version: 2
        """
        ),
        expect_error=None,
    )


def test_bad_type():
    _test_config(
        dedent(
            """
        project: foo
        registry: "registry.db"
        provider: local
        online_store:
            path: 100500
        """
        ),
        expect_error="1 validation error for RepoConfig\npath\n  Input should be a valid string",
    )


def test_no_project():
    _test_config(
        dedent(
            """
        registry: "registry.db"
        provider: local
        online_store:
            path: foo
        entity_key_serialization_version: 2
        """
        ),
        expect_error="1 validation error for RepoConfig\nproject\n  Field required",
    )


def test_invalid_project_name():
    _test_config(
        dedent(
            """
        project: foo-1
        registry: "registry.db"
        provider: local
        """
        ),
        expect_error="alphanumerical values ",
    )

    _test_config(
        dedent(
            """
        project: _foo
        registry: "registry.db"
        provider: local
        """
        ),
        expect_error="alphanumerical values ",
    )


def test_no_provider():
    _test_config(
        dedent(
            """
        project: foo
        registry: "registry.db"
        online_store:
            path: "blah"
        entity_key_serialization_version: 2
        """
        ),
        expect_error=None,
    )


def test_auth_config():
    _test_config(
        dedent(
            """
        project: foo
        auth:
            client_id: test_client_id
            client_secret: test_client_secret
            username: test_user_name
            password: test_password
            auth_discovery_url: http://localhost:8080/realms/master/.well-known/openid-configuration
        registry: "registry.db"
        provider: local
        online_store:
            path: foo
        entity_key_serialization_version: 2
        """
        ),
        expect_error="missing authentication type",
    )

    _test_config(
        dedent(
            """
        project: foo
        auth:
            type: not_valid_auth_type
            client_id: test_client_id
            client_secret: test_client_secret
            username: test_user_name
            password: test_password
            auth_discovery_url: http://localhost:8080/realms/master/.well-known/openid-configuration
        registry: "registry.db"
        provider: local
        online_store:
            path: foo
        entity_key_serialization_version: 2
        """
        ),
        expect_error="invalid authentication type=not_valid_auth_type",
    )

    oidc_server_repo_config = _test_config(
        dedent(
            """
        project: foo
        auth:
            type: oidc
            client_id: test_client_id
            auth_discovery_url: http://localhost:8080/realms/master/.well-known/openid-configuration
        registry: "registry.db"
        provider: local
        online_store:
            path: foo
        entity_key_serialization_version: 2
        """
        ),
        expect_error=None,
    )
    assert oidc_server_repo_config.auth["type"] == AuthType.OIDC.value
    assert isinstance(oidc_server_repo_config.auth_config, OidcAuthConfig)
    assert oidc_server_repo_config.auth_config.client_id == "test_client_id"
    assert (
        oidc_server_repo_config.auth_config.auth_discovery_url
        == "http://localhost:8080/realms/master/.well-known/openid-configuration"
    )

    oidc_client_repo_config = _test_config(
        dedent(
            """
        project: foo
        auth:
            type: oidc
            client_id: test_client_id
            client_secret: test_client_secret
            username: test_user_name
            password: test_password
            auth_discovery_url: http://localhost:8080/realms/master/.well-known/openid-configuration
        registry: "registry.db"
        provider: local
        online_store:
            path: foo
        entity_key_serialization_version: 2
        """
        ),
        expect_error=None,
    )
    assert oidc_client_repo_config.auth["type"] == AuthType.OIDC.value
    assert isinstance(oidc_client_repo_config.auth_config, OidcClientAuthConfig)
    assert oidc_client_repo_config.auth_config.client_id == "test_client_id"
    assert oidc_client_repo_config.auth_config.client_secret == "test_client_secret"
    assert oidc_client_repo_config.auth_config.username == "test_user_name"
    assert oidc_client_repo_config.auth_config.password == "test_password"
    assert (
        oidc_client_repo_config.auth_config.auth_discovery_url
        == "http://localhost:8080/realms/master/.well-known/openid-configuration"
    )

    no_auth_repo_config = _test_config(
        dedent(
            """
        project: foo
        registry: "registry.db"
        provider: local
        online_store:
            path: foo
        entity_key_serialization_version: 2
        """
        ),
        expect_error=None,
    )
    assert no_auth_repo_config.auth.get("type") == AuthType.NONE.value
    assert isinstance(no_auth_repo_config.auth_config, NoAuthConfig)

    k8_repo_config = _test_config(
        dedent(
            """
        auth:
            type: kubernetes
        project: foo
        registry: "registry.db"
        provider: local
        online_store:
            path: foo
        entity_key_serialization_version: 2
        """
        ),
        expect_error=None,
    )
    assert k8_repo_config.auth.get("type") == AuthType.KUBERNETES.value
    assert isinstance(k8_repo_config.auth_config, KubernetesAuthConfig)


def test_repo_config_init_expedia_provider():
    c = _test_config(
        dedent(
            """
        project: foo
        registry: "registry.db"
        provider: expedia
        entity_key_serialization_version: 2
        """
        ),
        expect_error=None,
    )
    assert c.registry_config == "registry.db"
    assert c.offline_config["type"] == "spark"
    assert c.online_config == "redis"
    assert c.batch_engine_config == "spark.engine"
    assert isinstance(c.online_store, RedisOnlineStoreConfig)
    assert isinstance(c.batch_engine, SparkMaterializationEngineConfig)
    assert isinstance(c.offline_store, SparkOfflineStoreConfig)


def test_repo_config_init_expedia_provider_with_online_store_config():
    c = _test_config(
        dedent(
            """
        project: foo
        registry: "registry.db"
        provider: expedia
        online_store:
            type: redis
            connection_string: localhost:6380
        entity_key_serialization_version: 2
        """
        ),
        expect_error=None,
    )
    assert c.registry_config == "registry.db"
    assert c.offline_config["type"] == "spark"
    assert c.online_config["type"] == "redis"
    assert c.online_config["connection_string"] == "localhost:6380"
    assert c.batch_engine_config == "spark.engine"
    assert isinstance(c.online_store, RedisOnlineStoreConfig)
    assert isinstance(c.batch_engine, SparkMaterializationEngineConfig)
    assert isinstance(c.offline_store, SparkOfflineStoreConfig)
