import tempfile
from pathlib import Path
from textwrap import dedent
from typing import Optional

from feast.infra.online_stores.sqlite import SqliteOnlineStoreConfig
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
            rc = load_repo_config(repo_path)
        except FeastConfigError as e:
            error = e

        if expect_error is not None:
            assert expect_error in str(error)
        else:
            print(f"error: {error}")
            assert error is None

        return rc


def test_nullable_online_store_aws():
    _test_config(
        dedent(
            """
        project: foo
        registry: "registry.db"
        provider: aws
        online_store: null
        """
        ),
        expect_error="__root__ -> offline_store -> cluster_id\n"
        "  field required (type=value_error.missing)",
    )


def test_nullable_online_store_gcp():
    _test_config(
        dedent(
            """
        project: foo
        registry: "registry.db"
        provider: gcp
        online_store: null
        """
        ),
        expect_error=None,
    )


def test_nullable_online_store_local():
    _test_config(
        dedent(
            """
        project: foo
        registry: "registry.db"
        provider: local
        online_store: null
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
        """
        ),
        expect_error=None,
    )
    assert isinstance(c.online_store, SqliteOnlineStoreConfig)


def test_gcp_config():
    _test_config(
        dedent(
            """
        project: foo
        registry: gs://registry.db
        provider: gcp
        """
        ),
        expect_error=None,
    )


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
        expect_error="__root__ -> online_store -> that_field_should_not_be_here\n"
        "  extra fields not permitted (type=value_error.extra)",
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
        expect_error="__root__ -> online_store -> path\n  str type expected",
    )


def test_no_project():
    _test_config(
        dedent(
            """
        registry: "registry.db"
        provider: local
        online_store:
            path: foo
        """
        ),
        expect_error="1 validation error for RepoConfig\n"
        "project\n"
        "  field required (type=value_error.missing)",
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
