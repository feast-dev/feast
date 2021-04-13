import tempfile
from pathlib import Path
from textwrap import dedent
from typing import Optional

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
        try:
            load_repo_config(repo_path)
        except FeastConfigError as e:
            error = e

        if expect_error is not None:
            assert expect_error in str(error)
        else:
            assert error is None


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
