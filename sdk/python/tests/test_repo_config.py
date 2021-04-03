import tempfile
from pathlib import Path
from textwrap import dedent
from typing import Optional

from feast.repo_config import FeastConfigError, load_repo_config


class TestRepoConfig:
    def _test_config(self, config_text, expect_error: Optional[str]):
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

    def test_basic(self) -> None:
        self._test_config(
            dedent(
                """
            project: foo
            registry: "registry.db"
            provider: local
            online_store:
                local:
                    path: "online_store.db"
            """
            ),
            expect_error=None,
        )

        self._test_config(
            dedent(
                """
            project: foo
            registry: "registry.db"
            provider: gcp
            """
            ),
            expect_error=None,
        )

    def test_errors(self) -> None:
        self._test_config(
            dedent(
                """
            project: foo
            registry: "registry.db"
            provider: local
            online_store:
                local:
                    that_field_should_not_be_here: yes
                    path: "online_store.db"
            """
            ),
            expect_error="online_store -> local -> that_field_should_not_be_here\n"
            "  extra fields not permitted (type=value_error.extra)",
        )

        self._test_config(
            dedent(
                """
            project: foo
            registry: "registry.db"
            provider: local
            online_store:
                local:
                    path: 100500
            """
            ),
            expect_error="1 validation error for RepoConfig\n"
            "online_store -> local -> path\n"
            "  str type expected (type=type_error.str)",
        )

        self._test_config(
            dedent(
                """
            registry: "registry.db"
            provider: local
            online_store:
                local:
                    path: foo
            """
            ),
            expect_error="1 validation error for RepoConfig\n"
            "project\n"
            "  field required (type=value_error.missing)",
        )
