import os
import tempfile
from contextlib import contextmanager
from pathlib import Path
from textwrap import dedent
from unittest import mock

from assertpy import assertpy

from tests.utils.cli_repo_creator import CliRunner


def test_3rd_party_providers() -> None:
    """
    Test running apply on third party providers
    """
    runner = CliRunner()
    # Check with incorrect built-in provider name (no dots)
    with setup_third_party_provider_repo("feast123") as repo_path:
        return_code, output = runner.run_with_output(["apply"], cwd=repo_path)
        assertpy.assert_that(return_code).is_equal_to(1)
        assertpy.assert_that(output).contains(b"Provider 'feast123' is not implemented")
    # Check with incorrect third-party provider name (with dots)
    with setup_third_party_provider_repo("feast_foo.Provider") as repo_path:
        return_code, output = runner.run_with_output(["apply"], cwd=repo_path)
        assertpy.assert_that(return_code).is_equal_to(1)
        assertpy.assert_that(output).contains(
            b"Could not import module 'feast_foo' while attempting to load class 'Provider'"
        )
    # Check with incorrect third-party provider name (with dots)
    with setup_third_party_provider_repo("foo.FooProvider") as repo_path:
        return_code, output = runner.run_with_output(["apply"], cwd=repo_path)
        assertpy.assert_that(return_code).is_equal_to(1)
        assertpy.assert_that(output).contains(
            b"Could not import class 'FooProvider' from module 'foo'"
        )
    # Check with correct third-party provider name
    with setup_third_party_provider_repo("foo.provider.FooProvider") as repo_path:
        return_code, output = runner.run_with_output(["apply"], cwd=repo_path)
        assertpy.assert_that(return_code).is_equal_to(0)


def test_3rd_party_registry_store() -> None:
    """
    Test running apply on third party registry stores
    """
    runner = CliRunner()
    # Check with incorrect built-in provider name (no dots)
    with setup_third_party_registry_store_repo("feast123") as repo_path:
        return_code, output = runner.run_with_output(["apply"], cwd=repo_path)
        assertpy.assert_that(return_code).is_equal_to(1)
        assertpy.assert_that(output).contains(
            b'Registry store class name should end with "RegistryStore"'
        )
    # Check with incorrect third-party registry store name (with dots)
    with setup_third_party_registry_store_repo("feast_foo.RegistryStore") as repo_path:
        return_code, output = runner.run_with_output(["apply"], cwd=repo_path)
        assertpy.assert_that(return_code).is_equal_to(1)
        assertpy.assert_that(output).contains(
            b"Could not import module 'feast_foo' while attempting to load class 'RegistryStore'"
        )
    # Check with incorrect third-party registry store name (with dots)
    with setup_third_party_registry_store_repo("foo.FooRegistryStore") as repo_path:
        return_code, output = runner.run_with_output(["apply"], cwd=repo_path)
        assertpy.assert_that(return_code).is_equal_to(1)
        assertpy.assert_that(output).contains(
            b"Could not import class 'FooRegistryStore' from module 'foo'"
        )
    # Check with correct third-party registry store name
    with setup_third_party_registry_store_repo(
        "foo.registry_store.FooRegistryStore"
    ) as repo_path:
        return_code, output = runner.run_with_output(["apply"], cwd=repo_path)
        assertpy.assert_that(return_code).is_equal_to(0)


def test_3rd_party_registry_store_with_fs_yaml_override() -> None:
    runner = CliRunner()

    fs_yaml_file = "test_fs.yaml"
    with setup_third_party_registry_store_repo(
        "foo.registry_store.FooRegistryStore", fs_yaml_file_name=fs_yaml_file
    ) as repo_path:
        return_code, output = runner.run_with_output(
            ["--feature-store-yaml", fs_yaml_file, "apply"], cwd=repo_path
        )
        assertpy.assert_that(return_code).is_equal_to(0)


def test_3rd_party_registry_store_with_fs_yaml_override_by_env_var() -> None:
    runner = CliRunner()

    fs_yaml_file = "test_fs.yaml"
    with setup_third_party_registry_store_repo(
        "foo.registry_store.FooRegistryStore", fs_yaml_file_name=fs_yaml_file
    ) as repo_path:
        custom_yaml_path = os.path.join(repo_path, fs_yaml_file)
        with mock.patch.dict(
            "os.environ", {"FEAST_FS_YAML_FILE_PATH": custom_yaml_path}, clear=True
        ):
            return_code, output = runner.run_with_output(["apply"], cwd=repo_path)
            assertpy.assert_that(return_code).is_equal_to(0)


@contextmanager
def setup_third_party_provider_repo(provider_name: str):
    with tempfile.TemporaryDirectory() as repo_dir_name:
        # Construct an example repo in a temporary dir
        repo_path = Path(repo_dir_name)

        repo_config = repo_path / "feature_store.yaml"

        repo_config.write_text(
            dedent(
                f"""
        project: foo
        registry: data/registry.db
        provider: {provider_name}
        online_store:
            path: data/online_store.db
            type: sqlite
        offline_store:
            type: file
        entity_key_serialization_version: 2
        """
            )
        )

        (repo_path / "foo").mkdir()
        repo_example = repo_path / "foo/provider.py"
        repo_example.write_text(
            (Path(__file__).parents[2] / "foo_provider.py").read_text()
        )

        yield repo_path


@contextmanager
def setup_third_party_registry_store_repo(
    registry_store: str, fs_yaml_file_name: str = "feature_store.yaml"
):
    with tempfile.TemporaryDirectory() as repo_dir_name:
        # Construct an example repo in a temporary dir
        repo_path = Path(repo_dir_name)

        repo_config = repo_path / fs_yaml_file_name

        repo_config.write_text(
            dedent(
                f"""
        project: foo
        registry:
            registry_store_type: {registry_store}
            path: foobar://foo.bar
        provider: local
        online_store:
            path: data/online_store.db
            type: sqlite
        offline_store:
            type: file
        entity_key_serialization_version: 2
        """
            )
        )

        (repo_path / "foo").mkdir()
        repo_example = repo_path / "foo/registry_store.py"
        repo_example.write_text(
            (Path(__file__).parents[2] / "foo_registry_store.py").read_text()
        )

        yield repo_path


def test_cli_configuration():
    """
    Unit test for the 'feast configuration' command
    """
    runner = CliRunner()

    with setup_third_party_provider_repo("local") as repo_path:
        # Run the 'feast configuration' command
        return_code, output = runner.run_with_output(["configuration"], cwd=repo_path)

        # Assertions
        assertpy.assert_that(return_code).is_equal_to(0)
        assertpy.assert_that(output).contains(b"project: foo")
        assertpy.assert_that(output).contains(b"provider: local")
        assertpy.assert_that(output).contains(b"type: sqlite")
        assertpy.assert_that(output).contains(b"path: data/online_store.db")
        assertpy.assert_that(output).contains(b"type: file")
        assertpy.assert_that(output).contains(b"entity_key_serialization_version: 2")
