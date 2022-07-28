import tempfile
from contextlib import contextmanager
from pathlib import Path
from textwrap import dedent
from assertpy import assertpy

from tests.utils.cli_repo_creator import CliRunner
from tests.utils.e2e_test_validation_functions import (
    NULLABLE_ONLINE_STORE_CONFIGS,
    make_feature_store_yaml,
)


@pytest.mark.parametrize("test_nullable_online_store", NULLABLE_ONLINE_STORE_CONFIGS)
def test_nullable_online_store(test_nullable_online_store) -> None:
    project = f"test_nullable_online_store{str(uuid.uuid4()).replace('-', '')[:8]}"
    runner = CliRunner()

    with tempfile.TemporaryDirectory() as repo_dir_name:
        try:
            repo_path = Path(repo_dir_name)
            feature_store_yaml = make_feature_store_yaml(
                project, test_nullable_online_store, repo_path
            )

            repo_config = repo_path / "feature_store.yaml"

            repo_config.write_text(dedent(feature_store_yaml))

            repo_example = repo_path / "example.py"
            repo_example.write_text(get_example_repo("example_feature_repo_1.py"))
            result = runner.run(["apply"], cwd=repo_path)
            assertpy.assert_that(result.returncode).is_equal_to(0)
        finally:
            runner.run(["teardown"], cwd=repo_path)


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
def setup_third_party_registry_store_repo(registry_store: str):
    with tempfile.TemporaryDirectory() as repo_dir_name:

        # Construct an example repo in a temporary dir
        repo_path = Path(repo_dir_name)

        repo_config = repo_path / "feature_store.yaml"

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
        """
            )
        )

        (repo_path / "foo").mkdir()
        repo_example = repo_path / "foo/registry_store.py"
        repo_example.write_text(
            (Path(__file__).parents[2] / "foo_registry_store.py").read_text()
        )

        yield repo_path
