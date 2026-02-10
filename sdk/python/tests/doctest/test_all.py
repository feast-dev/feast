import doctest
import importlib
import pkgutil
import sys
import traceback
import unittest

import feast
from feast.utils import _utc_now

FILES_TO_IGNORE = {"app"}


def setup_feature_store():
    """Prepares the local environment for a FeatureStore docstring test."""
    from datetime import timedelta

    from feast import Entity, FeatureStore, FeatureView, Field, FileSource
    from feast.repo_operations import init_repo
    from feast.types import Float32, Int64

    init_repo("project", "local")
    fs = FeatureStore(repo_path="project/feature_repo")
    driver = Entity(
        name="driver_id",
        description="driver id",
    )
    driver_hourly_stats = FileSource(
        path="data/driver_stats.parquet",
        timestamp_field="event_timestamp",
        created_timestamp_column="created",
    )
    driver_hourly_stats_view = FeatureView(
        name="driver_hourly_stats",
        entities=[driver],
        ttl=timedelta(seconds=86400 * 1),
        schema=[
            Field(name="conv_rate", dtype=Float32),
            Field(name="acc_rate", dtype=Float32),
            Field(name="avg_daily_trips", dtype=Int64),
        ],
        source=driver_hourly_stats,
    )
    fs.apply([driver_hourly_stats_view, driver])
    fs.materialize(
        start_date=_utc_now() - timedelta(hours=3),
        end_date=_utc_now() - timedelta(minutes=10),
    )


def teardown_feature_store():
    """Cleans up the local environment after a FeatureStore docstring test."""
    import shutil

    shutil.rmtree("project", ignore_errors=True)


def test_docstrings():
    """Runs all docstring tests.

    Imports all submodules of the feast package. Checks the submodules for docstring
    tests and runs them. Setup functions for a submodule named "feast.x.y.z" should be
    defined in this module as a function named "setup_x_y_z". Teardown functions can be
    defined similarly. Setup and teardown functions are per-submodule.
    """
    successful = True
    current_packages = [feast]
    failed_cases = []

    while current_packages:
        next_packages = []

        for package in current_packages:
            try:
                for _, name, is_pkg in pkgutil.walk_packages(package.__path__):
                    if name in FILES_TO_IGNORE:
                        continue

                    full_name = package.__name__ + "." + name
                    try:
                        # https://github.com/feast-dev/feast/issues/5088
                        # Skip ray_transformation doctests - they hang on macOS due to
                        # Ray worker spawning issues with uv-managed environments
                        if (
                            "ikv" not in full_name
                            and "milvus" not in full_name
                            and "openlineage" not in full_name
                            and "ray_transformation" not in full_name
                        ):
                            temp_module = importlib.import_module(full_name)
                            if is_pkg:
                                next_packages.append(temp_module)
                    except ModuleNotFoundError:
                        pass

                    # Retrieve the setup and teardown functions defined in this file.
                    relative_path_from_feast = full_name.split(".", 1)[1]
                    function_suffix = relative_path_from_feast.replace(".", "_")
                    setup_function_name = "setup_" + function_suffix
                    teardown_function_name = "teardown_" + function_suffix
                    setup_function = globals().get(setup_function_name)
                    teardown_function = globals().get(teardown_function_name)

                    # Execute the test with setup and teardown functions.
                    try:
                        if setup_function:
                            setup_function()

                        test_suite = doctest.DocTestSuite(
                            temp_module,
                            optionflags=doctest.ELLIPSIS,
                        )
                        if test_suite.countTestCases() > 0:
                            result = unittest.TextTestRunner(sys.stdout).run(test_suite)
                            if not result.wasSuccessful():
                                successful = False
                                failed_cases.append(result.failures)
                    except Exception as e:
                        successful = False
                        failed_cases.append(
                            (full_name, str(e) + traceback.format_exc())
                        )
                    finally:
                        if teardown_function:
                            teardown_function()
            except DeprecationWarning:  # To catch ray.tune.automl deprecation
                pass

        current_packages = next_packages

    if not successful:
        raise Exception(f"Docstring tests failed. Failed results: {failed_cases}")
