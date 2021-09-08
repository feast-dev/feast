import doctest
import importlib
import pkgutil
import sys
import unittest

import feast


def setup_feature_store():
    """Prepares the local environment for a FeatureStore docstring test."""
    from datetime import datetime, timedelta

    from feast import Entity, Feature, FeatureStore, FeatureView, FileSource, ValueType
    from feast.repo_operations import init_repo

    init_repo("feature_repo", "local")
    fs = FeatureStore(repo_path="feature_repo")
    driver = Entity(
        name="driver_id", value_type=ValueType.INT64, description="driver id",
    )
    driver_hourly_stats = FileSource(
        path="feature_repo/data/driver_stats.parquet",
        event_timestamp_column="event_timestamp",
        created_timestamp_column="created",
    )
    driver_hourly_stats_view = FeatureView(
        name="driver_hourly_stats",
        entities=["driver_id"],
        ttl=timedelta(seconds=86400 * 1),
        features=[
            Feature(name="conv_rate", dtype=ValueType.FLOAT),
            Feature(name="acc_rate", dtype=ValueType.FLOAT),
            Feature(name="avg_daily_trips", dtype=ValueType.INT64),
        ],
        batch_source=driver_hourly_stats,
    )
    fs.apply([driver_hourly_stats_view, driver])
    fs.materialize(
        start_date=datetime.utcnow() - timedelta(hours=3),
        end_date=datetime.utcnow() - timedelta(minutes=10),
    )


def teardown_feature_store():
    """Cleans up the local environment after a FeatureStore docstring test."""
    import shutil

    shutil.rmtree("feature_repo", ignore_errors=True)


def test_docstrings():
    """Runs all docstring tests.

    Imports all submodules of the feast package. Checks the submodules for docstring
    tests and runs them. Setup functions for a submodule named "feast.x.y.z" should be
    defined in this module as a function named "setup_x_y_z". Teardown functions can be
    defined similarly. Setup and teardown functions are per-submodule.
    """
    successful = True
    current_packages = [feast]

    while current_packages:
        next_packages = []

        for package in current_packages:
            for _, name, is_pkg in pkgutil.walk_packages(package.__path__):
                full_name = package.__name__ + "." + name

                try:
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
                        temp_module, optionflags=doctest.ELLIPSIS,
                    )
                    if test_suite.countTestCases() > 0:
                        result = unittest.TextTestRunner(sys.stdout).run(test_suite)
                        if not result.wasSuccessful():
                            successful = False
                except Exception:
                    successful = False
                finally:
                    if teardown_function:
                        teardown_function()

        current_packages = next_packages

    if not successful:
        raise Exception("Docstring tests failed.")
