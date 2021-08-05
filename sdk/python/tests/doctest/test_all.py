import doctest
import importlib
import pkgutil
import sys
import unittest

import feast


def setup_feature_store(docstring_tests):
    """Prepares the local environment for a FeatureStore docstring test."""
    from feast.repo_operations import init_repo

    init_repo("feature_repo", "local")


def teardown_feature_store(docstring_tests):
    """Cleans up the local environment after a FeatureStore docstring test."""
    import shutil

    shutil.rmtree("feature_repo", ignore_errors=True)
    shutil.rmtree("data", ignore_errors=True)


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
                    relative_path_from_feast = full_name.split(".", 1)[1]
                    function_suffix = relative_path_from_feast.replace(".", "_")
                    setup_function_name = "setup_" + function_suffix
                    teardown_function_name = "teardown_" + function_suffix
                    setup_function = globals().get(setup_function_name)
                    teardown_function = globals().get(teardown_function_name)

                    test_suite = doctest.DocTestSuite(
                        temp_module,
                        setUp=setup_function,
                        tearDown=teardown_function,
                        optionflags=doctest.ELLIPSIS,
                    )
                    if test_suite.countTestCases() > 0:
                        result = unittest.TextTestRunner(sys.stdout).run(test_suite)
                        if not result.wasSuccessful():
                            successful = False

                    if is_pkg:
                        next_packages.append(temp_module)
                except ModuleNotFoundError:
                    pass

        current_packages = next_packages

    if not successful:
        raise Exception("Docstring tests failed.")
