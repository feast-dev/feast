from pathlib import Path

import pytest


@pytest.fixture(scope="session")
def project_root():
    return Path(__file__).parent.parent.parent.parent


@pytest.fixture(scope="session")
def project_version(pytestconfig):
    if pytestconfig.getoption("feast_version"):
        return pytestconfig.getoption("feast_version")
    else:
        raise Exception("feast_version not set")
