def pytest_addoption(parser):
    parser.addoption("--core_url", action="store", default="localhost:6565")
    parser.addoption("--serving_url", action="store", default="localhost:6565")
    parser.addoption("--allow_dirty", action="store", default="false")
