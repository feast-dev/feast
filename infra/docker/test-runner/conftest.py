def pytest_addoption(parser):
    parser.addoption("--core_url", action="store", default="core:6565")
    parser.addoption("--serving_url", action="store", default="serving:6566")
    parser.addoption("--allow_dirty", action="store", default="False")
