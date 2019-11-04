def pytest_addoption(parser):
    parser.addoption("--core_url", action="store", default="localhost:6565")
    parser.addoption("--online_serving_url", action="store", default="localhost:6566")
    parser.addoption("--batch_serving_url", action="store", default="localhost:6567")
    parser.addoption("--allow_dirty", action="store", default="false")
