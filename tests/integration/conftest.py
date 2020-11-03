def pytest_addoption(parser):
    parser.addoption("--dataproc-cluster-name", action="store")
    parser.addoption("--dataproc-region", action="store")
    parser.addoption("--dataproc-project", action="store")
    parser.addoption("--dataproc-staging-location", action="store")
    parser.addoption("--redis-url", action="store")
    parser.addoption("--redis-cluster", action="store_true")
