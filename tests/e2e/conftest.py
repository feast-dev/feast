def pytest_addoption(parser):
    parser.addoption("--core_url", action="store", default="localhost:6565")
    parser.addoption("--serving_url", action="store", default="localhost:6566")
    parser.addoption("--allow_dirty", action="store", default="False")
    parser.addoption("--gcs_path", action="store", default="gs://feast-templocation-kf-feast/")
    # If prometheus_server_url is not empty, then the e2e test will validate the
    # prometheus metrics written by Feast. Example value: http://localhost:9090
    parser.addoption("--prometheus_server_url", action="store", default="")
