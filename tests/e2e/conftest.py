def pytest_addoption(parser):
    parser.addoption("--core_url", action="store", default="localhost:6565")
    parser.addoption("--serving_url", action="store", default="localhost:6566")
    parser.addoption("--jobcontroller_url", action="store", default="localhost:6570")
    parser.addoption("--allow_dirty", action="store", default="False")
    parser.addoption(
        "--gcs_path", action="store", default="gs://feast-templocation-kf-feast/"
    )
    parser.addoption("--enable_auth", action="store", default="False")
    parser.addoption("--kafka_brokers", action="store", default="localhost:9092")
