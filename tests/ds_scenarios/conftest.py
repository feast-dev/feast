import os


def pytest_addoption(parser):
    parser.addoption(
        "--core_url",
        action="store",
        default=os.getenv("FEAST_CORE_URL", "localhost:6565"),
    )
    parser.addoption(
        "--serving_url",
        action="store",
        default=os.getenv("FEAST_ONLINE_SERVING_URL", "localhost:6566"),
    )
    parser.addoption("--allow_dirty", action="store", default="False")
    parser.addoption("--project_name", action="store")
    parser.addoption("--initial_entity_id", action="store", default="1")
