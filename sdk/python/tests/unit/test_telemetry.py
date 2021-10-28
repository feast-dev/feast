from unittest.mock import patch

import pytest

from feast.usage import log_exceptions_and_usage, set_usage_attribute


@pytest.fixture(scope="function")
def dummy_exporter():
    event_log = []

    with patch("feast.usage._export", new=event_log.append):
        yield event_log


@pytest.fixture(autouse=True)
def usage_is_enabled():
    with patch("feast.usage._is_enabled", return_value=True):
        yield


def test_global_context_building(dummy_exporter):
    @log_exceptions_and_usage(event="test-event")
    def root_fn(provider):
        if provider == "one":
            provider_one()
        if provider == "two":
            provider_two()

    @log_exceptions_and_usage(provider="provider-one")
    def provider_one():
        dummy_layer()

    @log_exceptions_and_usage(provider="provider-two")
    def provider_two():
        set_usage_attribute("new-attr", "new-val")

    @log_exceptions_and_usage
    def dummy_layer():
        redis_store()

    @log_exceptions_and_usage(store="redis")
    def redis_store():
        set_usage_attribute("attr", "val")

    root_fn(provider="one")
    root_fn(provider="two")

    module_name = "test_telemetry.test_global_context_building.<locals>"

    assert dummy_exporter
    assert {
        "event": "test-event",
        "provider": "provider-one",
        "store": "redis",
        "attr": "val",
        "entrypoint": f"{module_name}.root_fn",
    }.items() <= dummy_exporter[0].items()
    assert dummy_exporter[0]["calls"][0]["fn_name"] == f"{module_name}.root_fn"
    assert dummy_exporter[0]["calls"][1]["fn_name"] == f"{module_name}.provider_one"
    assert dummy_exporter[0]["calls"][2]["fn_name"] == f"{module_name}.dummy_layer"
    assert dummy_exporter[0]["calls"][3]["fn_name"] == f"{module_name}.redis_store"

    assert (
        not {"store", "attr"} & dummy_exporter[1].keys()
    )  # check that context was reset
    assert {
        "event": "test-event",
        "provider": "provider-two",
        "new-attr": "new-val",
    }.items() <= dummy_exporter[1].items()


def test_exception_logging(dummy_exporter):
    @log_exceptions_and_usage(event="test-event")
    def root_fn():
        provider()

    @log_exceptions_and_usage(provider="provider-one")
    def provider():
        raise ValueError(1)

    with pytest.raises(ValueError):
        root_fn()

    assert dummy_exporter
    assert {
        "event": "test-event",
        "provider": "provider-one",
        "exception": repr(ValueError(1)),
        "entrypoint": "test_telemetry.test_exception_logging.<locals>.root_fn",
    }.items() <= dummy_exporter[0].items()
