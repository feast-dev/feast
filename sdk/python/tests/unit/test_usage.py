import datetime
import json
import time
from unittest.mock import patch

import pytest

from feast.usage import (
    RatioSampler,
    log_exceptions,
    log_exceptions_and_usage,
    set_usage_attribute,
    tracing_span,
)


@pytest.fixture(scope="function")
def dummy_exporter():
    event_log = []

    with patch(
        "feast.usage._export",
        new=lambda e: event_log.append(json.loads(json.dumps(e))),
    ):
        yield event_log


@pytest.fixture(scope="function", autouse=True)
def enabling_patch():
    with patch("feast.usage._is_enabled") as p:
        p.__bool__.return_value = True
        yield p


def test_logging_disabled(dummy_exporter, enabling_patch):
    enabling_patch.__bool__.return_value = False

    @log_exceptions_and_usage(event="test-event")
    def entrypoint():
        pass

    @log_exceptions(event="test-event")
    def entrypoint2():
        raise ValueError(1)

    entrypoint()
    with pytest.raises(ValueError):
        entrypoint2()

    assert not dummy_exporter


def test_global_context_building(dummy_exporter):
    @log_exceptions_and_usage(event="test-event")
    def entrypoint(provider):
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

    entrypoint(provider="one")
    entrypoint(provider="two")

    scope_name = "test_usage.test_global_context_building.<locals>"

    assert dummy_exporter
    assert {
        "event": "test-event",
        "provider": "provider-one",
        "store": "redis",
        "attr": "val",
        "entrypoint": f"{scope_name}.entrypoint",
    }.items() <= dummy_exporter[0].items()
    assert dummy_exporter[0]["calls"][0]["fn_name"] == f"{scope_name}.entrypoint"
    assert dummy_exporter[0]["calls"][1]["fn_name"] == f"{scope_name}.provider_one"
    assert dummy_exporter[0]["calls"][2]["fn_name"] == f"{scope_name}.dummy_layer"
    assert dummy_exporter[0]["calls"][3]["fn_name"] == f"{scope_name}.redis_store"

    assert (
        not {"store", "attr"} & dummy_exporter[1].keys()
    )  # check that context was reset
    assert {
        "event": "test-event",
        "provider": "provider-two",
        "new-attr": "new-val",
    }.items() <= dummy_exporter[1].items()


def test_exception_recording(dummy_exporter):
    @log_exceptions_and_usage(event="test-event")
    def entrypoint():
        provider()

    @log_exceptions_and_usage(provider="provider-one")
    def provider():
        raise ValueError(1)

    with pytest.raises(ValueError):
        entrypoint()

    assert dummy_exporter
    assert {
        "event": "test-event",
        "provider": "provider-one",
        "exception": repr(ValueError(1)),
        "entrypoint": "test_usage.test_exception_recording.<locals>.entrypoint",
    }.items() <= dummy_exporter[0].items()


def test_only_exception_logging(dummy_exporter):
    @log_exceptions(scope="exception-only")
    def failing_fn():
        raise ValueError(1)

    @log_exceptions_and_usage(scope="usage-and-exception")
    def entrypoint():
        failing_fn()

    with pytest.raises(ValueError):
        failing_fn()

    assert {
        "exception": repr(ValueError(1)),
        "scope": "exception-only",
        "entrypoint": "test_usage.test_only_exception_logging.<locals>.failing_fn",
    }.items() <= dummy_exporter[0].items()

    with pytest.raises(ValueError):
        entrypoint()

    assert {
        "exception": repr(ValueError(1)),
        "scope": "usage-and-exception",
        "entrypoint": "test_usage.test_only_exception_logging.<locals>.entrypoint",
    }.items() <= dummy_exporter[1].items()


def test_ratio_based_sampling(dummy_exporter):
    @log_exceptions_and_usage()
    def entrypoint():
        expensive_fn()

    @log_exceptions_and_usage(sampler=RatioSampler(ratio=0.1))
    def expensive_fn():
        pass

    for _ in range(100):
        entrypoint()

    assert len(dummy_exporter) == 10


def test_sampling_priority(dummy_exporter):
    @log_exceptions_and_usage(sampler=RatioSampler(ratio=0.3))
    def entrypoint():
        expensive_fn()

    @log_exceptions_and_usage(sampler=RatioSampler(ratio=0.01))
    def expensive_fn():
        other_fn()

    @log_exceptions_and_usage(sampler=RatioSampler(ratio=0.1))
    def other_fn():
        pass

    for _ in range(300):
        entrypoint()

    assert len(dummy_exporter) == 3


def test_time_recording(dummy_exporter):
    @log_exceptions_and_usage()
    def entrypoint():
        time.sleep(0.1)
        expensive_fn()

    @log_exceptions_and_usage()
    def expensive_fn():
        time.sleep(0.5)
        other_fn()

    @log_exceptions_and_usage()
    def other_fn():
        time.sleep(0.2)

    entrypoint()

    assert dummy_exporter
    calls = dummy_exporter[0]["calls"]
    assert call_length_ms(calls[0]) >= 800
    assert call_length_ms(calls[0]) > call_length_ms(calls[1]) >= 700
    assert call_length_ms(calls[1]) > call_length_ms(calls[2]) >= 200


def test_profiling_decorator(dummy_exporter):
    @log_exceptions_and_usage()
    def entrypoint():
        with tracing_span("custom_span"):
            time.sleep(0.1)

    entrypoint()

    assert dummy_exporter

    calls = dummy_exporter[0]["calls"]
    assert len(calls)
    assert call_length_ms(calls[0]) >= 100
    assert call_length_ms(calls[1]) >= 100

    assert (
        calls[1]["fn_name"]
        == "test_usage.test_profiling_decorator.<locals>.entrypoint.custom_span"
    )


def call_length_ms(call):
    return (
        datetime.datetime.fromisoformat(call["end"])
        - datetime.datetime.fromisoformat(call["start"])
    ).total_seconds() * 10 ** 3
