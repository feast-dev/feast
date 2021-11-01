import datetime
import json
import time
from unittest.mock import patch

import pytest

from feast.telemetry import (
    RatioSampler,
    enable_telemetry,
    log_exceptions,
    set_usage_attribute,
    tracing_span,
)


@pytest.fixture(scope="function")
def dummy_exporter():
    event_log = []

    with patch(
        "feast.telemetry._export",
        new=lambda e: event_log.append(json.loads(json.dumps(e))),
    ):
        yield event_log


@pytest.fixture(scope="function", autouse=True)
def enabling_patch():
    with patch("feast.telemetry._is_enabled", return_value=True) as p:
        yield p


def test_logging_disabled(dummy_exporter, enabling_patch):
    enabling_patch.return_value = False

    @enable_telemetry(event="test-event")
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
    @enable_telemetry(event="test-event")
    def entrypoint(provider):
        if provider == "one":
            provider_one()
        if provider == "two":
            provider_two()

    @enable_telemetry(provider="provider-one")
    def provider_one():
        dummy_layer()

    @enable_telemetry(provider="provider-two")
    def provider_two():
        set_usage_attribute("new-attr", "new-val")

    @enable_telemetry
    def dummy_layer():
        redis_store()

    @enable_telemetry(store="redis")
    def redis_store():
        set_usage_attribute("attr", "val")

    entrypoint(provider="one")
    entrypoint(provider="two")

    scope_name = "test_telemetry.test_global_context_building.<locals>"

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
    @enable_telemetry(event="test-event")
    def entrypoint():
        provider()

    @enable_telemetry(provider="provider-one")
    def provider():
        raise ValueError(1)

    with pytest.raises(ValueError):
        entrypoint()

    assert dummy_exporter
    assert {
        "event": "test-event",
        "provider": "provider-one",
        "exception": repr(ValueError(1)),
        "entrypoint": "test_telemetry.test_exception_recording.<locals>.entrypoint",
    }.items() <= dummy_exporter[0].items()


def test_only_exception_logging(dummy_exporter):
    @log_exceptions(scope="exception-only")
    def failing_fn():
        raise ValueError(1)

    @enable_telemetry(scope="usage-and-exception")
    def entrypoint():
        failing_fn()

    with pytest.raises(ValueError):
        failing_fn()

    assert {
        "exception": repr(ValueError(1)),
        "scope": "exception-only",
        "entrypoint": "test_telemetry.test_only_exception_logging.<locals>.failing_fn",
    }.items() <= dummy_exporter[0].items()

    with pytest.raises(ValueError):
        entrypoint()

    assert {
        "exception": repr(ValueError(1)),
        "scope": "usage-and-exception",
        "entrypoint": "test_telemetry.test_only_exception_logging.<locals>.entrypoint",
    }.items() <= dummy_exporter[1].items()


def test_ratio_based_sampling(dummy_exporter):
    @enable_telemetry()
    def entrypoint():
        expensive_fn()

    @enable_telemetry(sampler=RatioSampler(ratio=0.1))
    def expensive_fn():
        pass

    for _ in range(100):
        entrypoint()

    assert len(dummy_exporter) == 10


def test_sampling_priority(dummy_exporter):
    @enable_telemetry(sampler=RatioSampler(ratio=0.3))
    def entrypoint():
        expensive_fn()

    @enable_telemetry(sampler=RatioSampler(ratio=0.01))
    def expensive_fn():
        other_fn()

    @enable_telemetry(sampler=RatioSampler(ratio=0.1))
    def other_fn():
        pass

    for _ in range(300):
        entrypoint()

    assert len(dummy_exporter) == 3


def test_time_recording(dummy_exporter):
    @enable_telemetry()
    def entrypoint():
        time.sleep(0.1)
        expensive_fn()

    @enable_telemetry()
    def expensive_fn():
        time.sleep(0.5)
        other_fn()

    @enable_telemetry()
    def other_fn():
        time.sleep(0.2)

    entrypoint()

    assert dummy_exporter
    calls = dummy_exporter[0]["calls"]
    assert 900 > call_length_ms(calls[0]) >= 800
    assert call_length_ms(calls[0]) > call_length_ms(calls[1]) >= 700
    assert call_length_ms(calls[1]) > call_length_ms(calls[2]) >= 200


def test_profiling_decorator(dummy_exporter):
    @enable_telemetry()
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
        == "test_telemetry.test_profiling_decorator.<locals>.entrypoint.custom_span"
    )


def call_length_ms(call):
    return (
        datetime.datetime.fromisoformat(call["end"])
        - datetime.datetime.fromisoformat(call["start"])
    ).total_seconds() * 10 ** 3
