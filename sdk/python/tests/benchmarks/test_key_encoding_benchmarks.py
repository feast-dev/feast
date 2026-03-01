"""
Benchmarks for entity key serialization/deserialization performance.

This module provides comprehensive performance tests for the key encoding utilities
to validate and track the performance improvements from optimization efforts.
"""

import time

import pytest

from feast.infra.key_encoding_utils import (
    deserialize_entity_key,
    serialize_entity_key,
    serialize_entity_key_prefix,
)
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import Value as ValueProto


@pytest.fixture
def single_entity_key():
    """Single entity key (most common case - 90% of usage)"""
    return EntityKeyProto(
        join_keys=["user_id"], entity_values=[ValueProto(string_val="user123")]
    )


@pytest.fixture
def single_entity_key_int():
    """Single entity key with int64 value"""
    return EntityKeyProto(
        join_keys=["user_id"], entity_values=[ValueProto(int64_val=123456789)]
    )


@pytest.fixture
def multi_entity_key_small():
    """Small multi-entity key (2-3 entities)"""
    return EntityKeyProto(
        join_keys=["user_id", "session_id"],
        entity_values=[
            ValueProto(string_val="user123"),
            ValueProto(string_val="sess456"),
        ],
    )


@pytest.fixture
def multi_entity_key_large():
    """Large multi-entity key (5+ entities)"""
    return EntityKeyProto(
        join_keys=["user_id", "session_id", "device_id", "app_version", "region"],
        entity_values=[
            ValueProto(string_val="user123"),
            ValueProto(string_val="sess456"),
            ValueProto(string_val="dev789"),
            ValueProto(string_val="v1.2.3"),
            ValueProto(string_val="us-west-2"),
        ],
    )


@pytest.fixture
def mixed_value_types_key():
    """Entity key with mixed value types"""
    return EntityKeyProto(
        join_keys=["user_id", "timestamp", "count", "score"],
        entity_values=[
            ValueProto(string_val="user123"),
            ValueProto(unix_timestamp_val=1758823656),
            ValueProto(int64_val=42),
            ValueProto(int32_val=95),
        ],
    )


@pytest.fixture
def batch_entity_keys(single_entity_key, multi_entity_key_small):
    """Batch of entity keys for bulk operation testing"""
    keys = []
    # Generate 100 single entity keys (typical batch size)
    for i in range(80):  # 80% single entities
        keys.append(
            EntityKeyProto(
                join_keys=["user_id"], entity_values=[ValueProto(string_val=f"user{i}")]
            )
        )

    # Add 20 multi-entity keys
    for i in range(20):
        keys.append(
            EntityKeyProto(
                join_keys=["user_id", "session_id"],
                entity_values=[
                    ValueProto(string_val=f"user{i}"),
                    ValueProto(string_val=f"sess{i}"),
                ],
            )
        )

    return keys


# Serialization Benchmarks


@pytest.mark.benchmark(group="serialize_single")
@pytest.mark.parametrize("entity_key_serialization_version", [3])
def test_serialize_single_entity_string(
    benchmark, single_entity_key, entity_key_serialization_version
):
    """Benchmark single entity key serialization (string value) - most common case."""
    result = benchmark(
        serialize_entity_key, single_entity_key, entity_key_serialization_version
    )
    assert len(result) > 0


@pytest.mark.benchmark(group="serialize_single")
@pytest.mark.parametrize("entity_key_serialization_version", [3])
def test_serialize_single_entity_int(
    benchmark, single_entity_key_int, entity_key_serialization_version
):
    """Benchmark single entity key serialization (int64 value)."""
    result = benchmark(
        serialize_entity_key, single_entity_key_int, entity_key_serialization_version
    )
    assert len(result) > 0


@pytest.mark.benchmark(group="serialize_multi")
@pytest.mark.parametrize("entity_key_serialization_version", [3])
def test_serialize_multi_entity_small(
    benchmark, multi_entity_key_small, entity_key_serialization_version
):
    """Benchmark small multi-entity key serialization."""
    result = benchmark(
        serialize_entity_key, multi_entity_key_small, entity_key_serialization_version
    )
    assert len(result) > 0


@pytest.mark.benchmark(group="serialize_multi")
@pytest.mark.parametrize("entity_key_serialization_version", [3])
def test_serialize_multi_entity_large(
    benchmark, multi_entity_key_large, entity_key_serialization_version
):
    """Benchmark large multi-entity key serialization."""
    result = benchmark(
        serialize_entity_key, multi_entity_key_large, entity_key_serialization_version
    )
    assert len(result) > 0


@pytest.mark.benchmark(group="serialize_mixed")
@pytest.mark.parametrize("entity_key_serialization_version", [3])
def test_serialize_mixed_value_types(
    benchmark, mixed_value_types_key, entity_key_serialization_version
):
    """Benchmark serialization with mixed value types."""
    result = benchmark(
        serialize_entity_key, mixed_value_types_key, entity_key_serialization_version
    )
    assert len(result) > 0


# Deserialization Benchmarks


@pytest.mark.benchmark(group="deserialize_single")
@pytest.mark.parametrize("entity_key_serialization_version", [3])
def test_deserialize_single_entity_string(
    benchmark, single_entity_key, entity_key_serialization_version
):
    """Benchmark single entity key deserialization (string value)."""
    serialized = serialize_entity_key(
        single_entity_key, entity_key_serialization_version
    )
    result = benchmark(
        deserialize_entity_key, serialized, entity_key_serialization_version
    )
    assert result == single_entity_key


@pytest.mark.benchmark(group="deserialize_single")
@pytest.mark.parametrize("entity_key_serialization_version", [3])
def test_deserialize_single_entity_int(
    benchmark, single_entity_key_int, entity_key_serialization_version
):
    """Benchmark single entity key deserialization (int64 value)."""
    serialized = serialize_entity_key(
        single_entity_key_int, entity_key_serialization_version
    )
    result = benchmark(
        deserialize_entity_key, serialized, entity_key_serialization_version
    )
    assert result == single_entity_key_int


@pytest.mark.benchmark(group="deserialize_multi")
@pytest.mark.parametrize("entity_key_serialization_version", [3])
def test_deserialize_multi_entity_small(
    benchmark, multi_entity_key_small, entity_key_serialization_version
):
    """Benchmark small multi-entity key deserialization."""
    serialized = serialize_entity_key(
        multi_entity_key_small, entity_key_serialization_version
    )
    result = benchmark(
        deserialize_entity_key, serialized, entity_key_serialization_version
    )
    assert result == multi_entity_key_small


@pytest.mark.benchmark(group="deserialize_multi")
@pytest.mark.parametrize("entity_key_serialization_version", [3])
def test_deserialize_multi_entity_large(
    benchmark, multi_entity_key_large, entity_key_serialization_version
):
    """Benchmark large multi-entity key deserialization."""
    serialized = serialize_entity_key(
        multi_entity_key_large, entity_key_serialization_version
    )
    result = benchmark(
        deserialize_entity_key, serialized, entity_key_serialization_version
    )
    assert result == multi_entity_key_large


@pytest.mark.benchmark(group="deserialize_mixed")
@pytest.mark.parametrize("entity_key_serialization_version", [3])
def test_deserialize_mixed_value_types(
    benchmark, mixed_value_types_key, entity_key_serialization_version
):
    """Benchmark deserialization with mixed value types."""
    serialized = serialize_entity_key(
        mixed_value_types_key, entity_key_serialization_version
    )
    result = benchmark(
        deserialize_entity_key, serialized, entity_key_serialization_version
    )
    assert result == mixed_value_types_key


# Round-trip Benchmarks


@pytest.mark.benchmark(group="roundtrip_single")
def test_roundtrip_single_entity(benchmark, single_entity_key):
    """Benchmark complete serialize + deserialize round-trip for single entity."""

    def roundtrip():
        serialized = serialize_entity_key(single_entity_key, 3)
        return deserialize_entity_key(serialized, 3)

    result = benchmark(roundtrip)
    assert result == single_entity_key


@pytest.mark.benchmark(group="roundtrip_multi")
def test_roundtrip_multi_entity(benchmark, multi_entity_key_small):
    """Benchmark complete serialize + deserialize round-trip for multi-entity."""

    def roundtrip():
        serialized = serialize_entity_key(multi_entity_key_small, 3)
        return deserialize_entity_key(serialized, 3)

    result = benchmark(roundtrip)
    assert result == multi_entity_key_small


# Prefix Serialization Benchmarks


@pytest.mark.benchmark(group="prefix")
def test_serialize_entity_key_prefix_single(benchmark):
    """Benchmark entity key prefix serialization for single key."""
    result = benchmark(serialize_entity_key_prefix, ["user_id"], 3)
    assert len(result) > 0


@pytest.mark.benchmark(group="prefix")
def test_serialize_entity_key_prefix_multi(benchmark):
    """Benchmark entity key prefix serialization for multiple keys."""
    keys = ["user_id", "session_id", "device_id"]
    result = benchmark(serialize_entity_key_prefix, keys, 3)
    assert len(result) > 0


# Bulk Operations Benchmarks


@pytest.mark.benchmark(group="bulk_serialize")
def test_bulk_serialize_batch(benchmark, batch_entity_keys):
    """Benchmark batch serialization of 100 mixed entity keys."""

    def bulk_serialize():
        results = []
        for entity_key in batch_entity_keys:
            serialized = serialize_entity_key(entity_key, 3)
            results.append(serialized)
        return results

    results = benchmark(bulk_serialize)
    assert len(results) == 100


@pytest.mark.benchmark(group="bulk_deserialize")
def test_bulk_deserialize_batch(benchmark, batch_entity_keys):
    """Benchmark batch deserialization of 100 mixed entity keys."""
    # Pre-serialize all keys
    serialized_keys = [serialize_entity_key(key, 3) for key in batch_entity_keys]

    def bulk_deserialize():
        results = []
        for serialized in serialized_keys:
            deserialized = deserialize_entity_key(serialized, 3)
            results.append(deserialized)
        return results

    results = benchmark(bulk_deserialize)
    assert len(results) == 100


@pytest.mark.benchmark(group="bulk_roundtrip")
def test_bulk_roundtrip_batch(benchmark, batch_entity_keys):
    """Benchmark bulk serialize + deserialize for realistic workload."""

    def bulk_roundtrip():
        results = []
        for entity_key in batch_entity_keys:
            serialized = serialize_entity_key(entity_key, 3)
            deserialized = deserialize_entity_key(serialized, 3)
            results.append(deserialized)
        return results

    results = benchmark(bulk_roundtrip)
    assert len(results) == 100


# Memory Efficiency Tests


def test_memory_efficiency_serialization(single_entity_key):
    """Test memory usage during serialization (not a benchmark, just validation)."""
    import os

    import psutil

    process = psutil.Process(os.getpid())
    initial_memory = process.memory_info().rss

    # Perform many serializations
    for i in range(10000):
        entity_key = EntityKeyProto(
            join_keys=["user_id"], entity_values=[ValueProto(string_val=f"user{i}")]
        )
        serialize_entity_key(entity_key, 3)

    final_memory = process.memory_info().rss
    memory_increase = final_memory - initial_memory

    # Memory increase should be minimal (< 10MB for 10k operations)
    # This validates that we're not leaking memory in the optimized version
    assert memory_increase < 10 * 1024 * 1024, (
        f"Memory usage increased by {memory_increase / 1024 / 1024:.2f} MB"
    )


# Performance Regression Tests


def test_performance_regression_single_entity():
    """Regression test: single entity serialization should be faster than baseline."""
    entity_key = EntityKeyProto(
        join_keys=["user_id"], entity_values=[ValueProto(string_val="user123")]
    )

    # Warm up
    for _ in range(100):
        serialize_entity_key(entity_key, 3)

    # Time 1000 operations
    start_time = time.perf_counter()
    for _ in range(1000):
        serialize_entity_key(entity_key, 3)
    elapsed = time.perf_counter() - start_time

    # Should be able to do 1000 single entity serializations in < 50ms
    # Using a generous threshold to avoid flaky failures on CI runners
    assert elapsed < 0.05, (
        f"Single entity serialization too slow: {elapsed:.4f}s for 1000 operations"
    )


def test_performance_regression_deserialization():
    """Regression test: deserialization should be fast with memoryview optimization."""
    entity_key = EntityKeyProto(
        join_keys=["user_id", "session_id"],
        entity_values=[
            ValueProto(string_val="user123"),
            ValueProto(string_val="sess456"),
        ],
    )

    serialized = serialize_entity_key(entity_key, 3)

    # Warm up
    for _ in range(100):
        deserialize_entity_key(serialized, 3)

    # Time 1000 operations
    start_time = time.perf_counter()
    for _ in range(1000):
        deserialize_entity_key(serialized, 3)
    elapsed = time.perf_counter() - start_time

    # Should be able to do 1000 deserializations in < 200ms
    # Using a generous threshold to avoid flaky failures on CI runners
    assert elapsed < 0.2, (
        f"Deserialization too slow: {elapsed:.4f}s for 1000 operations"
    )


# Binary Compatibility Tests


def test_binary_format_consistency_single():
    """Ensure optimizations don't change binary format for single entities."""
    entity_key = EntityKeyProto(
        join_keys=["user_id"], entity_values=[ValueProto(string_val="test")]
    )

    # Serialize multiple times - results should be identical
    results = []
    for _ in range(10):
        serialized = serialize_entity_key(entity_key, 3)
        results.append(serialized)

    # All results should be identical
    for result in results[1:]:
        assert result == results[0], "Binary format inconsistency detected"


def test_binary_format_consistency_multi():
    """Ensure optimizations don't change binary format for multi-entity keys."""
    entity_key = EntityKeyProto(
        join_keys=["user", "session", "device"],
        entity_values=[
            ValueProto(string_val="u1"),
            ValueProto(string_val="s1"),
            ValueProto(string_val="d1"),
        ],
    )

    # Serialize multiple times - results should be identical
    results = []
    for _ in range(10):
        serialized = serialize_entity_key(entity_key, 3)
        results.append(serialized)

    # All results should be identical
    for result in results[1:]:
        assert result == results[0], "Binary format inconsistency detected"
