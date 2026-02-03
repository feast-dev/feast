#!/usr/bin/env python3
"""
Performance baseline testing for Feast optimizations.

Tests FeatureStore initialization time and feature service resolution performance.
Used to measure improvements from lazy loading and feature service caching.
"""
import time
import sys
import os
from pathlib import Path

# Add the feast SDK to the path
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "sdk" / "python"))

def benchmark_featurestore_initialization():
    """Benchmark FeatureStore initialization time."""
    from feast import FeatureStore

    # Clear any existing instances
    if 'feast' in sys.modules:
        del sys.modules['feast']

    start_time = time.time()
    store = FeatureStore(repo_path=".")
    init_time = time.time() - start_time

    print(f"FeatureStore initialization: {init_time:.4f}s")
    return init_time, store

def benchmark_feature_service_vs_direct():
    """Benchmark feature service vs direct feature access."""
    from feast import FeatureStore

    store = FeatureStore(repo_path=".")

    # Get feature service
    try:
        feature_services = store.list_feature_services()
        if not feature_services:
            print("No feature services found, skipping feature service benchmark")
            return None, None, None

        feature_service = feature_services[0]
        print(f"Using feature service: {feature_service.name}")

        # Extract direct features from feature service for comparison
        direct_features = []
        for projection in feature_service.feature_view_projections:
            direct_features.extend([f"{projection.name_to_use()}:{f.name}" for f in projection.features])

        if not direct_features:
            print("No features found in feature service")
            return None, None, None

        # Use first feature for testing
        direct_feature = direct_features[0]

        # Get entity values - try common entity names
        entities = []
        try:
            entities = [{"driver_id": 1001}]
        except Exception:
            try:
                entities = [{"user_id": 1}]
            except Exception:
                entities = [{"id": 1}]

        print(f"Using entities: {entities}")
        print(f"Direct feature: {direct_feature}")

        # Benchmark feature service access
        fs_times = []
        for i in range(5):  # 5 runs for average
            start_time = time.time()
            try:
                fs_response = store.get_online_features(feature_service, entities)
                fs_time = time.time() - start_time
                fs_times.append(fs_time)
                print(f"Feature service run {i+1}: {fs_time:.4f}s")
            except Exception as e:
                print(f"Feature service error on run {i+1}: {e}")

        # Benchmark direct feature access
        direct_times = []
        for i in range(5):  # 5 runs for average
            start_time = time.time()
            try:
                direct_response = store.get_online_features([direct_feature], entities)
                direct_time = time.time() - start_time
                direct_times.append(direct_time)
                print(f"Direct feature run {i+1}: {direct_time:.4f}s")
            except Exception as e:
                print(f"Direct feature error on run {i+1}: {e}")

        if fs_times and direct_times:
            avg_fs_time = sum(fs_times) / len(fs_times)
            avg_direct_time = sum(direct_times) / len(direct_times)

            print(f"\nAverage feature service time: {avg_fs_time:.4f}s")
            print(f"Average direct feature time: {avg_direct_time:.4f}s")
            print(f"Feature service overhead: {(avg_fs_time / avg_direct_time - 1) * 100:.1f}%")

            return avg_fs_time, avg_direct_time, feature_service.name

    except Exception as e:
        print(f"Feature benchmark error: {e}")

    return None, None, None

def run_baseline_benchmark():
    """Run complete baseline performance benchmark."""
    print("=== Feast Performance Baseline ===")
    print(f"Working directory: {os.getcwd()}")

    # Test 1: FeatureStore initialization
    print("\n1. FeatureStore Initialization Benchmark:")
    init_time, store = benchmark_featurestore_initialization()

    # Test 2: Feature service vs direct features
    print("\n2. Feature Service vs Direct Features Benchmark:")
    fs_time, direct_time, service_name = benchmark_feature_service_vs_direct()

    # Summary
    print("\n=== Performance Summary ===")
    print(f"FeatureStore init time: {init_time:.4f}s")
    if fs_time and direct_time:
        overhead = (fs_time / direct_time - 1) * 100
        print(f"Feature service time: {fs_time:.4f}s")
        print(f"Direct feature time: {direct_time:.4f}s")
        print(f"Feature service overhead: {overhead:.1f}%")
    else:
        print("Feature service benchmark unavailable")

    return {
        'init_time': init_time,
        'feature_service_time': fs_time,
        'direct_feature_time': direct_time,
        'service_name': service_name
    }

if __name__ == "__main__":
    results = run_baseline_benchmark()