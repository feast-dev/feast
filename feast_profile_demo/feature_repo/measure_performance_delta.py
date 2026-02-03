#!/usr/bin/env python3
"""
Performance delta measurement for Feast optimizations.

Measures before/after performance improvements from:
1. FeatureStore lazy initialization
2. Feature service caching

Expected improvements:
- FeatureStore init: 2.4s → 0.05s (48x improvement)
- Feature service: 16ms → 7ms (2.3x improvement)
"""
import time
import sys
import json
from pathlib import Path
from test_performance_baseline import run_baseline_benchmark

def measure_cold_start_improvements():
    """Measure cold start performance improvements multiple times."""
    print("=== Cold Start Performance Measurement ===")

    # Multiple runs to get consistent measurements
    init_times = []

    for run in range(3):
        print(f"\nRun {run + 1}/3:")

        # Clear module cache to simulate cold start
        modules_to_clear = [mod for mod in sys.modules.keys() if mod.startswith('feast')]
        for mod in modules_to_clear:
            if mod in sys.modules:
                del sys.modules[mod]

        start_time = time.time()
        try:
            from feast import FeatureStore
            store = FeatureStore(repo_path=".")
            init_time = time.time() - start_time
            init_times.append(init_time)
            print(f"  Cold start time: {init_time:.4f}s")
        except Exception as e:
            print(f"  Error in run {run + 1}: {e}")

    if init_times:
        avg_init_time = sum(init_times) / len(init_times)
        min_init_time = min(init_times)
        max_init_time = max(init_times)

        print(f"\nCold Start Results:")
        print(f"  Average: {avg_init_time:.4f}s")
        print(f"  Min: {min_init_time:.4f}s")
        print(f"  Max: {max_init_time:.4f}s")

        return avg_init_time

    return None

def measure_warm_access_performance():
    """Measure performance of subsequent accesses (warm cache)."""
    print("\n=== Warm Access Performance ===")

    from feast import FeatureStore
    store = FeatureStore(repo_path=".")

    # Access registry multiple times to test lazy loading
    warm_times = []
    for i in range(5):
        start_time = time.time()
        registry = store.registry  # This should be fast after first access
        access_time = time.time() - start_time
        warm_times.append(access_time)
        print(f"  Registry access {i+1}: {access_time:.6f}s")

    if warm_times:
        avg_warm_time = sum(warm_times) / len(warm_times)
        print(f"  Average warm access: {avg_warm_time:.6f}s")
        return avg_warm_time

    return None

def calculate_improvement_metrics(baseline_results, optimized_results):
    """Calculate improvement metrics and ratios."""
    print("\n=== Performance Improvement Analysis ===")

    improvements = {}

    # FeatureStore initialization improvement
    if baseline_results.get('init_time') and optimized_results.get('init_time'):
        baseline_init = baseline_results['init_time']
        optimized_init = optimized_results['init_time']
        init_improvement = baseline_init / optimized_init
        improvements['init_improvement'] = init_improvement

        print(f"FeatureStore Initialization:")
        print(f"  Baseline: {baseline_init:.4f}s")
        print(f"  Optimized: {optimized_init:.4f}s")
        print(f"  Improvement: {init_improvement:.1f}x faster")
        print(f"  Time saved: {baseline_init - optimized_init:.4f}s")

    # Feature service improvement
    if (baseline_results.get('feature_service_time') and
        optimized_results.get('feature_service_time')):
        baseline_fs = baseline_results['feature_service_time']
        optimized_fs = optimized_results['feature_service_time']
        fs_improvement = baseline_fs / optimized_fs
        improvements['feature_service_improvement'] = fs_improvement

        print(f"\nFeature Service Resolution:")
        print(f"  Baseline: {baseline_fs:.4f}s")
        print(f"  Optimized: {optimized_fs:.4f}s")
        print(f"  Improvement: {fs_improvement:.1f}x faster")
        print(f"  Time saved: {baseline_fs - optimized_fs:.4f}s")

    # Overall assessment
    if improvements:
        print(f"\n=== Optimization Success Assessment ===")

        # Check if we hit our targets
        init_target = 48.0  # 48x improvement target
        fs_target = 2.3     # 2.3x improvement target

        if 'init_improvement' in improvements:
            init_success = improvements['init_improvement'] >= init_target
            print(f"Init Optimization: {'✅ SUCCESS' if init_success else '❌ BELOW TARGET'}")
            print(f"  Target: {init_target}x, Achieved: {improvements['init_improvement']:.1f}x")

        if 'feature_service_improvement' in improvements:
            fs_success = improvements['feature_service_improvement'] >= fs_target
            print(f"Feature Service Optimization: {'✅ SUCCESS' if fs_success else '❌ BELOW TARGET'}")
            print(f"  Target: {fs_target}x, Achieved: {improvements['feature_service_improvement']:.1f}x")

    return improvements

def run_performance_delta_measurement():
    """Run complete performance delta measurement."""
    print("🚀 Measuring Feast Performance Optimizations")
    print("=" * 50)

    # Measure optimized performance
    print("Measuring optimized implementation...")
    optimized_results = run_baseline_benchmark()

    # Additional measurements for lazy loading
    cold_start_time = measure_cold_start_improvements()
    warm_access_time = measure_warm_access_performance()

    if cold_start_time:
        optimized_results['cold_start_time'] = cold_start_time
    if warm_access_time:
        optimized_results['warm_access_time'] = warm_access_time

    # Expected baseline values for comparison (from profiling)
    baseline_results = {
        'init_time': 2.458,  # From profiling analysis
        'feature_service_time': 0.016,  # 16ms
        'direct_feature_time': 0.007,   # 7ms
    }

    # Calculate improvements
    improvements = calculate_improvement_metrics(baseline_results, optimized_results)

    # Save results
    results = {
        'timestamp': time.time(),
        'baseline': baseline_results,
        'optimized': optimized_results,
        'improvements': improvements
    }

    results_file = Path("performance_delta_results.json")
    with open(results_file, 'w') as f:
        json.dump(results, f, indent=2)

    print(f"\n📊 Results saved to: {results_file}")
    print("\n🎯 Summary:")
    print(f"   FeatureStore init improvement: {improvements.get('init_improvement', 'N/A')}")
    print(f"   Feature service improvement: {improvements.get('feature_service_improvement', 'N/A')}")

    return results

if __name__ == "__main__":
    results = run_performance_delta_measurement()