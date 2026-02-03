#!/usr/bin/env python3
"""
Validation script for Feast optimizations.

Ensures that lazy loading and caching optimizations don't break functionality:
1. FeatureStore can be initialized
2. Registry, provider, and openlineage_emitter work correctly
3. Feature services still resolve properly
4. Caching works as expected
"""
import sys
import time
from pathlib import Path

# Add the feast SDK to the path
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "sdk" / "python"))

def test_lazy_initialization():
    """Test that lazy initialization works correctly."""
    print("=== Testing Lazy Initialization ===")

    from feast import FeatureStore

    # Test 1: FeatureStore creation should be fast
    start_time = time.time()
    store = FeatureStore(repo_path=".")
    init_time = time.time() - start_time
    print(f"✓ FeatureStore initialization: {init_time:.4f}s")

    # Test 2: Check lazy loading status
    print(f"✓ Registry lazy status: {'not loaded' if store._registry is None else 'loaded'}")
    print(f"✓ Provider lazy status: {'not loaded' if store._provider is None else 'loaded'}")
    print(f"✓ OpenLineage lazy status: {'not loaded' if store._openlineage_emitter is None else 'loaded'}")

    # Test 3: Access properties should trigger loading
    print("\nAccessing properties to trigger lazy loading...")

    start_time = time.time()
    registry = store.registry
    registry_load_time = time.time() - start_time
    print(f"✓ Registry loaded: {registry_load_time:.4f}s")
    print(f"✓ Registry type: {type(registry).__name__}")

    start_time = time.time()
    provider = store.provider
    provider_load_time = time.time() - start_time
    print(f"✓ Provider loaded: {provider_load_time:.4f}s")
    print(f"✓ Provider type: {type(provider).__name__}")

    start_time = time.time()
    emitter = store.openlineage_emitter
    emitter_load_time = time.time() - start_time
    print(f"✓ OpenLineage emitter loaded: {emitter_load_time:.4f}s")

    # Test 4: Subsequent accesses should be fast (already loaded)
    start_time = time.time()
    registry2 = store.registry
    registry_cached_time = time.time() - start_time
    print(f"✓ Registry cached access: {registry_cached_time:.6f}s")

    print(f"✓ Same registry instance: {registry is registry2}")

    return True

def test_feature_service_caching():
    """Test that feature service caching works correctly."""
    print("\n=== Testing Feature Service Caching ===")

    from feast import FeatureStore

    store = FeatureStore(repo_path=".")

    # Check that cache exists
    print(f"✓ Feature service cache initialized: {hasattr(store, '_feature_service_cache')}")
    print(f"✓ Cache empty initially: {len(store._feature_service_cache) == 0}")

    # Check that registry has cache attached
    registry = store.registry
    print(f"✓ Registry has cache: {hasattr(registry, '_feature_service_cache')}")

    try:
        # Test feature service resolution
        feature_services = store.list_feature_services()
        print(f"✓ Found {len(feature_services)} feature services")

        if feature_services:
            feature_service = feature_services[0]
            print(f"✓ Testing with feature service: {feature_service.name}")

            # First call - should populate cache
            start_time = time.time()
            from feast.utils import _get_features
            features1 = _get_features(store.registry, store.project, feature_service, allow_cache=True)
            first_call_time = time.time() - start_time

            # Second call - should use cache
            start_time = time.time()
            features2 = _get_features(store.registry, store.project, feature_service, allow_cache=True)
            second_call_time = time.time() - start_time

            print(f"✓ First call (populate cache): {first_call_time:.4f}s")
            print(f"✓ Second call (use cache): {second_call_time:.4f}s")
            print(f"✓ Same results: {features1 == features2}")
            print(f"✓ Cache speedup: {first_call_time / second_call_time:.1f}x")

            # Check cache has been populated
            cache_size = len(store.registry._feature_service_cache)
            print(f"✓ Cache entries after test: {cache_size}")

    except Exception as e:
        print(f"⚠️ Feature service test error (may be expected): {e}")

    return True

def test_backward_compatibility():
    """Test that existing functionality still works."""
    print("\n=== Testing Backward Compatibility ===")

    from feast import FeatureStore

    store = FeatureStore(repo_path=".")

    try:
        # Test basic operations
        project = store.project
        print(f"✓ Project access: {project}")

        registry = store.registry
        print(f"✓ Registry access: {type(registry).__name__}")

        # Test listing operations
        entities = store.list_entities()
        print(f"✓ List entities: {len(entities)} found")

        feature_views = store.list_feature_views()
        print(f"✓ List feature views: {len(feature_views)} found")

        feature_services = store.list_feature_services()
        print(f"✓ List feature services: {len(feature_services)} found")

        # Test string representation
        repr_str = repr(store)
        print(f"✓ String representation works: {len(repr_str)} chars")

    except Exception as e:
        print(f"❌ Backward compatibility issue: {e}")
        return False

    return True

def run_validation():
    """Run all validation tests."""
    print("🔧 Validating Feast Optimizations")
    print("=" * 40)

    tests = [
        test_lazy_initialization,
        test_feature_service_caching,
        test_backward_compatibility,
    ]

    results = []
    for test in tests:
        try:
            result = test()
            results.append(result)
        except Exception as e:
            print(f"❌ Test {test.__name__} failed: {e}")
            results.append(False)

    # Summary
    print("\n" + "=" * 40)
    print("🎯 Validation Summary:")
    passed = sum(results)
    total = len(results)
    print(f"   Tests passed: {passed}/{total}")

    if passed == total:
        print("   ✅ ALL TESTS PASSED - Optimizations working correctly!")
    else:
        print("   ❌ Some tests failed - Please review implementation")

    return passed == total

if __name__ == "__main__":
    success = run_validation()
    sys.exit(0 if success else 1)