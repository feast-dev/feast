"""
Component Isolation Performance Profiling

This script isolates and profiles specific Feast components to identify
individual bottlenecks without the overhead of the full feature serving pipeline.

Based on the implementation plan, this focuses on:
1. Protobuf serialization/deserialization
2. Provider interface abstraction overhead
3. Registry operations and parsing
4. Entity resolution algorithms
5. Async vs sync provider routing logic
6. Memory allocation patterns
"""

import time
import os
import sys
import tracemalloc
import asyncio
from typing import List, Dict, Any, Optional
import json

# Add the current directory to Python path to import profiling_utils
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from feast import FeatureStore
from feast.protos.feast.types import Value_pb2 as ValueProto
from feast.protos.feast.serving import ServingService_pb2 as serving
from google.protobuf.json_format import MessageToDict, ParseDict

from profiling_utils import (
    FeastProfiler,
    generate_test_entities,
    generate_feature_lists,
    memory_usage_analysis
)


class ComponentProfiler:
    """Profiler for individual Feast components."""

    def __init__(self, repo_path: str = "."):
        self.repo_path = repo_path
        self.store = None
        self.profiler = FeastProfiler(output_dir="profiling_results/components")

    def setup_feast_components(self):
        """Initialize Feast components for testing."""
        print("Setting up Feast components...")

        with self.profiler.profile_context("component_setup") as result:
            with self.profiler.time_operation("feature_store_init", result):
                self.store = FeatureStore(repo_path=self.repo_path)

            with self.profiler.time_operation("registry_load", result):
                self.registry = self.store.registry
                self.provider = self.store._get_provider()

            # Load test data
            with self.profiler.time_operation("test_data_prep", result):
                self.test_entities = generate_test_entities(100)
                self.feature_lists = generate_feature_lists()

    def profile_protobuf_operations(self):
        """Profile Protobuf serialization and deserialization."""
        print("\n--- Profiling Protobuf Operations ---")

        # Create test protobuf messages
        entity_rows = []
        for entity in self.test_entities[:20]:
            entity_row = serving.GetOnlineFeaturesRequestV2.EntityRow()
            for key, value in entity.items():
                if isinstance(value, int):
                    entity_row.fields[key].int64_val = value
                elif isinstance(value, float):
                    entity_row.fields[key].double_val = value
                else:
                    entity_row.fields[key].string_val = str(value)
            entity_rows.append(entity_row)

        # Profile protobuf to dict conversion
        with self.profiler.profile_context("protobuf_to_dict") as result:
            for i in range(100):
                with self.profiler.time_operation(f"message_to_dict_{i}", result):
                    for entity_row in entity_rows:
                        dict_result = MessageToDict(entity_row)

        # Profile dict to protobuf conversion
        test_dicts = [MessageToDict(row) for row in entity_rows[:5]]

        with self.profiler.profile_context("dict_to_protobuf") as result:
            for i in range(100):
                with self.profiler.time_operation(f"parse_dict_{i}", result):
                    for test_dict in test_dicts:
                        entity_row = serving.GetOnlineFeaturesRequestV2.EntityRow()
                        ParseDict(test_dict, entity_row)

        # Profile large message handling
        large_entity_rows = []
        for entity in self.test_entities:  # All 100 entities
            entity_row = serving.GetOnlineFeaturesRequestV2.EntityRow()
            for key, value in entity.items():
                if isinstance(value, int):
                    entity_row.fields[key].int64_val = value
                elif isinstance(value, float):
                    entity_row.fields[key].double_val = value
                else:
                    entity_row.fields[key].string_val = str(value)
            large_entity_rows.append(entity_row)

        with self.profiler.profile_context("large_protobuf_conversion") as result:
            with self.profiler.time_operation("large_to_dict", result):
                for entity_row in large_entity_rows:
                    dict_result = MessageToDict(entity_row)

    def profile_registry_operations(self):
        """Profile registry access patterns."""
        print("\n--- Profiling Registry Operations ---")

        # Profile repeated registry access
        with self.profiler.profile_context("registry_repeated_access") as result:
            for i in range(50):
                with self.profiler.time_operation(f"list_feature_views_{i}", result):
                    fvs = self.store.list_feature_views()

                with self.profiler.time_operation(f"get_feature_view_{i}", result):
                    fv = self.store.get_feature_view("driver_hourly_stats")

        # Profile registry parsing overhead
        with self.profiler.profile_context("registry_parsing") as result:
            with self.profiler.time_operation("registry_proto_parse", result):
                # Access the raw registry proto (skip if method signature changed)
                try:
                    registry_proto = self.registry._get_registry_proto(self.store.project)
                    has_proto = True
                except Exception:
                    # Skip proto parsing if method signature is different
                    has_proto = False

            with self.profiler.time_operation("feature_view_parsing", result):
                # Parse all feature views
                if has_proto:
                    for fv_proto in registry_proto.feature_views:
                        # Simulate feature view object creation overhead
                        name = fv_proto.spec.name
                        entities = [e for e in fv_proto.spec.entities]
                else:
                    # Alternative: use the public API
                    fvs = self.store.list_feature_views()
                    for fv in fvs:
                        name = fv.name
                        entities = fv.entities

        # Profile entity resolution
        with self.profiler.profile_context("entity_resolution") as result:
            entity_names = ["driver"]
            for i in range(100):
                with self.profiler.time_operation(f"resolve_entities_{i}", result):
                    entities = [self.store.get_entity(name) for name in entity_names]

    def profile_provider_abstraction(self):
        """Profile provider interface overhead."""
        print("\n--- Profiling Provider Abstraction ---")

        # Get feature views for testing
        feature_views = self.store.list_feature_views()
        driver_fv = self.store.get_feature_view("driver_hourly_stats")

        # Profile provider method calls
        with self.profiler.profile_context("provider_method_calls") as result:
            for i in range(20):
                with self.profiler.time_operation(f"provider_online_store_{i}", result):
                    online_store = self.provider.online_store

                with self.profiler.time_operation(f"provider_type_check_{i}", result):
                    # Check provider type and capabilities
                    provider_type = type(self.provider).__name__

        # Profile feature view resolution through provider
        entity_rows = self.test_entities[:10]

        with self.profiler.profile_context("provider_feature_resolution") as result:
            # Simulate the provider's feature resolution process
            with self.profiler.time_operation("feature_refs_creation", result):
                feature_refs = []
                for feature in self.feature_lists["standard"]:
                    fv_name, feature_name = feature.split(":", 1)
                    feature_refs.append((fv_name, feature_name))

            with self.profiler.time_operation("provider_validation", result):
                # Simulate provider validation overhead
                for ref in feature_refs:
                    fv_name, feature_name = ref
                    try:
                        fv = self.store.get_feature_view(fv_name)
                        # Check if feature exists in schema
                        feature_exists = any(f.name == feature_name for f in fv.schema)
                    except Exception:
                        pass

    def profile_async_vs_sync_patterns(self):
        """Profile async vs sync operation patterns."""
        print("\n--- Profiling Async vs Sync Patterns ---")

        # Simulate sync operations
        with self.profiler.profile_context("sync_operations") as result:
            for i in range(50):
                with self.profiler.time_operation(f"sync_registry_access_{i}", result):
                    fv = self.store.get_feature_view("driver_hourly_stats")
                    entities = self.store.list_entities()

        # Simulate async-like operations (even though registry is sync)
        async def async_registry_operations():
            with self.profiler.profile_context("async_pattern_simulation") as result:
                for i in range(50):
                    with self.profiler.time_operation(f"async_registry_access_{i}", result):
                        # Simulate async pattern with sync operations
                        await asyncio.sleep(0)  # Yield control
                        fv = self.store.get_feature_view("driver_hourly_stats")
                        await asyncio.sleep(0)
                        entities = self.store.list_entities()

        # Run async simulation
        asyncio.run(async_registry_operations())

        # Profile thread pool simulation (like feature_server.py does)
        from concurrent.futures import ThreadPoolExecutor

        def sync_operation():
            fv = self.store.get_feature_view("driver_hourly_stats")
            return fv

        with self.profiler.profile_context("thread_pool_overhead") as result:
            with ThreadPoolExecutor(max_workers=4) as executor:
                with self.profiler.time_operation("thread_pool_submission", result):
                    futures = []
                    for i in range(20):
                        future = executor.submit(sync_operation)
                        futures.append(future)

                with self.profiler.time_operation("thread_pool_collection", result):
                    results = [future.result() for future in futures]

    def profile_entity_resolution_algorithms(self):
        """Profile entity resolution and key handling."""
        print("\n--- Profiling Entity Resolution Algorithms ---")

        # Profile entity key creation and hashing
        with self.profiler.profile_context("entity_key_operations") as result:
            entity_keys = []
            with self.profiler.time_operation("entity_key_creation", result):
                for entity in self.test_entities:
                    # Simulate entity key creation process
                    key = f"driver_id={entity['driver_id']}"
                    entity_keys.append(key)

            with self.profiler.time_operation("entity_key_hashing", result):
                hashed_keys = [hash(key) for key in entity_keys]

        # Profile entity batch processing
        with self.profiler.profile_context("entity_batch_processing") as result:
            batch_sizes = [1, 10, 50, 100]
            for batch_size in batch_sizes:
                entities_batch = self.test_entities[:batch_size]

                with self.profiler.time_operation(f"batch_process_{batch_size}", result):
                    # Simulate batch processing overhead
                    processed = []
                    for entity in entities_batch:
                        # Entity validation and normalization
                        normalized = {
                            k: v for k, v in entity.items()
                            if k in ["driver_id", "val_to_add", "val_to_add_2"]
                        }
                        processed.append(normalized)

    def profile_memory_allocation_patterns(self):
        """Profile memory allocation patterns in key operations."""
        print("\n--- Profiling Memory Allocation Patterns ---")

        # Profile memory usage during feature retrieval simulation
        tracemalloc.start()

        with self.profiler.profile_context("memory_feature_retrieval") as result:
            snapshot1 = tracemalloc.take_snapshot()
            result.add_memory_snapshot("start", snapshot1)

            # Simulate feature retrieval operations
            with self.profiler.time_operation("memory_intensive_ops", result):
                large_responses = []
                for i in range(10):
                    # Simulate creating large response objects
                    response_data = {}
                    for entity in self.test_entities[:50]:
                        entity_response = {}
                        for feature in self.feature_lists["all_features"]:
                            entity_response[feature] = f"value_{i}_{entity['driver_id']}"
                        response_data[f"entity_{entity['driver_id']}"] = entity_response
                    large_responses.append(response_data)

            snapshot2 = tracemalloc.take_snapshot()
            result.add_memory_snapshot("peak", snapshot2)

            # Clear large objects
            del large_responses

            snapshot3 = tracemalloc.take_snapshot()
            result.add_memory_snapshot("end", snapshot3)

        tracemalloc.stop()

        # Analyze memory snapshots
        memory_analysis = memory_usage_analysis(snapshot2)
        print(f"Peak memory usage: {memory_analysis['total_mb']:.2f} MB")

    def profile_json_serialization(self):
        """Profile JSON serialization overhead."""
        print("\n--- Profiling JSON Serialization ---")

        # Create test data structures
        small_response = {
            "field_values": [
                {"driver_id": 1001, "conv_rate": 0.85, "acc_rate": 0.92}
            ]
        }

        large_response = {
            "field_values": [
                {
                    f"driver_id": entity["driver_id"],
                    f"conv_rate": 0.85 + (entity["driver_id"] % 100) / 1000,
                    f"acc_rate": 0.92 + (entity["driver_id"] % 50) / 1000,
                    f"avg_daily_trips": entity["driver_id"] * 10,
                    f"transformed_rate": entity["driver_id"] * 0.001
                }
                for entity in self.test_entities
            ]
        }

        # Profile small response serialization
        with self.profiler.profile_context("json_small_responses") as result:
            for i in range(1000):
                with self.profiler.time_operation(f"small_json_dumps_{i}", result):
                    json_str = json.dumps(small_response)

                with self.profiler.time_operation(f"small_json_loads_{i}", result):
                    parsed = json.loads(json_str)

        # Profile large response serialization
        with self.profiler.profile_context("json_large_responses") as result:
            for i in range(10):
                with self.profiler.time_operation(f"large_json_dumps_{i}", result):
                    json_str = json.dumps(large_response)

                with self.profiler.time_operation(f"large_json_loads_{i}", result):
                    parsed = json.loads(json_str)

    def run_comprehensive_profiling(self):
        """Run all component isolation profiling."""
        print("Starting Comprehensive Component Profiling")
        print("=" * 60)

        try:
            self.setup_feast_components()
            self.profile_registry_operations()
            self.profile_protobuf_operations()
            self.profile_provider_abstraction()
            self.profile_entity_resolution_algorithms()
            self.profile_async_vs_sync_patterns()
            self.profile_json_serialization()
            self.profile_memory_allocation_patterns()

            print("\n" + "=" * 60)
            print("COMPONENT PROFILING COMPLETE")

            # Generate reports
            self.profiler.print_summary()
            csv_file = self.profiler.generate_csv_report()

            # Save detailed profiles for memory-intensive operations
            for result in self.profiler.results:
                if any(keyword in result.name for keyword in
                      ['memory_', 'large_protobuf', 'thread_pool', 'large_responses']):
                    self.profiler.save_detailed_profile(result)

            print(f"\nDetailed analysis available in: {self.profiler.output_dir}/")

            return self.profiler.results

        except Exception as e:
            print(f"Error during profiling: {e}")
            import traceback
            traceback.print_exc()
            return None


def main():
    """Main entry point for component profiling."""
    print("Feast Component Isolation Performance Profiling")
    print("=" * 55)

    profiler = ComponentProfiler()
    results = profiler.run_comprehensive_profiling()

    if results:
        print("\nComponent Performance Summary:")

        # Identify bottleneck operations
        bottlenecks = []
        for result in results:
            for op, duration in result.timing_results.items():
                if duration > 0.001:  # Operations taking more than 1ms
                    bottlenecks.append((result.name, op, duration))

        # Sort by duration and show top bottlenecks
        bottlenecks.sort(key=lambda x: x[2], reverse=True)
        print("\nTop performance bottlenecks:")
        for i, (test, op, duration) in enumerate(bottlenecks[:10], 1):
            print(f"{i:2}. {test}.{op}: {duration:.4f}s")


if __name__ == "__main__":
    main()