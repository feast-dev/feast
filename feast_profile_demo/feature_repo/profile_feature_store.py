"""
Direct FeatureStore Performance Profiling

This script profiles the core FeatureStore.get_online_features() method
with different entity counts, feature counts, and scenarios to identify
bottlenecks in feature resolution, provider operations, and serialization.

Based on the implementation plan, this focuses on:
1. Registry access patterns and caching
2. Provider abstraction layer overhead
3. Feature resolution logic
4. Response serialization to dict
5. Cold start vs warm cache performance
"""

import time
import os
import sys
from typing import List, Dict, Any

# Add the current directory to Python path to import profiling_utils
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from feast import FeatureStore
from profiling_utils import (
    FeastProfiler,
    generate_test_entities,
    generate_feature_lists,
    create_performance_comparison
)


class FeatureStoreProfiler:
    """Profiler specifically for FeatureStore operations."""

    def __init__(self, repo_path: str = "."):
        self.repo_path = repo_path
        self.store = None
        self.profiler = FeastProfiler(output_dir="profiling_results/feature_store")

    def setup_feature_store(self):
        """Initialize the FeatureStore."""
        print("Setting up FeatureStore...")
        with self.profiler.profile_context("feature_store_init") as result:
            with self.profiler.time_operation("store_creation", result):
                self.store = FeatureStore(repo_path=self.repo_path)

            with self.profiler.time_operation("registry_load", result):
                # Trigger registry loading by accessing feature views
                _ = self.store.list_feature_views()

    def profile_entity_scaling(self):
        """Profile get_online_features with different entity counts."""
        print("\n--- Profiling Entity Count Scaling ---")

        entity_counts = [1, 5, 10, 50, 100]
        feature_list = generate_feature_lists()["standard"]

        for count in entity_counts:
            test_name = f"entity_scaling_{count}_entities"
            print(f"Testing {count} entities...")

            entities = generate_test_entities(count)

            with self.profiler.profile_context(test_name) as result:
                with self.profiler.time_operation("get_online_features", result):
                    response = self.store.get_online_features(
                        features=feature_list,
                        entity_rows=entities
                    )

                with self.profiler.time_operation("to_dict_conversion", result):
                    response_dict = response.to_dict()

                # Add metadata
                result.add_timing("entity_count", count)
                result.add_timing("feature_count", len(feature_list))
                result.add_timing("response_size", len(str(response_dict)))

    def profile_feature_scaling(self):
        """Profile get_online_features with different feature counts."""
        print("\n--- Profiling Feature Count Scaling ---")

        feature_lists = generate_feature_lists()
        entities = generate_test_entities(10)  # Fixed entity count

        for list_name, features in feature_lists.items():
            test_name = f"feature_scaling_{len(features)}_features_{list_name}"
            print(f"Testing {len(features)} features ({list_name})...")

            with self.profiler.profile_context(test_name) as result:
                with self.profiler.time_operation("get_online_features", result):
                    response = self.store.get_online_features(
                        features=features,
                        entity_rows=entities
                    )

                with self.profiler.time_operation("to_dict_conversion", result):
                    response_dict = response.to_dict()

                # Add metadata
                result.add_timing("entity_count", len(entities))
                result.add_timing("feature_count", len(features))
                result.add_timing("has_odfv", "odfv" in list_name)

    def profile_cold_vs_warm(self):
        """Profile cold start vs warm cache performance."""
        print("\n--- Profiling Cold vs Warm Performance ---")

        features = generate_feature_lists()["with_odfv"]
        entities = generate_test_entities(20)

        # Cold start - first request
        with self.profiler.profile_context("cold_start_request") as result:
            with self.profiler.time_operation("first_request", result):
                response1 = self.store.get_online_features(
                    features=features,
                    entity_rows=entities
                )

            with self.profiler.time_operation("first_to_dict", result):
                _ = response1.to_dict()

        # Warm cache - immediate second request
        with self.profiler.profile_context("warm_cache_request") as result:
            with self.profiler.time_operation("second_request", result):
                response2 = self.store.get_online_features(
                    features=features,
                    entity_rows=entities
                )

            with self.profiler.time_operation("second_to_dict", result):
                _ = response2.to_dict()

        # Multiple warm requests
        for i in range(3):
            with self.profiler.profile_context(f"warm_request_{i+3}") as result:
                with self.profiler.time_operation("warm_request", result):
                    response = self.store.get_online_features(
                        features=features,
                        entity_rows=entities
                    )
                    _ = response.to_dict()

    def profile_feature_services(self):
        """Profile using feature services vs direct feature lists."""
        print("\n--- Profiling Feature Services vs Direct Features ---")

        entities = generate_test_entities(10)

        # Direct feature list
        direct_features = [
            "driver_hourly_stats:conv_rate",
            "transformed_conv_rate:conv_rate_plus_val1",
            "transformed_conv_rate:conv_rate_plus_val2"
        ]

        with self.profiler.profile_context("direct_feature_list") as result:
            with self.profiler.time_operation("get_features_direct", result):
                response = self.store.get_online_features(
                    features=direct_features,
                    entity_rows=entities
                )

            with self.profiler.time_operation("to_dict_direct", result):
                _ = response.to_dict()

        # Feature service
        with self.profiler.profile_context("feature_service_v1") as result:
            with self.profiler.time_operation("get_feature_service", result):
                feature_service = self.store.get_feature_service("driver_activity_v1")

            with self.profiler.time_operation("get_features_service", result):
                response = self.store.get_online_features(
                    features=feature_service,
                    entity_rows=entities
                )

            with self.profiler.time_operation("to_dict_service", result):
                _ = response.to_dict()

    def profile_missing_entities(self):
        """Profile performance with missing entities."""
        print("\n--- Profiling Missing Entity Handling ---")

        features = generate_feature_lists()["standard"]

        # Mix of existing and missing entities
        entities = [
            {"driver_id": 1001, "val_to_add": 100, "val_to_add_2": 200},  # Exists
            {"driver_id": 1002, "val_to_add": 101, "val_to_add_2": 201},  # Exists
            {"driver_id": 9999, "val_to_add": 999, "val_to_add_2": 999},  # Missing
            {"driver_id": 8888, "val_to_add": 888, "val_to_add_2": 888},  # Missing
        ]

        with self.profiler.profile_context("mixed_missing_entities") as result:
            with self.profiler.time_operation("get_features_mixed", result):
                response = self.store.get_online_features(
                    features=features,
                    entity_rows=entities
                )

            with self.profiler.time_operation("to_dict_mixed", result):
                response_dict = response.to_dict()

            # Count missing vs found
            result.add_timing("total_entities", len(entities))
            result.add_timing("missing_entities", 2)  # We know 2 are missing

    def profile_large_batch(self):
        """Profile large batch requests to find scalability limits."""
        print("\n--- Profiling Large Batch Requests ---")

        features = generate_feature_lists()["minimal"]  # Keep features minimal
        large_entity_counts = [100, 500, 1000]

        for count in large_entity_counts:
            if count > 500:
                print(f"Testing {count} entities (this may take a while)...")

            entities = generate_test_entities(count, driver_id_range=(1001, 1010))

            test_name = f"large_batch_{count}_entities"

            with self.profiler.profile_context(test_name) as result:
                with self.profiler.time_operation("get_online_features_large", result):
                    response = self.store.get_online_features(
                        features=features,
                        entity_rows=entities
                    )

                with self.profiler.time_operation("to_dict_large", result):
                    response_dict = response.to_dict()

                result.add_timing("entity_count", count)
                result.add_timing("response_size_mb", len(str(response_dict)) / (1024 * 1024))

    def profile_registry_operations(self):
        """Profile registry access patterns."""
        print("\n--- Profiling Registry Operations ---")

        with self.profiler.profile_context("registry_operations") as result:
            with self.profiler.time_operation("list_feature_views", result):
                fvs = self.store.list_feature_views()

            with self.profiler.time_operation("list_entities", result):
                entities = self.store.list_entities()

            with self.profiler.time_operation("list_feature_services", result):
                services = self.store.list_feature_services()

            with self.profiler.time_operation("get_feature_view", result):
                fv = self.store.get_feature_view("driver_hourly_stats")

            with self.profiler.time_operation("get_entity", result):
                entity = self.store.get_entity("driver")

    def run_comprehensive_profiling(self):
        """Run all profiling scenarios."""
        print("Starting Comprehensive FeatureStore Profiling")
        print("=" * 60)

        try:
            self.setup_feature_store()
            self.profile_registry_operations()
            self.profile_cold_vs_warm()
            self.profile_entity_scaling()
            self.profile_feature_scaling()
            self.profile_feature_services()
            self.profile_missing_entities()
            self.profile_large_batch()

            print("\n" + "=" * 60)
            print("PROFILING COMPLETE")

            # Generate reports
            self.profiler.print_summary()
            csv_file = self.profiler.generate_csv_report()
            comparison_df = create_performance_comparison(self.profiler)

            # Save detailed profiles for key tests
            for result in self.profiler.results:
                if any(keyword in result.name for keyword in ['large_batch', 'entity_scaling_100', 'cold_start']):
                    self.profiler.save_detailed_profile(result)

            print(f"\nDetailed analysis available in: {self.profiler.output_dir}/")
            print("Use 'snakeviz <profile_file>.prof' for interactive analysis")

            return comparison_df

        except Exception as e:
            print(f"Error during profiling: {e}")
            import traceback
            traceback.print_exc()
            return None


def main():
    """Main entry point for FeatureStore profiling."""
    print("Feast FeatureStore Performance Profiling")
    print("=" * 50)

    profiler = FeatureStoreProfiler()
    results = profiler.run_comprehensive_profiling()

    if results is not None:
        print("\nTop performance bottlenecks identified:")

        # Sort by total time and show top issues
        timing_cols = [col for col in results.columns if col.startswith('timing_')]
        if timing_cols and len(results) > 0:
            print("\nAverage operation times:")
            for col in timing_cols:
                if col in results.columns:
                    avg_time = results[col].mean()
                    if not pd.isna(avg_time) and avg_time > 0:
                        print(f"  {col}: {avg_time:.4f}s")


if __name__ == "__main__":
    main()