"""
FastAPI Feature Server Performance Profiling

This script profiles Feast's FastAPI feature server endpoints to identify
bottlenecks in HTTP request handling, Protobuf serialization, JSON conversion,
and thread pool utilization.

Based on the implementation plan, this focuses on:
1. POST /get-online-features endpoint performance
2. Request parsing and validation overhead
3. Protobuf to JSON conversion (MessageToDict)
4. Thread pool utilization patterns
5. Concurrent request handling
6. Server startup overhead
"""

import time
import os
import sys
import subprocess
import signal
import asyncio
import aiohttp
import json
import threading
from typing import List, Dict, Any, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

# Add the current directory to Python path to import profiling_utils
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from profiling_utils import (
    FeastProfiler,
    generate_test_entities,
    generate_feature_lists
)


class FeatureServerProfiler:
    """Profiler for FastAPI feature server endpoints."""

    def __init__(self, repo_path: str = ".", host: str = "localhost", port: int = 6566):
        self.repo_path = repo_path
        self.host = host
        self.port = port
        self.base_url = f"http://{host}:{port}"
        self.server_process = None
        self.profiler = FeastProfiler(output_dir="profiling_results/feature_server")

    def start_feature_server(self, timeout: int = 30) -> bool:
        """Start the Feast feature server."""
        print(f"Starting Feast feature server on {self.host}:{self.port}...")

        with self.profiler.profile_context("server_startup") as result:
            with self.profiler.time_operation("server_start", result):
                # Start the server process
                cmd = ["feast", "serve", "--host", self.host, "--port", str(self.port)]
                self.server_process = subprocess.Popen(
                    cmd,
                    cwd=self.repo_path,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True
                )

            with self.profiler.time_operation("server_ready_wait", result):
                # Wait for server to be ready
                start_time = time.time()
                while time.time() - start_time < timeout:
                    try:
                        import requests
                        response = requests.get(f"{self.base_url}/health", timeout=1)
                        if response.status_code == 200:
                            print(f"Feature server ready after {time.time() - start_time:.2f}s")
                            return True
                    except Exception:
                        pass
                    time.sleep(0.5)

        print("Failed to start feature server within timeout")
        return False

    def stop_feature_server(self):
        """Stop the Feast feature server."""
        if self.server_process:
            print("Stopping feature server...")
            self.server_process.terminate()
            try:
                self.server_process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.server_process.kill()
                self.server_process.wait()
            self.server_process = None

    def create_get_online_features_payload(self, features: List[str], entities: List[Dict]) -> Dict:
        """Create payload for /get-online-features endpoint."""
        # Convert to the format expected by the API
        feature_refs = []
        for feature in features:
            if ":" in feature:
                fv_name, feature_name = feature.split(":", 1)
                feature_refs.append({
                    "feature_view_name": fv_name,
                    "feature_name": feature_name
                })
            else:
                # Handle feature services
                feature_refs.append({"feature_service_name": feature})

        return {
            "feature_service": None,  # Using individual features instead
            "entities": entities,
            "features": feature_refs,
            "full_feature_names": True
        }

    def profile_single_request(self, payload: Dict, test_name: str):
        """Profile a single HTTP request."""
        import requests

        with self.profiler.profile_context(f"single_request_{test_name}") as result:
            with self.profiler.time_operation("request_creation", result):
                headers = {"Content-Type": "application/json"}
                data = json.dumps(payload)

            with self.profiler.time_operation("http_request", result):
                response = requests.post(
                    f"{self.base_url}/get-online-features",
                    headers=headers,
                    data=data,
                    timeout=30
                )

            with self.profiler.time_operation("response_parsing", result):
                if response.status_code == 200:
                    response_data = response.json()
                else:
                    print(f"Request failed: {response.status_code} - {response.text}")
                    response_data = None

            # Add metadata
            result.add_timing("status_code", response.status_code)
            result.add_timing("response_size_bytes", len(response.content))
            if response_data:
                result.add_timing("feature_count", len(response_data.get("field_values", [])))

    async def profile_concurrent_requests(self, payloads: List[Dict], concurrency: int = 5):
        """Profile concurrent HTTP requests using aiohttp."""
        test_name = f"concurrent_{len(payloads)}_requests_{concurrency}_concurrent"

        with self.profiler.profile_context(test_name) as result:
            with self.profiler.time_operation("session_setup", result):
                async with aiohttp.ClientSession() as session:

                    async def make_request(payload: Dict, semaphore: asyncio.Semaphore):
                        async with semaphore:
                            async with session.post(
                                f"{self.base_url}/get-online-features",
                                headers={"Content-Type": "application/json"},
                                data=json.dumps(payload),
                                timeout=30
                            ) as response:
                                return await response.json() if response.status == 200 else None

            with self.profiler.time_operation("concurrent_requests", result):
                semaphore = asyncio.Semaphore(concurrency)
                tasks = [make_request(payload, semaphore) for payload in payloads]
                responses = await asyncio.gather(*tasks, return_exceptions=True)

            # Count successful responses
            successful = sum(1 for r in responses if r is not None and not isinstance(r, Exception))
            result.add_timing("successful_requests", successful)
            result.add_timing("total_requests", len(payloads))
            result.add_timing("concurrency", concurrency)

    def profile_request_size_scaling(self):
        """Profile how request size affects performance."""
        print("\n--- Profiling Request Size Scaling ---")

        features = generate_feature_lists()["standard"]
        entity_counts = [1, 10, 50, 100, 500]

        import requests
        for count in entity_counts:
            print(f"Testing {count} entities in single request...")
            entities = generate_test_entities(count)
            payload = self.create_get_online_features_payload(features, entities)

            test_name = f"request_size_{count}_entities"
            self.profile_single_request(payload, test_name)

    def profile_feature_complexity(self):
        """Profile different feature types and complexities."""
        print("\n--- Profiling Feature Complexity ---")

        feature_lists = generate_feature_lists()
        entities = generate_test_entities(10)

        import requests
        for list_name, features in feature_lists.items():
            print(f"Testing {list_name} feature set...")
            payload = self.create_get_online_features_payload(features, entities)

            test_name = f"feature_complexity_{list_name}"
            self.profile_single_request(payload, test_name)

    def profile_concurrent_load(self):
        """Profile concurrent request handling."""
        print("\n--- Profiling Concurrent Load ---")

        features = generate_feature_lists()["standard"]
        base_entities = generate_test_entities(5)

        # Create multiple different payloads
        payloads = []
        for i in range(20):
            entities = generate_test_entities(5, driver_id_range=(1001 + i, 1010 + i))
            payload = self.create_get_online_features_payload(features, entities)
            payloads.append(payload)

        # Test different concurrency levels
        concurrency_levels = [1, 3, 5, 10]

        for concurrency in concurrency_levels:
            print(f"Testing {len(payloads)} requests with concurrency {concurrency}...")
            asyncio.run(self.profile_concurrent_requests(payloads[:10], concurrency))

    def profile_feature_service_vs_direct(self):
        """Profile feature service requests vs direct feature requests."""
        print("\n--- Profiling Feature Service vs Direct Features ---")

        entities = generate_test_entities(10)

        # Direct features
        direct_features = [
            "driver_hourly_stats:conv_rate",
            "driver_hourly_stats:acc_rate"
        ]
        payload_direct = self.create_get_online_features_payload(direct_features, entities)
        self.profile_single_request(payload_direct, "direct_features")

        # Feature service (need to modify payload format for feature service)
        payload_service = {
            "feature_service": "driver_activity_v1",
            "entities": entities,
            "full_feature_names": True
        }

        import requests
        with self.profiler.profile_context("single_request_feature_service") as result:
            with self.profiler.time_operation("http_request", result):
                response = requests.post(
                    f"{self.base_url}/get-online-features",
                    headers={"Content-Type": "application/json"},
                    data=json.dumps(payload_service),
                    timeout=30
                )

            with self.profiler.time_operation("response_parsing", result):
                if response.status_code == 200:
                    response_data = response.json()
                    result.add_timing("status_code", response.status_code)

    def profile_error_handling(self):
        """Profile error handling performance."""
        print("\n--- Profiling Error Handling ---")

        import requests

        # Invalid feature request
        invalid_payload = {
            "features": [{"feature_view_name": "nonexistent", "feature_name": "fake"}],
            "entities": [{"driver_id": 1001}]
        }

        with self.profiler.profile_context("error_handling_invalid_feature") as result:
            with self.profiler.time_operation("invalid_request", result):
                response = requests.post(
                    f"{self.base_url}/get-online-features",
                    headers={"Content-Type": "application/json"},
                    data=json.dumps(invalid_payload),
                    timeout=30
                )

            result.add_timing("error_status_code", response.status_code)

        # Malformed JSON
        with self.profiler.profile_context("error_handling_malformed_json") as result:
            with self.profiler.time_operation("malformed_request", result):
                response = requests.post(
                    f"{self.base_url}/get-online-features",
                    headers={"Content-Type": "application/json"},
                    data="invalid json",
                    timeout=30
                )

            result.add_timing("malformed_status_code", response.status_code)

    def profile_health_endpoint(self):
        """Profile the health endpoint for baseline performance."""
        print("\n--- Profiling Health Endpoint ---")

        import requests
        with self.profiler.profile_context("health_endpoint") as result:
            for i in range(10):
                with self.profiler.time_operation(f"health_request_{i}", result):
                    response = requests.get(f"{self.base_url}/health", timeout=5)
                    result.add_timing(f"health_status_{i}", response.status_code)

    def run_comprehensive_profiling(self):
        """Run all feature server profiling scenarios."""
        print("Starting Comprehensive Feature Server Profiling")
        print("=" * 60)

        try:
            # Start the server
            if not self.start_feature_server():
                print("Failed to start feature server. Exiting.")
                return None

            # Wait a moment for server to fully initialize
            time.sleep(2)

            # Run profiling tests
            self.profile_health_endpoint()
            self.profile_request_size_scaling()
            self.profile_feature_complexity()
            self.profile_feature_service_vs_direct()
            self.profile_concurrent_load()
            self.profile_error_handling()

            print("\n" + "=" * 60)
            print("FEATURE SERVER PROFILING COMPLETE")

            # Generate reports
            self.profiler.print_summary()
            csv_file = self.profiler.generate_csv_report()

            # Save detailed profiles for key tests
            for result in self.profiler.results:
                if any(keyword in result.name for keyword in ['concurrent', 'request_size_500', 'startup']):
                    self.profiler.save_detailed_profile(result)

            print(f"\nDetailed analysis available in: {self.profiler.output_dir}/")

        except Exception as e:
            print(f"Error during profiling: {e}")
            import traceback
            traceback.print_exc()
        finally:
            # Always stop the server
            self.stop_feature_server()


def main():
    """Main entry point for feature server profiling."""
    print("Feast Feature Server Performance Profiling")
    print("=" * 50)

    # Check if requests and aiohttp are available
    try:
        import requests
        import aiohttp
    except ImportError as e:
        print(f"Required dependency missing: {e}")
        print("Please install with: pip install requests aiohttp")
        return

    profiler = FeatureServerProfiler()
    profiler.run_comprehensive_profiling()


if __name__ == "__main__":
    main()