"""
Shared utilities for Feast performance profiling.

This module provides common functionality for profiling different components
of Feast including cProfile management, timing measurements, memory tracking,
and report generation.
"""

import cProfile
import pstats
import time
import tracemalloc
import csv
import os
import io
import functools
from typing import Dict, List, Any, Optional, Callable
from contextlib import contextmanager
from datetime import datetime
import pandas as pd


class ProfilingResults:
    """Container for profiling results including timing and memory data."""

    def __init__(self, name: str):
        self.name = name
        self.timing_results: Dict[str, float] = {}
        self.memory_results: Dict[str, Any] = {}
        self.profiler_stats: Optional[pstats.Stats] = None
        self.start_time: Optional[float] = None
        self.end_time: Optional[float] = None

    def add_timing(self, operation: str, duration: float):
        """Add timing result for an operation."""
        self.timing_results[operation] = duration

    def add_memory_snapshot(self, operation: str, snapshot):
        """Add memory snapshot for an operation."""
        self.memory_results[operation] = snapshot

    def get_total_time(self) -> float:
        """Get total profiling duration."""
        if self.start_time and self.end_time:
            return self.end_time - self.start_time
        return 0.0


class FeastProfiler:
    """Main profiler class for Feast components."""

    def __init__(self, output_dir: str = "profiling_results"):
        self.output_dir = output_dir
        self.results: List[ProfilingResults] = []
        self._ensure_output_dir()

    def _ensure_output_dir(self):
        """Create output directory if it doesn't exist."""
        os.makedirs(self.output_dir, exist_ok=True)

    @contextmanager
    def profile_context(self, name: str, enable_memory: bool = True):
        """Context manager for profiling a block of code."""
        result = ProfilingResults(name)

        # Start memory tracking
        if enable_memory:
            tracemalloc.start()

        # Start cProfile
        profiler = cProfile.Profile()
        profiler.enable()

        # Start timing
        result.start_time = time.perf_counter()

        try:
            yield result
        finally:
            # Stop timing
            result.end_time = time.perf_counter()

            # Stop cProfile
            profiler.disable()
            result.profiler_stats = pstats.Stats(profiler)

            # Stop memory tracking
            if enable_memory:
                snapshot = tracemalloc.take_snapshot()
                result.add_memory_snapshot("final", snapshot)
                tracemalloc.stop()

            self.results.append(result)

    def profile_function(self, enable_memory: bool = True):
        """Decorator for profiling functions."""
        def decorator(func: Callable):
            @functools.wraps(func)
            def wrapper(*args, **kwargs):
                with self.profile_context(func.__name__, enable_memory) as result:
                    return func(*args, **kwargs)
            return wrapper
        return decorator

    @contextmanager
    def time_operation(self, name: str, result: ProfilingResults):
        """Context manager for timing individual operations."""
        start_time = time.perf_counter()
        try:
            yield
        finally:
            end_time = time.perf_counter()
            result.add_timing(name, end_time - start_time)

    def generate_csv_report(self, filename: str = None):
        """Generate CSV report summarizing all profiling results."""
        if filename is None:
            filename = f"profiling_summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"

        filepath = os.path.join(self.output_dir, filename)

        rows = []
        for result in self.results:
            # Basic stats
            row = {
                "test_name": result.name,
                "total_time": result.get_total_time(),
                "timestamp": datetime.now().isoformat()
            }

            # Add individual timing results
            for op, duration in result.timing_results.items():
                row[f"timing_{op}"] = duration

            # Add top function stats if available
            if result.profiler_stats:
                # Redirect stdout temporarily to capture print_stats output
                import sys
                from contextlib import redirect_stdout
                stats_io = io.StringIO()

                with redirect_stdout(stats_io):
                    result.profiler_stats.print_stats(10)

                # Parse top functions for CSV
                stats_text = stats_io.getvalue()
                lines = stats_text.split('\n')
                for i, line in enumerate(lines):
                    if 'cumulative' in line and i + 1 < len(lines):
                        # Extract top function data
                        top_function = lines[i + 1].strip()
                        if top_function:
                            row["top_function"] = top_function
                            break

            # Add memory stats if available
            if "final" in result.memory_results:
                snapshot = result.memory_results["final"]
                top_stats = snapshot.statistics('filename')[:5]
                total_memory = sum(stat.size for stat in top_stats)
                row["memory_mb"] = total_memory / (1024 * 1024)

            rows.append(row)

        # Write CSV
        if rows:
            with open(filepath, 'w', newline='') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=rows[0].keys())
                writer.writeheader()
                writer.writerows(rows)

        print(f"CSV report generated: {filepath}")
        return filepath

    def save_detailed_profile(self, result: ProfilingResults, filename: str = None):
        """Save detailed cProfile output for a specific result."""
        if filename is None:
            filename = f"{result.name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.prof"

        filepath = os.path.join(self.output_dir, filename)

        if result.profiler_stats:
            result.profiler_stats.dump_stats(filepath)
            print(f"Detailed profile saved: {filepath}")
            print(f"View with: snakeviz {filepath}")
            return filepath
        return None

    def print_summary(self):
        """Print a summary of all profiling results."""
        print("\n" + "="*60)
        print("FEAST PROFILING SUMMARY")
        print("="*60)

        for result in self.results:
            print(f"\nTest: {result.name}")
            print(f"Total Time: {result.get_total_time():.4f}s")

            if result.timing_results:
                print("Operation Timings:")
                for op, duration in result.timing_results.items():
                    print(f"  {op}: {duration:.4f}s")

            if result.profiler_stats:
                print("Top Functions (cumulative time):")
                result.profiler_stats.sort_stats('cumulative')
                result.profiler_stats.print_stats(5)

            if "final" in result.memory_results:
                snapshot = result.memory_results["final"]
                top_stats = snapshot.statistics('filename')[:3]
                total_memory = sum(stat.size for stat in top_stats)
                print(f"Memory Usage: {total_memory / (1024 * 1024):.2f} MB")

        print("="*60)


def generate_test_entities(count: int, driver_id_range: tuple = (1001, 1010)) -> List[Dict[str, Any]]:
    """Generate test entity rows for profiling."""
    import random

    entities = []
    start_id, end_id = driver_id_range

    for i in range(count):
        entities.append({
            "driver_id": random.randint(start_id, end_id),
            "val_to_add": random.randint(1, 1000),
            "val_to_add_2": random.randint(1000, 2000),
        })

    return entities


def generate_feature_lists() -> Dict[str, List[str]]:
    """Generate different feature lists for testing."""
    return {
        "minimal": ["driver_hourly_stats:conv_rate"],
        "standard": [
            "driver_hourly_stats:conv_rate",
            "driver_hourly_stats:acc_rate",
            "driver_hourly_stats:avg_daily_trips"
        ],
        "with_odfv": [
            "driver_hourly_stats:conv_rate",
            "driver_hourly_stats:acc_rate",
            "transformed_conv_rate:conv_rate_plus_val1",
            "transformed_conv_rate:conv_rate_plus_val2"
        ],
        "all_features": [
            "driver_hourly_stats:conv_rate",
            "driver_hourly_stats:acc_rate",
            "driver_hourly_stats:avg_daily_trips",
            "transformed_conv_rate:conv_rate_plus_val1",
            "transformed_conv_rate:conv_rate_plus_val2"
        ]
    }


def create_performance_comparison(profiler: FeastProfiler, output_file: str = None):
    """Create a performance comparison DataFrame from profiling results."""
    if output_file is None:
        output_file = f"performance_comparison_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"

    data = []
    for result in profiler.results:
        row = {
            'test_name': result.name,
            'total_time': result.get_total_time()
        }

        # Extract entity count and feature count from test name if possible
        name_parts = result.name.split('_')
        for part in name_parts:
            if 'entities' in part:
                try:
                    row['entity_count'] = int(part.replace('entities', ''))
                except ValueError:
                    pass
            elif 'features' in part:
                try:
                    row['feature_count'] = int(part.replace('features', ''))
                except ValueError:
                    pass

        # Add timing breakdowns
        for op, duration in result.timing_results.items():
            row[f'timing_{op}'] = duration

        # Add top function if available
        if result.profiler_stats:
            result.profiler_stats.sort_stats('cumulative')
            stats = result.profiler_stats.get_stats()
            if stats:
                # Get the function with highest cumulative time (excluding <built-in>)
                for func, (cc, nc, tt, ct, callers) in stats.items():
                    if not func[0].startswith('<') and ct > 0:
                        row['top_function'] = f"{func[2]}:{func[0]}:{func[1]}"
                        row['top_function_cumtime'] = ct
                        break

        data.append(row)

    df = pd.DataFrame(data)

    if output_file:
        filepath = os.path.join(profiler.output_dir, output_file)
        df.to_csv(filepath, index=False)
        print(f"Performance comparison saved: {filepath}")

    return df


def memory_usage_analysis(snapshot, top_n: int = 10):
    """Analyze memory usage from a tracemalloc snapshot."""
    top_stats = snapshot.statistics('lineno')[:top_n]

    print("Top memory allocations:")
    for index, stat in enumerate(top_stats, 1):
        print(f"{index:2}. {stat.traceback.format()[-1]}")
        print(f"    Size: {stat.size / 1024 / 1024:.2f} MB")
        print(f"    Count: {stat.count}")

    total = sum(stat.size for stat in snapshot.statistics('filename'))
    print(f"\nTotal allocated size: {total / 1024 / 1024:.2f} MB")

    return {
        'top_stats': top_stats,
        'total_mb': total / 1024 / 1024
    }


if __name__ == "__main__":
    # Example usage
    profiler = FeastProfiler()

    with profiler.profile_context("example_test") as result:
        with profiler.time_operation("setup", result):
            time.sleep(0.1)  # Simulate setup work

        with profiler.time_operation("main_work", result):
            time.sleep(0.2)  # Simulate main work

    profiler.print_summary()
    profiler.generate_csv_report()