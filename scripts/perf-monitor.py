#!/usr/bin/env python3
"""Performance monitoring for precommit hooks and tests"""

import time
import subprocess
import json
from pathlib import Path

def benchmark_command(cmd: str, description: str) -> dict:
    """Benchmark a command and return timing data"""
    print(f"Running: {description}")
    start_time = time.time()
    try:
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        end_time = time.time()
        duration = end_time - start_time
        success = result.returncode == 0

        print(f"  Duration: {duration:.2f}s - {'✅ SUCCESS' if success else '❌ FAILED'}")

        return {
            "description": description,
            "duration": duration,
            "success": success,
            "stdout_lines": len(result.stdout.splitlines()) if result.stdout else 0,
            "stderr_lines": len(result.stderr.splitlines()) if result.stderr else 0,
            "command": cmd
        }
    except Exception as e:
        duration = time.time() - start_time
        print(f"  Duration: {duration:.2f}s - ❌ ERROR: {str(e)}")
        return {
            "description": description,
            "duration": duration,
            "success": False,
            "error": str(e),
            "command": cmd
        }

def main():
    benchmarks = [
        ("make format-python", "Format Python code"),
        ("make lint-python", "Lint Python code"),
        ("make test-python-unit-fast", "Fast unit tests"),
        ("make precommit-check", "Combined precommit checks")
    ]

    print("🚀 Starting Feast performance benchmarks...")
    print("=" * 60)

    results = []
    total_start = time.time()

    for cmd, desc in benchmarks:
        result = benchmark_command(cmd, desc)
        results.append(result)
        print()

    total_duration = time.time() - total_start

    print("=" * 60)
    print(f"📊 Total benchmark time: {total_duration:.2f}s")
    print()

    # Print summary
    print("📋 Summary:")
    for result in results:
        status = "✅" if result["success"] else "❌"
        print(f"  {status} {result['description']}: {result['duration']:.2f}s")

    print()

    # Calculate performance improvements
    lint_time = sum(r['duration'] for r in results if 'lint' in r['description'].lower() or 'format' in r['description'].lower())
    print(f"🎯 Combined lint/format time: {lint_time:.2f}s")
    print(f"🎯 Target: <8s (current: {'✅' if lint_time < 8 else '❌'})")

    # Calculate other metrics
    test_time = sum(r['duration'] for r in results if 'test' in r['description'].lower())
    print(f"🎯 Test time: {test_time:.2f}s")
    print(f"🎯 Target: <120s (current: {'✅' if test_time < 120 else '❌'})")

    # Save results
    output_file = Path("performance-results.json")
    results_data = {
        "timestamp": time.time(),
        "total_duration": total_duration,
        "lint_format_time": lint_time,
        "results": results
    }

    output_file.write_text(json.dumps(results_data, indent=2))
    print(f"💾 Results saved to: {output_file}")

if __name__ == "__main__":
    main()
