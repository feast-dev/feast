#!/usr/bin/env python3
"""Benchmark stable and experimental feature-server containers side by side."""

from __future__ import annotations

import argparse
import concurrent.futures
import http.client
import json
import shutil
import statistics
import subprocess
import tempfile
import time
import urllib.error
import urllib.request
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path


DEFAULT_REQUEST = {
    "features": [
        "driver_hourly_stats:conv_rate",
        "driver_hourly_stats:acc_rate",
        "driver_hourly_stats:avg_daily_trips",
    ],
    "entities": {
        "driver_id": [1001, 1002, 1003],
    },
}


@dataclass
class BenchmarkResult:
    image: str
    endpoint: str
    total_requests: int
    concurrency: int
    warmup_requests: int
    successful_requests: int
    failed_requests: int
    elapsed_seconds: float
    requests_per_second: float
    mean_latency_ms: float
    median_latency_ms: float
    p95_latency_ms: float
    min_latency_ms: float
    max_latency_ms: float


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=__doc__,
    )
    parser.add_argument("--baseline-image", required=True)
    parser.add_argument("--experimental-image", required=True)
    parser.add_argument(
        "--sample-repo",
        default="examples/podman_local/feature_repo",
        help="Feature repo used to prepare and serve benchmark traffic.",
    )
    parser.add_argument("--requests", type=int, default=400)
    parser.add_argument("--concurrency", type=int, default=32)
    parser.add_argument("--warmup-requests", type=int, default=50)
    parser.add_argument("--workers", type=int, default=1)
    parser.add_argument("--worker-connections", type=int, default=1000)
    parser.add_argument("--max-requests", type=int, default=1000)
    parser.add_argument("--registry-ttl-sec", type=int, default=60)
    parser.add_argument(
        "--output",
        default="feature-server-experimental-benchmark.json",
        help="Path to the JSON benchmark report.",
    )
    return parser.parse_args()


def run(
    cmd: list[str], *, check: bool = True, capture_output: bool = True
) -> subprocess.CompletedProcess[str]:
    try:
        return subprocess.run(
            cmd,
            check=check,
            text=True,
            capture_output=capture_output,
        )
    except subprocess.CalledProcessError as exc:
        if exc.stderr:
            print(exc.stderr.strip())
        raise


def post_json(url: str, payload: dict) -> dict:
    body = json.dumps(payload).encode("utf-8")
    request = urllib.request.Request(
        url,
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        return json.loads(response.read().decode("utf-8"))


def wait_for_server(endpoint: str, payload: dict, timeout_seconds: int = 60) -> None:
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        try:
            response = post_json(f"{endpoint}/get-online-features", payload)
            if "metadata" in response and "results" in response:
                return
        except (
            TimeoutError,
            urllib.error.URLError,
            urllib.error.HTTPError,
            http.client.RemoteDisconnected,
            json.JSONDecodeError,
            ConnectionResetError,
        ):
            pass
        time.sleep(1)
    raise TimeoutError(
        f"Server at {endpoint} did not become ready within {timeout_seconds}s"
    )


def percentile(values_ms: list[float], percentile_value: float) -> float:
    if not values_ms:
        return 0.0
    if len(values_ms) == 1:
        return values_ms[0]
    ordered = sorted(values_ms)
    index = int(round((len(ordered) - 1) * percentile_value))
    return ordered[index]


def benchmark_endpoint(
    endpoint: str,
    image: str,
    request_payload: dict,
    requests: int,
    concurrency: int,
    warmup_requests: int,
) -> BenchmarkResult:
    for _ in range(warmup_requests):
        post_json(f"{endpoint}/get-online-features", request_payload)

    latencies_ms: list[float] = []
    failures = 0

    def one_request(_: int) -> float:
        start = time.perf_counter()
        response = post_json(f"{endpoint}/get-online-features", request_payload)
        if "metadata" not in response:
            raise RuntimeError(f"Unexpected response payload: {response}")
        return (time.perf_counter() - start) * 1000.0

    started = time.perf_counter()
    with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as executor:
        futures = [executor.submit(one_request, index) for index in range(requests)]
        for future in concurrent.futures.as_completed(futures):
            try:
                latencies_ms.append(future.result())
            except Exception:
                failures += 1
    elapsed_seconds = time.perf_counter() - started

    successes = len(latencies_ms)
    return BenchmarkResult(
        image=image,
        endpoint=endpoint,
        total_requests=requests,
        concurrency=concurrency,
        warmup_requests=warmup_requests,
        successful_requests=successes,
        failed_requests=failures,
        elapsed_seconds=elapsed_seconds,
        requests_per_second=(successes / elapsed_seconds) if elapsed_seconds else 0.0,
        mean_latency_ms=statistics.fmean(latencies_ms) if latencies_ms else 0.0,
        median_latency_ms=statistics.median(latencies_ms) if latencies_ms else 0.0,
        p95_latency_ms=percentile(latencies_ms, 0.95),
        min_latency_ms=min(latencies_ms) if latencies_ms else 0.0,
        max_latency_ms=max(latencies_ms) if latencies_ms else 0.0,
    )


def docker_run(
    repo_dir: Path, image: str, *args: str
) -> subprocess.CompletedProcess[str]:
    return run(
        [
            "docker",
            "run",
            "--rm",
            "-v",
            f"{repo_dir}:/feature_repo",
            image,
            *args,
        ]
    )


def prepare_repo(repo_dir: Path, baseline_image: str) -> None:
    today = datetime.now(timezone.utc).date().isoformat()
    docker_run(repo_dir, baseline_image, "feast", "-c", "/feature_repo", "apply")
    docker_run(
        repo_dir,
        baseline_image,
        "feast",
        "-c",
        "/feature_repo",
        "materialize-incremental",
        today,
    )


def start_server_container(
    repo_dir: Path,
    image: str,
    name: str,
    host_port: int,
    workers: int,
    worker_connections: int,
    max_requests: int,
    registry_ttl_sec: int,
) -> str:
    run(
        [
            "docker",
            "run",
            "--rm",
            "-d",
            "--name",
            name,
            "-p",
            f"{host_port}:6566",
            "-v",
            f"{repo_dir}:/feature_repo",
            image,
            "feast",
            "-c",
            "/feature_repo",
            "serve",
            "-h",
            "0.0.0.0",
            "--workers",
            str(workers),
            "--worker-connections",
            str(worker_connections),
            "--max-requests",
            str(max_requests),
            "--registry_ttl_sec",
            str(registry_ttl_sec),
            "--no-access-log",
        ]
    )
    return f"http://127.0.0.1:{host_port}"


def stop_server_container(name: str) -> None:
    run(["docker", "rm", "-f", name], check=False)


def find_free_port() -> int:
    import socket

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def main() -> int:
    args = parse_args()

    if shutil.which("docker") is None:
        raise RuntimeError(
            "docker is required to run the experimental feature server benchmark"
        )

    workspace = Path.cwd()
    sample_repo = (workspace / args.sample_repo).resolve()
    if not sample_repo.exists():
        raise FileNotFoundError(f"Sample repo not found: {sample_repo}")

    output_path = (workspace / args.output).resolve()

    with tempfile.TemporaryDirectory(
        prefix="feast-feature-server-benchmark-",
        dir=workspace,
    ) as temp_dir:
        temp_repo = Path(temp_dir) / "feature_repo"
        shutil.copytree(sample_repo, temp_repo)

        print(f"Preparing benchmark repo in {temp_repo}")
        prepare_repo(temp_repo, args.baseline_image)

        results: list[BenchmarkResult] = []
        for label, image in (
            ("baseline", args.baseline_image),
            ("experimental", args.experimental_image),
        ):
            container_name = f"feast-feature-server-{label}-{int(time.time())}"
            port = find_free_port()
            print(f"Starting {label} container {image} on port {port}")
            endpoint = start_server_container(
                temp_repo,
                image,
                container_name,
                port,
                args.workers,
                args.worker_connections,
                args.max_requests,
                args.registry_ttl_sec,
            )
            try:
                wait_for_server(endpoint, DEFAULT_REQUEST)
                result = benchmark_endpoint(
                    endpoint,
                    image,
                    DEFAULT_REQUEST,
                    args.requests,
                    args.concurrency,
                    args.warmup_requests,
                )
                results.append(result)
                print(
                    f"{label}: {result.requests_per_second:.2f} req/s, "
                    f"p95={result.p95_latency_ms:.2f}ms, "
                    f"failures={result.failed_requests}"
                )
            finally:
                stop_server_container(container_name)

    summary = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "sample_repo": str(sample_repo),
        "request_payload": DEFAULT_REQUEST,
        "results": [asdict(result) for result in results],
    }

    if len(results) == 2:
        baseline, experimental = results
        summary["comparison"] = {
            "throughput_gain_ratio": (
                experimental.requests_per_second / baseline.requests_per_second
                if baseline.requests_per_second
                else 0.0
            ),
            "latency_p95_improvement_ratio": (
                baseline.p95_latency_ms / experimental.p95_latency_ms
                if experimental.p95_latency_ms
                else 0.0
            ),
            "successful_request_delta": experimental.successful_requests
            - baseline.successful_requests,
        }

    output_path.write_text(json.dumps(summary, indent=2))
    print(f"Benchmark report written to {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
