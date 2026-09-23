#!/usr/bin/env python3
"""CLI E2E: feast mlflow subcommands."""

from __future__ import annotations

import os
import subprocess
import sys

from _common import ROOT, load_seed_state, require_mlflow_env

os.chdir(ROOT)
require_mlflow_env()
if "parquet_run_id" not in load_seed_state():
    print("ERROR: Run seed_mlflow.py first.", file=sys.stderr)
    sys.exit(1)


def run(cmd: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(cmd, capture_output=True, text=True, check=False)


help_result = run(["feast", "mlflow", "--help"])
if help_result.returncode != 0 or "sync-dataset" not in help_result.stdout:
    print(help_result.stderr or help_result.stdout, file=sys.stderr)
    sys.exit(1)

list_result = run(["feast", "mlflow", "list-sources"])
if list_result.returncode != 0:
    print(list_result.stderr, file=sys.stderr)
    sys.exit(1)

validate_result = run(
    ["feast", "mlflow", "validate-source", "mlflow_parquet_features"]
)
if validate_result.returncode != 0:
    print(validate_result.stderr or validate_result.stdout, file=sys.stderr)
    sys.exit(1)

dry_run = run(
    [
        "feast",
        "mlflow",
        "sync-dataset",
        "--feature-view",
        "mlflow_parquet_features",
        "--dry-run",
    ]
)
if dry_run.returncode != 0:
    print(dry_run.stderr or dry_run.stdout, file=sys.stderr)
    sys.exit(1)

print("test_cli: OK")
