"""Shared helpers for MLflow DataSource E2E scripts."""

from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent
STATE_PATH = ROOT / "seed_state.json"


def require_mlflow_env() -> str:
    uri = os.environ.get("MLFLOW_TRACKING_URI")
    if not uri:
        print(
            "ERROR: Set MLFLOW_TRACKING_URI (and MLFLOW_TRACKING_TOKEN for RHOAI auth).",
            file=sys.stderr,
        )
        sys.exit(1)
    return uri


def load_seed_state() -> dict:
    if not STATE_PATH.is_file():
        return {}
    return json.loads(STATE_PATH.read_text())


def save_seed_state(state: dict) -> None:
    STATE_PATH.write_text(json.dumps(state, indent=2))


def sample_frame() -> pd.DataFrame:
    now = datetime.now(tz=timezone.utc)
    return pd.DataFrame(
        {
            "record_id": ["r1", "r2", "r3"],
            "score": [0.9, 0.8, 0.95],
            "category": ["A", "B", "A"],
            "event_timestamp": [
                now,
                now,
                now,
            ],
        }
    )
