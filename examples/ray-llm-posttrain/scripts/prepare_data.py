#!/usr/bin/env python3
"""Prepare a small local parquet seed for the example.

Hugging Face tiny-webtext has no document_id / event_timestamp. Feast entity-less
retrieval needs those columns on the *source* data. We synthesize them here
(outside Feast) and write parquet — no Feast core changes required.

    PYTHONPATH=../../sdk/python python scripts/prepare_data.py
"""

from __future__ import annotations

import hashlib
from pathlib import Path

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
OUT_PATH = REPO_ROOT / "feature_repo" / "data" / "tiny_webtext.parquet"
SPLIT = "train[:2000]"
DATASET = "nampdn-ai/tiny-webtext"


def main() -> int:
    from datasets import load_dataset

    print(f"Loading {DATASET} split={SPLIT!r}...")
    ds = load_dataset(DATASET, split=SPLIT)
    df = ds.to_pandas()

    demo_base_ts = pd.Timestamp("2024-06-01", tz="UTC")
    demo_window_seconds = 30 * 24 * 3600

    humans = df["human"].fillna("").astype(str)
    bots = df["bot"].fillna("").astype(str)
    doc_ids: list[str] = []
    timestamps: list[pd.Timestamp] = []
    for human, bot in zip(humans, bots, strict=True):
        digest = hashlib.sha256(f"{human}\n{bot}".encode()).hexdigest()
        doc_ids.append(digest[:16])
        offset = int(digest[:8], 16) % demo_window_seconds
        timestamps.append(demo_base_ts + pd.Timedelta(seconds=offset))

    df = df.copy()
    df["document_id"] = doc_ids
    df["event_timestamp"] = timestamps

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(OUT_PATH, index=False)
    print(f"Wrote {len(df)} rows → {OUT_PATH}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
