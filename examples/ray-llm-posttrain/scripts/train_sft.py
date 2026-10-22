#!/usr/bin/env python3
"""Feast conversation features → training rows (paths match the blog).

Paths:
  A) Default: get_historical_features → to_ray_dataset() → preprocess sft_text
     (ODFVs do NOT run on to_ray_dataset)
  B) --via-df: get_historical_features → to_df() (ODFV train_example runs)

    PYTHONPATH=../../sdk/python python scripts/train_sft.py --dry-run
    PYTHONPATH=../../sdk/python python scripts/train_sft.py --dry-run --via-df
"""

from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
FEATURE_REPO = REPO_ROOT / "feature_repo"
DATA_DIR = REPO_ROOT / "data"

# Matches the blog Option A snippet (length gate)
_MIN_CHARS = 64


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--max-steps", type=int, default=20)
    parser.add_argument("--batch-size", type=int, default=2)
    parser.add_argument("--max-length", type=int, default=256)
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DATA_DIR / "gpt2-sft",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print a few training rows; skip the optional GPT-2 smoke",
    )
    parser.add_argument(
        "--via-df",
        action="store_true",
        help="Use to_df() so OnDemandFeatureView train_example runs",
    )
    return parser.parse_args()


def _date_window() -> tuple[datetime, datetime]:
    return (
        datetime(2024, 6, 1, tzinfo=timezone.utc),
        datetime(2024, 7, 1, tzinfo=timezone.utc),
    )


def _preprocess_sft_batch(batch):
    """Build sft_text from FeatureView columns (blog Option A)."""
    import pandas as pd

    if not isinstance(batch, pd.DataFrame):
        batch = pd.DataFrame(batch)

    human = batch["human"].fillna("").astype(str).str.strip()
    bot = batch["bot"].fillna("").astype(str).str.strip()
    ok = bot.str.len() >= _MIN_CHARS
    sft_text = (
        "<|im_start|>user\n" + human + "<|im_end|>\n"
        "<|im_start|>assistant\n" + bot + "<|im_end|>"
    )
    return pd.DataFrame({"sft_text": sft_text}).loc[ok].reset_index(drop=True)


def retrieve_via_ray_stream():
    """Option A: to_ray_dataset() + preprocess (ODFV does not run)."""
    from feast import FeatureStore

    store = FeatureStore(repo_path=str(FEATURE_REPO))
    start_date, end_date = _date_window()

    print("get_historical_features → to_ray_dataset() (preprocess sft_text on Ray)")
    job = store.get_historical_features(
        features=[
            "web_documents:human",
            "web_documents:bot",
            "web_documents:human_repeat_ratio",
            "web_documents:bot_repeat_ratio",
        ],
        start_date=start_date,
        end_date=end_date,
    )
    ds = job.to_ray_dataset()
    return ds.map_batches(_preprocess_sft_batch, batch_format="pandas")


def retrieve_via_df():
    """Option B: to_df() so ODFV train_example runs, then Ray from pandas."""
    import ray
    from feast import FeatureStore

    store = FeatureStore(repo_path=str(FEATURE_REPO))
    start_date, end_date = _date_window()

    print("get_historical_features → to_df() (ODFV train_example runs)")
    df = store.get_historical_features(
        features=store.get_feature_service("llm_posttrain"),
        start_date=start_date,
        end_date=end_date,
    ).to_df()

    if "is_trainable" not in df.columns or "sft_text" not in df.columns:
        raise RuntimeError("Expected ODFV columns is_trainable / sft_text from to_df()")

    mask = df["is_trainable"].fillna(False).astype(bool)
    mask &= df["sft_text"].fillna("").astype(str).str.len() > 0
    slim = df.loc[mask, ["sft_text"]].reset_index(drop=True)
    return ray.data.from_pandas(slim)


def train_gpt2_optional(
    ray_ds, *, max_steps: int, batch_size: int, max_length: int, output_dir: Path
) -> None:
    import torch
    from transformers import (
        AutoModelForCausalLM,
        AutoTokenizer,
        DataCollatorForLanguageModeling,
        Trainer,
        TrainingArguments,
    )

    rows = ray_ds.take(min(500, max(50, max_steps * batch_size * 4)))
    texts = [r["sft_text"] for r in rows if r.get("sft_text")]
    if not texts:
        raise RuntimeError("No trainable SFT rows")

    print(f"[optional] GPT-2 smoke on {len(texts)} rows, {max_steps} steps...")
    tokenizer = AutoTokenizer.from_pretrained("gpt2")
    if tokenizer.pad_token is None:
        tokenizer.pad_token = tokenizer.eos_token
    model = AutoModelForCausalLM.from_pretrained("gpt2")
    encodings = tokenizer(
        texts,
        truncation=True,
        max_length=max_length,
        padding="max_length",
        return_tensors="pt",
    )

    class _TextDataset(torch.utils.data.Dataset):
        def __len__(self) -> int:
            return encodings["input_ids"].shape[0]

        def __getitem__(self, idx: int) -> dict:
            return {
                "input_ids": encodings["input_ids"][idx],
                "attention_mask": encodings["attention_mask"][idx],
                "labels": encodings["input_ids"][idx].clone(),
            }

    output_dir.mkdir(parents=True, exist_ok=True)
    args = TrainingArguments(
        output_dir=str(output_dir),
        per_device_train_batch_size=batch_size,
        max_steps=max_steps,
        logging_steps=max(1, max_steps // 5),
        save_steps=max_steps,
        learning_rate=5e-5,
        report_to=[],
        remove_unused_columns=False,
    )
    trainer = Trainer(
        model=model,
        args=args,
        train_dataset=_TextDataset(),
        data_collator=DataCollatorForLanguageModeling(tokenizer=tokenizer, mlm=False),
    )
    trainer.train()
    trainer.save_model(str(output_dir))
    tokenizer.save_pretrained(str(output_dir))
    print(f"Saved optional checkpoint to {output_dir}")


def main() -> int:
    args = _parse_args()
    if not (FEATURE_REPO / "feature_store.yaml").exists():
        print(f"Missing feature repo at {FEATURE_REPO}", file=sys.stderr)
        return 1

    if args.via_df:
        ds = retrieve_via_df()
    else:
        ds = retrieve_via_ray_stream()

    sample = ds.take(3)
    print(f"Sample training rows: {len(sample)}")
    for i, row in enumerate(sample):
        preview = str(row.get("sft_text", row))[:160].replace("\n", "\\n")
        print(f"  [{i}] {preview}...")

    if args.dry_run:
        print("Done (trainer skipped).")
        return 0

    train_gpt2_optional(
        ds,
        max_steps=args.max_steps,
        batch_size=args.batch_size,
        max_length=args.max_length,
        output_dir=args.output_dir,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
