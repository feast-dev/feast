# How to Use Feast for SLM/LLM Post-Training (with Ray)

| Name | Type | Fields |
|---|---|---|
| `web_documents` | FeatureView | `human`, `bot`, `human_repeat_ratio`, `bot_repeat_ratio` |
| `train_example` | OnDemandFeatureView | `cleaned_human`, `cleaned_bot`, `char_count`, `is_trainable`, `sft_text` |
| `llm_posttrain` | FeatureService | `web_documents` + `train_example` |

Source data is **prepared parquet** (`document_id` + `event_timestamp` already present). No Feast core patches.

## Paths

| Flag | What happens |
|---|---|
| (default) | `to_ray_dataset()` + preprocess `sft_text` (ODFV does **not** run) |
| `--via-df` | `to_df()` so ODFV `train_example` runs |

## Setup

```bash
uv pip install -e "../../sdk/python[ray]" -r requirements.txt
PYTHONPATH=../../sdk/python python scripts/prepare_data.py
cd feature_repo && feast apply && cd ..
```

## Run (data load only)

```bash
PYTHONPATH=../../sdk/python python scripts/train_sft.py --dry-run
PYTHONPATH=../../sdk/python python scripts/train_sft.py --dry-run --via-df
```

## Blog

[How to Use Feast for SLM/LLM Post-Training with Ray](/blog/feast-ray-llm-posttrain)
