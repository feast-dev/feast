---
title: "How to Use Feast for SLM/LLM Post-Training with Ray"
description: "Keep conversation features in Feast, retrieve them for training, then stream into your trainer with Ray."
date: 2026-07-14
authors: ["Chaitanya Patel"]
---

# How to Use Feast for SLM/LLM Post-Training with Ray

Your support bot answers a lot of tickets. It’s fine—but it sounds generic. The team wants a smaller model that talks more like *your* agents: your refund wording, your product names, your tone.

So someone says: **fine-tune on our real chats.**

That part sounds easy. The messy part is the data—exports, notebook cleaning, and prompt formatting scattered across training scripts.

This post walks through the [ray-llm-posttrain example](https://github.com/feast-dev/feast/tree/master/examples/ray-llm-posttrain):

1. Put conversation features in Feast  
2. Retrieve them with `get_historical_features` (entity-less date range)  
3. Get rows into your trainer — stream with Ray **or** materialize with `.to_df()`  

You bring your own trainer. GPT-2 in the script is optional smoke only.

## What’s in the example

| Name | Type | What it holds |
|---|---|---|
| `web_documents` | [FeatureView](https://docs.feast.dev/getting-started/concepts/feature-view) | `human`, `bot`, `human_repeat_ratio`, `bot_repeat_ratio` |
| `train_example` | [OnDemandFeatureView](https://docs.feast.dev/reference/beta-on-demand-feature-view) | `cleaned_human`, `cleaned_bot`, `char_count`, `is_trainable`, `sft_text` |
| `llm_posttrain` | FeatureService | Bundles `web_documents` + `train_example` |

Full definitions live in [feature_definitions.py](https://github.com/feast-dev/feast/blob/master/examples/ray-llm-posttrain/feature_repo/feature_definitions.py). Ray is the [offline store](https://docs.feast.dev/reference/offline-stores/ray) and one way to stream rows out—not a separate feature catalog.

This example stays on **supported Feast APIs only** (no core patches). Conversation rows already include `document_id` and `event_timestamp` before Feast reads them.

## Step 1: Point Feast at conversation data

### Ray offline store (local)

From the example [feature_store.yaml](https://github.com/feast-dev/feast/blob/master/examples/ray-llm-posttrain/feature_repo/feature_store.yaml). Cap Ray resources on a laptop—see [Ray offline store: resource management](https://docs.feast.dev/reference/offline-stores/ray#important-resource-management):

```yaml
project: ray_llm_posttrain
registry: data/registry.db
provider: local

offline_store:
  type: ray
  storage_path: data/ray_storage
  enable_ray_logging: false
  ray_conf:
    num_cpus: 2
    object_store_memory: 104857600
    _memory: 524288000

batch_engine:
  type: ray.engine
  max_workers: 2

online_store:
  type: sqlite
  path: data/online_store.db

entity_key_serialization_version: 3
auth:
  type: no_auth
```

You can also start from the built-in template:

```bash
feast init -t ray my_ray_project
```

See the [Ray template / offline store docs](https://docs.feast.dev/reference/offline-stores/ray#quick-start-with-ray-template) and the related blog [Scaling ML with Feast and Ray](/blog/feast-ray-distributed-processing).

### Demo seed: prepare parquet, then `RaySource`

[RaySource](https://docs.feast.dev/reference/data-sources/ray) tells Feast how to load data through Ray. Hugging Face is only used in a **prepare script**—not as a live Feast source that invents timestamps at retrieval time.

`nampdn-ai/tiny-webtext` has no `document_id` / `event_timestamp`. Entity-less retrieval needs those columns on the source. We add them **outside Feast**, write parquet, then point Feast at that file (supported path):

```bash
PYTHONPATH=../../sdk/python python scripts/prepare_data.py
# → feature_repo/data/tiny_webtext.parquet
```

```python
from feast.infra.offline_stores.contrib.ray_offline_store.ray_source import RaySource

tiny_web = RaySource(
    name="tiny_webtext",
    reader_type="parquet",
    path="data/tiny_webtext.parquet",
    timestamp_field="event_timestamp",
)
```

In production you’d skip the HF prepare step and register your real conversation store (warehouse / lake / parquet) that already has join keys and timestamps.

More reader types are in the [Ray data source reference](https://docs.feast.dev/reference/data-sources/ray#supported-reader_type-values).

### Feature view

```python
web_documents = FeatureView(
    name="web_documents",
    entities=[document],
    ttl=timedelta(days=365),
    schema=[
        Field(name="human", dtype=String),
        Field(name="bot", dtype=String),
        Field(name="human_repeat_ratio", dtype=Float64),
        Field(name="bot_repeat_ratio", dtype=Float64),
    ],
    source=tiny_web,
    online=False,
)
```

### Optional: OnDemandFeatureView for derived training features

If you want Feast to own `sft_text` / quality gates (same idea as in the [ODFV docs](https://docs.feast.dev/reference/beta-on-demand-feature-view)):

```python
@on_demand_feature_view(
    sources=[web_documents],
    schema=[
        Field(name="cleaned_human", dtype=String),
        Field(name="cleaned_bot", dtype=String),
        Field(name="char_count", dtype=Int64),
        Field(name="is_trainable", dtype=Bool),
        Field(name="sft_text", dtype=String),
    ],
    mode="pandas",
)
def train_example(inputs):
    cleaned_human = inputs["human"].fillna("").astype(str).str.strip()
    cleaned_bot = inputs["bot"].fillna("").astype(str).str.strip()
    # ... length + repeat-ratio gate ...
    sft_text = (
        "<|im_start|>user\n" + cleaned_human + "<|im_end|>\n"
        "<|im_start|>assistant\n" + cleaned_bot + "<|im_end|>"
    )
    return pd.DataFrame({...})
```

```python
llm_posttrain = FeatureService(
    name="llm_posttrain",
    features=[web_documents, train_example],
)
```

Apply:

```bash
cd examples/ray-llm-posttrain/feature_repo
feast apply
```

## Step 2: Retrieve for training (entity-less)

No `entity_df`—just a date window. That pattern is covered in [Historical Features Without Entity IDs](/blog/entity-less-historical-features-retrieval) and the [FAQ](https://docs.feast.dev/getting-started/faq#how-do-i-run-get_historical_features-without-providing-an-entity-dataframe):

```python
from datetime import datetime, timezone
from feast import FeatureStore

store = FeatureStore(repo_path="feature_repo")

job = store.get_historical_features(
    features=[
        "web_documents:human",
        "web_documents:bot",
        "web_documents:human_repeat_ratio",
        "web_documents:bot_repeat_ratio",
    ],
    start_date=datetime(2024, 6, 1, tzinfo=timezone.utc),
    end_date=datetime(2024, 7, 1, tzinfo=timezone.utc),
)
```

Then choose how you turn that job into training rows.

## Step 3: Two ways into the trainer

| Path | ODFV runs? | When to use |
|---|---|---|
| `job.to_ray_dataset()` then preprocess | **No** | Stream FeatureView columns; shape `sft_text` yourself |
| `job.to_df()` / `to_arrow()` | **Yes** | Want `train_example` outputs from Feast |

Pick **Option A** when you want full control over text formatting or need custom preprocessing (e.g., multi-turn chat templates, tokenization-aware truncation). Pick **Option B** when you want Feast to enforce quality gates consistently across training and serving.

### Option A — Stream with Ray, preprocess yourself

`to_ray_dataset()` returns a Ray Dataset of retrieved FeatureView columns. It does **not** apply OnDemandFeatureViews. Build training text with Ray `map_batches` (as in [train_sft.py](https://github.com/feast-dev/feast/blob/master/examples/ray-llm-posttrain/scripts/train_sft.py)):

```python
ds = job.to_ray_dataset()

def preprocess_sft(batch):
    import pandas as pd

    if not isinstance(batch, pd.DataFrame):
        batch = pd.DataFrame(batch)
    human = batch["human"].fillna("").astype(str).str.strip()
    bot = batch["bot"].fillna("").astype(str).str.strip()
    ok = bot.str.len() >= 64
    sft_text = (
        "<|im_start|>user\n" + human + "<|im_end|>\n"
        "<|im_start|>assistant\n" + bot + "<|im_end|>"
    )
    return pd.DataFrame({"sft_text": sft_text}).loc[ok].reset_index(drop=True)

train_ds = ds.map_batches(preprocess_sft, batch_format="pandas")
# → hand train_ds to your SLM/LLM trainer
```

Run the example default path:

```bash
PYTHONPATH=../../sdk/python python scripts/train_sft.py --dry-run
```

### Option B — Use the ODFV, then train

Materialize with `.to_df()` so `train_example` runs (same retrieval/serving idea as in the [ODFV overview](https://docs.feast.dev/reference/beta-on-demand-feature-view#why-use-on-demand-feature-views)):

```python
df = store.get_historical_features(
    features=store.get_feature_service("llm_posttrain"),
    start_date=datetime(2024, 6, 1, tzinfo=timezone.utc),
    end_date=datetime(2024, 7, 1, tzinfo=timezone.utc),
).to_df()

trainable = df[df["is_trainable"] & df["sft_text"].astype(str).str.len().gt(0)]
# trainable["sft_text"] → your trainer
# or: import ray; ray.data.from_pandas(trainable[["sft_text"]])
```

```bash
PYTHONPATH=../../sdk/python python scripts/train_sft.py --dry-run --via-df
```

### The same ODFV at serving time

The `train_example` ODFV runs identically during online serving — the quality gate and formatting logic stay in one place:

```python
# At inference time, the same ODFV runs on the fly
features = store.get_online_features(
    features=["train_example:sft_text", "train_example:is_trainable"],
    entity_rows=[{"document_id": "doc_42"}],
).to_dict()
# features["sft_text"], features["is_trainable"] — same logic as training
```

## Try the full example

```bash
cd examples/ray-llm-posttrain
uv pip install -e "../../sdk/python[ray]" -r requirements.txt
PYTHONPATH=../../sdk/python python scripts/prepare_data.py
cd feature_repo && feast apply && cd ..

PYTHONPATH=../../sdk/python python scripts/train_sft.py --dry-run
PYTHONPATH=../../sdk/python python scripts/train_sft.py --dry-run --via-df

# optional GPT-2 smoke
PYTHONPATH=../../sdk/python python scripts/train_sft.py --max-steps 20
```

Details: [ray-llm-posttrain README](https://github.com/feast-dev/feast/tree/master/examples/ray-llm-posttrain).

## Takeaways

1. **Keep conversation features in Feast** — this example’s `web_documents`.  
2. **Stream with Ray** — `to_ray_dataset()`, then preprocess training text yourself.  
3. **Want ODFVs** — `.to_df()` / `.to_arrow()` to materialize, then train.  
4. **Bring your own trainer** — GPT-2 in the example is optional.  

## References

**This example**

- [ray-llm-posttrain example](https://github.com/feast-dev/feast/tree/master/examples/ray-llm-posttrain)  
- [feature_definitions.py](https://github.com/feast-dev/feast/blob/master/examples/ray-llm-posttrain/feature_repo/feature_definitions.py)  
- [train_sft.py](https://github.com/feast-dev/feast/blob/master/examples/ray-llm-posttrain/scripts/train_sft.py)  

**Docs**

- [Ray offline store](https://docs.feast.dev/reference/offline-stores/ray)  
- [Ray data source](https://docs.feast.dev/reference/data-sources/ray)  
- [Ray compute engine](https://docs.feast.dev/reference/compute-engine/ray)  
- [On demand feature views](https://docs.feast.dev/reference/beta-on-demand-feature-view)  
- [Feature retrieval](https://docs.feast.dev/getting-started/concepts/feature-retrieval)  
- [FAQ: historical features without entity dataframe](https://docs.feast.dev/getting-started/faq#how-do-i-run-get_historical_features-without-providing-an-entity-dataframe)  

**Related blogs & tutorials**

- [Historical Features Without Entity IDs](/blog/entity-less-historical-features-retrieval)  
- [Scaling ML with Feast and Ray](/blog/feast-ray-distributed-processing)  
- [Validating historical features](https://docs.feast.dev/tutorials/validating-historical-features)  
