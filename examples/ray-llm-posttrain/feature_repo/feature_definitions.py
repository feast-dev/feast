"""Feast feature definitions for Ray + ODFV LLM post-training.

Pipeline (supported Feast APIs only):
  scripts/prepare_data.py  → parquet with document_id + event_timestamp
    → RaySource (parquet)
    → FeatureView web_documents
    → OnDemandFeatureView train_example
    → FeatureService llm_posttrain
"""

from __future__ import annotations

from datetime import timedelta
from pathlib import Path

from feast import Entity, FeatureService, FeatureView, Field, ValueType
from feast.infra.offline_stores.contrib.ray_offline_store.ray_source import RaySource
from feast.on_demand_feature_view import on_demand_feature_view
from feast.types import Bool, Float64, Int64, String

_REPO_DIR = Path(__file__).resolve().parent
_PARQUET = str(_REPO_DIR / "data" / "tiny_webtext.parquet")

document = Entity(
    name="document",
    join_keys=["document_id"],
    value_type=ValueType.STRING,
    description="Document id (added by scripts/prepare_data.py)",
)

# Parquet already has document_id + event_timestamp (see prepare_data.py).
# Do not rely on BatchFeatureView UDFs to invent timestamps during entity-less
# retrieval — that path is not supported without Feast core changes.
tiny_web = RaySource(
    name="tiny_webtext",
    reader_type="parquet",
    path=_PARQUET,
    timestamp_field="event_timestamp",
)

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
    description="Conversation columns from prepared parquet",
    tags={"use_case": "llm_posttrain", "source": "parquet"},
)


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
    """Quality gate + human→bot SFT formatting."""
    import pandas as pd

    min_chars = 64
    max_repeat_ratio = 0.65

    cleaned_human = inputs["human"].fillna("").astype(str).str.strip()
    cleaned_bot = inputs["bot"].fillna("").astype(str).str.strip()
    char_count = cleaned_bot.str.len().astype("int64")

    human_ratio = inputs["human_repeat_ratio"].fillna(1.0).astype(float)
    bot_ratio = inputs["bot_repeat_ratio"].fillna(1.0).astype(float)
    is_trainable = (
        (char_count >= min_chars)
        & (human_ratio <= max_repeat_ratio)
        & (bot_ratio <= max_repeat_ratio)
    )

    sft_text = (
        "<|im_start|>user\n" + cleaned_human + "<|im_end|>\n"
        "<|im_start|>assistant\n" + cleaned_bot + "<|im_end|>"
    )

    return pd.DataFrame(
        {
            "cleaned_human": cleaned_human,
            "cleaned_bot": cleaned_bot,
            "char_count": char_count,
            "is_trainable": is_trainable,
            "sft_text": sft_text,
        }
    )


llm_posttrain = FeatureService(
    name="llm_posttrain",
    features=[web_documents, train_example],
    tags={"use_case": "llm_posttrain", "model": "gpt2"},
)
