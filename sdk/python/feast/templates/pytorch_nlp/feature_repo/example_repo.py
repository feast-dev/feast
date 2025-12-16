"""
PyTorch NLP Sentiment Analysis Feature Repository

This template demonstrates sentiment analysis using:
- Text feature engineering for NLP
- PyTorch + Hugging Face transformers integration
- On-demand sentiment prediction features
- Online and offline feature serving patterns
"""

from datetime import timedelta
from pathlib import Path

import pandas as pd

from feast import (
    Entity,
    FeatureService,
    FeatureView,
    Field,
    FileSource,
    RequestSource,
    ValueType,
)
from feast.on_demand_feature_view import on_demand_feature_view
from feast.types import Array, Float32, Int64, String

# Configuration
repo_path = Path(__file__).parent
data_path = repo_path / "data"

# Define entities - primary keys for joining data
text_entity = Entity(
    name="text",
    join_keys=["text_id"],
    value_type=ValueType.STRING,
    description="Unique identifier for text samples",
)

user_entity = Entity(
    name="user",
    join_keys=["user_id"],
    value_type=ValueType.STRING,
    description="User who created the text content",
)

# Data source - points to the parquet file created by bootstrap
sentiment_source = FileSource(
    name="sentiment_data_source",
    path=str(data_path / "sentiment_data.parquet"),
    timestamp_field="event_timestamp",
    created_timestamp_column="created",
)

# Feature view for text metadata and engineered features
text_features_fv = FeatureView(
    name="text_features",
    entities=[text_entity],
    ttl=timedelta(days=7),  # Keep features for 7 days
    schema=[
        Field(name="text_content", dtype=String, description="Raw text content"),
        Field(
            name="sentiment_label",
            dtype=String,
            description="Ground truth sentiment label",
        ),
        Field(
            name="sentiment_score",
            dtype=Float32,
            description="Ground truth sentiment score",
        ),
        Field(name="text_length", dtype=Int64, description="Character count of text"),
        Field(name="word_count", dtype=Int64, description="Word count of text"),
        Field(
            name="exclamation_count",
            dtype=Int64,
            description="Number of exclamation marks",
        ),
        Field(name="caps_ratio", dtype=Float32, description="Ratio of capital letters"),
        Field(
            name="emoji_count", dtype=Int64, description="Number of emoji characters"
        ),
    ],
    online=True,
    source=sentiment_source,
    tags={"team": "nlp", "domain": "sentiment_analysis"},
)

# Feature view for user-level aggregations
user_stats_fv = FeatureView(
    name="user_stats",
    entities=[user_entity],
    ttl=timedelta(days=30),  # User stats change less frequently
    schema=[
        Field(
            name="user_avg_sentiment",
            dtype=Float32,
            description="User's average sentiment score",
        ),
        Field(
            name="user_text_count",
            dtype=Int64,
            description="Total number of texts by user",
        ),
        Field(
            name="user_avg_text_length",
            dtype=Float32,
            description="User's average text length",
        ),
    ],
    online=True,
    source=sentiment_source,
    tags={"team": "nlp", "domain": "user_behavior"},
)

# Request source for real-time inference
text_input_request = RequestSource(
    name="text_input",
    schema=[
        Field(
            name="input_text",
            dtype=String,
            description="Text to analyze at request time",
        ),
        Field(
            name="model_name", dtype=String, description="Model to use for prediction"
        ),
    ],
)


# On-demand feature view for real-time sentiment prediction
@on_demand_feature_view(
    sources=[text_input_request],
    schema=[
        Field(name="predicted_sentiment", dtype=String),
        Field(name="sentiment_confidence", dtype=Float32),
        Field(name="positive_prob", dtype=Float32),
        Field(name="negative_prob", dtype=Float32),
        Field(name="neutral_prob", dtype=Float32),
        Field(name="text_embedding", dtype=Array(Float32)),
    ],
)
def sentiment_prediction(inputs: pd.DataFrame) -> pd.DataFrame:
    """
    Real-time sentiment prediction using pre-trained models.

    This function demonstrates how to integrate PyTorch/HuggingFace models
    directly into Feast feature views for real-time inference.
    """
    try:
        import numpy as np
        from transformers import pipeline
    except ImportError:
        # Fallback to dummy predictions if dependencies aren't available
        df = pd.DataFrame()
        df["predicted_sentiment"] = ["neutral"] * len(inputs)
        df["sentiment_confidence"] = np.array([0.5] * len(inputs), dtype=np.float32)
        df["positive_prob"] = np.array([0.33] * len(inputs), dtype=np.float32)
        df["negative_prob"] = np.array([0.33] * len(inputs), dtype=np.float32)
        df["neutral_prob"] = np.array([0.34] * len(inputs), dtype=np.float32)
        df["text_embedding"] = [[np.float32(0.0)] * 384] * len(inputs)
        return df

    # Initialize model (in production, you'd want to cache this)
    model_name = "cardiffnlp/twitter-roberta-base-sentiment-latest"
    try:
        # Use sentiment pipeline for convenience (force CPU to avoid MPS forking issues)
        sentiment_pipeline = pipeline(
            "sentiment-analysis",
            model=model_name,
            tokenizer=model_name,
            return_all_scores=True,
            device="cpu",  # Force CPU to avoid MPS forking issues on macOS
        )

    except Exception:
        # Fallback if model loading fails
        df = pd.DataFrame()
        df["predicted_sentiment"] = ["neutral"] * len(inputs)
        df["sentiment_confidence"] = np.array([0.5] * len(inputs), dtype=np.float32)
        df["positive_prob"] = np.array([0.33] * len(inputs), dtype=np.float32)
        df["negative_prob"] = np.array([0.33] * len(inputs), dtype=np.float32)
        df["neutral_prob"] = np.array([0.34] * len(inputs), dtype=np.float32)
        df["text_embedding"] = [[np.float32(0.0)] * 384] * len(inputs)
        return df

    results = []

    for text in inputs["input_text"]:
        try:
            # Get sentiment predictions
            predictions = sentiment_pipeline(text)

            # Parse results (RoBERTa model returns LABEL_0, LABEL_1, LABEL_2)
            label_map = {
                "LABEL_0": "negative",
                "LABEL_1": "neutral",
                "LABEL_2": "positive",
            }

            scores = {
                label_map.get(pred["label"], pred["label"]): pred["score"]
                for pred in predictions
            }

            # Get best prediction
            best_pred = max(predictions, key=lambda x: x["score"])
            predicted_sentiment = label_map.get(best_pred["label"], best_pred["label"])
            confidence = best_pred["score"]

            # Get embeddings (simplified - dummy embeddings for demo)
            # In a real implementation, you'd run the model to get embeddings
            # For this demo, we'll create a dummy embedding
            embedding = np.random.rand(384).tolist()  # DistilBERT size

            results.append(
                {
                    "predicted_sentiment": predicted_sentiment,
                    "sentiment_confidence": np.float32(confidence),
                    "positive_prob": np.float32(scores.get("positive", 0.0)),
                    "negative_prob": np.float32(scores.get("negative", 0.0)),
                    "neutral_prob": np.float32(scores.get("neutral", 0.0)),
                    "text_embedding": [np.float32(x) for x in embedding],
                }
            )

        except Exception:
            # Fallback for individual text processing errors
            results.append(
                {
                    "predicted_sentiment": "neutral",
                    "sentiment_confidence": np.float32(0.5),
                    "positive_prob": np.float32(0.33),
                    "negative_prob": np.float32(0.33),
                    "neutral_prob": np.float32(0.34),
                    "text_embedding": [np.float32(0.0)] * 384,
                }
            )

    return pd.DataFrame(results)


# Feature services group related features for model serving
sentiment_analysis_v1 = FeatureService(
    name="sentiment_analysis_v1",
    features=[
        text_features_fv[["text_content", "text_length", "word_count"]],
        sentiment_prediction,
    ],
    description="Basic sentiment analysis features for model v1",
)

sentiment_analysis_v2 = FeatureService(
    name="sentiment_analysis_v2",
    features=[
        text_features_fv,  # All text features
        user_stats_fv[["user_avg_sentiment", "user_text_count"]],  # User context
        sentiment_prediction,  # Real-time predictions
    ],
    description="Advanced sentiment analysis with user context for model v2",
)

# Feature service for training data (historical features only)
sentiment_training_features = FeatureService(
    name="sentiment_training_features",
    features=[
        text_features_fv,
        user_stats_fv,
    ],
    description="Historical features for model training and evaluation",
)
