"""
Static Artifacts Loading for PyTorch NLP Template

This module demonstrates how to load static artifacts (models, lookup tables, etc.)
into the Feast feature server at startup for efficient real-time inference.

Supported artifact types:
- Small ML models (transformers, scikit-learn, etc.)
- Lookup tables and reference data
- Configuration parameters
- Pre-computed embeddings

Note: Feast is not optimized for large language models. For LLM inference,
use dedicated model serving solutions like vLLM, TensorRT-LLM, or TGI.
"""

from pathlib import Path
from typing import Any, Dict, Optional

from fastapi import FastAPI
from fastapi.logger import logger


def load_sentiment_model():
    """Load sentiment analysis model for real-time inference."""
    try:
        from transformers import pipeline

        logger.info("Loading sentiment analysis model...")
        model = pipeline(
            "sentiment-analysis",
            model="cardiffnlp/twitter-roberta-base-sentiment-latest",
            tokenizer="cardiffnlp/twitter-roberta-base-sentiment-latest",
            return_all_scores=True,
            device="cpu",  # Force CPU to avoid MPS forking issues on macOS
        )
        logger.info("✅ Sentiment analysis model loaded successfully")
        return model
    except ImportError:
        logger.warning(
            "⚠️ Transformers not available, sentiment model will use fallback"
        )
        return None
    except Exception as e:
        logger.warning(f"⚠️ Failed to load sentiment model: {e}")
        return None


def load_lookup_tables() -> Dict[str, Any]:
    """Load static lookup tables for feature engineering."""
    # Example: Load static mappings that are expensive to compute at request time
    return {
        "sentiment_labels": {
            "LABEL_0": "negative",
            "LABEL_1": "neutral",
            "LABEL_2": "positive",
        },
        "emoji_sentiment": {"😊": "positive", "😞": "negative", "😐": "neutral"},
        "domain_categories": {"twitter.com": "social", "news.com": "news"},
    }


def load_user_embeddings() -> Optional[Dict[str, Any]]:
    """Load pre-computed user embeddings if available."""
    # Example: Load static user embeddings for recommendation features
    embeddings_file = Path(__file__).parent / "data" / "user_embeddings.npy"

    if embeddings_file.exists():
        try:
            import numpy as np

            embeddings = np.load(embeddings_file)
            logger.info(f"✅ Loaded user embeddings: {embeddings.shape}")
            return {"embeddings": embeddings}
        except Exception as e:
            logger.warning(f"⚠️ Failed to load user embeddings: {e}")

    return None


def load_artifacts(app: FastAPI):
    """
    Main function called by Feast feature server to load static artifacts.

    This function is called during server startup and should store artifacts
    in app.state for access by on-demand feature views.
    """
    logger.info("🔄 Loading static artifacts for PyTorch NLP template...")

    # Load sentiment analysis model
    app.state.sentiment_model = load_sentiment_model()

    # Load lookup tables
    app.state.lookup_tables = load_lookup_tables()

    # Load user embeddings (optional)
    app.state.user_embeddings = load_user_embeddings()

    # Also set global references for easier access from on-demand feature views
    try:
        import example_repo

        example_repo._sentiment_model = app.state.sentiment_model
        example_repo._lookup_tables = app.state.lookup_tables
        logger.info("✅ Global artifact references updated")
    except ImportError:
        logger.warning("⚠️ Could not update global artifact references")

    logger.info("✅ Static artifacts loading completed")


def get_static_artifact(app_state: Any, name: str) -> Any:
    """
    Helper function to safely access static artifacts from app.state.

    Args:
        app_state: FastAPI app.state object
        name: Name of the artifact to retrieve

    Returns:
        The requested artifact or None if not found
    """
    return getattr(app_state, name, None)


# Convenience functions for accessing specific artifacts
def get_sentiment_model(app_state: Any):
    """Get the pre-loaded sentiment analysis model."""
    return get_static_artifact(app_state, "sentiment_model")


def get_lookup_tables(app_state: Any) -> Dict[str, Any]:
    """Get the pre-loaded lookup tables."""
    return get_static_artifact(app_state, "lookup_tables") or {}


def get_user_embeddings(app_state: Any):
    """Get the pre-loaded user embeddings."""
    return get_static_artifact(app_state, "user_embeddings")
