# [Alpha] Static Artifacts Loading

**Warning**: This is an experimental feature. To our knowledge, this is stable, but there are still rough edges in the experience. Contributions are welcome!

## Overview

Static Artifacts Loading allows you to load models, lookup tables, and other static resources once during feature server startup instead of loading them on each request. These artifacts are cached in memory and accessible to on-demand feature views for real-time inference.

This feature optimizes the performance of on-demand feature views that require external resources by eliminating the overhead of repeatedly loading the same artifacts during request processing.

### Why Use Static Artifacts Loading?

Static artifacts loading enables data scientists and ML engineers to:

1. **Improve performance**: Eliminate model loading overhead from each feature request
2. **Enable complex transformations**: Use pre-trained models in on-demand feature views without performance penalties
3. **Share resources**: Multiple feature views can access the same loaded artifacts
4. **Simplify deployment**: Package models and lookup tables with your feature repository

Common use cases include:
- Sentiment analysis using pre-trained transformers models
- Text classification with small neural networks
- Lookup-based transformations using static dictionaries
- Embedding generation with pre-computed vectors

## How It Works

1. **Feature Repository Setup**: Create a `static_artifacts.py` file in your feature repository root
2. **Server Startup**: When `feast serve` starts, it automatically looks for and loads the artifacts
3. **Memory Storage**: Artifacts are stored in the FastAPI application state and accessible via global references
4. **Request Processing**: On-demand feature views access pre-loaded artifacts for fast transformations

## Example 1: Basic Model Loading

Create a `static_artifacts.py` file in your feature repository:

```python
# static_artifacts.py
from fastapi import FastAPI
from transformers import pipeline

def load_sentiment_model():
    """Load sentiment analysis model."""
    return pipeline(
        "sentiment-analysis",
        model="cardiffnlp/twitter-roberta-base-sentiment-latest",
        device="cpu"
    )

def load_artifacts(app: FastAPI):
    """Load static artifacts into app.state."""
    app.state.sentiment_model = load_sentiment_model()

    # Update global references for access from feature views
    import example_repo
    example_repo._sentiment_model = app.state.sentiment_model
```

Use the pre-loaded model in your on-demand feature view:

```python
# example_repo.py
import pandas as pd
from feast.on_demand_feature_view import on_demand_feature_view
from feast import Field
from feast.types import String, Float32

# Global reference for static artifacts
_sentiment_model = None

@on_demand_feature_view(
    sources=[text_input_request],
    schema=[
        Field(name="predicted_sentiment", dtype=String),
        Field(name="sentiment_confidence", dtype=Float32),
    ],
)
def sentiment_prediction(inputs: pd.DataFrame) -> pd.DataFrame:
    """Sentiment prediction using pre-loaded model."""
    global _sentiment_model

    results = []
    for text in inputs["input_text"]:
        predictions = _sentiment_model(text)
        best_pred = max(predictions, key=lambda x: x["score"])

        results.append({
            "predicted_sentiment": best_pred["label"],
            "sentiment_confidence": best_pred["score"],
        })

    return pd.DataFrame(results)
```

## Example 2: Multiple Artifacts with Lookup Tables

Load multiple types of artifacts:

```python
# static_artifacts.py
from fastapi import FastAPI
from transformers import pipeline
import json
from pathlib import Path

def load_sentiment_model():
    """Load sentiment analysis model."""
    return pipeline("sentiment-analysis", model="distilbert-base-uncased-finetuned-sst-2-english")

def load_lookup_tables():
    """Load static lookup tables."""
    return {
        "sentiment_labels": {"NEGATIVE": "negative", "POSITIVE": "positive"},
        "domain_categories": {"twitter.com": "social", "news.com": "news", "github.com": "tech"},
        "priority_users": {"user_123", "user_456", "user_789"}
    }

def load_config():
    """Load application configuration."""
    return {
        "model_threshold": 0.7,
        "max_text_length": 512,
        "default_sentiment": "neutral"
    }

def load_artifacts(app: FastAPI):
    """Load all static artifacts."""
    app.state.sentiment_model = load_sentiment_model()
    app.state.lookup_tables = load_lookup_tables()
    app.state.config = load_config()

    # Update global references
    import example_repo
    example_repo._sentiment_model = app.state.sentiment_model
    example_repo._lookup_tables = app.state.lookup_tables
    example_repo._config = app.state.config
```

Use multiple artifacts in feature transformations:

```python
# example_repo.py
import pandas as pd
from feast.on_demand_feature_view import on_demand_feature_view

# Global references for static artifacts
_sentiment_model = None
_lookup_tables: dict = {}
_config: dict = {}

@on_demand_feature_view(
    sources=[text_input_request, user_input_request],
    schema=[
        Field(name="predicted_sentiment", dtype=String),
        Field(name="is_priority_user", dtype=Bool),
        Field(name="domain_category", dtype=String),
    ],
)
def enriched_prediction(inputs: pd.DataFrame) -> pd.DataFrame:
    """Multi-artifact feature transformation."""
    global _sentiment_model, _lookup_tables, _config

    results = []
    for i, row in inputs.iterrows():
        text = row["input_text"]
        user_id = row["user_id"]
        domain = row.get("domain", "")

        # Use pre-loaded model
        predictions = _sentiment_model(text)
        sentiment_scores = {pred["label"]: pred["score"] for pred in predictions}

        # Use lookup tables
        predicted_sentiment = _lookup_tables["sentiment_labels"].get(
            max(sentiment_scores, key=sentiment_scores.get),
            _config["default_sentiment"]
        )

        is_priority = user_id in _lookup_tables["priority_users"]
        category = _lookup_tables["domain_categories"].get(domain, "unknown")

        results.append({
            "predicted_sentiment": predicted_sentiment,
            "is_priority_user": is_priority,
            "domain_category": category,
        })

    return pd.DataFrame(results)
```

## Container Deployment

Static artifacts work with containerized deployments. Include your artifacts in the container image:

```dockerfile
FROM python:3.11-slim

# Install dependencies
COPY requirements.txt .
RUN pip install -r requirements.txt

# Copy feature repository including static_artifacts.py
COPY feature_repo/ /app/feature_repo/

WORKDIR /app/feature_repo

# Start feature server
CMD ["feast", "serve", "--host", "0.0.0.0"]
```

The server will automatically load static artifacts during container startup.

## Supported Artifact Types

### Recommended Artifacts
- **Small ML models**: Sentiment analysis, text classification, small neural networks
- **Lookup tables**: Label mappings, category dictionaries, user segments
- **Configuration data**: Model parameters, feature mappings, business rules
- **Pre-computed embeddings**: User vectors, item features, static representations

### Not Recommended
- **Large Language Models**: Use dedicated serving solutions (vLLM, TensorRT-LLM, TGI)
- **Models requiring specialized hardware**: GPU clusters, TPUs
- **Frequently updated models**: Consider model registries with versioning
- **Large datasets**: Use feature views with proper data sources instead

## Error Handling

Static artifacts loading includes graceful error handling:
- **Missing file**: Server starts normally without static artifacts
- **Loading errors**: Warnings are logged, feature views should implement fallback logic
- **Partial failures**: Successfully loaded artifacts remain available

Always implement fallback behavior in your feature transformations:

```python
@on_demand_feature_view(...)
def robust_prediction(inputs: pd.DataFrame) -> pd.DataFrame:
    global _sentiment_model

    results = []
    for text in inputs["input_text"]:
        if _sentiment_model is not None:
            # Use pre-loaded model
            predictions = _sentiment_model(text)
            sentiment = max(predictions, key=lambda x: x["score"])["label"]
        else:
            # Fallback when artifacts aren't available
            sentiment = "neutral"

        results.append({"predicted_sentiment": sentiment})

    return pd.DataFrame(results)
```

## Starting the Feature Server

Start the feature server as usual:

```bash
feast serve
```

You'll see log messages indicating artifact loading:

```
INFO:fastapi:Loading static artifacts from static_artifacts.py
INFO:fastapi:Static artifacts loading completed
INFO:uvicorn:Application startup complete
```

## Template Example

The PyTorch NLP template demonstrates static artifacts loading:

```bash
feast init my-nlp-project -t pytorch_nlp
cd my-nlp-project/feature_repo
feast serve
```

This template includes a complete example with sentiment analysis model loading, lookup tables, and integration with on-demand feature views.

## Performance Considerations

- **Startup time**: Artifacts are loaded during server initialization, which may increase startup time
- **Memory usage**: All artifacts remain in memory for the server's lifetime
- **Concurrency**: Artifacts are shared across all request threads
- **Container resources**: Ensure sufficient memory allocation for your artifacts

## Configuration

Currently, static artifacts loading uses convention-based configuration:
- **File name**: Must be named `static_artifacts.py`
- **Location**: Must be in the feature repository root directory
- **Function name**: Must implement `load_artifacts(app: FastAPI)` function

## Limitations

- File name and location are currently fixed (not configurable)
- Artifacts are loaded synchronously during startup
- No built-in artifact versioning or hot reloading
- Limited to Python-based artifacts (no external binaries)

## Contributing

This is an alpha feature and we welcome contributions! Areas for improvement:
- Configurable artifact file locations
- Asynchronous artifact loading
- Built-in artifact versioning
- Performance monitoring and metrics
- Integration with model registries

Please report issues and contribute improvements via the [Feast GitHub repository](https://github.com/feast-dev/feast).