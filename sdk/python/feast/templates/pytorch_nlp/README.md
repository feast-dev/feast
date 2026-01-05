# PyTorch NLP Sentiment Analysis with Feast

This template demonstrates how to build a complete sentiment analysis pipeline using **Feast** (Feature Store) with **PyTorch** and **Hugging Face Transformers**. It showcases modern MLOps practices for NLP including feature engineering, model serving, and real-time inference.

## 🎯 What You'll Learn

- **Feast Fundamentals**: Feature stores, entities, feature views, and services
- **NLP Feature Engineering**: Text preprocessing and feature extraction patterns
- **PyTorch Integration**: Using pre-trained Hugging Face models with Feast
- **Real-time Serving**: Online feature serving for production inference
- **MLOps Patterns**: Model versioning, performance monitoring, and data governance

## 🚀 Quick Start

### Prerequisites

- Python 3.8+
- pip or conda for package management

### 1. Initialize the Project

```bash
feast init my-sentiment-project -t pytorch_nlp
cd my-sentiment-project
```

### 2. Install Dependencies

```bash
# Install Feast with NLP support (includes PyTorch, transformers, and ML utilities)
pip install feast[nlp]
```

### 3. Apply and Materialize Features

```bash
cd feature_repo
feast apply
feast materialize-incremental $(date -u +"%Y-%m-%dT%H:%M:%S")
```

### 4. Start Feature Server

```bash
feast serve --host 0.0.0.0 --port 6566
```

### 5. Test with Python (Optional)

```bash
python test_workflow.py
```

## 📊 What's Included

### Sample Dataset
- **1000 synthetic text samples** with sentiment labels (positive/negative/neutral)
- **Engineered features**: text length, word count, emoji count, etc.
- **User context**: aggregated user statistics and behavior patterns
- **Dynamic timestamps** generated within the past 30 days for realistic demo experience

### Feature Engineering Pipeline
- **Text Features**: Content, metadata, and linguistic characteristics
- **User Features**: Historical sentiment patterns and engagement metrics
- **Real-time Features**: On-demand sentiment prediction using pre-trained models

### Model Integration
- **Pre-trained Models**: CardiffNLP Twitter-RoBERTa for sentiment analysis
- **Embedding Generation**: Text vectorization for similarity and clustering
- **Confidence Scoring**: Prediction confidence and probability distributions

## 🌐 HTTP Feature Server

Once you've started the feature server with `feast serve`, you can query features via HTTP API:

### Basic Materialized Features

Query stored text and user features:

```bash
curl -X POST \
  "http://localhost:6566/get-online-features" \
  -H "Content-Type: application/json" \
  -d '{
    "features": [
      "text_features:text_content",
      "text_features:sentiment_label",
      "user_stats:user_avg_sentiment"
    ],
    "entities": {
      "text_id": ["text_0000", "text_0001"],
      "user_id": ["user_080", "user_091"]
    }
  }'
```

**Example Response:**
```json
{
  "metadata": {"feature_names": ["text_id","user_id","sentiment_label","text_content","user_avg_sentiment"]},
  "results": [
    {"values": ["text_0000"], "statuses": ["PRESENT"]},
    {"values": ["user_080"], "statuses": ["PRESENT"]},
    {"values": ["positive"], "statuses": ["PRESENT"]},
    {"values": ["Having an amazing day at the beach with friends!"], "statuses": ["PRESENT"]},
    {"values": [0.905], "statuses": ["PRESENT"]}
  ]
}
```

### On-Demand Sentiment Predictions

Get real-time sentiment analysis:

```bash
curl -X POST \
  "http://localhost:6566/get-online-features" \
  -H "Content-Type: application/json" \
  -d '{
    "features": [
      "sentiment_prediction:predicted_sentiment",
      "sentiment_prediction:sentiment_confidence",
      "sentiment_prediction:positive_prob"
    ],
    "entities": {
      "input_text": ["I love this amazing product!", "This service is terrible"],
      "model_name": ["cardiffnlp/twitter-roberta-base-sentiment-latest", "cardiffnlp/twitter-roberta-base-sentiment-latest"]
    }
  }'
```

### Feature Service (Complete Feature Set)

Query using predefined feature service:

```bash
curl -X POST \
  "http://localhost:6566/get-online-features" \
  -H "Content-Type: application/json" \
  -d '{
    "feature_service": "sentiment_analysis_v2",
    "entities": {
      "text_id": ["text_0000"],
      "user_id": ["user_080"],
      "input_text": ["This is an amazing experience!"],
      "model_name": ["cardiffnlp/twitter-roberta-base-sentiment-latest"]
    }
  }'
```

**Note**: Use actual entity combinations from your generated data. Run `head data/sentiment_data.parquet` to see available `text_id` and `user_id` values.

## 🏗️ Project Structure

```
my-sentiment-project/
├── README.md                     # This file
└── feature_repo/
    ├── feature_store.yaml        # Feast configuration
    ├── example_repo.py           # Feature definitions (uses pre-loaded artifacts)
    ├── static_artifacts.py       # Static artifacts loading (models, lookup tables)
    ├── test_workflow.py          # Complete demo workflow
    └── data/                     # Generated sample data
        └── sentiment_data.parquet
```

## 🔧 Key Components

### Entities
- **`text`**: Unique identifier for text samples
- **`user`**: User who created the content

### Feature Views
- **`text_features`**: Raw text content and engineered features
- **`user_stats`**: User-level aggregated statistics and behavior

### On-Demand Features
- **`sentiment_prediction`**: Real-time sentiment analysis using PyTorch models
- **Features**: predicted sentiment, confidence scores, probability distributions, embeddings

### Feature Services
- **`sentiment_analysis_v1`**: Basic sentiment features for simple models
- **`sentiment_analysis_v2`**: Advanced features with user context
- **`sentiment_training_features`**: Historical features for model training

## ⚙️ Configuration

This template is configured for **local development** using SQLite - no external dependencies required!

### Current Configuration (`feature_store.yaml`)

```yaml
project: my_project
provider: local                    # Local provider (no cloud)
registry: data/registry.db         # SQLite registry
online_store:
  type: sqlite                     # SQLite online store (NOT Redis)
  path: data/online_store.db       # Local SQLite file
offline_store:
  type: file                       # Local file-based offline store
```

### Why SQLite?
- ✅ **Zero setup** - Works immediately after `feast init`
- ✅ **Self-contained** - All data in local files
- ✅ **No external services** - No Redis/cloud required
- ✅ **Perfect for demos** - Easy to share and understand

## 🚀 Static Artifacts Loading

This template demonstrates **static artifacts loading** - a performance optimization that loads models, lookup tables, and other artifacts once at feature server startup instead of on each request.

### What are Static Artifacts?

Static artifacts are pre-loaded resources that remain constant during server operation:
- **Small ML models** (sentiment analysis, classification, small neural networks)
- **Lookup tables and mappings** (label encoders, category mappings)
- **Configuration data** (model parameters, feature mappings)
- **Pre-computed embeddings** (user embeddings, item features)

### Performance Benefits

**Before (Per-Request Loading):**
```python
def sentiment_prediction(inputs):
    # ❌ Model loads on every request - slow!
    model = pipeline("sentiment-analysis", model="...")
    return model(inputs["text"])
```

**After (Startup Loading):**
```python
# ✅ Model loads once at server startup
def sentiment_prediction(inputs):
    global _sentiment_model  # Pre-loaded model
    return _sentiment_model(inputs["text"])
```

**Performance Impact:**
- 🚀 **10-100x faster** inference (no model loading overhead)
- 💾 **Lower memory usage** (shared model across requests)
- ⚡ **Better scalability** (consistent response times)

### How It Works

1. **Startup**: Feast server loads `static_artifacts.py` during initialization
2. **Loading**: `load_artifacts(app)` function stores models in `app.state`
3. **Access**: On-demand feature views access pre-loaded artifacts via global references

```python
# static_artifacts.py - Define what to load
def load_artifacts(app: FastAPI):
    app.state.sentiment_model = load_sentiment_model()
    app.state.lookup_tables = load_lookup_tables()

    # Update global references for easy access
    import example_repo
    example_repo._sentiment_model = app.state.sentiment_model
    example_repo._lookup_tables = app.state.lookup_tables

# example_repo.py - Use pre-loaded artifacts
_sentiment_model = None  # Set by static_artifacts.py

def sentiment_prediction(inputs):
    global _sentiment_model
    if _sentiment_model is not None:
        return _sentiment_model(inputs["text"])
    else:
        return fallback_predictions()
```

### Scope and Limitations

**✅ Great for:**
- Small to medium models (< 1GB)
- Fast-loading models (sentiment analysis, classification)
- Lookup tables and reference data
- Configuration parameters
- Pre-computed embeddings

**❌ Not recommended for:**
- **Large Language Models (LLMs)** - Use dedicated serving solutions like vLLM, TGI, or TensorRT-LLM
- Models requiring GPU clusters
- Frequently updated models
- Models with complex initialization dependencies

**Note:** Feast is optimized for feature serving, not large model inference. For production LLM workloads, use specialized model serving platforms.

### Customizing Static Artifacts

To add your own artifacts, modify `static_artifacts.py`:

```python
def load_custom_embeddings():
    """Load pre-computed user embeddings."""
    embeddings_file = Path(__file__).parent / "data" / "user_embeddings.npy"
    if embeddings_file.exists():
        import numpy as np
        return {"embeddings": np.load(embeddings_file)}
    return None

def load_artifacts(app: FastAPI):
    # Load your custom artifacts
    app.state.custom_embeddings = load_custom_embeddings()
    app.state.config_params = {"threshold": 0.7, "top_k": 10}

    # Make them available to feature views
    import example_repo
    example_repo._custom_embeddings = app.state.custom_embeddings
```

## 📚 Detailed Usage

### 1. Feature Store Setup

```python
from feast import FeatureStore

store = FeatureStore(repo_path=".")
```

### 2. Training Data Retrieval

```python
# Get historical features for model training
from datetime import datetime
import pandas as pd

entity_df = pd.DataFrame({
    "text_id": ["text_0000", "text_0001", "text_0002"],
    "user_id": ["user_080", "user_091", "user_052"],  # Use actual generated user IDs
    "event_timestamp": [datetime.now(), datetime.now(), datetime.now()]  # Current timestamps
})

training_df = store.get_historical_features(
    entity_df=entity_df,
    features=[
        "text_features:text_content",
        "text_features:sentiment_label",
        "text_features:text_length",
        "user_stats:user_avg_sentiment",
    ],
).to_df()

print(f"Retrieved {len(training_df)} training samples")
print(training_df.head())
```

### 3. Real-time Inference

```python
# Get features for online serving (use actual entity combinations)
entity_rows = [
    {"text_id": "text_0000", "user_id": "user_080"},
    {"text_id": "text_0001", "user_id": "user_091"}
]

online_features = store.get_online_features(
    features=store.get_feature_service("sentiment_analysis_v1"),
    entity_rows=entity_rows,
).to_dict()

print("Online features:", online_features)
```

### 4. On-Demand Sentiment Prediction

```python
# Real-time sentiment analysis
prediction_rows = [{
    "input_text": "I love this product!",
    "model_name": "cardiffnlp/twitter-roberta-base-sentiment-latest"
}]

predictions = store.get_online_features(
    features=[
        "sentiment_prediction:predicted_sentiment",
        "sentiment_prediction:sentiment_confidence",
    ],
    entity_rows=prediction_rows,
).to_dict()
```

## 🚀 Complete End-to-End Demo

Here's a step-by-step walkthrough of the entire template workflow:

### 1. Initialize and Setup

```bash
# Create new project
feast init my-sentiment-demo -t pytorch_nlp
cd my-sentiment-demo

# Install dependencies
pip install torch>=2.0.0 transformers>=4.30.0

# Navigate to feature repository
cd feature_repo
```

### 2. Apply Feature Store Configuration

```bash
# Register entities, feature views, and services
feast apply
```

**Expected Output:**
```
Created entity text
Created entity user
Created feature view text_features
Created feature view user_stats
Created on demand feature view sentiment_prediction
Created feature service sentiment_analysis_v1
Created feature service sentiment_analysis_v2
```

### 3. Materialize Features

```bash
# Load features into online store
feast materialize-incremental $(date -u +"%Y-%m-%dT%H:%M:%S")
```

**Expected Output:**
```
Materializing 2 feature views to 2025-XX-XX XX:XX:XX+00:00 into the sqlite online store.
text_features: ████████████████████████████████████████
user_stats: ████████████████████████████████████████
```

### 4. Start Feature Server

```bash
# Start HTTP feature server
feast serve --host 0.0.0.0 --port 6566
```

**Expected Output:**
```
Starting gunicorn 23.0.0
Listening at: http://0.0.0.0:6566
```

### 5. Query Features

In a new terminal, test the feature server:

```bash
# Check actual entity IDs in your data
python -c "
import pandas as pd
df = pd.read_parquet('data/sentiment_data.parquet')
print('Sample entities:', df.head())
"

# Test with actual entity combinations
curl -X POST \
  "http://localhost:6566/get-online-features" \
  -H "Content-Type: application/json" \
  -d '{
    "features": ["text_features:text_content", "text_features:sentiment_label"],
    "entities": {
      "text_id": ["text_0000"],
      "user_id": ["user_XXX"]
    }
  }' | jq
```

## 🎮 Customization Examples

### Adding New Features

```python
# In example_repo.py, add to text_features_fv schema:
Field(name="hashtag_count", dtype=Int64, description="Number of hashtags"),
Field(name="mention_count", dtype=Int64, description="Number of @mentions"),
Field(name="url_count", dtype=Int64, description="Number of URLs"),
```

### Using Different Models

```python
# In the sentiment_prediction function, change model:
model_name = "nlptown/bert-base-multilingual-uncased-sentiment"
# or
model_name = "distilbert-base-uncased-finetuned-sst-2-english"
```

### Adding Custom Transformations

```python
@on_demand_feature_view(
    sources=[text_input_request],
    schema=[Field(name="toxicity_score", dtype=Float32)],
)
def toxicity_detection(inputs: pd.DataFrame) -> pd.DataFrame:
    # Implement toxicity detection logic
    pass
```

## 📈 Production Considerations

### Scaling to Production

1. **Cloud Deployment**: Use AWS, GCP, or Azure providers instead of local
2. **Vector Store**: Replace SQLite with Milvus for similarity search
3. **Model Serving**: Deploy models with KServe or other serving framework
4. **Monitoring**: Add feature drift detection and model performance tracking

### Performance Optimization

**Current Architecture:**
- ✅ **Static artifacts loading** at server startup (see `static_artifacts.py`)
- ✅ **Pre-loaded models** cached in memory for fast inference
- CPU-only operation to avoid multiprocessing issues
- SQLite-based storage for fast local access

**Implemented Optimizations:**
- **Startup-time Model Loading**: ✅ Models load once at server startup via `static_artifacts.py`
- **Memory-efficient Caching**: ✅ Models stored in `app.state` and accessed via global references
- **Fallback Handling**: ✅ Graceful degradation when artifacts fail to load

**Additional Production Optimizations:**
1. **Batch Inference**: Process multiple texts together for efficiency
2. **Feature Materialization**: Pre-compute expensive features offline
3. **Async Processing**: Use async patterns for real-time serving
4. **Model Serving Layer**: Use dedicated model servers (TorchServe, vLLM) for large models

### Production Configuration Examples

**Note**: The demo uses SQLite (above). These are examples for production deployment:

```yaml
# feature_store.yaml for AWS production (requires Redis setup)
project: sentiment_analysis_prod
provider: aws
registry: s3://my-bucket/feast/registry.pb
online_store:
  type: redis                      # Requires separate Redis server
  connection_string: redis://my-redis-cluster:6379
offline_store:
  type: bigquery
  project_id: my-gcp-project

# feature_store.yaml for GCP production (requires cloud services)
project: sentiment_analysis_prod
provider: gcp
registry: gs://my-bucket/feast/registry.pb
online_store:
  type: redis                      # Requires separate Redis server
  connection_string: redis://my-redis-cluster:6379
offline_store:
  type: bigquery
  project_id: my-gcp-project
```

## 🤝 Contributing

This template is designed to be extended and customized:

1. **Add new feature transformations** in `example_repo.py`
2. **Experiment with different models** in the `sentiment_prediction` function
3. **Extend the test workflow** with additional evaluation metrics
4. **Add new data sources** (Twitter API, product reviews, etc.)

## 📖 Resources

- [Feast Documentation](https://docs.feast.dev/)
- [Hugging Face Transformers](https://huggingface.co/docs/transformers/)
- [PyTorch Documentation](https://pytorch.org/docs/)

## 🐛 Troubleshooting

### Common Issues

**ImportError: No module named 'transformers'**
```bash
pip install torch transformers
```

**Model download timeout**
```python
# Set environment variable for Hugging Face cache
export HF_HOME=/path/to/cache
```

**Feature store initialization fails**
```bash
# Reset the feature store
feast teardown
feast apply
```

**On-demand features return defaults**
- This is expected if PyTorch/transformers aren't installed
- The template includes fallback dummy predictions for demonstration

### Getting Help

- Check the [Feast GitHub Issues](https://github.com/feast-dev/feast/issues)
- Join the [Feast Slack Community](https://slack.feast.dev/)
- Review the [PyTorch Forums](https://discuss.pytorch.org/)

---

**Happy Feature Engineering! 🎉**

Built with ❤️ using Feast, PyTorch, and Hugging Face.
