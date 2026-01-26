---
title: Feast Joins the PyTorch Ecosystem
description: We're excited to announce that Feast has officially joined the PyTorch ecosystem, bringing feature stores to the broader ML community.
date: 2025-01-26
authors: ["Feast Team"]
---

<div class="hero-image">
  <img src="/images/blog/rocket.png" alt="Feast Joins PyTorch Ecosystem" loading="lazy">
</div>

# Feast Joins the PyTorch Ecosystem 🎉

We're thrilled to announce that Feast has officially joined the [PyTorch ecosystem](https://pytorch.org/blog/feast-joins-the-pytorch-ecosystem/)! This is a significant milestone for the Feast community and represents our commitment to making feature stores accessible to the broader machine learning community.

## What This Means for the ML Community

By joining the PyTorch ecosystem, Feast becomes part of a vibrant community of tools and libraries that power modern machine learning workflows. PyTorch has become the de facto standard for ML research and production, and we're excited to bring feature store capabilities to PyTorch users.

### Why Feature Stores Matter for PyTorch Users

Feature stores solve critical challenges that ML engineers face when moving from model development to production:

1. **Consistent Feature Engineering**: Ensure that features computed during training match those used in production
2. **Low-Latency Feature Serving**: Serve features with millisecond latency for real-time inference
3. **Feature Reusability**: Share features across teams and models to accelerate development
4. **Point-in-Time Correctness**: Prevent data leakage by ensuring training data reflects what would have been available at prediction time
5. **Data Infrastructure Abstraction**: Decouple your ML code from underlying data infrastructure

## How Feast Works with PyTorch

Feast integrates seamlessly with PyTorch workflows, whether you're training models locally or deploying them at scale. Here's a typical workflow:

### Training with Feast and PyTorch

```python
from feast import FeatureStore
import torch
from torch.utils.data import Dataset, DataLoader
import pandas as pd
from datetime import datetime

# Initialize Feast
store = FeatureStore(repo_path=".")

# Retrieve historical features for training
entity_df = pd.DataFrame({
    "user_id": [1001, 1002, 1003],
    "event_timestamp": [datetime(2025, 1, 1), datetime(2025, 1, 2), datetime(2025, 1, 3)]
})

training_df = store.get_historical_features(
    entity_df=entity_df,
    features=[
        "user_features:age",
        "user_features:activity_score",
        "item_features:popularity"
    ]
).to_df()

# Add your labels (from your training data source)
# training_df['label'] = ...

# Create PyTorch Dataset
class FeatureDataset(Dataset):
    def __init__(self, df, feature_cols):
        self.features = torch.tensor(df[feature_cols].values, dtype=torch.float32)
        self.labels = torch.tensor(df['label'].values, dtype=torch.float32)
    
    def __len__(self):
        return len(self.features)
    
    def __getitem__(self, idx):
        return self.features[idx], self.labels[idx]

# Train your PyTorch model
feature_cols = ['age', 'activity_score', 'popularity']
dataset = FeatureDataset(training_df, feature_cols)
dataloader = DataLoader(dataset, batch_size=32, shuffle=True)

# Define and train your model
model = torch.nn.Sequential(
    torch.nn.Linear(len(feature_cols), 64),
    torch.nn.ReLU(),
    torch.nn.Linear(64, 1)
)
# Training loop...
```

### Serving Features for Real-Time Inference

Once your PyTorch model is trained, Feast makes it easy to serve features in production:

```python
# Get online features for real-time prediction
online_features = store.get_online_features(
    entity_rows=[{"user_id": 1001}],
    features=[
        "user_features:age",
        "user_features:activity_score",
        "item_features:popularity"
    ]
).to_dict()

# Convert to tensor and run inference
# Note: Feature keys in the dict will use the format "feature_view__feature_name"
feature_values = [
    online_features['age'][0],
    online_features['activity_score'][0],
    online_features['popularity'][0]
]
feature_tensor = torch.tensor(feature_values, dtype=torch.float32)

with torch.no_grad():
    prediction = model(feature_tensor)
```

## Key Benefits for PyTorch Users

### 1. Production-Ready Feature Engineering

Feast helps you move from notebook experiments to production-ready feature pipelines. Define your features once and use them consistently across training and serving.

### 2. Flexible Data Sources

Feast supports a wide range of data sources including:
- BigQuery, Snowflake, Redshift for batch features
- Redis, DynamoDB, PostgreSQL for online features
- Parquet files for local development

### 3. Scalability

Whether you're training on a laptop or serving millions of predictions per second, Feast scales with your needs.

### 4. Open Source and Extensible

Feast is fully open source with a pluggable architecture, making it easy to extend and customize for your specific use cases.

## Getting Started

Ready to use Feast with PyTorch? Here's how to get started:

```bash
# Install Feast
pip install feast

# Initialize a new feature repository
feast init my_feature_repo
cd my_feature_repo

# Apply feature definitions
feast apply

# Start serving features
feast serve
```

Check out our [documentation](https://docs.feast.dev) for comprehensive guides and examples.

## Join the Community

We're excited to be part of the PyTorch ecosystem and look forward to collaborating with the community. Here's how you can get involved:

- **GitHub**: Star us at [github.com/feast-dev/feast](https://github.com/feast-dev/feast)
- **Slack**: Join our [community Slack](https://slack.feast.dev)
- **Documentation**: Explore our [docs](https://docs.feast.dev)
- **Examples**: Check out [example projects](https://github.com/feast-dev/feast/tree/master/examples)

## What's Next?

We're committed to making Feast the best feature store for PyTorch users. Some of our upcoming priorities include:

- Enhanced PyTorch integration examples
- Performance optimizations for large-scale deployments
- Additional connectors for popular data sources
- Improved developer experience

Thank you to the PyTorch team for welcoming us to the ecosystem, and to the Feast community for your continued support!

## Learn More

- [PyTorch Blog Announcement](https://pytorch.org/blog/feast-joins-the-pytorch-ecosystem/)
- [Feast Documentation](https://docs.feast.dev)
- [Feast GitHub Repository](https://github.com/feast-dev/feast)
- [What is a Feature Store?](./what-is-a-feature-store)

---

Have questions or feedback? Reach out to us on [Slack](https://slack.feast.dev) or [GitHub Discussions](https://github.com/feast-dev/feast/discussions).
