# Introduction

Feast (**Fea**ture **St**ore) is an [open-source](https://github.com/feast-dev/feast) feature store that helps teams define, manage, validate, and serve machine learning features in production at scale. Whether you are training batch or real-time models, Feast reuses your existing data infrastructure to empower both training and inference workflows.

## Why Feast?
- **Consistent Features**: Manage an offline store (batch training) and an online store (real-time serving) under one feature server.
- **Avoid Data Leakage**: Generate point-in-time correct feature sets to ensure no future data leaks into model training.
- **Decouple ML from Data Infra**: Provide a single data access layer that abstracts storage from retrieval, ensuring portability across batch and real-time environments.

## Core Components
- **[Offline Store](getting-started/components/offline-store.md)**: Manages historical feature extraction for large-scale batch scoring or model training.
- **[Online Store](getting-started/components/online-store.md)**: Powers low-latency feature serving for real-time production systems.
- **Python SDK** and **[Feature Server](reference/feature-servers/README.md)**: Provide feature management and retrieval APIs, plus a service for non-Python consumers.
- **[UI](reference/alpha-web-ui.md)** and **[CLI tool](reference/feast-cli-commands.md)**: Let you explore and manage feature definitions.

## Getting Started
- **[Quickstart](getting-started/quickstart.md)**: Fastest way to try Feast with a sample project.
- **[Concepts](getting-started/concepts/)**: Learn key concepts such as entities, feature views, data sources, and transformations.
- **[Architecture](getting-started/architecture/)**: Understand how Feast components interact under the hood.
- **[Tutorials](tutorials/tutorials-overview/)**: Explore end-to-end examples using Feast in real machine learning applications.
- **[How to Guides](how-to-guides/feast-snowflake-gcp-aws/)**: Set up Feast on AWS, GCP, Snowflake, and more.
- **[FAQ](faq.md)**: Frequently asked questions about installation, usage, and limitations.

For contributing guidelines and more advanced usage, see **[Contributing](project/contributing.md)**.