# Frequently Asked Questions (FAQ)

## 1. What is Feast?
Feast (Feature Store) is an [open-source](https://github.com/feast-dev/feast) platform that helps teams define, manage, validate, and serve machine learning features to production environments, whether in batch or real-time.

## 2. Who is Feast for?
- **Data Scientists**: Easily define, store, and retrieve features for model development and deployment.
- **MLOps Engineers**: Connect existing data infrastructure, ensuring data scientists can ship features to production without getting bogged down in engineering details.
- **Data Engineers**: Maintain a single source of truth for feature data and definitions through a unified catalog.
- **AI Engineers**: Scale AI applications by seamlessly integrating richer data, enabling efficient fine-tuning and serving of models.

## 3. Is Feast an ETL/ELT system?
No. Feast is not a general-purpose data pipelining system. Users typically rely on tools like [dbt](https://www.getdbt.com/) or other ETL/ELT solutions to handle upstream data transformations. Feast does support some [transformations](getting-started/architecture/feature-transformation.md), but it’s not meant to replace an end-to-end ETL/ELT workflow.

## 4. Is Feast a data warehouse or a database?
No. Feast does not replace your data warehouse. It orchestrates and serves data that typically resides in systems like BigQuery, Snowflake, DynamoDB, and Redis. It’s a lightweight layer to make features consistently available at training and serving time.

## 5. What online and offline store backends does Feast support?
Feast supports multiple backends for both online and offline stores, allowing users to integrate with their existing data infrastructure.

### Offline store:
- **BigQuery**
- **Snowflake**
- **Redshift**
- **File (Parquet/Local/Cloud Storage)**
- **Trino/Presto**
- **PostgreSQL** (experimental)

### Online store:
- **Redis**
- **DynamoDB**
- **Datastore (GCP)**
- **SQLite** (for local/dev use)
- **PostgreSQL**

## 6. Which problems does Feast solve?
Feast is designed to address key challenges in machine learning operationalization (MLOps), especially around managing and serving features consistently across training and inference. It solves problems such as:

- **Training-serving skew**: By ensuring features used in training are exactly the same as those used in production, Feast eliminates inconsistencies that lead to model performance drops.
- **Feature reuse and discoverability**: Centralizes and documents feature definitions, making it easier to reuse across teams and projects.
- **Low-latency feature serving**: Enables real-time feature retrieval for online prediction through high-performance online stores like Redis or DynamoDB.
- **Data consistency and freshness**: Ensures that models always use the most up-to-date feature values, whether in batch or real-time environments.
- **Infrastructure abstraction**: Allows teams to work with a consistent API regardless of the underlying data infrastructure (cloud, on-premise, etc.).

## 7. Which problems does Feast not fully solve?
- **Reproducible model training / experiment management**: Feast tracks feature and model metadata but doesn't version datasets or manage train/test splits. Tools like [DVC](https://dvc.org/), [MLflow](https://www.mlflow.org/), and [Kubeflow](https://www.kubeflow.org/) are more suited for that.
- **Batch feature engineering**: Feast supports some on-demand and streaming transformations, and is investing in batch transformations, but it’s not a full-fledged batch pipeline solution.
- **Native streaming feature ingestion**: Users can push streaming features into Feast, but Feast does not manage streaming pipelines directly (no built-in streaming pull logic).
- **Lineage**: Although Feast ties feature values to model versions, it’s not a complete lineage solution. There are community plugins for [DataHub](https://datahubproject.io/docs/generated/ingestion/sources/feast/) and [Amundsen](https://github.com/amundsen-io/amundsen/blob/4a9d60176767c4d68d1cad5b093320ea22e26a49/databuilder/databuilder/extractor/feast_extractor.py).
- **Data quality / drift detection**: Feast has experimental integrations with [Great Expectations](https://greatexpectations.io/) but does not provide a comprehensive solution for data drift or data quality across pipelines, labels, or model versions.

## 8. Can you give examples of use cases?
Many companies use Feast to power:
- **Online recommendations**, leveraging pre-computed historical user/item features.
- **Fraud detection**, by comparing new transaction patterns against historical data in real time.
- **Churn prediction**, generating feature values for all users in batch at regular intervals.
- **Credit scoring**, using historical features to assess default probability.

## 9. How can I get started with Feast?
- **[Quickstart](getting-started/quickstart.md)** for a hands-on intro.
- **[Running Feast with Snowflake/GCP/AWS](how-to-guides/feast-snowflake-gcp-aws/)** for more complex setups.
- **[Tutorials](tutorials/tutorials-overview/)** for in-depth examples and best practices.

## 10. Where can I learn more about contributing?
Check out our [Contributing Guide](project/contributing.md) for detailed instructions on how to contribute to Feast, including our coding standards, documentation practices, and release process.

---

**Still have questions?**  
Feel free to open an issue or start a discussion in our [GitHub repository](https://github.com/feast-dev/feast) to get more help.