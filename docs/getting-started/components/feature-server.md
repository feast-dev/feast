# Feature Server

The Feature Server is a core architectural component in Feast, designed to provide low-latency feature retrieval and updates for machine learning applications.

It is a REST API server built using [FastAPI](https://fastapi.tiangolo.com/) and exposes a limited set of endpoints to serve features, push data, and support materialization operations. The server is scalable, flexible, and designed to work seamlessly with various deployment environments, including local setups and cloud-based systems.

## Motivation

In machine learning workflows, real-time access to feature values is critical for enabling low-latency predictions. The Feature Server simplifies this requirement by:

1. **Serving Features:** Allowing clients to retrieve feature values for specific entities in real-time, reducing the complexity of direct interactions with the online store.
2. **Data Integration:** Providing endpoints to push feature data directly into the online or offline store, ensuring data freshness and consistency.
3. **Scalability:** Supporting horizontal scaling to handle high request volumes efficiently.
4. **Standardized API:** Exposing HTTP/JSON endpoints that integrate seamlessly with various programming languages and ML pipelines.
5. **Secure Communication:** Supporting TLS (SSL) for secure data transmission in production environments.

## Architecture

The Feature Server operates as a stateless service backed by two key components:

- **[Online Store](./online-store.md):** The primary data store used for low-latency feature retrieval.
- **[Registry](./registry.md):** The metadata store that defines feature sets, feature views, and their relationships to entities.

## Key Features

1. **RESTful API:** Provides standardized endpoints for feature retrieval and data pushing.
2. **CLI Integration:** Easily managed through the Feast CLI with commands like `feast serve`.
3. **Flexible Deployment:** Can be deployed locally, via Docker, or on Kubernetes using Helm charts.
4. **Scalability:** Designed for distributed deployments to handle large-scale workloads.
5. **TLS Support:** Ensures secure communication in production setups.

## Endpoints Overview

| Endpoint                     | Description                                                             |
|------------------------------|-------------------------------------------------------------------------|
| `/get-online-features`       | Retrieves feature values for specified entities and feature references. |
| `/push`                      | Pushes feature data to the online and/or offline store.                 |
| `/materialize`               | Materializes features within a specific time range to the online store. |
| `/materialize-incremental`   | Incrementally materializes features up to the current timestamp.        |
| `/retrieve-online-documents` | Supports Vector Similarity Search for RAG (Alpha end-ponit)             |
| `/docs`                      | API Contract for available endpoints                                    | 

