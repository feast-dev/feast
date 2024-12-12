# Milvus Online Store for Feast

This module implements a Milvus online store for Feast, providing vector similarity search capabilities.

## Overview
The Milvus online store implementation allows Feast to use Milvus as a backend for storing and retrieving feature values, with support for vector similarity search operations.

## Configuration
To use Milvus as an online store, add the following to your `feature_store.yaml`:

```yaml
online_store:
    type: milvus
    host: localhost  # Milvus server host
    port: 19530     # Milvus server port
    user: optional_user     # Optional username
    password: optional_pass # Optional password
```

## Features
- Vector similarity search support
- Configurable connection parameters
- Support for various distance metrics (cosine, L2, IP)
- Batch operations for efficient data handling
