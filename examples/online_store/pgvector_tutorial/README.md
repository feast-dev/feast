# PGVector Tutorial with Feast

This tutorial demonstrates how to use PostgreSQL with the pgvector extension as a vector database backend for Feast. You'll learn how to set up pgvector, create embeddings, store them in Feast, and perform similarity searches.

## Prerequisites

- Python 3.8+
- Docker (for running PostgreSQL with pgvector)
- Feast installed (`pip install 'feast[postgres]'`)

## Setup

1. Start a PostgreSQL container with pgvector:

```bash
docker run -d \
  --name postgres-pgvector \
  -e POSTGRES_USER=feast \
  -e POSTGRES_PASSWORD=feast \
  -e POSTGRES_DB=feast \
  -p 5432:5432 \
  pgvector/pgvector:pg16
```

2. Initialize the pgvector extension:

```bash
docker exec -it postgres-pgvector psql -U feast -c "CREATE EXTENSION IF NOT EXISTS vector;"
```

## Project Structure

```
pgvector_tutorial/
├── README.md
├── feature_store.yaml    # Feast configuration
├── data/                 # Data directory
│   └── sample_data.parquet  # Sample data with embeddings
└── pgvector_example.py   # Example script
```

## Tutorial Steps

1. Configure Feast with pgvector
2. Generate sample data with embeddings
3. Define feature views
4. Register and apply feature definitions
5. Perform vector similarity search

Follow the instructions in `pgvector_example.py` to run the complete example.

## How It Works

This tutorial demonstrates:

- Setting up PostgreSQL with pgvector extension
- Configuring Feast to use pgvector as the online store
- Generating embeddings for text data
- Storing embeddings in Feast feature views
- Performing vector similarity searches using Feast's retrieval API

The pgvector extension enables PostgreSQL to store and query vector embeddings efficiently, making it suitable for similarity search applications like semantic search and recommendation systems.