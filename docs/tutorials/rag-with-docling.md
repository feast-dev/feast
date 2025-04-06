# Retrieval Augmented Generation (RAG) with Feast

This tutorial demonstrates how to use Feast with [Docling](https://github.com/doclingjs/docling) and [Milvus](https://milvus.io/) to build a Retrieval Augmented Generation (RAG) application. You'll learn how to store document embeddings in Feast and retrieve the most relevant documents for a given query.

## Overview

RAG is a technique that combines generative models (e.g., LLMs) with retrieval systems to generate contextually relevant output for a particular goal (e.g., question and answering). Feast makes it easy to store and retrieve document embeddings for RAG applications by providing integrations with vector databases like Milvus.

The typical RAG process involves:
1. Sourcing text data relevant for your application
2. Transforming each text document into smaller chunks of text
3. Transforming those chunks of text into embeddings
4. Inserting those chunks of text along with some identifier for the chunk and document in a database
5. Retrieving those chunks of text along with the identifiers at run-time to inject that text into the LLM's context
6. Calling some API to run inference with your LLM to generate contextually relevant output
7. Returning the output to some end user

## Prerequisites

- Python 3.10 or later
- Feast installed with Milvus support: `pip install feast[milvus]`
- A basic understanding of feature stores and vector embeddings

## Step 1: Configure Milvus in Feast

Create a `feature_store.yaml` file with the following configuration:

```yaml
project: rag
provider: local
registry: data/registry.db
online_store:
  type: milvus
  path: data/online_store.db
  vector_enabled: true
  embedding_dim: 384
  index_type: "IVF_FLAT"

offline_store:
  type: file
entity_key_serialization_version: 3
# By default, no_auth for authentication and authorization, other possible values kubernetes and oidc. Refer the documentation for more details.
auth:
  type: no_auth
```

## Step 2: Define your Data Sources and Views

Create a `feature_repo.py` file to define your entities, data sources, and feature views:

```python
from datetime import timedelta
from feast import Entity, FeatureView, Field, FileSource
from feast.types import Array, Float32, Int64, String, UnixTimestamp, ValueType

# Define entities
document = Entity(
    name="document_id",
    description="Document ID",
    value_type=ValueType.INT64,
)

# Define data source
source = FileSource(
    path="data/embedded_documents.parquet",
    timestamp_field="event_timestamp",
    created_timestamp_column="created_timestamp",
)

# Define the view for retrieval
document_embeddings = FeatureView(
    name="embedded_documents",
    entities=[document],
    schema=[
        Field(
            name="vector",
            dtype=Array(Float32),
            vector_index=True,                # Vector search enabled
            vector_search_metric="COSINE",    # Distance metric configured
        ),
        Field(name="document_id", dtype=Int64),
        Field(name="created_timestamp", dtype=UnixTimestamp),
        Field(name="sentence_chunks", dtype=String),
        Field(name="event_timestamp", dtype=UnixTimestamp),
    ],
    source=source,
    ttl=timedelta(hours=24),
)
```

## Step 3: Update your Registry

Apply the feature view definitions to the registry:

```bash
feast apply
```

## Step 4: Ingest your Data

Process your documents, generate embeddings, and ingest them into the Feast online store:

```python
from feast import FeatureStore
import pandas as pd
import numpy as np
from transformers import AutoTokenizer, AutoModel
import torch
import torch.nn.functional as F

# Initialize FeatureStore
store = FeatureStore(".")

# Function to generate embeddings
def mean_pooling(model_output, attention_mask):
    token_embeddings = model_output[0]
    input_mask_expanded = attention_mask.unsqueeze(-1).expand(token_embeddings.size()).float()
    return torch.sum(token_embeddings * input_mask_expanded, 1) / torch.clamp(
        input_mask_expanded.sum(1), min=1e-9
    )

def generate_embeddings(sentences, tokenizer, model):
    encoded_input = tokenizer(
        sentences, padding=True, truncation=True, return_tensors="pt"
    )
    with torch.no_grad():
        model_output = model(**encoded_input)
    sentence_embeddings = mean_pooling(model_output, encoded_input["attention_mask"])
    sentence_embeddings = F.normalize(sentence_embeddings, p=2, dim=1)
    return sentence_embeddings.detach().cpu().numpy()

# Example data
data = {
    "document_id": [1, 2, 3],
    "sentence_chunks": [
        "New York City is the most populous city in the United States.",
        "Los Angeles is the second most populous city in the United States.",
        "Chicago is the third most populous city in the United States."
    ],
    "event_timestamp": pd.to_datetime(["2023-01-01", "2023-01-01", "2023-01-01"]),
    "created_timestamp": pd.to_datetime(["2023-01-01", "2023-01-01", "2023-01-01"])
}

# Load model and tokenizer
tokenizer = AutoTokenizer.from_pretrained("sentence-transformers/all-MiniLM-L6-v2")
model = AutoModel.from_pretrained("sentence-transformers/all-MiniLM-L6-v2")

# Generate embeddings
embeddings = generate_embeddings(data["sentence_chunks"], tokenizer, model)

# Create DataFrame with embeddings
df = pd.DataFrame(data)
df["vector"] = embeddings.tolist()

# Write to online store
store.write_to_online_store(feature_view_name='embedded_documents', df=df)
```

## Step 5: Retrieve Relevant Documents

Now you can retrieve the most relevant documents for a given query:

```python
from feast import FeatureStore

# Initialize FeatureStore
store = FeatureStore(".")

# Generate query embedding
query = "What is the largest city in the US?"
tokenizer = AutoTokenizer.from_pretrained("sentence-transformers/all-MiniLM-L6-v2")
model = AutoModel.from_pretrained("sentence-transformers/all-MiniLM-L6-v2")
query_embedding = generate_embeddings([query], tokenizer, model)[0].tolist()

# Retrieve similar documents
context_data = store.retrieve_online_documents_v2(
    features=[
        "embedded_documents:vector",
        "embedded_documents:document_id",
        "embedded_documents:sentence_chunks",
    ],
    query=query_embedding,
    top_k=3,
    distance_metric='COSINE',
).to_df()

print(context_data)
```

## Step 6: Use Retrieved Documents for Generation

Finally, you can use the retrieved documents as context for an LLM:

```python
from openai import OpenAI
import os

client = OpenAI(
    api_key=os.environ.get("OPENAI_API_KEY"),
)

# Format documents for context
def format_documents(context_data, base_prompt):
    documents = "\n".join([f"Document {i+1}: {row['embedded_documents__sentence_chunks']}" 
                          for i, row in context_data.iterrows()])
    return f"{base_prompt}\n\nContext documents:\n{documents}"

BASE_PROMPT = """You are a helpful assistant that answers questions based on the provided context."""
FULL_PROMPT = format_documents(context_data, BASE_PROMPT)

# Generate response
response = client.chat.completions.create(
    model="gpt-4o-mini",
    messages=[
        {"role": "system", "content": FULL_PROMPT},
        {"role": "user", "content": query}
    ],
)

print(response.choices[0].message.content)
```

## Why Feast for RAG?

Feast makes it remarkably easy to set up and manage a RAG system by:

1. Simplifying vector database configuration and management
2. Providing a consistent API for both writing and reading embeddings
3. Supporting both batch and real-time data ingestion
4. Enabling versioning and governance of your document repository
5. Offering seamless integration with multiple vector database backends
6. Providing a unified API for managing both feature data and document embeddings

For more details on using vector databases with Feast, see the [Vector Database documentation](../reference/alpha-vector-database.md).

The complete demo code is available in the [GitHub repository](https://github.com/feast-dev/feast/tree/master/examples/rag-docling).
