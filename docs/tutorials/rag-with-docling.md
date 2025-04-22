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
- Feast installed with Milvus support: `pip install feast[milvus, nlp]`
- A basic understanding of feature stores and vector embeddings

## Step 0: Download, Compute, and Export the Docling Sample Dataset
```python
import os
import io
import pypdf
import logging
import hashlib 
from datetime import datetime
import requests
import pandas as pd
from transformers import AutoTokenizer
from sentence_transformers import SentenceTransformer

from docling.datamodel.base_models import ConversionStatus, InputFormat
from docling.datamodel.pipeline_options import PdfPipelineOptions
from docling.document_converter import DocumentConverter, PdfFormatOption
from docling.chunking import HybridChunker
logging.basicConfig(level=logging.INFO)
_log = logging.getLogger(__name__)

# Base URL for PDFs
BASE_URL = 'https://raw.githubusercontent.com/DS4SD/docling/refs/heads/main/tests/data/pdf/'
PDF_FILES = [
    '2203.01017v2.pdf', '2305.03393v1-pg9.pdf', '2305.03393v1.pdf',
    'amt_handbook_sample.pdf', 'code_and_formula.pdf', 'picture_classification.pdf',
    'redp5110_sampled.pdf', 'right_to_left_01.pdf', 'right_to_left_02.pdf', 'right_to_left_03.pdf'
]
INPUT_DOC_PATHS = [os.path.join(BASE_URL, pdf_file) for pdf_file in PDF_FILES]

# Configure PDF processing
pipeline_options = PdfPipelineOptions()
pipeline_options.generate_page_images = True

doc_converter = DocumentConverter(
    format_options={InputFormat.PDF: PdfFormatOption(pipeline_options=pipeline_options)}
)

# Load tokenizer and embedding model
EMBED_MODEL_ID = "sentence-transformers/all-MiniLM-L6-v2"
MAX_TOKENS = 64  # Small token limit for demonstration
tokenizer = AutoTokenizer.from_pretrained(EMBED_MODEL_ID)
embedding_model = SentenceTransformer(EMBED_MODEL_ID)

chunker = HybridChunker(tokenizer=tokenizer, max_tokens=MAX_TOKENS, merge_peers=True)

def embed_text(text: str) -> list[float]:
    """Generate an embedding for a given text."""
    return embedding_model.encode([text], normalize_embeddings=True).tolist()[0]

def generate_document_rows(conv_results):
    """
    Generator that yields one row per chunk from each successfully converted document.
    Each yielded dict contains:
      - file_name: Name of the source file.
      - raw_markdown: Serialized text for the chunk.
      - chunk_embedding: The embedding vector for that chunk.
    """
    processed_docs = 0
    for conv_res in conv_results:
        if conv_res.status != ConversionStatus.SUCCESS:
            continue

        processed_docs += 1
        file_name = conv_res.input.file.stem  # FIX: Use `.file.stem` instead of `.path`

        # Extract the document object (which contains iterate_items)
        document = conv_res.document
        try:
            document_markdown = document.export_to_markdown()
        except:
            document_markdown = ""
        if document is None:
            _log.warning(f"Document conversion failed for {file_name}")
            continue

        # Process each chunk from the document
        for chunk in chunker.chunk(dl_doc=document):  # Use `document` here!
            raw_chunk = chunker.serialize(chunk=chunk)
            embedding = embed_text(raw_chunk)
            yield {
                "file_name": file_name,
                "full_document_markdown": document_markdown,
                "raw_chunk_markdown": raw_chunk,
                "chunk_embedding": embedding,
            }
    _log.info(f"Processed {processed_docs} documents successfully.")

def generate_chunk_id(file_name: str, raw_chunk_markdown: str) -> str:
    """Generate a unique chunk ID based on file_name and raw_chunk_markdown."""
    unique_string = f"{file_name}-{raw_chunk_markdown}"
    return hashlib.sha256(unique_string.encode()).hexdigest()

conv_results = doc_converter.convert_all(INPUT_DOC_PATHS, raises_on_error=False)

# Build a DataFrame where each row is a unique chunk record
rows = list(generate_document_rows(conv_results))
df = pd.DataFrame.from_records(rows)
output_dict = {}
for file_name in PDF_FILES:
    try:
        r = requests.get(BASE_URL + file_name)
        pdf_bytes = io.BytesIO(r.content)
        output_dict[file_name] = pdf_bytes.getvalue()
    except Exception as e:
        print(f"error with {file_name} \n{e}")

odf = pd.DataFrame.from_dict(output_dict, orient='index', columns=['bytes']).reset_index()
odf.rename({"index": "file_name"}, axis=1, inplace=True)
odf['file_name'] = odf['file_name'].str.replace('.pdf', '')
finaldf = df.merge(odf, on='file_name', how='left')
finaldf["chunk_id"] = finaldf.apply(lambda row: generate_chunk_id(row["file_name"], row["raw_chunk_markdown"]), axis=1)

finaldf['created'] = datetime.now()

pdf_example = pypdf.PdfReader(io.BytesIO(finaldf['bytes'].values[0]))
finaldf.drop(['full_document_markdown', 'bytes'], axis=1).to_parquet('feature_repo/data/docling_samples.parquet', index=False)
odf.to_parquet('feature_repo/data/metadata_samples.parquet', index=False)
```
## Step 1: Configure Milvus in Feast

Create a `feature_store.yaml` file with the following configuration:

```yaml
project: docling-rag
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
auth:
  type: no_auth
```

## Step 2: Define your Data Sources and Views

Create a `feature_repo.py` file to define your entities, data sources, and feature views:

```python
from datetime import timedelta

import pandas as pd
from feast import (
    FeatureView,
    Field,
    FileSource,
    Entity,
    RequestSource,
)
from feast.data_format import ParquetFormat
from feast.types import Float64, Array, String, ValueType, PdfBytes
from feast.on_demand_feature_view import on_demand_feature_view
from sentence_transformers import SentenceTransformer
from typing import Dict, Any, List

import hashlib
from docling.datamodel.base_models import DocumentStream

import io
from docling.document_converter import DocumentConverter
from transformers import AutoTokenizer
from sentence_transformers import SentenceTransformer
from docling.chunking import HybridChunker

# Load tokenizer and embedding model
EMBED_MODEL_ID = "sentence-transformers/all-MiniLM-L6-v2"
MAX_TOKENS = 64  # Small token limit for demonstration

tokenizer = AutoTokenizer.from_pretrained(EMBED_MODEL_ID)
embedding_model = SentenceTransformer(EMBED_MODEL_ID)
chunker = HybridChunker(tokenizer=tokenizer, max_tokens=MAX_TOKENS, merge_peers=True)

def embed_text(text: str) -> list[float]:
    """Generate an embedding for a given text."""
    return embedding_model.encode([text], normalize_embeddings=True).tolist()[0]

def generate_chunk_id(file_name: str, raw_chunk_markdown: str="") -> str:
    """Generate a unique chunk ID based on file_name and raw_chunk_markdown."""
    unique_string = f"{file_name}-{raw_chunk_markdown}" if raw_chunk_markdown != "" else f"{file_name}"
    return hashlib.sha256(unique_string.encode()).hexdigest()

# Define entities
chunk = Entity(
    name="chunk_id",
    description="Chunk ID",
    value_type=ValueType.STRING,
    join_keys=["chunk_id"],
)

document = Entity(
    name="document_id",
    description="Document ID",
    value_type=ValueType.STRING,
    join_keys=["document_id"],
)

source = FileSource(
    file_format=ParquetFormat(),
    path="./data/docling_samples.parquet",
    timestamp_field="created",
)

input_request_pdf = RequestSource(
    name="pdf_request_source",
    schema=[
        Field(name="document_id", dtype=String),        
        Field(name="pdf_bytes", dtype=PdfBytes),
        Field(name="file_name", dtype=String),
    ],
)

# Define the view for retrieval
docling_example_feature_view = FeatureView(
    name="docling_feature_view",
    entities=[chunk],
    schema=[
        Field(name="file_name", dtype=String),
        Field(name="raw_chunk_markdown", dtype=String),
        Field(
            name="vector",
            dtype=Array(Float64),
            vector_index=True,
            vector_search_metric="COSINE",
        ),
        Field(name="chunk_id", dtype=String),
    ],
    source=source,
    ttl=timedelta(hours=2),
)

@on_demand_feature_view(
    entities=[chunk, document],
    sources=[input_request_pdf],
    schema=[
        Field(name="document_id", dtype=String),
        Field(name="chunk_id", dtype=String),
        Field(name="chunk_text", dtype=String),
        Field(
            name="vector",
            dtype=Array(Float64),
            vector_index=True,
            vector_search_metric="L2",
        ),
    ],
    mode="python",
    write_to_online_store=True,
    singleton=True,
)
def docling_transform_docs(inputs: dict[str, Any]):
    document_ids, chunks, embeddings, chunk_ids = [], [], [], []
    buf = io.BytesIO(
        inputs["pdf_bytes"],
    )
    doc_source = DocumentStream(name=inputs["file_name"], stream=buf)
    converter = DocumentConverter()
    result = converter.convert(doc_source)
    for i, chunk in enumerate(chunker.chunk(dl_doc=result.document)):
        raw_chunk = chunker.serialize(chunk=chunk)
        embedding = embed_text(raw_chunk)
        chunk_id = f"chunk-{i}"
        document_ids.append(inputs["document_id"])
        chunks.append(raw_chunk)
        chunk_ids.append(chunk_id)
        embeddings.append(embedding)
    return {
        "document_id": document_ids,
        "chunk_id": chunk_ids,
        "vector": embeddings,
        "chunk_text": chunks,
    }
```

## Step 3: Update your Registry

Apply the feature view definitions to the registry:

```bash
feast apply
```

## Step 4: Ingest your Data

Process your documents, generate embeddings, and ingest them into the Feast online store:

```python
import pandas as pd 
from feast import FeatureStore

store = FeatureStore(repo_path=".")

df = pd.read_parquet("./data/docling_samples.parquet")
mdf = pd.read_parquet("./data/metadata_samples.parquet")
df['chunk_embedding'] = df['vector'].apply(lambda x: x.tolist())
embedding_length = len(df['vector'][0])
print(f'embedding length = {embedding_length}')
df['created'] = pd.Timestamp.now()
mdf['created'] = pd.Timestamp.now()

# Ingesting transformed data to the feature view that has no associated transformation
store.write_to_online_store(feature_view_name='docling_feature_view', df=df)

# Turning off transformation on writes is as simple as changing the default behavior
store.write_to_online_store(
    feature_view_name='docling_transform_docs', 
    df=df[df['document_id']!='doc-1'], 
    transform_on_write=False,
)

# Now we can transform a raw PDF on the fly
store.write_to_online_store(
    feature_view_name='docling_transform_docs', 
    df=mdf[mdf['document_id']=='doc-1'], 
    transform_on_write=True, # this is the default
)
```

## Step 5: Retrieve Relevant Documents

Now you can retrieve the most relevant documents for a given query:

```python
from feast import FeatureStore

# Initialize FeatureStore
store = FeatureStore(".")

# Generate query embedding
question = 'Who are the authors of the paper?'
query_embedding = embed_text(question)

# Retrieve similar documents
context_data = store.retrieve_online_documents_v2(
    features=[
        "docling_feature_view:vector",
        "docling_feature_view:file_name",
        "docling_feature_view:raw_chunk_markdown",
        "docling_feature_view:chunk_id",
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
        {"role": "user", "content": query_embedding}
    ],
)

print('\n'.join([c.message.content for c in response.choices]))
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
