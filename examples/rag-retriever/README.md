# End-to-end RAG Fine Tuning example using Feast and Milvus.

## Introduction
This example notebook provides a step-by-step demonstration of building and using a RAG system with Feast and the custom FeastRagRetriever. The notebook walks through:

1. Data Preparation
   - Loads a subset of the [Wikipedia DPR dataset](https://huggingface.co/datasets/facebook/wiki_dpr) (1% of training data)
   - Implements text chunking with configurable chunk size and overlap
   - Processes text into manageable passages with unique IDs

2. Embedding Generation
   - Uses `all-MiniLM-L6-v2` sentence transformer model
   - Generates 384-dimensional embeddings for text passages
   - Demonstrates batch processing with GPU support

3. Feature Store Setup
   - Creates a Parquet file as the historical data source
   - Configures Feast with the feature repository
   - Demonstrates writing embeddings from data source to Milvus online store which can be used for model training later

4. RAG System Implementation
   - **Embedding Model**: `all-MiniLM-L6-v2` (configurable)
   - **Generator Model**: `granite-3.2-2b-instruct` (configurable)
   - **Vector Store**: Custom implementation with Feast integration
   - **Retriever**: Custom implementation extending HuggingFace's RagRetriever

5. Query Demonstration
   - Perform inference with retrieved context

## Requirements
 - A Kubernetes cluster with:
   - GPU nodes available (for model inference)
   - At least 200GB of storage
   - A standalone Milvus deployment. See example [here](https://github.com/milvus-io/milvus-helm/tree/master/charts/milvus).

## Running the example
Clone this repository: https://github.com/feast-dev/feast.git
Navigate to the examples/rag-retriever directory. Here you will find the following files: 

* **feature_repo/feature_store.yaml**
  This is the core configuration file for the RAG project's feature store, configuring a Milvus online store on a local provider. 
  * In order to configure Milvus you should:
     - Update `feature_store.yaml` with your Milvus connection details:
       - host
       - port (default: 19530)
       - credentials (if required)

* **__feature_repo/ragproject_repo.py__**
  This is the Feast feature repository configuration that defines the schema and data source for Wikipedia passage embeddings. 

* **__rag_feast.ipynb__**
  This is a notebook demonstrating the implementation of a RAG system using Feast. The notebook provides:

  - A complete end-to-end example of building a RAG system with:
    - Data preparation using the Wiki DPR dataset
    - Text chunking and preprocessing
    - Vector embedding generation using sentence-transformers
    - Integration with Milvus vector store
    - Inference utilising a custom RagRetriever: FeastRagRetriever
  - Uses `all-MiniLM-L6-v2` for generating embeddings
  - Implements `granite-3.2-2b-instruct` as the generator model

Open `rag_feast.ipynb` and follow the steps in the notebook to run the example.

## Using DocEmbedder for Simplified Ingestion

As an alternative to the manual data preparation steps in the notebook above, Feast provides the `DocEmbedder` class that automates the entire document-to-embeddings pipeline: chunking, embedding generation, FeatureView creation, and writing to the online store.

### Install Dependencies

```bash
pip install feast[milvus,rag]
```

### Quick Start

```python
from feast import DocEmbedder
from datasets import load_dataset

# Load your dataset
dataset = load_dataset("facebook/wiki_dpr", "psgs_w100.nq.exact", split="train[:1%]",
                       with_index=False, trust_remote_code=True)
df = dataset.select(range(100)).to_pandas()

# DocEmbedder handles everything in one step
embedder = DocEmbedder(
    repo_path="feature_repo_docembedder/",
    feature_view_name="text_feature_view",
)

result = embedder.embed_documents(
    documents=df,
    id_column="id",
    source_column="text",
    column_mapping=("text", "text_embedding"),
)
```

### What DocEmbedder Does

1. **Generates a FeatureView**: Automatically creates a Python file with Entity and FeatureView definitions compatible with `feast apply`
2. **Applies the repo**: Registers the FeatureView in the Feast registry and deploys infrastructure (e.g., Milvus collection)
3. **Chunks documents**: Splits text into smaller passages using `TextChunker` (configurable chunk size, overlap, etc.)
4. **Generates embeddings**: Produces vector embeddings using `MultiModalEmbedder` (defaults to `all-MiniLM-L6-v2`)
5. **Writes to online store**: Stores the processed data in your configured online store (e.g., Milvus)

### Customization

* **Custom Chunker**: Subclass `BaseChunker` for your own chunking strategy
* **Custom Embedder**: Subclass `BaseEmbedder` to use a different embedding model
* **Logical Layer Function**: Provide a `SchemaTransformFn` to control how the output maps to your FeatureView schema

### Example Notebook

See **`rag_feast_docembedder.ipynb`** for a complete end-to-end example that uses DocEmbedder with the Wiki DPR dataset and then queries the results using `FeastRAGRetriever`.

## FeastRagRetriver Low Level Design

<img src="images/FeastRagRetriever.png" width="800" height="450" alt="Low level design for feast rag retriever">

## Helpful Information
- Ensure your Milvus instance is properly configured and running
- Vector dimensions and similarity metrics can be adjusted in the feature store configuration
- The example uses Wikipedia data, but the system can be adapted for other datasets
