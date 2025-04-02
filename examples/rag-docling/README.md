# 🚀 Quickstart: RAG, Milvus, and Docling with Feast

This project demonstrates how to use **Feast** to power a **Retrieval-Augmented Generation (RAG)** application.

In particular, this example expands on the basic RAG demo to show:
1. How to transform PDFs into text data with [Docling](https://docling-project.github.io/docling/) that can be used by LLMs
2. How to use [Milvus](https://milvus.io/) as a vector database to store and retrieve embeddings for RAG
3. How to transform PDFs with Docling during ingestion

## 💡 Why Use Feast for RAG?

- **Online retrieval of features:** Ensure real-time access to precomputed document embeddings and other structured data.
- **Declarative feature definitions:** Define feature views and entities in a Python file and empower Data Scientists to easily ship scalabe RAG applications with all of the existing benefits of Feast.
- **Vector search:** Leverage Feast’s integration with vector databases like **Milvus** to find relevant documents based on a similarity metric (e.g., cosine).
- **Structured and unstructured context:** Retrieve both embeddings and traditional features, injecting richer context into LLM prompts.
- **Versioning and reusability:** Collaborate across teams with discoverable, versioned feature transformations.

---

## 📂 Project Structure

- **`data/`**: Contains the demo data, including Wikipedia summaries of cities with sentence embeddings stored in a Parquet file.
- **`example_repo.py`**: Defines the feature views and entity configurations for Feast.
- **`feature_store.yaml`**: Configures the offline and online stores (using local files and Milvus Lite in this demo).

The project has two main notebooks:
1. [`docling-demo.ipynb`](./docling-demo.ipynb): Demonstrates how to use Docling to extract text from PDFs and store the text in a Parquet file.
2. [`docling-quickstart.ipynb`](./docling-quickstart.ipynb): Shows how to use Feast to ingest the text data and store and retrieve it from the online store.