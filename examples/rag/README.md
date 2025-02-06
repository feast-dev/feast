# 🚀 Quickstart: Retrieval-Augmented Generation (RAG) using Feast and Large Language Models (LLMs)

This project demonstrates how to use **Feast** to power a **Retrieval-Augmented Generation (RAG)** application. 
The RAG architecture combines retrieval of documents (using vector search) with In-Context-Learning (ICL) through a 
**Large Language Model (LLM)** to answer user questions accurately using structured and unstructured data.

## 💡 Why Use Feast for RAG?

- **Online retrieval of features:** Ensure real-time access to precomputed document embeddings and other structured data.
- **Declarative feature definitions:** Define feature views and entities in a Python file and empower Data Scientists to easily ship scalabe RAG applications with all of the existing benefits of Feast.
- **Vector search:** Leverage Feast’s integration with vector databases like **Milvus** to find relevant documents based on a similarity metric (e.g., cosine).
- **Structured and unstructured context:** Retrieve both embeddings and traditional features, injecting richer context into LLM prompts.
- **Versioning and reusability:** Collaborate across teams with discoverable, versioned data pipelines.

---

## 📂 Project Structure

- **`data/`**: Contains the demo data, including Wikipedia summaries of cities with sentence embeddings stored in a Parquet file.
- **`example_repo.py`**: Defines the feature views and entity configurations for Feast.
- **`feature_store.yaml`**: Configures the offline and online stores (using local files and Milvus Lite in this demo).
- **`test_workflow.py`**: Demonstrates key Feast commands to define, retrieve, and push features.

---

## 🛠️ Setup

1. **Install the necessary packages**:
   ```bash
   pip install feast torch transformers openai
   ```
2. Initialize and inspect the feature store:

   ```bash
     feast apply
   ```

3. Materialize features into the online store:

   ```python
   store.write_to_online_store(feature_view_name='city_embeddings', df=df)
   ``` 
4. Run a query:

- Prepare your question:
`question = "Which city has the largest population in New York?"`
- Embed the question using sentence-transformers/all-MiniLM-L6-v2.
- Retrieve the top K most relevant documents using Milvus vector search.
- Pass the retrieved context to the OpenAI model for conversational output.

## 🛠️ Key Commands for Data Scientists
- Apply feature definitions:

```bash 
feast apply 
```

- Materialize features to the online store:
```python
store.write_to_online_store(feature_view_name='city_embeddings', df=df)
```

- Inspect retrieved features using Python:
```python
context_data = store.retrieve_online_documents_v2(
    features=[
        "city_embeddings:vector",
        "city_embeddings:item_id",
        "city_embeddings:state",
        "city_embeddings:sentence_chunks",
        "city_embeddings:wiki_summary",
    ],
    query=query,
    top_k=3,
    distance_metric='COSINE',
).to_df()
display(context_data)
```

📊 Example Output
When querying: Which city has the largest population in New York?

The model provides:

```
The largest city in New York is New York City, often referred to as NYC. It is the most populous city in the United States, with an estimated population of 8,335,897 in 2022.
```