---
title: Retrieval Augmented Generation with Feast 
description: How Feast empowers ML Engineers to ship RAG applications to Production.
date: 2025-03-17
authors: ["Francisco Javier Arceo"]
---

<div class="hero-image">
  <img src="/images/blog/space.jpg" alt="Exploring the Possibilities of AI" loading="lazy">
</div>


## Why Feature Stores Make Sense for GenAI

Feature stores have been developed over the [past decade](./what-is-a-feature-store.md) to address the challenges AI 
practitioners face in managing, serving, and scaling machine learning models in production.

Some of the key challenges include:
* Accessing the right raw data
* Building features from raw data
* Combining features into training data
* Calculating and serving features in production
* Monitoring features in production

Feast was specifically designed to address these challenges.

These same challenges extend naturally to Generative AI (GenAI) applications. While GenAI shares many of the production 
challenges faced by traditional machine learning models, the key difference is that it starts with a foundation model.

For GenAI use cases, feature stores enable the efficient management of context and metadata, both during 
training/fine-tuning and at inference time. A key advantage is the ability to treat LLM context, including prompts, 
as features. This means you can manage not only input context, tokenization, chunking, and embeddings, but also track 
and version the context used during model inference, ensuring consistency, transparency, and reproducibility across 
models and iterations.

With Feast, ML engineers can streamline the embedding generation process, ensure consistency across both offline and 
online environments, and track the lineage of data and transformations. By leveraging a feature store, GenAI 
applications benefit from enhanced scalability, maintainability, and reproducibility, making them ideal for complex 
AI applications and enterprise needs.

## Feast Supports Now RAG

With the rise of generative AI applications, the need to serve vectors has grown quickly. Feast now has alpha support 
for vector similarity search to power retrieval augmented generation (RAG) systems in production.

This allows ML Engineers and Data Scientists to use the power of their feature store to easily deploy GenAI 
applications using RAG to production. More importantly, Feast offers the flexibility to customize and scale your 
production RAG applications through our scalable transformation systems (streaming, request-time, and batch). 

<div class="content-image">
  <img src="/images/blog/milvus-rag" alt="Retrieval Augmented Generation with Milvus and Feast" loading="lazy">
</div>


## Retrieval Augmented Generation (RAG)
[RAG](https://en.wikipedia.org/wiki/Retrieval-augmented_generation) is a technique that combines generative models 
(e.g., LLMs) with retrieval systems to generate contextually relevant output for a particular goal (e.g., 
question and answering).

The typical RAG process involves:
1. Sourcing text data relevant for your application
2. Transforming each text document into smaller chunks of text
3. Transforming those chunks of text into embeddings
4. Inserting those chunks of text along with some identifier for the chunk and document in some database
5. Retrieving those chunks of text along with the identifiers at run-time to inject that text into the LLM's context
6. Calling some API to run inference with your LLM to generate contextually relevant output
7. Returning the output to some end user

Implicit in (1)-(4) is the potential of scaling to large amounts of data (i.e., using some form of distributed computing),
orchestrating that scaling through some batch or streaming pipeline, and customization of key transformation decisions
(e.g., tokenization, model, chunking, data format, etc.). This is again where Feast shines.

## Powering Retrieval in Production
To power the Retrieval step of RAG in Production, we need:
1. Ingestion
1. Transformation
1. Indexing
1. Retrieval

Building high availability software that can handle these steps and scale as your needs and data scales is a 
non-trivial task. This is a strength of Feast, using the power of Kuberenetes, large scale data frameworks like 
Spark and Flink, and the ability to ingest and transform data in real-time through the Feast Feature Server is 
a powerful combination.

## Beyond Vector Similarity Search
RAG patterns often use vector similarity search for the retrieval step, but this is not the
only retrieval pattern that can be useful. In fact, standard entity-based retrieval can be very powerful for 
applications where relevant user-context is necessary.

For example, many RAG applications are customer Chat Bots and they benefit significantly from user data (e.g., 
account balance, location, etc.) to generate contextually relevant output. Feast can help you manage this user data
using its existing entity based retrieval patterns.

## The Benefits of Feast
Fine tuning is the holy grail to optimize your RAG systems, and by logging the documents/data and context retrieved 
and during inference, you can ensure that you can fine-tune both the generator and *the retriever* your LLMs for 
your particular needs.

This means that Feast can help you not only serve your documents, user data, and other metadata for production 
RAG applications, but it can also help you scale your embeddings on large amounts of data (e.g,. using Spark to embed 
gigabytes of documents), re-use the same code online and offline, track changes to your transformations, data sources, and 
RAG-sources to provide you with replayability and data lineage, and prepare your datasets so you can fine tune your
embedding, retrieval, or generator models later.

Historically, Feast catered to Data Scientists and ML Engineers who implemented their own types of data/feature transformations but, now, 
many RAG providers handle this out of the box for you. We will invest in creating extendable implementations to make it easier 
to customize your applications.

## Feast Powered by Milvus

[Milvus](https://milvus.io/) is a high performance vector database that provides a powerful and efficient way to store 
and retrieve embeddings. By using Feast with Milvus, you can easily deploy RAG applications to production and scale 
your retrieval systems on Kubernetes using the Feast Operator or the [Feature Server Helm Chart](https://github.com/feast-dev/feast/tree/master/infra/charts/feast-feature-server).

This tutorial will walk you through building a basic RAG application with Milvus and Feast; i.e., ingesting embedded 
documents in Milvus and retrieving the most similar documents for a given query embedding.

This example consists of 5 steps:
1. Configuring Milvus
2. Defining your Data Sources and Views
3. Updating your Registry
4. Ingesting the Data
5. Retrieving the Data

The full demo is available on our [GitHub repository](https://github.com/feast-dev/feast/tree/master/examples/rag).

### Step 1: Configure Milvus
Configure milvus in a simple `yaml` file.
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

### Step 2: Define your Data Sources and Views
You define your data declaratively using Feast's `FeatureView` and `Entity` objects, which are meant to be an easy way
to give your software engineers and data scientists a common language to define data they want to ship to production.
Here is an example of how you might define a `FeatureView` for a document embedding
```python
document = Entity(
    name="document_id",
    description="Document ID",
    value_type=ValueType.INT64,
)

source = FileSource(
    file_format=ParquetFormat(),
    path="./data/my_data.parquet",
    timestamp_field="event_timestamp",
)

# Define the view for retrieval
city_embeddings_feature_view = FeatureView(
    name="city_embeddings",
    entities=[document],
    schema=[
        Field(
            name="vector",
            dtype=Array(Float32),
            vector_index=True,                # Vector search enabled
            vector_search_metric="COSINE",    # Distance metric configured
        ),
        Field(name="state", dtype=String),
        Field(name="sentence_chunks", dtype=String),
        Field(name="wiki_summary", dtype=String),
    ],
    source=source,
    ttl=timedelta(hours=2),
)
```

### Step 3: Update your Registry 
After we have defined our code we use the `feast apply` syntax in the same folder as the `feature_store.yaml` file and 
update the registry with our metadata.
```bash
feast apply
```

### Step 4: Ingest your Data
Now that we have defined our metadata, we can ingest our data into Milvus using the following code:
```python
store.write_to_online_store(feature_view_name='city_embeddings', df=df)
```

### Step 5: Retrieve your Data
Now that the data is actually stored in Milvus, we can easily query it using the SDK (and corresponding REST API) to 
retrieve the most similar documents for a given query embedding.
```python
context_data = store.retrieve_online_documents_v2(
    features=[
        "city_embeddings:vector",
        "city_embeddings:document_id",
        "city_embeddings:state",
        "city_embeddings:sentence_chunks",
        "city_embeddings:wiki_summary",
    ],
    query=query_embedding,
    top_k=3,
    distance_metric='COSINE',
).to_df()
```

### What are the benefits from using Feast?
You get a lot for free.
1. Data dictionary/metadata catalog autogenerated from code
3. UI exposing the metadata catalog 
2. FastAPI Server to serve your data
3. Role Based Access Control (RBAC) to govern access to your data
6. Deployment on Kubernetes using our Helm Chart or our Operator
7. Multiple vector database providers
8. Multiple data warehouse providers
9. Support for different data formats
10. Support for stream and batch processors  (e.g., Spark and Flink)

## The Future of Feast and GenAI

Feast will continue to invest in GenAI use cases. 

In particular, we will invest in (1) NLP as a first-class citizen, (2) add support for images, (3) support for 
transforming unstructured data (e.g., PDFs), (4) an enhanced GenAI focused feature server to allow our end-users to 
more easily ship RAG to production, (4) an out of the box chat UI meant for internal development and fast iteration, 
and (5) investing in [Milvus]([url](https://milvus.io/intro)) as a fully supported online store.
