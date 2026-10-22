# Feast Launches Support for Vector Databases 🚀

*July 25, 2024* | *Daniel Dowler, Francisco Javier Arceo*

## Feast and Vector Databases

With the rise of generative AI applications, the need to serve vectors has grown quickly. We are pleased to announce that Feast now supports (as an experimental feature in Alpha) embedding vector features for popular GenAI use-cases such as RAG (retrieval augmented generation).

An important consideration is that GenAI applications using embedding vectors stand to benefit from a formal feature framework, just as traditional ML applications do. We are excited about adding support for embedding vector features because of the opportunity to improve GenAI backend operations. The integration of embedding vectors as features into Feast, allows GenAI developers to take advantage of MLOps best practices, lowering development time, improving quality of work, and sets the stage for [Retrieval Augmented Fine Tuning](https://techcommunity.microsoft.com/t5/ai-ai-platform-blog/retrieval-augmented-fine-tuning-raft-with-azure-ai/ba-p/3979114).

## Setting Up a Document Embedding Feature View

The [feast-workshop repo example](https://github.com/feast-dev/feast-workshop/tree/main) shows how Feast users can define feature views with vector database sources. They can easily convert text queries to embedding vectors, which are then matched against a vector database to retrieve closest vector records. All of this works seamlessly within the Feast toolset, so that vector features become a natural addition to the Feast feature store solution.

Defining a feature backed by a vector database is very similar to defining other types of features in Feast. Specifically, we can use the FeatureView class with an Array type field.

```python
from datetime import timedelta
from feast import FeatureView
from feast.types import Array, Float32
from feast.field import Field

for key, value in sorted(features.items()):
    print(key, " : ", value)

print_online_features(features)
```

## Supported Vector Databases

The Feast development team has conducted preliminary testing with the following vector stores:

* SQLite
* Postgres with the PGVector extension
* Elasticsearch

There are many more vector store solutions available, and we are excited about discovering how Feast may work with them to support vector feature use-cases. We welcome community contributions in this area–if you have any thoughts feel free to join the conversation on GitHub

## Final Thoughts

Feast brings formal feature operations support to AI/ML teams, enabling them to produce models faster and at higher levels of quality. The need for feature store support naturally extends to vector embeddings as features from vector databases (i.e., online stores). Vector storage and retrieval is an active space with lots of development and solutions. We are excited by where the space is moving, and look forward to Feast's role in operationalizing embedding vectors as first class features in the MLOps ecosystem.

If you are new to feature stores and MLOps, this is a great time to give Feast a try. Check out [Feast documentation](https://feast.dev/) and the [Feast GitHub](https://github.com/feast-dev/feast) page for more on getting started. Big thanks to [Hao Xu](https://www.linkedin.com/in/hao-xu-a04436103/) and the community for their contributions to this effort.
