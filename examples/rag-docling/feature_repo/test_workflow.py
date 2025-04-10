import warnings

import pandas as pd
from feast import FeatureStore
from example_repo import docling_transform_docs, embed_text

warnings.filterwarnings('ignore')
store = FeatureStore(repo_path=".")

def write_to_online_store():
    df = pd.read_parquet("./data/docling_samples.parquet")
    mdf = pd.read_parquet("./data/metadata_samples.parquet")
    df['chunk_embedding'] = df['vector'].apply(lambda x: x.tolist())
    df['created'] = pd.Timestamp.now()
    mdf['created'] = pd.Timestamp.now()

    store.apply([docling_transform_docs])

    print("writing the precomputed embeddings to the docling_feature_view in the online store")
    store.write_to_online_store(feature_view_name='docling_feature_view', df=df)

    print("transforming the pdfs during ingestion and writing to the docling_transform_docs view in the online store")
    store.write_to_online_store("docling_transform_docs", mdf)

def run_demo():
    question = "What's the name of this paper?"
    query_embedding = embed_text(question)

    print("retrieving the top k documents from the docling_feature_view")
    # Retrieve top k documents
    docling_features = store.retrieve_online_documents_v2(
        features=[
            "docling_feature_view:vector",
            "docling_feature_view:chunk_id",
            "docling_feature_view:raw_chunk_markdown",
            "docling_feature_view:file_name",
        ],
        query=query_embedding,
        top_k=3,
    )
    print("docling features =")
    print(docling_features.to_df())

    print("retrieving the top k documents from the docling_transform_docs view")
    # Retrieve top k documents
    features = store.retrieve_online_documents_v2(
        features=[
            "docling_transform_docs:vector",
            "docling_transform_docs:chunk_id",
            "docling_transform_docs:chunk_text",
            "docling_transform_docs:document_id",
        ],
        query=query_embedding,
        top_k=3,
    )
    print("docling transformed features =")
    print(features.to_df())

def run_entity_retrieval_demo():
    entity_retrieved_features = store.get_online_features(
        features=[
            "docling_transform_docs:vector",
            "docling_transform_docs:chunk_id",
            "docling_transform_docs:chunk_text",
            "docling_transform_docs:document_id",
        ],
        entity_rows=[{"document_id": "doc-1", "chunk_id": "chunk-1"}],
    )
    print("docling transformed features retrieved via entity =")
    print(entity_retrieved_features.to_df())

    # store.teardown()
if __name__ == "__main__":
    # write_to_online_store()
    # run_demo()
    run_entity_retrieval_demo()
