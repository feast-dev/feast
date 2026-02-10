from datetime import datetime

import pandas as pd
import torch
import torch.nn.functional as F
from feast import FeatureStore
from transformers import AutoModel, AutoTokenizer

from example_repo import city, city_metadata, city_qa_v1, city_summary_embeddings

TOKENIZER = "sentence-transformers/all-MiniLM-L6-v2"
MODEL = "sentence-transformers/all-MiniLM-L6-v2"


def mean_pooling(model_output, attention_mask):
    token_embeddings = model_output[0]
    input_mask_expanded = (
        attention_mask.unsqueeze(-1).expand(token_embeddings.size()).float()
    )
    return torch.sum(token_embeddings * input_mask_expanded, 1) / torch.clamp(
        input_mask_expanded.sum(1), min=1e-9
    )


def run_model(sentences, tokenizer, model):
    encoded_input = tokenizer(
        sentences, padding=True, truncation=True, return_tensors="pt"
    )
    with torch.no_grad():
        model_output = model(**encoded_input)
    sentence_embeddings = mean_pooling(model_output, encoded_input["attention_mask"])
    sentence_embeddings = F.normalize(sentence_embeddings, p=2, dim=1)
    return sentence_embeddings


def run_demo():
    print("City Information Q&A - RAG Demo")
    
    store = FeatureStore(repo_path=".")
    print("\n[1/5] Applying feature definitions...")
    store.apply([city, city_summary_embeddings, city_metadata, city_qa_v1])
    print("      Registered:")
    print("        - Entity: city (city_id)")
    print("        - Feature View: city_summary_embeddings (vector search)")
    print("        - Feature View: city_metadata (scalar metadata)")
    print("        - Feature Service: city_qa_v1")

    print("\n[2/6] Materializing feature views into online store...")
    store.materialize(
        feature_views=["city_summary_embeddings", "city_metadata"],
        start_date=datetime(1970, 1, 1),
        end_date=datetime.now(),
        disable_event_timestamp=True,
    )
    print("      city_summary_embeddings and city_metadata materialized from parquet")

    print("\n[3/6] Verifying data...")
    df = pd.read_parquet("./data/city_wikipedia_summaries_with_embeddings.parquet")
    embedding_length = len(df["vector"][0])
    print(f"      Parquet: {len(df)} city records with {embedding_length}-dim embeddings")

    print("\n[4/6] Running semantic search...")
    question = "the most populous city in the state of New York"
    print(f'      Question: "{question}"')

    tokenizer = AutoTokenizer.from_pretrained(TOKENIZER)
    model = AutoModel.from_pretrained(MODEL)
    query_embedding = run_model(question, tokenizer, model)
    query = query_embedding.detach().cpu().numpy().tolist()[0]

    # Retrieve top 3 documents via vector search
    print("\n[5/6] Retrieving top 3 documents...")
    features = store.retrieve_online_documents_v2(
        features=[
            "city_summary_embeddings:vector",
            "city_summary_embeddings:city_id",
            "city_summary_embeddings:sentence_chunks",
        ],
        query=query,
        top_k=3,
    )

    print("\n" + "=" * 70)
    print("Retrieved Documents (Top 3) - Vector Search Results")
    print("=" * 70)
    results_df = features.to_df()
    print(results_df[["city_id", "sentence_chunks", "distance"]].to_string())

    print("\n[6/6] Metadata lookup - Get city details by ID")
    print("=" * 70)
    # Get metadata for the top result
    if len(results_df) > 0:
        top_city_id = int(results_df["city_id"].iloc[0])
        print(f"      Looking up metadata for city_id={top_city_id}")
        
        metadata_features = store.get_online_features(
            features=[
                "city_metadata:state",
                "city_metadata:wiki_summary",
            ],
            entity_rows=[{"city_id": top_city_id}],
        ).to_dict()
        
        print(f"      State: {metadata_features['state'][0]}")
        print(f"      Summary: {metadata_features['wiki_summary'][0][:200]}...")

    store.teardown()
    print("\n[Done] Feature store torn down.")


if __name__ == "__main__":
    run_demo()
