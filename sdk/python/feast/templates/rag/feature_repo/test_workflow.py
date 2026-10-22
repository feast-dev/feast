from datetime import datetime

import pandas as pd
import torch
import torch.nn.functional as F
from example_repo import (
    city,
    city_metadata,
    city_qa_v1,
    city_qa_v2,
    city_summary_embeddings,
    city_summary_embeddings_realtime,
)
from transformers import AutoModel, AutoTokenizer

from feast import FeatureStore
from feast.data_source import PushMode

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
    print("\n[1/7] Applying feature definitions...")
    store.apply(
        [
            city,
            city_summary_embeddings,
            city_metadata,
            city_summary_embeddings_realtime,
            city_qa_v1,
            city_qa_v2,
        ]
    )
    print(
        "      Entity, 3 feature views (batch + push), 2 feature services registered."
    )

    print("\n[2/7] Materializing batch views into online store...")
    store.materialize(
        feature_views=["city_summary_embeddings", "city_metadata"],
        start_date=datetime(1970, 1, 1),
        end_date=datetime.now(),
        disable_event_timestamp=True,
    )
    print("      city_summary_embeddings, city_metadata materialized from parquet.")

    print("\n[3/7] Verifying batch data...")
    df = pd.read_parquet("./data/city_wikipedia_summaries_with_embeddings.parquet")
    embedding_length = len(df["vector"][0])
    print(f"      Parquet: {len(df)} rows, {embedding_length}-dim vectors.")

    tokenizer = AutoTokenizer.from_pretrained(TOKENIZER)
    model = AutoModel.from_pretrained(MODEL)

    print("\n[4/7] Pushing one document to PushSource...")
    push_text = "Demo pushed city for testing RAG and real-time ingestion."
    push_embedding = run_model([push_text], tokenizer, model)
    push_vector = push_embedding.detach().cpu().numpy().tolist()[0]
    push_df = pd.DataFrame.from_dict(
        {
            "city_id": [99999],
            "event_timestamp": [datetime.now()],
            "vector": [push_vector],
            "sentence_chunks": [push_text],
        }
    )
    store.push("city_summaries_push_source", push_df, to=PushMode.ONLINE)
    print("      Pushed city_id=99999 to city_summary_embeddings_realtime.")

    print("\n[5/7] Vector search (batch view)...")
    question = "the most populous city in the state of New York"
    print(f'      Query: "{question}"')

    query_embedding = run_model(question, tokenizer, model)
    query = query_embedding.detach().cpu().numpy().tolist()[0]

    features_batch = store.retrieve_online_documents_v2(
        features=[
            "city_summary_embeddings:vector",
            "city_summary_embeddings:city_id",
            "city_summary_embeddings:sentence_chunks",
        ],
        query=query,
        top_k=3,
    )
    print("      Top 3 (city_summary_embeddings):")
    results_batch_df = features_batch.to_df()
    print(results_batch_df[["city_id", "sentence_chunks", "distance"]].to_string())

    print("\n[6/7] Vector search (realtime view, includes pushed doc)...")
    question_realtime = "Demo pushed city for testing RAG"
    query_realtime = (
        run_model([question_realtime], tokenizer, model)
        .detach()
        .cpu()
        .numpy()
        .tolist()[0]
    )
    features_realtime = store.retrieve_online_documents_v2(
        features=[
            "city_summary_embeddings_realtime:vector",
            "city_summary_embeddings_realtime:city_id",
            "city_summary_embeddings_realtime:sentence_chunks",
        ],
        query=query_realtime,
        top_k=3,
    )
    results_realtime_df = features_realtime.to_df()
    print("      Top 3 (city_summary_embeddings_realtime):")
    print(results_realtime_df[["city_id", "sentence_chunks", "distance"]].to_string())
    if any(results_realtime_df["city_id"] == 99999):
        print("      Pushed doc (city_id=99999) in results.")

    print("\n[7/7] Metadata via Feature Service V2 (city_qa_v2)...")
    top_city_id = int(results_batch_df["city_id"].iloc[0])

    metadata_features = store.get_online_features(
        features=store.get_feature_service("city_qa_v2"),
        entity_rows=[{"city_id": top_city_id}],
    ).to_dict()

    print(f"      city_id={top_city_id} -> state: {metadata_features['state'][0]}")
    print(f"      wiki_summary: {metadata_features['wiki_summary'][0][:200]}...")

    store.teardown()
    print("\nDone.")


if __name__ == "__main__":
    run_demo()
