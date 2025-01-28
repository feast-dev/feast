import pandas as pd
import torch
import torch.nn.functional as F
from feast import FeatureStore
from transformers import AutoTokenizer, AutoModel
from example_repo import city_embeddings_feature_view, item

TOKENIZER = "sentence-transformers/all-MiniLM-L6-v2"
MODEL = "sentence-transformers/all-MiniLM-L6-v2"


def mean_pooling(model_output, attention_mask):
    token_embeddings = model_output[
        0
    ]  # First element of model_output contains all token embeddings
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
    # Compute token embeddings
    with torch.no_grad():
        model_output = model(**encoded_input)

    sentence_embeddings = mean_pooling(model_output, encoded_input["attention_mask"])
    sentence_embeddings = F.normalize(sentence_embeddings, p=2, dim=1)
    return sentence_embeddings

def run_demo():
    store = FeatureStore(repo_path=".")
    df = pd.read_parquet("./data/city_wikipedia_summaries_with_embeddings.parquet")
    embedding_length = len(df['vector'][0])
    print(f'embedding length = {embedding_length}')

    store.apply([city_embeddings_feature_view, item])
    fields = [
        f.name for f in city_embeddings_feature_view.features
    ] + city_embeddings_feature_view.entities + [city_embeddings_feature_view.batch_source.timestamp_field]
    print('\ndata=')
    print(df[fields].head().T)
    store.write_to_online_store("city_embeddings", df[fields][0:3])


    question = "the most populous city in the state of New York is New York"
    tokenizer = AutoTokenizer.from_pretrained(TOKENIZER)
    model = AutoModel.from_pretrained(MODEL)
    query_embedding = run_model(question, tokenizer, model)
    query = query_embedding.detach().cpu().numpy().tolist()[0]

    # Retrieve top k documents
    features = store.retrieve_online_documents_v2(
        features=[
            "city_embeddings:vector",
            "city_embeddings:item_id",
            "city_embeddings:state",
            "city_embeddings:sentence_chunks",
            "city_embeddings:wiki_summary",
        ],
        query=query,
        top_k=3,
    )
    print("features =")
    print(features.to_df())
    store.teardown()

if __name__ == "__main__":
    run_demo()
