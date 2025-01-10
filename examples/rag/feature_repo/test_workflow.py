import pandas as pd
import torch
import torch.nn.functional as F
from feast import FeatureStore
from pymilvus import MilvusClient, DataType, FieldSchema
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

    print('\ndata=')
    print(df.head().T)

    store.apply([city_embeddings_feature_view, item])
    store.write_to_online_store("city_embeddings", df)

    client = MilvusClient(uir="http://localhost:19530", token="username:password")
    fields = [
        FieldSchema(name="id", dtype=DataType.INT64, is_primary=True),
        FieldSchema(name='state', dtype=DataType.STRING, description="State"),
        FieldSchema(name='wiki_summary', dtype=DataType.STRING, description="State"),
        FieldSchema(name='sentence_chunks', dtype=DataType.STRING, description="Sentence Chunks"),
        FieldSchema(name="item_id", dtype=DataType.INT64, default_value=0, description="Item"),
        FieldSchema(name="vector", dtype=DataType.FLOAT_VECTOR, dim=embedding_length, description="vector")
    ]
    cols = [f.name for f in fields]
    client.insert(
        collection_name="demo_collection",
        data=df[cols].to_dict(orient="records"),
        schema=fields,
    )
    print('\n')
    print('collections', client.list_collections())
    print('query results =', client.query(
        collection_name="rag_city_embeddings",
        filter="item_id == 0",
        # output_fields=['city_embeddings', 'item_id', 'city_name'],
    ))
    print('query results2 =', client.query(
        collection_name="rag_city_embeddings",
        filter="item_id >= 0",
        output_fields=["count(*)"]
        # output_fields=['city_embeddings', 'item_id', 'city_name'],
    ))
    question = "the most populous city in the U.S. state of Texas?"
    tokenizer = AutoTokenizer.from_pretrained(TOKENIZER)
    model = AutoModel.from_pretrained(MODEL)
    query_embedding = run_model(question, tokenizer, model)
    query = query_embedding.detach().cpu().numpy().tolist()[0]

    # Retrieve top k documents
    features = store.retrieve_online_documents(
        feature=None,
        features=["city_embeddings:vector", "city_embeddings:item_id", "city_embeddings:state"],
        query=query,
        top_k=3
    )
    print("features", features.to_df())


if __name__ == "__main__":
    run_demo()
