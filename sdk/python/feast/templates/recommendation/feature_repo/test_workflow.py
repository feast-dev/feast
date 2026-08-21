import subprocess
import sys
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

from feast import FeatureStore


def run_demo() -> None:
    try:
        from sentence_transformers import SentenceTransformer
    except ImportError:
        print("sentence-transformers is required: pip install sentence-transformers")
        sys.exit(1)

    model = SentenceTransformer("all-MiniLM-L6-v2")
    products_path = Path("data/products.parquet")
    products = pd.read_parquet(products_path)
    descriptions = (products["product_name"] + ". " + products["description"]).tolist()
    embeddings = model.encode(descriptions, normalize_embeddings=True)
    products["embedding"] = [
        embedding.astype(np.float32).tolist() for embedding in embeddings
    ]
    products.to_parquet(products_path, allow_truncated_timestamps=True)

    print("\nApplying feature definitions")
    subprocess.run(["feast", "apply"], check=True)

    print("\nLoading features into the online store")
    store = FeatureStore(repo_path=".")
    store.materialize_incremental(end_date=datetime.now())

    query = "gaming laptop accessories"
    print(f"\nSearching for: {query}")
    query_embedding = model.encode([query], normalize_embeddings=True)[0].tolist()

    results = store.retrieve_online_documents_v2(
        features=[
            "product_embeddings:embedding",
            "product_embeddings:product_id",
            "product_embeddings:product_name",
            "product_embeddings:category",
            "product_embeddings:price",
            "product_embeddings:rating",
        ],
        query=query_embedding,
        top_k=5,
    ).to_dict()

    if results and len(results.get("product_id", [])) > 0:
        num_results = len(results["product_id"])
        print(f"  Top {num_results} recommendations:")
        for i in range(num_results):
            name = results["product_name"][i]
            category = results["category"][i]
            price = results["price"][i]
            rating = results["rating"][i]
            print(f"    {i + 1}. {name} [{category}] - ${price:.2f} (rating: {rating})")
    else:
        print("  No results found.")

    print("\nTearing down the feature store")
    subprocess.run(["feast", "teardown"], check=True)


if __name__ == "__main__":
    run_demo()
