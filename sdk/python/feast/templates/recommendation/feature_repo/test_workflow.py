import subprocess
import sys
from datetime import datetime

from feast import FeatureStore


def run_demo():
    store = FeatureStore(repo_path=".")

    print("\n--- Run feast apply ---")
    subprocess.run(["feast", "apply"])

    print("\n--- Load features into online store ---")
    store.materialize_incremental(end_date=datetime.now())

    print("\n--- Product Recommendation Search ---")
    try:
        from sentence_transformers import SentenceTransformer
    except ImportError:
        print("sentence-transformers is required: pip install sentence-transformers")
        sys.exit(1)

    model = SentenceTransformer("all-MiniLM-L6-v2")

    query = "gaming laptop accessories"
    print(f"\n  Query: '{query}'")
    query_embedding = model.encode([query], normalize_embeddings=True)[0].tolist()

    results = store.retrieve_online_documents_v2(
        features=[
            "product_embeddings:embedding",
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

    print("\n--- Run feast teardown ---")
    subprocess.run(["feast", "teardown"])


if __name__ == "__main__":
    run_demo()
