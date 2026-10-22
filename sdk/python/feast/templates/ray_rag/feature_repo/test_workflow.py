"""
Feast-Ray RAG Pipeline Demo

This script demonstrates:
1. Ray offline store for distributed data I/O
2. Ray compute engine for parallel embedding generation
3. Milvus vector search for semantic similarity
4. Complete RAG pipeline from data to search results

Usage:
    1. feast apply
    2. feast materialize --disable-event-timestamp
    3. python test_workflow.py
"""

import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent))

try:
    from sentence_transformers import SentenceTransformer

    from feast import FeatureStore
except ImportError as e:
    print(f"Missing dependency: {e}")
    print("💡 Install with: pip install feast[ray] sentence-transformers")
    sys.exit(1)


def main():
    """Run the RAG pipeline demonstration."""

    store = FeatureStore(repo_path=".")
    feature_views = store.list_feature_views()
    print(f"Feature views: {len(feature_views)}")

    print("Vector similarity search with Feast ...")
    try:
        model = SentenceTransformer("sentence-transformers/all-MiniLM-L6-v2")
        query = "Crime drama about organized crime families"

        print(f"\n   Query: '{query}'")
        # Generate query embedding
        query_embedding = model.encode([query], normalize_embeddings=True)[0].tolist()

        # Use Feast's retrieve_online_documents_v2 API
        # Request all fields we want to display
        results = store.retrieve_online_documents_v2(
            features=[
                "document_embeddings:embedding",
                "document_embeddings:movie_name",
                "document_embeddings:movie_director",
                "document_embeddings:movie_genres",
                "document_embeddings:movie_rating",
            ],
            query=query_embedding,
            top_k=3,
        ).to_dict()

        if results and len(results.get("document_id_pk", [])) > 0:
            print("   📊 Top 3 results:")
            num_results = len(results["document_id_pk"])
            for i in range(num_results):
                name = results.get("movie_name", ["Unknown"] * num_results)[i]
                director = results.get("movie_director", ["Unknown"] * num_results)[i]
                genres = results.get("movie_genres", ["Unknown"] * num_results)[i]
                print(f"      {i + 1}. {name}")
                print(f"         Director: {director} | Genres: {genres}")
        else:
            print("No results found")

    except Exception as e:
        print(f"Search failed: {e}")
        return

    print("\n📚 What was demonstrated:")
    print("   ✅ Ray-based distributed embedding generation")
    print("   ✅ Milvus vector storage and retrieval")
    print("   ✅ Similarity search")
    print("   ✅ Raw Data to Search workflow")

    print("\n🚀 Next steps:")
    print("   • Scale to larger datasets")
    print("   • Connect to distributed Ray cluster")


if __name__ == "__main__":
    main()
