# Milvus Tutorial with Feast
#
# This example demonstrates how to use Milvus
# as a vector database backend for Feast.

import os
import subprocess
from datetime import datetime, timedelta

import pandas as pd

# For generating embeddings
try:
    from sentence_transformers import SentenceTransformer
except ImportError:
    print("Installing sentence_transformers...")
    subprocess.check_call(["pip", "install", "sentence-transformers"])
    from sentence_transformers import SentenceTransformer

from feast import FeatureStore, Entity, FeatureView, Field, FileSource
from feast.data_format import ParquetFormat
from feast.types import Float32, Array, String
from feast.value_type import ValueType

# Create data directory if it doesn't exist
os.makedirs("data", exist_ok=True)


# Step 1: Generate sample data with embeddings
def generate_sample_data():
    print("Generating sample data with embeddings...")

    # Sample product data
    products = [
        {"id": 1, "name": "Smartphone",
         "description": "A high-end smartphone with advanced camera features and long battery life."},
        {"id": 2, "name": "Laptop",
         "description": "Powerful laptop with fast processor and high-resolution display for professional use."},
        {"id": 3, "name": "Headphones",
         "description": "Wireless noise-cancelling headphones with premium sound quality."},
        {"id": 4, "name": "Smartwatch",
         "description": "Fitness tracking smartwatch with heart rate monitoring and sleep analysis."},
        {"id": 5, "name": "Tablet",
         "description": "Lightweight tablet with vibrant display perfect for reading and browsing."},
        {"id": 6, "name": "Camera",
         "description": "Professional digital camera with high-resolution sensor and interchangeable lenses."},
        {"id": 7, "name": "Speaker",
         "description": "Bluetooth speaker with rich bass and long battery life for outdoor use."},
        {"id": 8, "name": "Gaming Console",
         "description": "Next-generation gaming console with 4K graphics and fast loading times."},
        {"id": 9, "name": "E-reader",
         "description": "E-ink display reader with backlight for comfortable reading in any lighting condition."},
        {"id": 10, "name": "Smart TV",
         "description": "4K smart television with built-in streaming apps and voice control."}
    ]

    # Create DataFrame
    df = pd.DataFrame(products)

    # Generate embeddings using sentence-transformers
    model = SentenceTransformer('all-MiniLM-L6-v2')  # Small, fast model with 384-dim embeddings
    embeddings = model.encode(df['description'].tolist())

    # Add embeddings and timestamp to DataFrame
    df['embedding'] = embeddings.tolist()
    df['event_timestamp'] = datetime.now() - timedelta(days=1)
    df['created_timestamp'] = datetime.now() - timedelta(days=1)

    # Save to parquet file
    parquet_path = "data/sample_data.parquet"
    df.to_parquet(parquet_path, index=False)

    print(f"Sample data saved to {parquet_path}")
    return parquet_path


# Step 2: Define feature repository
def create_feature_definitions(data_path):
    print("Creating feature definitions...")

    product = Entity(
        name="product_id",
        description="Product ID",
        join_keys=["id"],
        value_type=ValueType.INT64,
    )

    source = FileSource(
        file_format=ParquetFormat(),
        path=data_path,
        timestamp_field="event_timestamp",
        created_timestamp_column="created_timestamp",
    )

    # Define feature view with vector embeddings
    product_embeddings = FeatureView(
        name="product_embeddings",
        entities=[product],
        ttl=timedelta(days=30),
        schema=[
            Field(
                name="embedding",
                dtype=Array(Float32),
                vector_index=True,  # Mark as vector field
            ),
            Field(name="name", dtype=String),
            Field(name="description", dtype=String),
        ],
        source=source,
        online=True,
    )

    return product, product_embeddings


def setup_feature_store(product, product_embeddings):
    print("Setting up feature store...")

    store = FeatureStore(repo_path=".")

    store.apply([product, product_embeddings])

    # Materialize features to online store
    store.materialize(
        start_date=datetime.now() - timedelta(days=2),
        end_date=datetime.now(),
    )

    print("Feature store setup complete")
    return store


# Step 4: Perform vector similarity search
def perform_similarity_search(store, query_text: str, top_k: int = 3):
    print(f"\nPerforming similarity search for: '{query_text}'")

    # Generate embedding for query text
    model = SentenceTransformer('all-MiniLM-L6-v2')
    query_embedding = model.encode(query_text).tolist()

    # Perform similarity search using vector embeddings with version 2 API
    try:
        results = store.retrieve_online_documents_v2(
            features=["product_embeddings:embedding", "product_embeddings:name", "product_embeddings:description"],
            query=query_embedding,
            top_k=top_k,
            distance_metric="L2"
        ).to_df()

        # Print results
        print(f"\nTop {top_k} similar products:")
        for i, row in results.iterrows():
            print(f"\n{i + 1}. Name: {row['product_embeddings__name']}")
            print(f"   Description: {row['product_embeddings__description']}")
            print(f"   Distance: {row['distance']}")

        return results
    except Exception as e:
        print(f"Error performing search: {e}")
        return None


# Main function to run the example
def main():
    print("=== Milvus Tutorial with Feast ===")

    # Check if Milvus is running
    print("\nEnsure Milvus is running:")
    print("docker compose up -d")

    input("\nPress Enter to continue once Milvus is ready...")

    # Generate sample data
    data_path = generate_sample_data()

    # Create feature definitions
    product, product_embeddings = create_feature_definitions(data_path)

    # Setup feature store
    store = setup_feature_store(product, product_embeddings)

    # Perform similarity searches
    perform_similarity_search(store, "wireless audio device with good sound", top_k=3)
    perform_similarity_search(store, "portable computing device for work", top_k=3)

    print("\n=== Tutorial Complete ===")
    print("You've successfully set up Milvus with Feast and performed vector similarity searches!")


if __name__ == "__main__":
    main()
