import numpy as np

from tests.example_repos.example_feature_repo_1 import document_embeddings
from tests.utils.rag_test_utils import MockVectorStore, example_feature_store

# showing intended use of example_feature_store fixture
_ = example_feature_store


def test_vector_store_initialization(example_feature_store):
    """Test vector store initialization."""
    print("Testing vector store initialization...")

    # Apply the feature view first
    example_feature_store.apply([document_embeddings])

    doc_view = example_feature_store.get_feature_view("document_embeddings")
    store = MockVectorStore(
        repo_path=str(example_feature_store.repo_path),
        rag_view=doc_view,
        features=[
            "document_embeddings:content",
            "document_embeddings:Embeddings",
            "document_embeddings:item_id",
        ],
    )
    assert store.rag_view == doc_view
    assert store.features == [
        "document_embeddings:content",
        "document_embeddings:Embeddings",
        "document_embeddings:item_id",
    ]


def test_vector_store_query(example_feature_store):
    """Test vector store query method."""
    print("Testing vector store query...")

    # Apply the feature view first
    example_feature_store.apply([document_embeddings])

    doc_view = example_feature_store.get_feature_view("document_embeddings")
    store = MockVectorStore(
        repo_path=str(example_feature_store.repo_path),
        rag_view=doc_view,
        features=[
            "document_embeddings:content",
            "document_embeddings:Embeddings",
            "document_embeddings:item_id",
        ],
    )

    # Test query with vector
    query_vector = np.array([0.1] * 8)  # 8-dimensional query vector to match schema
    print("Querying with vector...")
    response = store.query(query_vector=query_vector, query_string=None, top_k=5)
    assert response is not None

    # Test query with text
    print("Querying with text...")
    response = store.query(query_vector=None, query_string="test query", top_k=5)
    assert response is not None
