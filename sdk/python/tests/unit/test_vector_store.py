import pytest
import numpy as np
from tests.utils.rag_test_utils import feature_store, MockVectorStore


@pytest.fixture(autouse=True)
def cleanup_milvus():
    """Ensure Milvus resources are cleaned up between tests."""
    yield
    # This will run after each test
    import gc
    gc.collect()  # Force garbage collection to help release resources
    # Add a small delay to ensure resources are released
    import time
    time.sleep(0.1)


def test_vector_store_initialization(feature_store):
    """Test vector store initialization."""
    print("Testing vector store initialization...")
    # Apply the feature view first
    feature_store.apply([feature_store.get_feature_view("document_embeddings")])
    
    doc_view = feature_store.get_feature_view("document_embeddings")
    store = MockVectorStore(
        repo_path=str(feature_store.repo_path),
        rag_view=doc_view,
        features=["document_embeddings:content", "document_embeddings:Embeddings", "document_embeddings:item_id"]
    )
    assert store.rag_view == doc_view
    assert store.features == ["document_embeddings:content", "document_embeddings:Embeddings", "document_embeddings:item_id"]


def test_vector_store_query(feature_store):
    """Test vector store query method."""
    print("Testing vector store query...")
    # Apply the feature view first
    feature_store.apply([feature_store.get_feature_view("document_embeddings")])
    
    doc_view = feature_store.get_feature_view("document_embeddings")
    store = MockVectorStore(
        repo_path=str(feature_store.repo_path),
        rag_view=doc_view,
        features=["document_embeddings:content", "document_embeddings:Embeddings", "document_embeddings:item_id"]
    )

    # Test query with vector
    query_vector = np.array([0.1] * 10)  # 10-dimensional query vector to match schema
    print("Querying with vector...")
    response = store.query(
        query_vector=query_vector,
        query_string=None,
        top_k=5
    )
    assert response is not None

    # Test query with text
    print("Querying with text...")
    response = store.query(
        query_vector=None,
        query_string="test query",
        top_k=5
    )
    assert response is not None
