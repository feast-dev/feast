import os
from unittest.mock import MagicMock, Mock, patch

import numpy as np
import pytest
import torch
from transformers import (
    BartConfig,
    BertConfig,
    PreTrainedModel,
    PreTrainedTokenizer,
    RagConfig,
)

from feast import FeatureStore
from feast.rag_retriever import FeastIndex, FeastRAGRetriever
from feast.repo_config import RepoConfig
from tests.utils.rag_test_utils import MockVectorStore


class MockTokenizer(PreTrainedTokenizer):
    """Mock tokenizer for testing."""

    def __init__(self):
        self._vocab = {"[PAD]": 0, "[UNK]": 1, "[CLS]": 2, "[SEP]": 3}
        self._vocab_size = 1000
        self._pad_token = "[PAD]"
        self._pad_token_id = 0
        self.model_max_length = 512
        self.padding_side = "right"
        self.truncation_side = "right"
        super().__init__()

    def encode(self, text, **kwargs):
        return [1, 2, 3]  # Mock token IDs

    def decode(self, ids, **kwargs):
        return "decoded text"

    def get_vocab(self):
        """Returns the vocabulary as a dictionary of token to index."""
        return self._vocab.copy()

    @property
    def vocab_size(self):
        return self._vocab_size

    @property
    def pad_token(self):
        return self._pad_token

    @property
    def pad_token_id(self):
        return self._pad_token_id

    def _tokenize(self, text, **kwargs):
        # Simple whitespace tokenizer for testing
        return text.split()

    def _convert_token_to_id(self, token):
        # Return 0 for pad, 1 for unk, 2 for cls, 3 for sep, else 4
        return self._vocab.get(token, 4)


class MockModel(PreTrainedModel):
    """Mock model for testing."""

    def __init__(self, config=None):
        if config is None:
            from transformers import PretrainedConfig

            config = PretrainedConfig()
            config.hidden_size = 768
        super().__init__(config)
        self.config = config
        import torch

        self.dummy_param = torch.nn.Parameter(torch.zeros(1))
        self._modules = {}  # Required for nn.Module
        self._device = torch.device("cpu")  # Internal device storage

    @property
    def device(self):
        return self._device

    def forward(self, **kwargs):
        return MagicMock(
            last_hidden_state=torch.randn(1, 8, 8)
        )  # Match the retrieval_vector_size=8 from config


@pytest.fixture
def simple_feature_store(temp_dir):
    """Create a simple feature store without auth for RAG tests."""
    return FeatureStore(
        config=RepoConfig(
            registry=os.path.join(temp_dir, "registry.db"),
            project="default",
            provider="local",
            entity_key_serialization_version=2,
        )
    )


@pytest.fixture
def rag_retriever(simple_feature_store):
    """Create a RAG retriever instance for testing."""
    # Import the required objects
    from tests.example_repos.example_feature_repo_1 import (
        document_embeddings,
    )

    print("Creating retriever...")
    rag_config = RagConfig(
        question_encoder=BertConfig(hidden_size=768).to_dict(),
        generator=BartConfig().to_dict(),
        retrieval_vector_size=8,  # Match the embedding size in the feature view
        n_docs=2,
    )

    retriever = FeastRAGRetriever(
        question_encoder_tokenizer=MockTokenizer(),
        question_encoder=MockModel(),
        generator_tokenizer=MockTokenizer(),
        generator_model=MockModel(),
        feast_repo_path=str(simple_feature_store.repo_path),
        feature_view=document_embeddings,
        features=[
            "document_embeddings:content",
            "document_embeddings:title",
            "document_embeddings:Embeddings",
        ],
        search_type="hybrid",
        config=rag_config,
        index=FeastIndex(),
        id_field="item_id",
        text_field="content",
    )

    # Replace the vector store with our mock
    retriever._vector_store = MockVectorStore(
        repo_path=str(simple_feature_store.repo_path),
        rag_view=document_embeddings,
        features=[
            "document_embeddings:content",
            "document_embeddings:title",
            "document_embeddings:Embeddings",
        ],
    )

    try:
        yield retriever
    finally:
        # Cleanup
        if hasattr(retriever, "_vector_store"):
            retriever._vector_store.close()
            retriever._vector_store = None
            import gc

            gc.collect()


# Basic initialization and configuration tests
def test_feast_index_initialization():
    """Test FeastIndex initialization."""
    index = FeastIndex()
    assert index is not None


def test_rag_retriever_initialization(rag_retriever):
    """Test RAG retriever initialization."""
    print("Testing initialization...")
    assert rag_retriever.search_type == "hybrid"
    assert rag_retriever.id_field == "item_id"
    assert rag_retriever.text_field == "content"
    assert len(rag_retriever.features) == 3
    assert rag_retriever.format_document is not None  # Should have default formatter


def test_rag_retriever_custom_format_document(simple_feature_store):
    """Test RAG retriever initialization with custom document formatter."""
    from tests.example_repos.example_feature_repo_1 import document_embeddings

    def custom_formatter(doc):
        return f"Custom: {doc.get('content', '')}"

    retriever = FeastRAGRetriever(
        question_encoder_tokenizer=MockTokenizer(),
        question_encoder=MockModel(),
        generator_tokenizer=MockTokenizer(),
        generator_model=MockModel(),
        feast_repo_path=str(simple_feature_store.repo_path),
        feature_view=document_embeddings,
        features=[
            "document_embeddings:content",
            "document_embeddings:title",
            "document_embeddings:Embeddings",
        ],
        search_type="hybrid",
        config=RagConfig(
            question_encoder=BertConfig(hidden_size=768).to_dict(),
            generator=BartConfig().to_dict(),
            retrieval_vector_size=8,
            n_docs=2,
        ),
        index=FeastIndex(),
        id_field="item_id",
        text_field="content",
        format_document=custom_formatter,
    )
    assert retriever.format_document == custom_formatter


def test_default_format_document(rag_retriever):
    """Test the default document formatting function."""
    doc = {
        "content": "test content",
        "title": "test title",
        "Embeddings": [0.1, 0.2, 0.3] * 10,  # Long vector to be skipped
    }
    formatted = rag_retriever._default_format_document(doc)
    assert "Content: test content" in formatted
    assert "Title: test title" in formatted
    assert "Embeddings" not in formatted  # Vector should be skipped


def test_rag_retriever_invalid_search_type(simple_feature_store):
    """Test RAG retriever initialization with invalid search type."""
    from tests.example_repos.example_feature_repo_1 import (
        document_embeddings,
    )

    with pytest.raises(ValueError):
        FeastRAGRetriever(
            question_encoder_tokenizer=MockTokenizer(),
            question_encoder=MockModel(),
            generator_tokenizer=MockTokenizer(),
            generator_model=MockModel(),
            feast_repo_path=str(simple_feature_store.repo_path),
            feature_view=document_embeddings,
            features=["content", "title", "Embeddings"],
            search_type="invalid",
            config=RagConfig(
                question_encoder=BertConfig(hidden_size=768).to_dict(),
                generator=BartConfig().to_dict(),
                retrieval_vector_size=8,
                n_docs=2,
            ),
            index=FeastIndex(),
        )


# Search functionality tests
def test_retrieve_with_text_search(rag_retriever):
    """Test retrieving documents using text search only."""
    # Create a new retriever with text search type
    text_retriever = FeastRAGRetriever(
        question_encoder_tokenizer=MockTokenizer(),
        question_encoder=MockModel(),
        generator_tokenizer=MockTokenizer(),
        generator_model=MockModel(),
        feast_repo_path=str(rag_retriever.feast_repo_path),
        feature_view=rag_retriever.feature_view,
        features=rag_retriever.features,
        search_type="text",
        config=rag_retriever.config,
        index=FeastIndex(),
        id_field=rag_retriever.id_field,
        text_field=rag_retriever.text_field,
    )

    # Mock the vector store's query method
    mock_response = MagicMock()
    mock_response.to_dict.return_value = {
        "content": ["doc1 content", "doc2 content"],
        "item_id": [1, 2],
        "Embeddings": [np.random.rand(8).tolist(), np.random.rand(8).tolist()],
    }
    text_retriever.vector_store.query = Mock(return_value=mock_response)

    # Test text search with query string only
    # Create empty question hidden states since we're only doing text search
    question_hidden_states = np.zeros((1, 8, 8))  # (batch_size, seq_len, hidden_dim)

    doc_embeds, doc_ids, doc_dicts = text_retriever.retrieve(
        question_hidden_states=question_hidden_states, n_docs=2, query="test query"
    )

    # Verify the results
    assert doc_embeds.shape == (1, 2, 8)  # (batch_size, n_docs, embedding_dim)
    assert len(doc_ids) == 1  # One batch
    assert len(doc_dicts) == 1  # One batch
    assert len(doc_dicts[0]["text"]) == 2  # Two documents
    assert len(doc_dicts[0]["id"]) == 2  # Two document IDs

    # Verify that vector_store.query was called with text parameter only
    text_retriever.vector_store.query.assert_called_once()
    call_args = text_retriever.vector_store.query.call_args[1]
    assert call_args["query_vector"] is None  # No vector search
    assert call_args["query_string"] == "test query"  # Text search was used
    assert call_args["top_k"] == 2  # Correct number of documents requested


def test_retrieve_with_vector_search(rag_retriever):
    """Test retrieving documents using vector search only."""
    # Create a new retriever with vector search type
    vector_retriever = FeastRAGRetriever(
        question_encoder_tokenizer=MockTokenizer(),
        question_encoder=MockModel(),
        generator_tokenizer=MockTokenizer(),
        generator_model=MockModel(),
        feast_repo_path=str(rag_retriever.feast_repo_path),
        feature_view=rag_retriever.feature_view,
        features=rag_retriever.features,
        search_type="vector",
        config=rag_retriever.config,
        index=FeastIndex(),
        id_field=rag_retriever.id_field,
        text_field=rag_retriever.text_field,
    )

    # Create mock question hidden states
    question_hidden_states = np.random.rand(
        1, 8, 8
    )  # (batch_size, seq_len, hidden_dim)

    # Mock the vector store's query method
    mock_response = MagicMock()
    mock_response.to_dict.return_value = {
        "content": ["doc1 content", "doc2 content"],
        "item_id": [1, 2],
        "Embeddings": [np.random.rand(8).tolist(), np.random.rand(8).tolist()],
    }
    vector_retriever.vector_store.query = Mock(return_value=mock_response)

    # Test vector search with hidden states only
    doc_embeds, doc_ids, doc_dicts = vector_retriever.retrieve(
        question_hidden_states=question_hidden_states,
        n_docs=2,
        query=None,  # No text search
    )

    # Verify the results
    assert doc_embeds.shape == (1, 2, 8)  # (batch_size, n_docs, embedding_dim)
    assert len(doc_ids) == 1  # One batch
    assert len(doc_dicts) == 1  # One batch
    assert len(doc_dicts[0]["text"]) == 2  # Two documents
    assert len(doc_dicts[0]["id"]) == 2  # Two document IDs

    # Verify that vector_store.query was called with vector parameter only
    vector_retriever.vector_store.query.assert_called_once()
    call_args = vector_retriever.vector_store.query.call_args[1]
    assert call_args["query_vector"] is not None  # Vector search was used
    assert call_args["query_string"] is None  # No text search
    assert call_args["top_k"] == 2  # Correct number of documents requested


def test_retrieve_with_hybrid_search(rag_retriever):
    """Test retrieving documents using hybrid search (both vector and text search)."""
    # Create mock question hidden states
    question_hidden_states = np.random.rand(
        1, 8, 8
    )  # (batch_size, seq_len, hidden_dim)

    # Mock the vector store's query method
    mock_response = MagicMock()
    mock_response.to_dict.return_value = {
        "content": ["doc1 content", "doc2 content"],
        "item_id": [1, 2],
        "Embeddings": [np.random.rand(8).tolist(), np.random.rand(8).tolist()],
    }
    rag_retriever.vector_store.query = Mock(return_value=mock_response)

    # Test hybrid search with both vector and text query
    doc_embeds, doc_ids, doc_dicts = rag_retriever.retrieve(
        question_hidden_states=question_hidden_states, n_docs=2, query="test query"
    )

    # Verify the results
    assert doc_embeds.shape == (1, 2, 8)  # (batch_size, n_docs, embedding_dim)
    assert len(doc_ids) == 1  # One batch
    assert len(doc_dicts) == 1  # One batch
    assert len(doc_dicts[0]["text"]) == 2  # Two documents
    assert len(doc_dicts[0]["id"]) == 2  # Two document IDs

    # Verify that vector_store.query was called with both vector and text parameters
    rag_retriever.vector_store.query.assert_called_once()
    call_args = rag_retriever.vector_store.query.call_args[1]
    assert call_args["query_vector"] is not None  # Vector search was used
    assert call_args["query_string"] == "test query"  # Text search was used
    assert call_args["top_k"] == 2  # Correct number of documents requested


def test_retrieve_documents(rag_retriever):
    """Test retrieving documents using the RAG retriever."""
    # Create mock question hidden states with 8 dimensions to match test data
    question_hidden_states = np.random.rand(
        1, 8, 8
    )  # (batch_size, seq_len, hidden_dim)

    # Mock the retrieve method
    doc_embeds, doc_ids, doc_dicts = rag_retriever.retrieve(
        question_hidden_states=question_hidden_states, n_docs=2, query="test query"
    )

    # Verify the results
    assert doc_embeds.shape == (1, 2, 8)  # (batch_size, n_docs, embedding_dim)
    assert len(doc_ids) == 1  # One batch
    assert len(doc_dicts) == 1  # One batch
    assert len(doc_dicts[0]["text"]) == 2  # Two documents
    assert len(doc_dicts[0]["id"]) == 2  # Two document IDs


# End-to-end functionality test
def test_generate_answer(rag_retriever):
    """Test generating an answer using the RAG retriever."""
    # Mock the retrieve method using patch
    with patch.object(
        rag_retriever,
        "retrieve",
        return_value=(
            np.array([[[0.1] * 8, [0.2] * 8]]),  # 8-dimensional embeddings
            np.array([[1, 2]]),
            [
                {
                    "text": ["context1", "context2"],
                    "id": ["doc1", "doc2"],
                    "title": ["Doc 1", "Doc 2"],
                }
            ],
        ),
    ) as mock_retrieve:
        # Mock the generator model's generate method
        rag_retriever.generator_model.generate = Mock(
            return_value=torch.tensor([[1, 2, 3]])
        )

        # Generate an answer
        answer = rag_retriever.generate_answer(
            "test query", top_k=2, max_new_tokens=100
        )

        # Verify the answer
        assert isinstance(answer, str)
        assert len(answer) > 0

        # Verify that retrieve was called with correct parameters
        mock_retrieve.assert_called_once()
        call_args = mock_retrieve.call_args[1]
        assert call_args["n_docs"] == 2
        assert call_args["query"] == "test query"

        # Verify that generate was called with correct parameters
        rag_retriever.generator_model.generate.assert_called_once()
        call_args = rag_retriever.generator_model.generate.call_args[1]
        assert call_args["max_new_tokens"] == 100
