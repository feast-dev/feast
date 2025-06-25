from datetime import datetime
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pytest

from tests.utils.cli_repo_creator import CliRunner, get_example_repo


@pytest.fixture
def mock_milvus():
    with patch("pymilvus.MilvusClient") as mock_client:
        mock_instance = MagicMock()
        mock_client.return_value = mock_instance
        yield mock_instance


def create_test_data():
    """Create test data for RAG testing."""
    # Create sample documents with embeddings
    data = {
        "item_id": ["doc1", "doc2", "doc3", "doc4"],
        "content": [
            "This is document 1 about machine learning",
            "This is document 2 about deep learning",
            "This is document 3 about neural networks",
            "This is document 4 about AI",
        ],
        "title": ["Doc 1", "Doc 2", "Doc 3", "Doc 4"],
        "Embeddings": [
            [0.1] * 10,  # 10-dimensional embeddings as a list
            [0.2] * 10,
            [0.3] * 10,
            [0.4] * 10,
        ],
        "event_timestamp": [datetime.now()] * 4,
        "created_timestamp": [datetime.now()] * 4,
    }
    df = pd.DataFrame(data)

    # Ensure Embeddings column is a list of lists with correct dimensions
    df["Embeddings"] = df["Embeddings"].apply(
        lambda x: x if len(x) == 10 else [0.0] * 10
    )

    # Convert to numpy array to match what the feature store expects
    df["Embeddings"] = df["Embeddings"].apply(lambda x: np.array(x, dtype=np.float32))

    # Ensure the DataFrame has the correct shape and column order
    df = df.reindex(
        columns=[
            "item_id",
            "content",
            "title",
            "Embeddings",
            "event_timestamp",
            "created_timestamp",
        ]
    )

    # Convert Embeddings to list of lists to match Array(Float32) type
    df["Embeddings"] = df["Embeddings"].apply(lambda x: x.tolist())

    # Ensure each embedding has exactly 10 dimensions
    df["Embeddings"] = df["Embeddings"].apply(
        lambda x: x[:10] if len(x) > 10 else x + [0.0] * (10 - len(x))
    )

    return df


@pytest.fixture
def example_feature_store():
    """Create a feature store using example repo."""
    runner = CliRunner()
    # Patch the run method to always succeed for teardown
    with patch.object(runner, "run") as mock_run:

        def run_side_effect(cmd, *args, **kwargs):
            if cmd == ["teardown"]:
                mock_result = MagicMock()
                mock_result.returncode = 0
                mock_result.stdout = b""
                mock_result.stderr = b""
                return mock_result
            # For other commands, call the real method
            return CliRunner.run(runner, cmd, *args, **kwargs)

        mock_run.side_effect = run_side_effect

        with runner.local_repo(
            get_example_repo("example_feature_repo_1.py"),
            offline_store="file",
            online_store="milvus",
            apply=False,
            teardown=True,
        ) as store:
            from tests.example_repos.example_feature_repo_1 import document_embeddings

            store.apply([document_embeddings])
            yield store


class MockVectorStore:
    """Mock vector store that simulates Milvus behavior."""

    def __init__(self, repo_path=None, rag_view=None, features=None):
        self.repo_path = repo_path
        self.rag_view = rag_view
        self.features = features
        self.client = MagicMock()
        self._mock_data = {
            "content": ["doc1 content", "doc2 content", "doc3 content"],
            "item_id": [1, 2, 3],
            "Embeddings": [
                np.random.rand(8).tolist(),  # Changed from 10 to 8 dimensions
                np.random.rand(8).tolist(),
                np.random.rand(8).tolist(),
            ],
        }

    def query(self, query_vector=None, query_string=None, top_k=5):
        """Mock query method that returns predefined results."""
        mock_response = MagicMock()
        mock_response.to_dict.return_value = {
            "content": self._mock_data["content"][:top_k],
            "item_id": self._mock_data["item_id"][:top_k],
            "Embeddings": self._mock_data["Embeddings"][:top_k],
        }
        return mock_response

    def close(self):
        """Mock close method."""
        if hasattr(self, "client"):
            self.client.close()
            self.client = None
