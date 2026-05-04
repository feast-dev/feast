import os
from unittest.mock import MagicMock, Mock, patch

import numpy as np
import pandas as pd
import pytest

from feast.chunker import BaseChunker, ChunkingConfig, TextChunker
from feast.doc_embedder import (
    DocEmbedder,
    SchemaTransformFn,
    default_schema_transform_fn,
)
from feast.embedder import BaseEmbedder, EmbeddingConfig, MultiModalEmbedder


@pytest.fixture
def sample_documents_df():
    """Small DataFrame with 3 documents of varying lengths."""
    return pd.DataFrame(
        {
            "id": ["doc1", "doc2", "doc3"],
            "text": [
                " ".join([f"word{i}" for i in range(150)]),
                " ".join([f"term{i}" for i in range(200)]),
                " ".join([f"item{i}" for i in range(50)]),
            ],
            "type": ["article", "paper", "note"],
        }
    )


class TestTextChunker:
    def test_basic_chunking(self):
        """Given ~200 words, returns chunks with correct keys and count."""
        chunker = TextChunker()
        text = " ".join([f"word{i}" for i in range(200)])

        chunks = chunker.load_parse_and_chunk(
            source=text, source_id="doc1", source_column="text"
        )

        assert len(chunks) > 0
        for chunk in chunks:
            assert "chunk_id" in chunk
            assert "original_id" in chunk
            assert "text" in chunk
            assert "chunk_index" in chunk
            assert chunk["original_id"] == "doc1"

        assert len(chunks) == 3
        assert chunks[0]["chunk_id"] == "doc1_0"
        assert chunks[1]["chunk_id"] == "doc1_1"
        assert chunks[2]["chunk_id"] == "doc1_2"
        assert chunks[0]["chunk_index"] == 0
        assert chunks[2]["chunk_index"] == 2

    def test_overlap(self):
        """Consecutive chunks share exactly chunk_overlap words."""
        config = ChunkingConfig(
            chunk_size=10, chunk_overlap=3, min_chunk_size=3, max_chunk_chars=None
        )
        chunker = TextChunker(config=config)
        text = " ".join([f"w{i}" for i in range(20)])

        chunks = chunker.load_parse_and_chunk(
            source=text, source_id="doc1", source_column="text"
        )
        assert len(chunks) >= 2

        words_0 = chunks[0]["text"].split()
        words_1 = chunks[1]["text"].split()

        assert words_0[-3:] == words_1[:3]

    def test_min_chunk_size_filters_small_trailing(self):
        """Trailing chunk smaller than min_chunk_size is dropped."""
        config = ChunkingConfig(
            chunk_size=10, chunk_overlap=0, min_chunk_size=5, max_chunk_chars=None
        )
        chunker = TextChunker(config=config)
        text = " ".join([f"w{i}" for i in range(13)])

        chunks = chunker.load_parse_and_chunk(
            source=text, source_id="doc1", source_column="text"
        )

        assert len(chunks) == 1

    def test_max_chunk_chars_truncation(self):
        """Chunks exceeding max_chunk_chars are truncated."""
        config = ChunkingConfig(
            chunk_size=100, chunk_overlap=0, min_chunk_size=1, max_chunk_chars=50
        )
        chunker = TextChunker(config=config)
        text = " ".join([f"longword{i}" for i in range(200)])

        chunks = chunker.load_parse_and_chunk(
            source=text, source_id="doc1", source_column="text"
        )

        assert len(chunks) > 0
        for chunk in chunks:
            assert len(chunk["text"]) <= 50

    def test_empty_text_returns_no_chunks(self):
        """Empty string produces zero chunks."""
        chunker = TextChunker()

        chunks = chunker.load_parse_and_chunk(
            source="", source_id="doc1", source_column="text"
        )

        assert chunks == []

    def test_chunk_dataframe(self, sample_documents_df):
        """Chunking a multi-row DataFrame returns a flattened DataFrame."""
        chunker = TextChunker()

        result = chunker.chunk_dataframe(
            df=sample_documents_df,
            id_column="id",
            source_column="text",
            type_column="type",
        )

        assert isinstance(result, pd.DataFrame)
        assert "chunk_id" in result.columns
        assert "original_id" in result.columns
        assert "text" in result.columns
        assert "chunk_index" in result.columns
        assert len(result) >= len(sample_documents_df)
        assert set(result["original_id"].unique()) == {"doc1", "doc2", "doc3"}


def test_base_chunker_cannot_instantiate():
    """BaseChunker is abstract and cannot be instantiated directly."""
    with pytest.raises(TypeError):
        BaseChunker()


def test_base_embedder_cannot_instantiate():
    """BaseEmbedder is abstract and cannot be instantiated directly."""
    with pytest.raises(TypeError):
        BaseEmbedder()


class TestMultiModalEmbedder:
    def test_supported_modalities(self):
        """After init, supported_modalities returns text and image."""
        embedder = MultiModalEmbedder()
        modalities = embedder.supported_modalities

        assert "text" in modalities
        assert "image" in modalities

    def test_unsupported_modality_raises(self):
        """Calling embed with unsupported modality raises ValueError."""
        embedder = MultiModalEmbedder()

        with pytest.raises(ValueError, match="Unsupported modality: 'video'"):
            embedder.embed(["test"], "video")

    def test_text_embed_mocked(self):
        """Text embedding calls encode with correct batch_size and show_progress."""
        embedder = MultiModalEmbedder()

        mock_model = MagicMock()
        mock_model.encode.return_value = np.array([[0.1, 0.2, 0.3], [0.4, 0.5, 0.6]])
        embedder._text_model = mock_model

        result = embedder.embed(["hello", "world"], "text")

        mock_model.encode.assert_called_once_with(
            ["hello", "world"],
            batch_size=64,
            show_progress_bar=True,
        )
        assert result.shape == (2, 3)

    def test_text_embed_custom_config(self):
        """Text embedding respects custom EmbeddingConfig values."""
        config = EmbeddingConfig(batch_size=16, show_progress=False)
        embedder = MultiModalEmbedder(config=config)

        mock_model = MagicMock()
        mock_model.encode.return_value = np.array([[0.1, 0.2]])
        embedder._text_model = mock_model

        embedder.embed(["hello"], "text")

        mock_model.encode.assert_called_once_with(
            ["hello"],
            batch_size=16,
            show_progress_bar=False,
        )

    def test_lazy_loading(self):
        """Models are None until first accessed."""
        embedder = MultiModalEmbedder()

        assert embedder._text_model is None
        assert embedder._image_model is None
        assert embedder._image_processor is None

    def test_get_embedding_dim_text(self):
        """get_embedding_dim for text delegates to text_model."""
        embedder = MultiModalEmbedder()
        mock_model = MagicMock()
        mock_model.get_sentence_embedding_dimension.return_value = 384
        embedder._text_model = mock_model

        assert embedder.get_embedding_dim("text") == 384
        mock_model.get_sentence_embedding_dimension.assert_called_once()

    def test_get_embedding_dim_unknown_modality(self):
        """get_embedding_dim returns None for unrecognized modalities."""
        embedder = MultiModalEmbedder()

        assert embedder.get_embedding_dim("audio") is None

    def test_embed_dataframe(self):
        """embed_dataframe adds embedding columns via column_mapping."""
        embedder = MultiModalEmbedder()
        embedder.embed = Mock(return_value=np.array([[0.1, 0.2], [0.3, 0.4]]))

        df = pd.DataFrame({"text": ["hello", "world"], "id": [1, 2]})
        result = embedder.embed_dataframe(
            df, column_mapping={"text": ("text", "text_embedding")}
        )

        assert "text_embedding" in result.columns
        assert len(result) == 2
        embedder.embed.assert_called_once_with(["hello", "world"], "text")


def test_schema_transform_fn_protocol_check():
    """A matching function is recognized as SchemaTransformFn."""

    def my_fn(df: pd.DataFrame) -> pd.DataFrame:
        return df

    assert isinstance(my_fn, SchemaTransformFn)


def test_default_schema_transform_fn_output():
    """default_schema_transform_fn transforms columns correctly."""
    input_df = pd.DataFrame(
        {
            "chunk_id": ["c1", "c2"],
            "text": ["hello", "world"],
            "text_embedding": [[0.1, 0.2], [0.3, 0.4]],
            "original_id": ["doc1", "doc1"],
        }
    )

    result = default_schema_transform_fn(input_df)

    assert list(result.columns) == [
        "passage_id",
        "text",
        "embedding",
        "event_timestamp",
        "source_id",
    ]
    assert result["passage_id"].tolist() == ["c1", "c2"]
    assert result["text"].tolist() == ["hello", "world"]
    assert result["embedding"].tolist() == [[0.1, 0.2], [0.3, 0.4]]
    assert result["source_id"].tolist() == ["doc1", "doc1"]
    assert len(result["event_timestamp"]) == 2


class TestDocEmbedder:
    @patch("feast.repo_operations.apply_total")
    @patch("feast.repo_config.load_repo_config")
    def test_init_no_feature_view(self, mock_load_config, mock_apply_total, tmp_path):
        """With create_feature_view=False, generate_repo_file is not called."""
        mock_chunker = MagicMock(spec=BaseChunker)
        mock_embedder = MagicMock(spec=BaseEmbedder)

        with patch("feast.doc_embedder.generate_repo_file") as mock_gen:
            doc_embedder = DocEmbedder(
                repo_path=str(tmp_path),
                feature_view_name="test_view",
                chunker=mock_chunker,
                embedder=mock_embedder,
                create_feature_view=False,
            )

        mock_gen.assert_not_called()
        assert doc_embedder.repo_path == str(tmp_path)
        assert doc_embedder.feature_view_name == "test_view"
        assert doc_embedder.chunker is mock_chunker
        assert doc_embedder.embedder is mock_embedder
        assert doc_embedder.schema_transform_fn is default_schema_transform_fn

    @patch("feast.repo_operations.apply_total")
    @patch("feast.repo_config.load_repo_config")
    def test_init_creates_feature_view(
        self, mock_load_config, mock_apply_total, tmp_path
    ):
        """With create_feature_view=True, generate_repo_file and apply_repo are called."""
        mock_chunker = MagicMock(spec=BaseChunker)
        mock_embedder = MagicMock(spec=BaseEmbedder)
        mock_embedder.get_embedding_dim.return_value = None

        with patch("feast.doc_embedder.generate_repo_file") as mock_gen:
            DocEmbedder(
                repo_path=str(tmp_path),
                feature_view_name="test_view",
                chunker=mock_chunker,
                embedder=mock_embedder,
                create_feature_view=True,
            )

        mock_gen.assert_called_once_with(
            repo_path=str(tmp_path),
            feature_view_name="test_view",
            vector_length=384,
        )
        mock_load_config.assert_called_once()
        mock_apply_total.assert_called_once()

    @patch("feast.repo_operations.apply_total")
    @patch("feast.repo_config.load_repo_config")
    def test_init_creates_feature_view_with_explicit_vector_length(
        self, mock_load_config, mock_apply_total, tmp_path
    ):
        """Explicit vector_length overrides auto-detection."""
        mock_chunker = MagicMock(spec=BaseChunker)
        mock_embedder = MagicMock(spec=BaseEmbedder)

        with patch("feast.doc_embedder.generate_repo_file") as mock_gen:
            DocEmbedder(
                repo_path=str(tmp_path),
                feature_view_name="test_view",
                chunker=mock_chunker,
                embedder=mock_embedder,
                create_feature_view=True,
                vector_length=256,
            )

        mock_gen.assert_called_once_with(
            repo_path=str(tmp_path),
            feature_view_name="test_view",
            vector_length=256,
        )

    @patch("feast.repo_operations.apply_total")
    @patch("feast.repo_config.load_repo_config")
    def test_init_creates_feature_view_auto_detects_vector_length(
        self, mock_load_config, mock_apply_total, tmp_path
    ):
        """Auto-detected vector_length from embedder is used when not explicitly set."""
        mock_chunker = MagicMock(spec=BaseChunker)
        mock_embedder = MagicMock(spec=BaseEmbedder)
        mock_embedder.get_embedding_dim.return_value = 512

        with patch("feast.doc_embedder.generate_repo_file") as mock_gen:
            DocEmbedder(
                repo_path=str(tmp_path),
                feature_view_name="test_view",
                chunker=mock_chunker,
                embedder=mock_embedder,
                create_feature_view=True,
            )

        mock_gen.assert_called_once_with(
            repo_path=str(tmp_path),
            feature_view_name="test_view",
            vector_length=512,
        )
        mock_embedder.get_embedding_dim.assert_called_once_with("text")

    @patch("feast.repo_operations.apply_total")
    @patch("feast.repo_config.load_repo_config")
    @patch("feast.feature_store.FeatureStore")
    def test_embed_documents_full_chain(
        self, mock_fs_cls, mock_load_config, mock_apply_total, tmp_path
    ):
        """embed_documents wires chunk -> embed -> schema_transform -> save correctly."""
        mock_chunker = MagicMock(spec=BaseChunker)
        chunked_df = pd.DataFrame(
            {
                "chunk_id": ["doc1_0", "doc1_1"],
                "original_id": ["doc1", "doc1"],
                "text": ["chunk text 1", "chunk text 2"],
                "chunk_index": [0, 1],
            }
        )
        mock_chunker.chunk_dataframe.return_value = chunked_df

        mock_embedder = MagicMock(spec=BaseEmbedder)
        embedded_df = chunked_df.copy()
        embedded_df["text_embedding"] = [[0.1, 0.2], [0.3, 0.4]]
        mock_embedder.embed_dataframe.return_value = embedded_df

        transformed_df = pd.DataFrame(
            {
                "passage_id": ["doc1_0", "doc1_1"],
                "text": ["chunk text 1", "chunk text 2"],
                "embedding": [[0.1, 0.2], [0.3, 0.4]],
                "source_id": ["doc1", "doc1"],
            }
        )
        logical_fn_tracker = MagicMock()

        def mock_logical_fn(df: pd.DataFrame) -> pd.DataFrame:
            logical_fn_tracker(df)
            return transformed_df

        doc_embedder = DocEmbedder(
            repo_path=str(tmp_path),
            chunker=mock_chunker,
            embedder=mock_embedder,
            schema_transform_fn=mock_logical_fn,
            create_feature_view=False,
        )

        input_df = pd.DataFrame(
            {
                "id": ["doc1"],
                "text": ["some long document text"],
                "type": ["article"],
            }
        )

        result = doc_embedder.embed_documents(
            documents=input_df,
            id_column="id",
            source_column="text",
            type_column="type",
        )

        mock_chunker.chunk_dataframe.assert_called_once()
        chunk_kwargs = mock_chunker.chunk_dataframe.call_args[1]
        assert chunk_kwargs["id_column"] == "id"
        assert chunk_kwargs["source_column"] == "text"
        assert chunk_kwargs["type_column"] == "type"

        mock_embedder.embed_dataframe.assert_called_once()
        embed_args = mock_embedder.embed_dataframe.call_args
        pd.testing.assert_frame_equal(embed_args[0][0], chunked_df)
        assert embed_args[1]["column_mapping"] == {"text": ("text", "text_embedding")}

        logical_fn_tracker.assert_called_once()
        pd.testing.assert_frame_equal(logical_fn_tracker.call_args[0][0], embedded_df)

        mock_fs_cls.return_value.write_to_online_store.assert_called_once()
        save_kwargs = mock_fs_cls.return_value.write_to_online_store.call_args[1]
        assert save_kwargs["feature_view_name"] == "text_feature_view"

        pd.testing.assert_frame_equal(result, transformed_df)

    @patch("feast.repo_operations.apply_total")
    @patch("feast.repo_config.load_repo_config")
    @patch("feast.feature_store.FeatureStore")
    def test_embed_documents_default_column_mapping(
        self, mock_fs_cls, mock_load_config, mock_apply_total, tmp_path
    ):
        """When column_mapping=None, default mapping is used."""
        mock_chunker = MagicMock(spec=BaseChunker)
        mock_chunker.chunk_dataframe.return_value = pd.DataFrame(
            {
                "chunk_id": ["c1"],
                "original_id": ["d1"],
                "text": ["t"],
                "chunk_index": [0],
            }
        )

        mock_embedder = MagicMock(spec=BaseEmbedder)
        mock_embedder.embed_dataframe.return_value = pd.DataFrame(
            {
                "chunk_id": ["c1"],
                "original_id": ["d1"],
                "text": ["t"],
                "chunk_index": [0],
                "text_embedding": [[0.1]],
            }
        )

        doc_embedder = DocEmbedder(
            repo_path=str(tmp_path),
            chunker=mock_chunker,
            embedder=mock_embedder,
            create_feature_view=False,
        )

        doc_embedder.embed_documents(
            documents=pd.DataFrame({"id": ["d1"], "text": ["t"]}),
            id_column="id",
            source_column="text",
            column_mapping=None,
        )

        embed_call = mock_embedder.embed_dataframe.call_args
        assert embed_call[1]["column_mapping"] == {"text": ("text", "text_embedding")}

    @patch("feast.repo_operations.apply_total")
    @patch("feast.repo_config.load_repo_config")
    @patch("feast.feature_store.FeatureStore")
    def test_embed_documents_custom_column_mapping(
        self, mock_fs_cls, mock_load_config, mock_apply_total, tmp_path
    ):
        """Custom column_mapping is forwarded to the embedder."""
        mock_chunker = MagicMock(spec=BaseChunker)
        mock_chunker.chunk_dataframe.return_value = pd.DataFrame(
            {
                "chunk_id": ["c1"],
                "original_id": ["d1"],
                "content": ["t"],
                "chunk_index": [0],
            }
        )

        mock_embedder = MagicMock(spec=BaseEmbedder)
        mock_embedder.embed_dataframe.return_value = pd.DataFrame(
            {
                "chunk_id": ["c1"],
                "original_id": ["d1"],
                "content": ["t"],
                "chunk_index": [0],
                "content_embedding": [[0.1]],
            }
        )

        def mock_logical_fn(df: pd.DataFrame) -> pd.DataFrame:
            return pd.DataFrame({"passage_id": ["c1"], "embedding": [[0.1]]})

        doc_embedder = DocEmbedder(
            repo_path=str(tmp_path),
            chunker=mock_chunker,
            embedder=mock_embedder,
            schema_transform_fn=mock_logical_fn,
            create_feature_view=False,
        )

        custom_mapping = ("text", "content_embedding")
        doc_embedder.embed_documents(
            documents=pd.DataFrame({"id": ["d1"], "content": ["t"]}),
            id_column="id",
            source_column="content",
            column_mapping=custom_mapping,
        )

        embed_call = mock_embedder.embed_dataframe.call_args
        assert embed_call[1]["column_mapping"] == {
            "content": ("text", "content_embedding")
        }

    @patch("feast.repo_operations.apply_total")
    @patch("feast.repo_config.load_repo_config")
    def test_apply_repo_restores_cwd(
        self, mock_load_config, mock_apply_total, tmp_path
    ):
        """apply_repo restores os.getcwd() even if apply_total changes it."""
        mock_chunker = MagicMock(spec=BaseChunker)
        mock_embedder = MagicMock(spec=BaseEmbedder)

        doc_embedder = DocEmbedder(
            repo_path=str(tmp_path),
            chunker=mock_chunker,
            embedder=mock_embedder,
            create_feature_view=False,
        )

        cwd_before = os.getcwd()
        mock_apply_total.side_effect = lambda **kwargs: os.chdir(str(tmp_path))

        doc_embedder.apply_repo()

        assert os.getcwd() == cwd_before

    @patch("feast.repo_operations.apply_total")
    @patch("feast.repo_config.load_repo_config")
    @patch("feast.feature_store.FeatureStore")
    def test_save_to_online_store(
        self, mock_fs_cls, mock_load_config, mock_apply_total, tmp_path
    ):
        """save_to_online_store creates FeatureStore and writes data."""
        mock_chunker = MagicMock(spec=BaseChunker)
        mock_embedder = MagicMock(spec=BaseEmbedder)

        doc_embedder = DocEmbedder(
            repo_path=str(tmp_path),
            chunker=mock_chunker,
            embedder=mock_embedder,
            create_feature_view=False,
        )

        test_df = pd.DataFrame({"col": [1, 2]})
        doc_embedder.save_to_online_store(df=test_df, feature_view_name="my_view")

        mock_fs_cls.assert_called_with(repo_path=str(tmp_path))
        mock_fs_cls.return_value.write_to_online_store.assert_called_once_with(
            feature_view_name="my_view",
            df=test_df,
        )


@patch("feast.repo_operations.apply_total")
@patch("feast.repo_config.load_repo_config")
@patch("feast.feature_store.FeatureStore")
def test_end_to_end_pipeline(mock_fs_cls, mock_load_config, mock_apply_total, tmp_path):
    """Full pipeline: real TextChunker + mocked embedder + default schema transform."""
    chunker = TextChunker(
        config=ChunkingConfig(
            chunk_size=10,
            chunk_overlap=2,
            min_chunk_size=3,
            max_chunk_chars=None,
        )
    )

    mock_embedder = MagicMock(spec=BaseEmbedder)

    def fake_embed_dataframe(df, column_mapping):
        df = df.copy()
        for _src_col, (_modality, out_col) in column_mapping.items():
            df[out_col] = [[0.1, 0.2, 0.3]] * len(df)
        return df

    mock_embedder.embed_dataframe.side_effect = fake_embed_dataframe

    doc_embedder = DocEmbedder(
        repo_path=str(tmp_path),
        chunker=chunker,
        embedder=mock_embedder,
        schema_transform_fn=default_schema_transform_fn,
        create_feature_view=False,
    )

    documents = pd.DataFrame(
        {
            "id": ["doc1", "doc2"],
            "text": [
                " ".join([f"word{i}" for i in range(30)]),
                " ".join([f"term{i}" for i in range(25)]),
            ],
        }
    )

    result = doc_embedder.embed_documents(
        documents=documents,
        id_column="id",
        source_column="text",
    )

    assert "passage_id" in result.columns
    assert "text" in result.columns
    assert "embedding" in result.columns
    assert "event_timestamp" in result.columns
    assert "source_id" in result.columns
    assert len(result) > 2
    assert set(result["source_id"].unique()) == {"doc1", "doc2"}
    mock_fs_cls.return_value.write_to_online_store.assert_called_once()
