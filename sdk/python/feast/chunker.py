from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Optional

import pandas as pd


@dataclass
class ChunkingConfig:
    chunk_size: int = 100
    chunk_overlap: int = 20
    min_chunk_size: int = 20
    max_chunk_chars: Optional[int] = 500


class BaseChunker(ABC):
    """
    Abstract base class for document chunking.

    Subclasses implement load_parse_and_chunk() with their own:
    - Loading logic
    - Parsing logic
    - Chunking strategy
    """

    def __init__(self, config: Optional[ChunkingConfig] = None):
        self.config = config or ChunkingConfig()

    @abstractmethod
    def load_parse_and_chunk(
        self,
        source: Any,
        source_id: str,
        source_column: str,
        source_type: Optional[str] = None,
    ) -> list[dict]:
        """
        Load, parse, and chunk a document.

        Args:
            source: File path, raw text, bytes, etc.
            source_id: Document identifier.
            source_type: Optional type hint.
            source_column: The column containing the document sources.

        Returns:
            List of chunk dicts with keys:
                - chunk_id: str
                - original_id: str
                - text: str
                - chunk_index: int
                - (any additional metadata)
        """
        pass

    def chunk_dataframe(
        self,
        df: pd.DataFrame,
        id_column: str,
        source_column: str,
        type_column: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        Chunk all documents in a DataFrame.

        Args:
            df: The DataFrame containing the documents to chunk.
            id_column: The column containing the document IDs.
            source_column: The column containing the document sources.
            type_column: The column containing the document types.
        """

        all_chunks = []
        for row in df.itertuples(index=False):
            chunks = self.load_parse_and_chunk(
                getattr(row, source_column),
                str(getattr(row, id_column)),
                source_column,
                getattr(row, type_column) if type_column else None,
            )
            all_chunks.extend(chunks)

        if not all_chunks:
            return pd.DataFrame(
                columns=["chunk_id", "original_id", source_column, "chunk_index"]
            )
        return pd.DataFrame(all_chunks)


class TextChunker(BaseChunker):
    """Default chunker for plain text. Chunks by word count."""

    def load_parse_and_chunk(
        self,
        source: Any,
        source_id: str,
        source_column: str,
        source_type: Optional[str] = None,
    ) -> list[dict]:
        # Load
        text = self._load(source)

        # Chunk by words
        return self._chunk_by_words(text, source_id, source_column)

    def _load(self, source: Any) -> str:
        from pathlib import Path

        if isinstance(source, Path) and source.exists():
            return Path(source).read_text()
        if isinstance(source, str):
            if source.endswith(".txt") and Path(source).exists():
                return Path(source).read_text()
        return str(source)

    def _chunk_by_words(
        self, text: str, source_id: str, source_column: str
    ) -> list[dict]:
        words = text.split()
        chunks = []

        step = self.config.chunk_size - self.config.chunk_overlap
        if step <= 0:
            raise ValueError(
                f"chunk_overlap ({self.config.chunk_overlap}) must be less than "
                f"chunk_size ({self.config.chunk_size})"
            )
        chunk_index = 0

        for i in range(0, len(words), step):
            chunk_words = words[i : i + self.config.chunk_size]

            if len(chunk_words) < self.config.min_chunk_size:
                continue

            chunk_text = " ".join(chunk_words)
            if self.config.max_chunk_chars:
                chunk_text = chunk_text[: self.config.max_chunk_chars]

            chunks.append(
                {
                    "chunk_id": f"{source_id}_{chunk_index}",
                    "original_id": source_id,
                    source_column: chunk_text,
                    "chunk_index": chunk_index,
                }
            )
            chunk_index += 1

        return chunks
