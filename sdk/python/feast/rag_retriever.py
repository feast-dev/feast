# Copyright 2019 The Feast Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
from typing import Any, Callable, Optional

import numpy as np
from transformers import (
    PreTrainedModel,
    PreTrainedTokenizer,
    RagRetriever,
)

from feast import FeatureStore, FeatureView
from feast.torch_wrapper import get_torch
from feast.vector_store import FeastVectorStore


class FeastIndex:
    """Dummy index required by HuggingFace's RagRetriever."""

    def __init__(self):
        """Initialize the Feast index."""
        pass


class FeastRAGRetriever(RagRetriever):
    """RAG retriever implementation that uses Feast as a backend."""

    VALID_SEARCH_TYPES = {"text", "vector", "hybrid"}

    def __init__(
        self,
        question_encoder_tokenizer: PreTrainedTokenizer,
        question_encoder: PreTrainedModel,
        generator_tokenizer: PreTrainedTokenizer,
        generator_model: PreTrainedModel,
        feast_repo_path: str,
        feature_view: FeatureView,
        features: list[str],
        search_type: str,
        config: dict[str, Any],
        index: FeastIndex,
        format_document: Optional[Callable[[dict[str, Any]], str]] = None,
        id_field: str = "",
        text_field: str = "",
        **kwargs,
    ):
        """Initialize the Feast RAG retriever.

        Args:
            question_encoder_tokenizer: Tokenizer for encoding questions
            question_encoder: Model for encoding questions
            generator_tokenizer: Tokenizer for the generator model
            generator_model: The generator model
            feast_repo_path: Path to the Feast repository
            feature_view: Feast FeatureView containing the document data
            features: List of feature names to use from the feature view
            search_type: Type of search to perform (text, vector, or hybrid)
            config: Configuration for the retriever
            index: Index instance (must be FeastIndex)
            format_document: Optional function to format retrieved documents
            id_field: Field to use as document ID
            text_field: Field to use as text field name
            **kwargs: Additional arguments passed to RagRetriever
        """
        if search_type.lower() not in self.VALID_SEARCH_TYPES:
            raise ValueError(
                f"Unsupported search_type {search_type}. "
                f"Must be one of: {self.VALID_SEARCH_TYPES}"
            )

        # move to gpu if available
        torch = get_torch()
        self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        self.question_encoder = question_encoder.to(self.device)  # type: ignore
        self.generator_model = generator_model.to(self.device)  # type: ignore

        self.question_encoder_tokenizer = question_encoder_tokenizer
        self.generator_tokenizer = generator_tokenizer

        super().__init__(
            config=config,
            question_encoder_tokenizer=self.question_encoder_tokenizer,
            generator_tokenizer=self.generator_tokenizer,
            index=index,
            init_retrieval=False,
            **kwargs,
        )

        self.feast_repo_path = feast_repo_path
        self.search_type = search_type.lower()
        self.format_document = format_document or self._default_format_document
        self.id_field = id_field
        self.text_field = text_field
        self.feature_view = feature_view
        self.features = features

        # Initialize these lazily for compatability with distributed training/tuning
        self._feature_store = None
        self._vector_store = None

    @property
    def feature_store(self):
        # Initialize FeatureStore lazily
        if self._feature_store is None:
            self._feature_store = FeatureStore(repo_path=self.feast_repo_path)
        return self._feature_store

    @property
    def vector_store(self):
        # Initialize FeastVectorStore lazily
        if self._vector_store is None:
            self._vector_store = FeastVectorStore(
                repo_path=self.feast_repo_path,
                rag_view=self.feature_view,
                features=self.features,
            )
        return self._vector_store

    def retrieve(  # type: ignore [override]
        self,
        question_hidden_states: np.ndarray,
        n_docs: int,
        query: Optional[str] = None,
    ) -> tuple[np.ndarray, np.ndarray, list[dict]]:
        """
        Overrides the base `retrieve` method to fetch documents from Feast.

        This method processes a batch of questions, queries the vector store,
        and formats the results into the 3-part tuple expected by the RAG model:
        (document_embeddings, document_ids, document_dictionaries).

        Args:
            question_hidden_states (np.ndarray):
                Hidden state representation of the question from the encoder.
                Expected shape is (batch_size, seq_len, hidden_dim).
            n_docs (int):
                Number of top documents to retrieve.
            query (Optional[str]):
                Optional raw query string. If not provided and search_type is "text" or "hybrid",
                it will be decoded from question_hidden_states.

        Returns:
            Tuple containing:
                - retrieved_doc_embeds (np.ndarray):
                    Embeddings of the retrieved documents with shape (batch_size, n_docs, embed_dim).
                - doc_ids (np.ndarray):
                    Array of document IDs or passage identifiers with shape (batch_size, n_docs).
                - doc_dicts (list[dict]):
                    List of dictionaries containing document metadata and text.
                    Each dictionary has keys "text", "id", and "title".
        """
        batch_size = question_hidden_states.shape[0]

        # Convert the question hidden states into a list of 1D query vectors.
        pooled_query_vectors = []
        for i in range(batch_size):
            pooled = question_hidden_states[i]
            # Perform normalization to create a unit vector.
            norm = np.linalg.norm(pooled)
            if norm > 0:
                pooled = pooled / norm
            pooled_query_vectors.append(pooled)

        # Determine embedding dimension for padding
        emb_dim = (
            pooled_query_vectors[0].shape[-1]
            if pooled_query_vectors and pooled_query_vectors[0] is not None
            else self.config.retrieval_vector_size
        )

        # Retrieve documents for each query in batch
        batch_embeddings, batch_doc_ids, batch_metadata = [], [], []

        for i in range(batch_size):
            query_vector = pooled_query_vectors[i]
            if isinstance(query, list):
                query_text = query[i] if i < len(query) else None
            else:
                query_text = query

            # Query Feast to get the raw document data
            response = self.vector_store.query(
                query_vector=query_vector if self.search_type != "text" else None,
                query_string=query_text if self.search_type != "vector" else None,
                top_k=n_docs,
            )
            results_dict = response.to_dict()
            # Dynamically get data using the configured feature names
            texts = results_dict.get(self.text_field, [])
            ids = results_dict.get(self.id_field, [])
            num_retrieved = len(texts)

            embeddings = []
            embedding_field_name = None
            for field in self.feature_view.schema:
                if hasattr(field, "vector_index") and field.vector_index:
                    embedding_field_name = field.name
                    break

            if embedding_field_name:
                embeddings = results_dict.get(embedding_field_name, [])
            else:
                raise ValueError(
                    "Warning: No field with 'vector_index=True' found in schema, cannot extract embeddings"
                )

            # Initialize lists for the current query's results
            current_query_embeddings, current_query_ids, current_query_texts = (
                [],
                [],
                [],
            )

            # Process retrieved documents up to n_docs
            emb_array = np.array([])
            if num_retrieved > 0:
                emb_array = np.array(
                    [np.asarray(emb, dtype=np.float32) for emb in embeddings]
                )
                if emb_array.ndim == 1:  # Reshape if it's a flat array
                    emb_array = emb_array.reshape(num_retrieved, -1)

            for k in range(n_docs):
                if k < num_retrieved:
                    # Append actual retrieved data
                    current_query_texts.append(texts[k])
                    current_query_embeddings.append(emb_array[k])
                    try:
                        current_query_ids.append(int(ids[k]))
                    except (ValueError, TypeError):
                        current_query_ids.append(-1)  # Use placeholder for invalid IDs
                else:
                    # Pad with empty/zero values if fewer than n_docs were found
                    current_query_texts.append("")
                    current_query_embeddings.append(np.zeros(emb_dim, dtype=np.float32))
                    current_query_ids.append(-1)

            # Collate results for the current query
            batch_embeddings.append(np.array(current_query_embeddings))
            batch_doc_ids.append(np.array(current_query_ids))
            batch_metadata.append(
                {
                    "text": current_query_texts,
                    "id": current_query_ids,
                    "title": [""]
                    * n_docs,  # RAG model expects a "title" key during Forward pass
                }
            )

        # Return the Collated Batch Results
        # Convert lists of arrays into single batch arrays.
        return (
            np.array(batch_embeddings, dtype=np.float32),
            np.array(batch_doc_ids, dtype=np.int64),
            batch_metadata,
        )

    def generate_answer(
        self, query: str, top_k: int = 5, max_new_tokens: int = 100
    ) -> str:
        """A helper method to generate an answer for a single query string.

        Args:
            query: The query to answer
            top_k: Number of documents to retrieve
            max_new_tokens: Maximum number of tokens to generate

        Returns:
            Generated answer string
        """
        if not self.question_encoder or not self.generator_model:
            raise ValueError(
                "`question_encoder` and `generator_model` must be provided to use `generate_answer`."
            )
        torch = get_torch()
        inputs = self.question_encoder_tokenizer(query, return_tensors="pt").to(
            self.question_encoder.device
        )
        question_embeddings = self.question_encoder(**inputs).pooler_output
        question_embeddings = (
            question_embeddings.detach().cpu().to(torch.float32).numpy()
        )
        _, _, doc_batch = self.retrieve(question_embeddings, n_docs=top_k, query=query)

        contexts = doc_batch[0]["text"] if doc_batch else []
        context_str = "\n\n".join(filter(None, contexts))

        prompt = f"Context: {context_str}\n\nQuestion: {query}\n\nAnswer:"

        generator_inputs = self.generator_tokenizer(prompt, return_tensors="pt").to(
            self.generator_model.device
        )
        output_ids = self.generator_model.generate(
            **generator_inputs, max_new_tokens=max_new_tokens
        )

        return self.generator_tokenizer.decode(output_ids[0], skip_special_tokens=True)

    def _default_format_document(self, doc: dict[str, Any]) -> str:
        """Default document formatting function.

        Args:
            doc: Document dictionary to format

        Returns:
            Formatted document string
        """
        lines = []
        for key, value in doc.items():
            # Skip vectors by checking for long float lists
            if (
                isinstance(value, list)
                and len(value) > 10
                and all(isinstance(x, (float, int)) for x in value)
            ):
                continue
            lines.append(f"{key.replace('_', ' ').capitalize()}: {value}")
        return "\n".join(lines)
