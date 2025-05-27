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
from typing import Callable, Dict, List, Optional, Union, Any, Tuple

import numpy as np
from sentence_transformers import SentenceTransformer
from transformers import RagRetriever

from feast import FeatureStore
from feast.vector_store import VectorStore


class FeastIndex:
    """Dummy index required by HuggingFace's RagRetriever."""
    
    def __init__(self, vector_store: VectorStore):
        """Initialize the Feast index.
        
        Args:
            vector_store: Vector store instance to use for retrieval
        """
        self.vector_store = vector_store

    def get_top_docs(self, query_vectors: np.ndarray, n_docs: int = 5):
        """Get top documents (not implemented).
        
        This method is required by the RagRetriever interface but is not used
        as we override the retrieve method in FeastRAGRetriever.
        """
        raise NotImplementedError("get_top_docs is not yet implemented.")

    def get_doc_dicts(self, doc_ids: List[str]):
        """Get document dictionaries (not implemented).
        
        This method is required by the RagRetriever interface but is not used
        as we override the retrieve method in FeastRAGRetriever.
        """
        raise NotImplementedError("get_doc_dicts is not yet implemented.")


class FeastRAGRetriever(RagRetriever):
    """RAG retriever implementation that uses Feast as a backend."""

    VALID_SEARCH_TYPES = {"text", "vector", "hybrid"}

    def __init__(
        self,
        question_encoder_tokenizer,
        question_encoder,
        generator_tokenizer,
        generator_model,
        feast_repo_path: str,
        vector_store: VectorStore,
        search_type: str,
        config: Dict[str, Any],
        index: FeastIndex,
        format_document: Optional[Callable[[Dict[str, Any]], str]] = None,
        id_field: str = "",
        query_encoder_model: Union[str, SentenceTransformer] = "all-MiniLM-L6-v2",
        **kwargs,
    ):
        """Initialize the Feast RAG retriever.
        
        Args:
            question_encoder_tokenizer: Tokenizer for encoding questions
            question_encoder: Model for encoding questions
            generator_tokenizer: Tokenizer for the generator model
            generator_model: The generator model
            feast_repo_path: Path to the Feast repository
            vector_store: Vector store instance to use for retrieval
            search_type: Type of search to perform (text, vector, or hybrid)
            config: Configuration for the retriever
            index: Index instance (must be FeastIndex)
            format_document: Optional function to format retrieved documents
            id_field: Field to use as document ID
            query_encoder_model: Model to use for encoding queries
            **kwargs: Additional arguments passed to RagRetriever
        """
        if search_type.lower() not in self.VALID_SEARCH_TYPES:
            raise ValueError(
                f"Unsupported search_type {search_type}. "
                f"Must be one of: {self.VALID_SEARCH_TYPES}"
            )
        super().__init__(
            config=config,
            question_encoder_tokenizer=question_encoder_tokenizer,
            generator_tokenizer=generator_tokenizer,
            index=index,
            init_retrieval=False,
            **kwargs,
        )
        self.question_encoder = question_encoder
        self.generator_model = generator_model
        self.generator_tokenizer = generator_tokenizer
        self.feast = FeatureStore(repo_path=feast_repo_path)
        self.vector_store = vector_store
        self.search_type = search_type.lower()
        self.format_document = format_document or self._default_format_document
        self.id_field = id_field

        if isinstance(query_encoder_model, str):
            self.query_encoder = SentenceTransformer(query_encoder_model)
        else:
            self.query_encoder = query_encoder_model

    def retrieve(
        self, 
        question_hidden_states: np.ndarray, 
        n_docs: int
    ) -> Tuple[np.ndarray, List[Dict[str, str]]]:
        """Retrieve documents using hidden states (implements RagRetriever interface).
        
        Args:
            question_hidden_states: Hidden states from the question encoder
            n_docs: Number of documents to retrieve
            
        Returns:
            Tuple of (doc_scores, doc_dicts) where:
                - doc_scores: Cosine similarity scores, shape (1, n_docs)
                - doc_dicts: List of dictionaries containing the retrieved documents
        """
        # For text-only search, convert hidden states to query string
        if self.search_type == "text":
            query = self.question_encoder_tokenizer.decode(
                question_hidden_states.argmax(axis=-1),
                skip_special_tokens=True
            )
            results = self.vector_store.query(query_string=query, top_k=n_docs)
            # For text search, we'll compute scores using the query's vector representation
            query_vector = self.query_encoder.encode(query, convert_to_numpy=True)
        
        # For vector or hybrid search, use the hidden states directly
        else:
            # Convert question hidden states to vector using our query encoder
            query_vector = self.query_encoder.encode(
                self.question_encoder_tokenizer.decode(
                    question_hidden_states.argmax(axis=-1),
                    skip_special_tokens=True
                ),
                convert_to_numpy=True
            )
            
            if self.search_type == "vector":
                results = self.vector_store.query(
                    query_vector=query_vector,
                    top_k=n_docs
                )
            elif self.search_type == "hybrid":
                query = self.question_encoder_tokenizer.decode(
                    question_hidden_states.argmax(axis=-1),
                    skip_special_tokens=True
                )
                results = self.vector_store.query(
                    query_string=query,
                    query_vector=query_vector,
                    top_k=n_docs
                )
            else:
                raise ValueError(f"Unsupported search type: {self.search_type}")
        
        # Calculate cosine similarity scores
        doc_embeddings = np.array([doc["embedding"] for doc in results])
        
        # Normalize vectors for cosine similarity
        query_norm = np.linalg.norm(query_vector)
        doc_norms = np.linalg.norm(doc_embeddings, axis=1)
        
        # Avoid division by zero
        query_norm = np.maximum(query_norm, 1e-10)
        doc_norms = np.maximum(doc_norms, 1e-10)
        
        # Calculate cosine similarities
        similarities = np.dot(doc_embeddings, query_vector) / (doc_norms * query_norm)
        
        # Reshape to match RAG's expected format (batch_size=1, n_docs)
        doc_scores = similarities.reshape(1, -1)
        
        # Create document dictionaries in RAG format
        doc_dicts = [{"text": doc["text"]} for doc in results]
        
        return doc_scores, doc_dicts

    def generate_answer(
        self, query: str, top_k: int = 5, max_new_tokens: int = 100
    ) -> str:
        """Generate an answer for a query using retrieved context.
        
        Args:
            query: The query to answer
            top_k: Number of documents to retrieve
            max_new_tokens: Maximum number of tokens to generate
            
        Returns:
            Generated answer string
        """
        # Convert query to hidden states format expected by retrieve
        inputs = self.question_encoder_tokenizer(
            query, return_tensors="pt", padding=True, truncation=True
        )
        question_hidden_states = self.question_encoder(**inputs).last_hidden_state
        
        # Get documents using retrieve method
        doc_scores, doc_dicts = self.retrieve(question_hidden_states, n_docs=top_k)
        
        # Format context from retrieved documents
        contexts = [doc["text"] for doc in doc_dicts]
        context = "\n\n".join(contexts)
        
        prompt = (
            f"Use the following context to answer the question. Context:\n{context}\n\n"
            f"Question: {query}\nAnswer:"
        )
        
        self.generator_tokenizer.pad_token = self.generator_tokenizer.eos_token
        inputs = self.generator_tokenizer(
            prompt, return_tensors="pt", padding=True, truncation=True
        )
        input_ids = inputs["input_ids"]
        attention_mask = inputs["attention_mask"]
        output_ids = self.generator_model.generate(
            input_ids=input_ids,
            attention_mask=attention_mask,
            max_new_tokens=max_new_tokens,
            pad_token_id=self.generator_tokenizer.pad_token_id,
        )
        return self.generator_tokenizer.decode(output_ids[0], skip_special_tokens=True)

    def _default_format_document(self, doc: Dict[str, Any]) -> str:
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
