from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Callable, List, Optional

if TYPE_CHECKING:
    import numpy as np
import pandas as pd


@dataclass
class EmbeddingConfig:
    batch_size: int = 64
    show_progress: bool = True


class BaseEmbedder(ABC):
    """
    Abstract base class for embedding generation.

    Supports multiple modalities via routing.
    Users can register custom modality handlers.
    """

    def __init__(self, config: Optional[EmbeddingConfig] = None):
        self.config = config or EmbeddingConfig()

        # Registry: modality -> embedding function
        self._modality_handlers: dict[str, Callable[[List[Any]], "np.ndarray"]] = {}

        # Register default modalities (subclass can override)
        self._register_default_modalities()

    def _register_default_modalities(self) -> None:
        """Override in subclass to register default modality handlers."""
        pass

    def register_modality(
        self,
        modality: str,
        handler: Callable[[List[Any]], "np.ndarray"],
    ) -> None:
        """
        Register a handler for a modality.

        Args:
            modality: Name of modality ("text", "image", "video", etc.)
            handler: Function that takes list of inputs and returns embeddings.
        """
        self._modality_handlers[modality] = handler

    @property
    def supported_modalities(self) -> List[str]:
        """Return list of supported modalities."""
        return list(self._modality_handlers.keys())

    def get_embedding_dim(self, modality: str) -> Optional[int]:
        """
        Return the embedding dimension for a given modality.

        Subclasses should override this to return the actual dimension
        so that auto-generated FeatureView schemas use the correct vector_length.

        Args:
            modality: The modality to query (e.g. "text", "image").

        Returns:
            The embedding dimension, or None if unknown.
        """
        return None

    @abstractmethod
    def embed(self, inputs: List[Any], modality: str) -> "np.ndarray":
        """
        Generate embeddings for inputs of a given modality.

        Args:
            inputs: List of inputs.
            modality: Type of content ("text", "image", "video", etc.)

        Returns:
            numpy array of shape (len(inputs), embedding_dim)
        """
        pass

    def embed_dataframe(
        self,
        df: pd.DataFrame,
        column_mapping: dict[str, tuple[str, str]],
    ) -> pd.DataFrame:
        """
        Add embeddings for multiple columns with modality routing.

        Args:
            df: Input DataFrame.
            column_mapping: Dict mapping source_column -> (modality, output_column).
                Example: {
                    "text": ("text", "text_embedding"),
                    "image_path": ("image", "image_embedding"),
                    "video_path": ("video", "video_embedding"),
                }
        """
        df = df.copy()

        for source_column, (modality, output_column) in column_mapping.items():
            inputs = df[source_column].tolist()
            embeddings = self.embed(inputs, modality)
            df[output_column] = pd.Series(
                [emb.tolist() for emb in embeddings], dtype=object, index=df.index
            )

        return df


class MultiModalEmbedder(BaseEmbedder):
    """
    Multi-modal embedder with built-in support for common modalities.

    Supports: text, image, video (extensible)
    """

    def __init__(
        self,
        text_model: str = "all-MiniLM-L6-v2",
        image_model: str = "openai/clip-vit-base-patch32",
        config: Optional[EmbeddingConfig] = None,
    ):
        self.text_model_name = text_model
        self.image_model_name = image_model

        # Lazy-loaded models
        self._text_model = None
        self._image_model = None
        self._image_processor = None

        super().__init__(config)

    def _register_default_modalities(self) -> None:
        """Register built-in modality handlers."""
        self.register_modality("text", self._embed_text)
        self.register_modality("image", self._embed_image)
        # Future: add more as needed
        # self.register_modality("video", self._embed_video)
        # self.register_modality("audio", self._embed_audio)

    def embed(self, inputs: List[Any], modality: str) -> "np.ndarray":
        """Route to appropriate handler based on modality."""
        if modality not in self._modality_handlers:
            raise ValueError(
                f"Unsupported modality: '{modality}'. "
                f"Supported: {self.supported_modalities}"
            )

        handler = self._modality_handlers[modality]
        return handler(inputs)

    def get_embedding_dim(self, modality: str) -> Optional[int]:
        """
        Return the embedding dimension for a given modality.

        For "text", this queries the SentenceTransformer model's dimension
        (which triggers lazy model loading).

        Args:
            modality: The modality to query (e.g. "text", "image").

        Returns:
            The embedding dimension, or None if unknown.
        """
        if modality == "text":
            return self.text_model.get_sentence_embedding_dimension()
        elif modality == "image":
            return self.image_model.config.vision_config.hidden_size
        return None

    # Text Embedding
    @property
    def text_model(self):
        if self._text_model is None:
            from sentence_transformers import SentenceTransformer

            self._text_model = SentenceTransformer(self.text_model_name)
        return self._text_model

    def _embed_text(self, inputs: List[str]) -> "np.ndarray":
        return self.text_model.encode(
            inputs,
            batch_size=self.config.batch_size,
            show_progress_bar=self.config.show_progress,
        )

    # Image Embedding
    @property
    def image_model(self):
        if self._image_model is None:
            from transformers import CLIPModel

            self._image_model = CLIPModel.from_pretrained(self.image_model_name)
        return self._image_model

    @property
    def image_processor(self):
        if self._image_processor is None:
            from transformers import CLIPProcessor

            self._image_processor = CLIPProcessor.from_pretrained(self.image_model_name)
        return self._image_processor

    def _embed_image(self, inputs: List[Any]) -> "np.ndarray":
        from pathlib import Path

        import numpy as np
        from PIL import Image

        all_embeddings: List["np.ndarray"] = []
        batch_size = self.config.batch_size

        for start in range(0, len(inputs), batch_size):
            batch = inputs[start : start + batch_size]
            images = []
            opened: List[Image.Image] = []
            try:
                for inp in batch:
                    if isinstance(
                        inp, (str, Path)
                    ):  # If the input string path is too large that It gives error and we could not open the image.
                        img = Image.open(inp)
                        opened.append(img)
                        images.append(img)
                    else:
                        images.append(inp)

                processed = self.image_processor(images=images, return_tensors="pt")
            finally:
                for opened_img in opened:
                    opened_img.close()

            embeddings = self.image_model.get_image_features(**processed)
            embeddings = embeddings / embeddings.norm(p=2, dim=-1, keepdim=True)
            all_embeddings.append(embeddings.detach().numpy())

        return np.concatenate(all_embeddings, axis=0)
