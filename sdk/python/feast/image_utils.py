# Copyright 2024 The Feast Authors
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

"""
Image processing utilities for Feast image search capabilities.
Provides image embedding generation and combination functions for multi-modal search.
"""

import io
from typing import List

try:
    import timm
    import torch
    from PIL import Image
    from sklearn.preprocessing import normalize
    from timm.data import resolve_data_config
    from timm.data.transforms_factory import create_transform

    _image_dependencies_available = True
except ImportError:
    _image_dependencies_available = False


COMBINATION_STRATEGIES = ["weighted_sum", "concatenate", "average"]


def _check_image_dependencies():
    """Check if image processing dependencies are available."""
    if not _image_dependencies_available:
        raise ImportError(
            "Image processing dependencies are not installed. "
            "Please install with: pip install feast[image]"
        )


class ImageFeatureExtractor:
    """
    Extract image embeddings using pre-trained vision models.
    This class uses timm (PyTorch Image Models) to generate embeddings
    from images using pre-trained vision models like ResNet, ViT, etc.

    Examples:
        Basic usage::

            extractor = ImageFeatureExtractor()
            with open("image.jpg", "rb") as f:
                image_bytes = f.read()
            embedding = extractor.extract_embedding(image_bytes)

        Using different models::

            # ResNet-50
            extractor = ImageFeatureExtractor("resnet50")
            embedding = extractor.extract_embedding(image_bytes)
            # ViT model
            extractor = ImageFeatureExtractor("vit_base_patch16_224")
            embedding = extractor.extract_embedding(image_bytes)
    """

    def __init__(self, model_name: str = "resnet34"):
        """
        Initialize with a pre-trained model.
        Args:
            model_name: Model name from timm library. Popular choices:
                - "resnet34": Fast, good for general use (default)
                - "resnet50": Better accuracy than ResNet-34
                - "vit_base_patch16_224": Vision Transformer, high accuracy
                - "efficientnet_b0": Good balance of speed and accuracy
                - "mobilenetv3_large_100": Fast inference for mobile/edge
        Raises:
            ImportError: If image processing dependencies are not installed
            RuntimeError: If the specified model cannot be loaded
        """
        _check_image_dependencies()

        try:
            self.model_name = model_name
            self.model = timm.create_model(
                model_name, pretrained=True, num_classes=0, global_pool="avg"
            )
            self.model.eval()

            config = resolve_data_config({}, model=model_name)
            self.preprocess = create_transform(**config)

        except Exception as e:
            raise RuntimeError(f"Failed to load model '{model_name}': {e}")

    def extract_embedding(self, image_bytes: bytes) -> List[float]:
        """
        Extract embedding from image bytes.
        Args:
            image_bytes: Image data as bytes (JPEG, PNG, WebP, etc.)
        Returns:
            Normalized embedding vector as list of floats
        Raises:
            ValueError: If image cannot be processed or is invalid
        """
        try:
            image = Image.open(io.BytesIO(image_bytes)).convert("RGB")
            input_tensor = self.preprocess(image).unsqueeze(0)

            with torch.no_grad():
                output = self.model(input_tensor)

            feature_vector = output.squeeze().numpy()
            normalized = normalize(feature_vector.reshape(1, -1), norm="l2")
            return normalized.flatten().tolist()

        except Exception as e:
            raise ValueError(f"Failed to extract embedding from image: {e}")

    def batch_extract_embeddings(
        self, image_bytes_list: List[bytes]
    ) -> List[List[float]]:
        """
        Extract embeddings from multiple images in batch for efficiency.
        Args:
            image_bytes_list: List of image data as bytes
        Returns:
            List of normalized embedding vectors
        Raises:
            ValueError: If any image cannot be processed
        """
        embeddings = []

        images = []
        for image_bytes in image_bytes_list:
            try:
                image = Image.open(io.BytesIO(image_bytes)).convert("RGB")
                preprocessed = self.preprocess(image)
                images.append(preprocessed)
            except Exception as e:
                raise ValueError(f"Failed to preprocess image: {e}")

        batch_tensor = torch.stack(images)

        with torch.no_grad():
            outputs = self.model(batch_tensor)

        for output in outputs:
            feature_vector = output.numpy()
            normalized = normalize(feature_vector.reshape(1, -1), norm="l2")
            embeddings.append(normalized.flatten().tolist())

        return embeddings


def combine_embeddings(
    text_embedding: List[float],
    image_embedding: List[float],
    strategy: str = "weighted_sum",
    text_weight: float = 0.5,
    image_weight: float = 0.5,
) -> List[float]:
    """
    Combine text and image embeddings search.
    This function provides several strategies for combining embeddings from
    different modalities (text and image) into a single vector for search.

    Args:
        text_embedding: Text embedding vector
        image_embedding: Image embedding vector
        strategy: Combination strategy (default: "weighted_sum")
        text_weight: Weight for text embedding (for weighted strategies)
        image_weight: Weight for image embedding (for weighted strategies)

    Returns:
        Combined embedding vector as list of floats

    Raises:
        ValueError: If weights don't sum to 1.0 for weighted_sum strategy

    Examples:
        Weighted combination (emphasize image)::

            combined = combine_embeddings(
                [0.1, 0.2], [0.8, 0.9],  # text_emb, image_emb
                strategy="weighted_sum",
                text_weight=0.3, image_weight=0.7
            )

        Concatenation for full information::

            combined = combine_embeddings(
                [0.1, 0.2], [0.8, 0.9],  # text_emb, image_emb
                strategy="concatenate"
            )
    """
    if strategy == "weighted_sum":
        if abs(text_weight + image_weight - 1.0) > 1e-6:
            raise ValueError(
                "text_weight + image_weight must equal 1.0 for weighted_sum"
            )

        max_dim = max(len(text_embedding), len(image_embedding))
        text_padded = text_embedding + [0.0] * (max_dim - len(text_embedding))
        image_padded = image_embedding + [0.0] * (max_dim - len(image_embedding))

        combined = [
            text_weight * t + image_weight * i
            for t, i in zip(text_padded, image_padded)
        ]
        return combined

    elif strategy == "concatenate":
        return text_embedding + image_embedding

    elif strategy == "average":
        max_dim = max(len(text_embedding), len(image_embedding))
        text_padded = text_embedding + [0.0] * (max_dim - len(text_embedding))
        image_padded = image_embedding + [0.0] * (max_dim - len(image_embedding))

        combined = [(t + i) / 2.0 for t, i in zip(text_padded, image_padded)]
        return combined

    else:
        raise ValueError(
            f"Unknown combination strategy: {strategy}. "
            f"Supported strategies: {', '.join(COMBINATION_STRATEGIES)}"
        )


def validate_image_format(image_bytes: bytes) -> bool:
    """
    Validate that the provided bytes represent a valid image.
    Args:
        image_bytes: Image data as bytes
    Returns:
        True if valid image, False otherwise
    """
    try:
        with Image.open(io.BytesIO(image_bytes)) as img:
            img.verify()
        return True
    except Exception:
        return False


def get_image_metadata(image_bytes: bytes) -> dict:
    """
    Extract metadata from image bytes.
    Args:
        image_bytes: Image data as bytes
    Returns:
        Dictionary with image metadata (format, size, mode, etc.)
    Raises:
        ValueError: If image cannot be processed
    """
    try:
        with Image.open(io.BytesIO(image_bytes)) as img:
            return {
                "format": img.format,
                "mode": img.mode,
                "width": img.width,
                "height": img.height,
                "size_bytes": len(image_bytes),
            }
    except Exception as e:
        raise ValueError(f"Failed to extract image metadata: {e}")
