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

import io

import pytest
from PIL import Image

pytest.importorskip("torch")
pytest.importorskip("timm")
pytest.importorskip("sklearn")

from feast.image_utils import (
    ImageFeatureExtractor,
    combine_embeddings,
    get_image_metadata,
    validate_image_format,
)


@pytest.mark.skip(reason="Image utilities tests are skipped because they call out to a 3rd-party api.")
class TestImageFeatureExtractor:
    """Test ImageFeatureExtractor functionality."""

    def create_test_image(self, color="red", size=(224, 224), format="JPEG"):
        """Create a test image as bytes."""
        img = Image.new("RGB", size, color=color)
        output = io.BytesIO()
        img.save(output, format=format)
        return output.getvalue()

    def test_init_default_model(self):
        """Test initialization with default model."""
        extractor = ImageFeatureExtractor()
        assert extractor.model_name == "resnet34"

    def test_init_custom_model(self):
        """Test initialization with custom model."""
        extractor = ImageFeatureExtractor("resnet18")
        assert extractor.model_name == "resnet18"

    def test_extract_embedding_basic(self):
        """Test basic embedding extraction."""
        extractor = ImageFeatureExtractor()
        image_bytes = self.create_test_image()

        embedding = extractor.extract_embedding(image_bytes)

        assert isinstance(embedding, list)
        assert len(embedding) > 0
        assert all(isinstance(x, float) for x in embedding)

    def test_extract_embedding_different_formats(self):
        """Test embedding extraction with different image formats."""
        extractor = ImageFeatureExtractor()

        # Test JPEG
        jpeg_bytes = self.create_test_image(format="JPEG")
        jpeg_embedding = extractor.extract_embedding(jpeg_bytes)
        assert len(jpeg_embedding) > 0

        # Test PNG
        png_bytes = self.create_test_image(format="PNG")
        png_embedding = extractor.extract_embedding(png_bytes)
        assert len(png_embedding) > 0

        # Embeddings should be same length
        assert len(jpeg_embedding) == len(png_embedding)

    def test_extract_embedding_invalid_image(self):
        """Test embedding extraction with invalid image data."""
        extractor = ImageFeatureExtractor()

        with pytest.raises(ValueError, match="Failed to extract embedding"):
            extractor.extract_embedding(b"invalid image data")

    def test_batch_extract_embeddings(self):
        """Test batch embedding extraction."""
        extractor = ImageFeatureExtractor()
        image_list = [
            self.create_test_image("red"),
            self.create_test_image("blue"),
            self.create_test_image("green"),
        ]

        embeddings = extractor.batch_extract_embeddings(image_list)

        assert len(embeddings) == 3
        assert all(isinstance(emb, list) for emb in embeddings)
        assert all(len(emb) > 0 for emb in embeddings)
        assert len(set(len(emb) for emb in embeddings)) == 1


class TestCombineEmbeddings:
    """Test embedding combination functionality."""

    def test_weighted_sum_basic(self):
        """Test basic weighted sum combination."""
        text_emb = [1.0, 2.0, 3.0]
        image_emb = [0.5, 1.0, 1.5]

        combined = combine_embeddings(
            text_emb,
            image_emb,
            strategy="weighted_sum",
            text_weight=0.6,
            image_weight=0.4,
        )

        expected = [0.8, 1.6, 2.4]
        assert len(combined) == 3
        for i, val in enumerate(expected):
            assert abs(combined[i] - val) < 1e-6

    def test_weighted_sum_different_dimensions(self):
        """Test weighted sum with different embedding dimensions."""
        text_emb = [1.0, 2.0]
        image_emb = [0.5, 1.0, 1.5]

        combined = combine_embeddings(
            text_emb,
            image_emb,
            strategy="weighted_sum",
            text_weight=0.5,
            image_weight=0.5,
        )

        assert len(combined) == 3
        expected = [0.75, 1.5, 0.75]
        for i, val in enumerate(expected):
            assert abs(combined[i] - val) < 1e-6

    def test_concatenate_strategy(self):
        """Test concatenation strategy."""
        text_emb = [1.0, 2.0]
        image_emb = [3.0, 4.0]

        combined = combine_embeddings(text_emb, image_emb, strategy="concatenate")

        assert combined == [1.0, 2.0, 3.0, 4.0]

    def test_average_strategy(self):
        """Test average strategy."""
        text_emb = [2.0, 4.0]
        image_emb = [1.0, 2.0]

        combined = combine_embeddings(text_emb, image_emb, strategy="average")

        expected = [1.5, 3.0]
        assert len(combined) == 2
        for i, val in enumerate(expected):
            assert abs(combined[i] - val) < 1e-6

    def test_invalid_strategy(self):
        """Test invalid combination strategy."""
        text_emb = [1.0, 2.0]
        image_emb = [3.0, 4.0]

        with pytest.raises(ValueError, match="Unknown combination strategy"):
            combine_embeddings(text_emb, image_emb, strategy="invalid")

    def test_invalid_weights(self):
        """Test invalid weight values."""
        text_emb = [1.0, 2.0]
        image_emb = [3.0, 4.0]

        with pytest.raises(ValueError, match="must equal 1.0"):
            combine_embeddings(
                text_emb,
                image_emb,
                strategy="weighted_sum",
                text_weight=0.3,
                image_weight=0.5,  # Don't sum to 1.0
            )


class TestImageValidation:
    """Test image validation utilities."""

    def create_test_image(self, color="red", size=(100, 100), format="JPEG"):
        """Create a test image as bytes."""
        img = Image.new("RGB", size, color=color)
        output = io.BytesIO()
        img.save(output, format=format)
        return output.getvalue()

    def test_validate_image_format_valid(self):
        """Test validation with valid image."""
        image_bytes = self.create_test_image()
        assert validate_image_format(image_bytes) is True

    def test_validate_image_format_invalid(self):
        """Test validation with invalid image data."""
        assert validate_image_format(b"invalid data") is False

    def test_get_image_metadata(self):
        """Test getting image metadata."""
        image_bytes = self.create_test_image(size=(200, 150), format="PNG")
        metadata = get_image_metadata(image_bytes)

        assert metadata["format"] == "PNG"
        assert metadata["mode"] == "RGB"
        assert metadata["width"] == 200
        assert metadata["height"] == 150
        assert metadata["size_bytes"] == len(image_bytes)

    def test_get_image_metadata_invalid(self):
        """Test getting metadata from invalid image."""
        with pytest.raises(ValueError, match="Failed to extract image metadata"):
            get_image_metadata(b"invalid data")
