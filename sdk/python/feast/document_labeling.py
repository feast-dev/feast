import json
from typing import Any, Dict, List, Optional

from feast.feature import Feature


class DocumentLabel:
    def __init__(
        self,
        chunk_id: str,
        document_id: str,
        label: str,
        confidence: Optional[float] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ):
        self.chunk_id = chunk_id
        self.document_id = document_id
        self.label = label
        self.confidence = confidence
        self.metadata = metadata or {}

    def to_dict(self) -> Dict[str, Any]:
        return {
            "chunk_id": self.chunk_id,
            "document_id": self.document_id,
            "label": self.label,
            "confidence": self.confidence,
            "metadata": self.metadata,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "DocumentLabel":
        return cls(
            chunk_id=data["chunk_id"],
            document_id=data["document_id"],
            label=data["label"],
            confidence=data.get("confidence"),
            metadata=data.get("metadata", {}),
        )


def store_document_label(feature: Feature, label: DocumentLabel) -> None:
    if not hasattr(feature, "labels") or feature.labels is None:
        if hasattr(feature, "_labels"):
            feature._labels = {}
        else:
            return

    labels_dict = feature.labels if hasattr(feature, "labels") else feature._labels
    labels_key = "document_labels"
    if labels_key not in labels_dict:
        labels_dict[labels_key] = "[]"

    existing_labels = json.loads(labels_dict[labels_key])
    existing_labels.append(label.to_dict())
    labels_dict[labels_key] = json.dumps(existing_labels)


def get_document_labels(feature: Feature) -> List[DocumentLabel]:
    labels_dict = None
    if hasattr(feature, "labels") and feature.labels:
        labels_dict = feature.labels
    elif hasattr(feature, "_labels") and feature._labels:
        labels_dict = feature._labels

    if not labels_dict or "document_labels" not in labels_dict:
        return []

    labels_data = json.loads(labels_dict["document_labels"])
    return [DocumentLabel.from_dict(label_dict) for label_dict in labels_data]


def remove_document_label(feature: Feature, chunk_id: str, document_id: str) -> bool:
    labels_dict = None
    if hasattr(feature, "labels") and feature.labels:
        labels_dict = feature.labels
    elif hasattr(feature, "_labels") and feature._labels:
        labels_dict = feature._labels

    if not labels_dict or "document_labels" not in labels_dict:
        return False

    existing_labels = json.loads(labels_dict["document_labels"])
    original_length = len(existing_labels)

    filtered_labels = [
        label
        for label in existing_labels
        if not (label["chunk_id"] == chunk_id and label["document_id"] == document_id)
    ]

    if len(filtered_labels) < original_length:
        labels_dict["document_labels"] = json.dumps(filtered_labels)
        return True

    return False
