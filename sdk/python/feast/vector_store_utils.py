import hashlib
from typing import Any, Dict, List, Optional

from feast.errors import FeatureViewNotFoundException
from feast.feature_view import FeatureView


def feature_view_to_vs_id(project: str, feature_view_name: str) -> str:
    digest = hashlib.sha256(f"{project}:{feature_view_name}".encode()).hexdigest()[:24]
    return f"vs_{digest}"


def _has_vector_index(fv: FeatureView) -> bool:
    return any(getattr(f, "vector_index", False) for f in fv.schema)


def build_vector_store_object(project: str, fv: FeatureView) -> Dict[str, Any]:
    return {
        "id": feature_view_to_vs_id(project, fv.name),
        "object": "vector_store",
        "name": fv.name,
        "status": "completed",
        "created_at": int(fv.created_timestamp.timestamp())
        if fv.created_timestamp
        else 0,
    }


class VectorStoreRegistry:
    """Cached mapping from vs_{hash} IDs to FeatureViews.

    Built once at server startup, refreshed on registry TTL cycle.
    """

    def __init__(self, store: Any):
        self._store = store
        self._id_to_fv: Dict[str, FeatureView] = {}
        self._vector_store_objects: List[Dict[str, Any]] = []
        self.refresh()

    def refresh(self) -> None:
        project = self._store.project
        id_to_fv: Dict[str, FeatureView] = {}
        objects: List[Dict[str, Any]] = []
        for fv in self._store.list_feature_views():
            if _has_vector_index(fv):
                vs_id = feature_view_to_vs_id(project, fv.name)
                id_to_fv[vs_id] = fv
                objects.append(build_vector_store_object(project, fv))
        self._id_to_fv = id_to_fv
        self._vector_store_objects = objects

    def resolve(self, vs_id: str) -> FeatureView:
        fv = self._id_to_fv.get(vs_id)
        if fv is None:
            raise FeatureViewNotFoundException(vs_id)
        return fv

    def list_vector_stores(self) -> List[Dict[str, Any]]:
        return self._vector_store_objects

    def get_vector_store(self, vs_id: str) -> Optional[Dict[str, Any]]:
        fv = self._id_to_fv.get(vs_id)
        if fv is None:
            return None
        return build_vector_store_object(self._store.project, fv)
