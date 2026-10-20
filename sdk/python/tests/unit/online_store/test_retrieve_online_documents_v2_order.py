from datetime import datetime, timedelta
from unittest.mock import MagicMock

from feast import Entity, FeatureStore, FeatureView, Field, FileSource
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from feast.types import Int64, String


def _document(item_id: int, title: str, distance: float):
    return (
        datetime(2026, 1, 1),
        EntityKeyProto(
            join_keys=["item_id"], entity_values=[ValueProto(int64_val=item_id)]
        ),
        {
            "title": ValueProto(string_val=title),
            "distance": ValueProto(float_val=distance),
        },
    )


def test_features_stay_with_their_entity_in_ranked_order():
    """Documents ranked out of entity-key order must not be re-paired.

    The store ranks item 3 first. Grouping the response by unique entity sorts
    the entities to (1, 3) while the entity column stays (3, 1), which used to
    hand item 3 the features of item 1.
    """
    feature_view = FeatureView(
        name="items",
        entities=[Entity(name="item", join_keys=["item_id"])],
        schema=[Field(name="title", dtype=String), Field(name="item_id", dtype=Int64)],
        source=FileSource(path="unused.parquet", timestamp_field="event_timestamp"),
        ttl=timedelta(days=1),
    )
    provider = MagicMock()
    provider.retrieve_online_documents_v2.return_value = [
        _document(3, "three", 0.1),
        _document(1, "one", 0.5),
        _document(3, "three again", 0.9),
    ]
    store = FeatureStore.__new__(FeatureStore)
    store.config = MagicMock()

    response = store._retrieve_from_online_store_v2(
        provider=provider,
        table=feature_view,
        requested_features=["title"],
        query=[0.0],
        top_k=3,
        distance_metric="L2",
        query_string=None,
    ).to_dict()

    assert response["item_id"] == [3, 1, 3]
    assert response["title"] == ["three", "one", "three again"]
    assert response["distance"] == [
        ValueProto(float_val=d).float_val for d in (0.1, 0.5, 0.9)
    ]
