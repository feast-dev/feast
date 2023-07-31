from pymilvus.client.stub import Milvus
from pymilvus.client.types import DataType

from tests.expediagroup.milvus_online_store_creator import MilvusOnlineStoreCreator


def test_milvus_start_stop():
    # this is just an example how to start / stop Milvus. Once a real test is implemented this test can be deleted
    online_store_creator = MilvusOnlineStoreCreator("milvus")
    online_store_creator.create_online_store()

    # access through a stub
    milvus = Milvus(online_store_creator.host, 19530)

    # Create collection demo_collection if it doesn't exist.
    collection_name = "example_collection_"

    ok = milvus.has_collection(collection_name)
    if not ok:
        fields = {
            "fields": [
                {"name": "key", "type": DataType.INT64, "is_primary": True},
                {
                    "name": "vector",
                    "type": DataType.FLOAT_VECTOR,
                    "params": {"dim": 32},
                },
            ],
            "auto_id": True,
        }

        milvus.create_collection(collection_name, fields)

    collection = milvus.describe_collection(collection_name)
    assert collection.get("collection_name") == collection_name

    online_store_creator.teardown()
