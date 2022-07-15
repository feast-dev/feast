import pytest

from feast.infra.key_encoding_utils import serialize_entity_key
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import Value as ValueProto


@pytest.mark.parametrize(
    "entity_key,expected_contains",
    [
        (
                EntityKeyProto(
                    join_keys=["customer"],
                    entity_values=[ValueProto(int64_val=int(2 ** 31))],
                ),
                b"customer",
        ),
        (
                EntityKeyProto(
                    join_keys=["user"], entity_values=[ValueProto(int32_val=int(2 ** 15))]
                ),
                b"user",
        ),
    ],
)
def test_serialize_entity_key(entity_key, expected_contains):
    output = serialize_entity_key(entity_key)
    assert output.find(expected_contains) >= 0
