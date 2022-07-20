import pytest

from feast.infra.key_encoding_utils import serialize_entity_key
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import Value as ValueProto


def test_serialize_entity_key():
    # Should be fine
    serialize_entity_key(
        EntityKeyProto(
            join_keys=["user"], entity_values=[ValueProto(int64_val=int(2**15))]
        ),
        entity_key_serialization_version=2,
    )
    # True int64, but should also be fine.
    serialize_entity_key(
        EntityKeyProto(
            join_keys=["user"], entity_values=[ValueProto(int64_val=int(2**31))]
        ),
        entity_key_serialization_version=2,
    )

    # Old serialization scheme, should fail.
    with pytest.raises(BaseException):
        serialize_entity_key(
            EntityKeyProto(
                join_keys=["user"], entity_values=[ValueProto(int64_val=int(2**31))]
            ),
        )
