from concurrent import futures
import concurrent.futures
import sys
import time
from datetime import datetime

import grpc
import os

from grpc_health.v1.health import HealthServicer
from grpc_health.v1 import health_pb2, health_pb2_grpc

from connector_python import Connector_pb2_grpc
from connector_python import Connector_pb2
from connector_python import ServingService_pb2
from connector_python.ServingService_pb2 import FeatureReferenceV2 as FeatureReferenceV2Proto
from connector_python.Connector_pb2 import ConnectorFeature as ConnectorFeatureProto
from connector_python.Connector_pb2 import ConnectorFeatureList as ConnectorFeatureListProto
from connector_python.ServingService_pb2 import FeatureList as FeatureListProto
from connector_python.EntityKey_pb2 import EntityKey as EntityKeyProto
from connector_python.Value_pb2 import Value as ValueProto
from connector_python.Value_pb2 import ValueType
from google.protobuf.timestamp_pb2 import Timestamp

import typing
from typing import (
    Any,
    ByteString,
    Callable,
    Dict,
    List,
    Optional,
    Sequence,
    Tuple,
    Union,
)

# sdk/python/feast/infra/online_stores/helpers.py
from redis import Redis
import mmh3
import struct

def _redis_key(project: str, entity_key: EntityKeyProto) -> bytes:
    key: List[bytes] = [serialize_entity_key(entity_key), project.encode("utf-8")]
    return b"".join(key)

def _mmh3(key: str):
    """
    Calculate murmur3_32 hash which is equal to scala version which is using little endian:
        https://stackoverflow.com/questions/29932956/murmur3-hash-different-result-between-python-and-java-implementation
        https://stackoverflow.com/questions/13141787/convert-decimal-int-to-little-endian-string-x-x
    """
    key_hash = mmh3.hash(key, signed=False)
    return bytes.fromhex(struct.pack("<Q", key_hash).hex()[:8])

def serialize_entity_key(entity_key: EntityKeyProto) -> bytes:
    """
    Serialize entity key to a bytestring so it can be used as a lookup key in a hash table.

    We need this encoding to be stable; therefore we cannot just use protobuf serialization
    here since it does not guarantee that two proto messages containing the same data will
    serialize to the same byte string[1].

    [1] https://developers.google.com/protocol-buffers/docs/encoding
    """
    sorted_keys, sorted_values = zip(
        *sorted(zip(entity_key.join_keys, entity_key.entity_values))
    )

    output: List[bytes] = []
    for k in sorted_keys:
        output.append(struct.pack("<I", ValueType.STRING))
        output.append(k.encode("utf8"))
    for v in sorted_values:
        val_bytes, value_type = _serialize_val(v.WhichOneof("val"), v)

        output.append(struct.pack("<I", value_type))

        output.append(struct.pack("<I", len(val_bytes)))
        output.append(val_bytes)

    return b"".join(output)

def _serialize_val(value_type, v: ValueProto) -> Tuple[bytes, int]:
    if value_type == "string_val":
        return v.string_val.encode("utf8"), ValueType.STRING
    elif value_type == "bytes_val":
        return v.bytes_val, ValueType.BYTES
    elif value_type == "int32_val":
        return struct.pack("<i", v.int32_val), ValueType.INT32
    elif value_type == "int64_val":
        return struct.pack("<l", v.int64_val), ValueType.INT64
    else:
        raise ValueError(f"Value type not supported for Firestore: {v}")

# sdk/python/feast/infra/online_stores/redis.py
class ConnectorOnlineStore(Connector_pb2_grpc.OnlineStoreServicer):
    """Implementation of OnlineStore service."""
    def __init__(self, project, host: str, port: str):
        self._client = Redis(host=host, port=port)
        self.project = project
        self.host = host
        self.port = port

    def OnlineRead(self, request, context):
        response = Connector_pb2.OnlineReadResponse()

        feature_view = request.view
        project = self.project
        requested_features = request.features
        entity_keys = request.entityKeys

        hset_keys = [_mmh3(f"{feature_view}:{k}") for k in requested_features]

        ts_key = f"_ts:{feature_view}"
        hset_keys.append(ts_key)
        requested_features.append(ts_key)
        results : List[ConnectorFeatureListProto] = [None] * len(entity_keys)
        keys = [None] * len(entity_keys)
        for index, entity_key in enumerate(entity_keys):
            redis_key_bin = _redis_key(self.project, entity_key)
            keys[index] = redis_key_bin
            values = self._client.hmget(redis_key_bin, hset_keys)
            feature_list = self._get_features_for_entity(
                values, feature_view, requested_features
            )
            results[index] = feature_list
        return Connector_pb2.OnlineReadResponse(results=results)

    def _get_features_for_entity(
        self,
        values: List[ByteString],
        feature_view: str,
        requested_features: List[str],
    ) -> ConnectorFeatureListProto:

        res_val = dict(zip(requested_features, values))
        res_ts = Timestamp()
        ts_val = res_val.pop(f"_ts:{feature_view}")
        if ts_val:
            res_ts.ParseFromString(bytes(ts_val))
        feature_list = [None] * len(res_val)

        index = 0
        for feature_name, val_bin in res_val.items():
            val = ValueProto()
            if val_bin:
                val.ParseFromString(bytes(val_bin))
            feature_ref = FeatureReferenceV2Proto(feature_view_name=feature_view, feature_name=feature_name)
            feature_list[index] = ConnectorFeatureProto(timestamp=res_ts, reference=feature_ref, value=val)
            index += 1

        return ConnectorFeatureListProto(featureList= feature_list)


def serve():
    # We need to build a health service to work with go-plugin
    health = HealthServicer()
    health.set("plugin", health_pb2.HealthCheckResponse.ServingStatus.Value('SERVING'))

    # Start the server.
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    Connector_pb2_grpc.add_OnlineStoreServicer_to_server(ConnectorOnlineStore(project="test_repo", host="localhost", port="6379"), server)
    health_pb2_grpc.add_HealthServicer_to_server(health, server)
    server.add_insecure_port('127.0.0.1:1234')
    server.start()

    # Output information
    print("1|1|tcp|127.0.0.1:1234|grpc")
    sys.stdout.flush()

    try:
        while True:
            time.sleep(60 * 60 * 24)
    except KeyboardInterrupt:
        server.stop(0)

if __name__ == '__main__':
    serve()
