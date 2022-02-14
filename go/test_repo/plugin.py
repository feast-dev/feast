from concurrent import futures
import sys
import time

import grpc

from grpc_health.v1.health import HealthServicer
from grpc_health.v1 import health_pb2, health_pb2_grpc

from connector_python import Connector_pb2_grpc
from connector_python import Connector_pb2
from connector_python import ServingService_pb2
from connector_python.ServingService_pb2 import FeatureList as FeatureListProto
from connector_python.EntityKey_pb2 import EntityKey as EntityKeyProto
from connector_python.Value_pb2 import Value as ValueProto
from google.protobuf.timestamp_pb2 import Timestamp

from typing import (
    Any,
    ByteString,
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

# sdk/python/feast/usage.py
import contextlib
import contextvars
import dataclasses
import os
import typing
import uuid
from datetime import datetime

sys.path.append("/Users/lycao/Documents/feast/go/test_repo/connector_python")

# sdk/python/feast/infra/online_stores/helpers.py
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

# sdk/python/feast/usage.py

@dataclasses.dataclass
class FnCall:
    fn_name: str
    id: str

    start: datetime
    end: typing.Optional[datetime] = None

    parent_id: typing.Optional[str] = None


class Sampler:
    def should_record(self, event) -> bool:
        raise NotImplementedError

    @property
    def priority(self):
        return 0


class AlwaysSampler(Sampler):
    def should_record(self, event) -> bool:
        return True


class UsageContext:
    attributes: typing.Dict[str, typing.Any]

    call_stack: typing.List[FnCall]
    completed_calls: typing.List[FnCall]

    exception: typing.Optional[Exception] = None
    traceback: typing.Optional[typing.Tuple[str, int, str]] = None

    sampler: Sampler = AlwaysSampler()

    def __init__(self):
        self.attributes = {}
        self.call_stack = []
        self.completed_calls = []


_context = contextvars.ContextVar("usage_context", default=UsageContext())

@contextlib.contextmanager
def tracing_span(name):
    """
    Context manager for wrapping heavy parts of code in tracing span
    """
    if _is_enabled:
        ctx = _context.get()
        if not ctx.call_stack:
            raise RuntimeError("tracing_span must be called in usage context")

        last_call = ctx.call_stack[-1]
        fn_call = FnCall(
            id=uuid.uuid4().hex,
            parent_id=last_call.id,
            fn_name=f"{last_call.fn_name}.{name}",
            start=datetime.utcnow(),
        )
    try:
        yield
    finally:
        if _is_enabled:
            fn_call.end = datetime.utcnow()
            ctx.completed_calls.append(fn_call)


# sdk/python/feast/infra/online_stores/redis.py
class ConnectorOnlineStore(Connector_pb2_grpc.OnlineStoreServicer):
    """Implementation of OnlineStore service."""
    def __init__(self, project, host: str, port: str):
        self._client = Redis(host=host, port=port)
        self.project = project
        self.host = host
        self.port = port

    def OnlineRead(self, request, context):
        response = {'results': [[]]}

        feature_view = request.View
        project = config.project
        requested_features = request.Features
        entity_keys = request.EntityKeys

        hset_keys = [_mmh3(f"{feature_view}:{k}") for k in requested_features]

        ts_key = f"_ts:{feature_view}"
        hset_keys.append(ts_key)
        requested_features.append(ts_key)

        keys = []
        for entity_key in entity_keys:
            redis_key_bin = _redis_key(self.project, entity_key)
            keys.append(redis_key_bin)
        with self.client.pipeline() as pipe:
            for redis_key_bin in keys:
                pipe.hmget(redis_key_bin, hset_keys)
            # TODO
            with tracing_span(name="remote_call"):
                redis_values = pipe.execute()
        for values in redis_values:
            feature_list = self._get_features_for_entity(
                values, feature_view, requested_features
            )
            response['results'].append(feature_list)
        return response

    def _get_features_for_entity(
        self,
        values: List[ByteString],
        feature_view: str,
        requested_features: List[str],
    ) -> Optional[List[FeatureListProto]]:

        res_val = dict(zip(requested_features, values))

        res_ts = Timestamp()
        ts_val = res_val.pop(f"_ts:{feature_view}")
        if ts_val:
            res_ts.ParseFromString(bytes(ts_val))
        timestamp = datetime.fromtimestamp(res_ts.seconds)
        feature_list = [None] * len(requested_features)

        index = 0
        for feature_name, val_bin in res_val.items():
            val = ValueProto()
            if val_bin:
                val.ParseFromString(bytes(val_bin))
            feature_ref = {'feature_view_name': feature_view, 'feature_name': feature_name} #ServingService_pb2.FeatureReferenceV2()
            feature_list[index] = {'timestamp': timestamp, 'reference': feature_ref, 'value': val}
            index += 1

        return feature_list


def serve():
    # We need to build a health service to work with go-plugin
    health = HealthServicer()
    health.set("plugin", health_pb2.HealthCheckResponse.ServingStatus.Value('SERVING'))

    # Start the server.
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    Connector_pb2_grpc.add_OnlineStoreServicer_to_server(ConnectorOnlineStore(project="test_repo", host=":", port="6379"), server)
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
