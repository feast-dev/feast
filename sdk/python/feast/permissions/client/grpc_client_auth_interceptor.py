import logging

import grpc

from feast.errors import FeastError
from feast.permissions.auth.auth_type import AuthType
from feast.permissions.auth_model import AuthConfig
from feast.permissions.client.client_auth_token import get_auth_token

logger = logging.getLogger(__name__)


class GrpcClientAuthHeaderInterceptor(
    grpc.UnaryUnaryClientInterceptor,
    grpc.UnaryStreamClientInterceptor,
    grpc.StreamUnaryClientInterceptor,
    grpc.StreamStreamClientInterceptor,
):
    def __init__(self, auth_config: AuthConfig):
        self._auth_config = auth_config

    def intercept_unary_unary(
        self, continuation, client_call_details, request_iterator
    ):
        return self._handle_call(continuation, client_call_details, request_iterator)

    def intercept_unary_stream(
        self, continuation, client_call_details, request_iterator
    ):
        return self._handle_call(continuation, client_call_details, request_iterator)

    def intercept_stream_unary(
        self, continuation, client_call_details, request_iterator
    ):
        return self._handle_call(continuation, client_call_details, request_iterator)

    def intercept_stream_stream(
        self, continuation, client_call_details, request_iterator
    ):
        return self._handle_call(continuation, client_call_details, request_iterator)

    def _handle_call(self, continuation, client_call_details, request_iterator):
        if self._auth_config.type != AuthType.NONE.value:
            client_call_details = self._append_auth_header_metadata(client_call_details)
        result = continuation(client_call_details, request_iterator)
        if result.exception() is not None:
            mapped_error = FeastError.from_error_detail(result.exception().details())
            if mapped_error is not None:
                raise mapped_error
        return result

    def _append_auth_header_metadata(self, client_call_details):
        logger.debug(
            "Intercepted the grpc api method call to inject Authorization header "
        )
        metadata = client_call_details.metadata or []
        access_token = get_auth_token(self._auth_config)
        metadata.append((b"authorization", b"Bearer " + access_token.encode("utf-8")))
        client_call_details = client_call_details._replace(metadata=metadata)
        return client_call_details
