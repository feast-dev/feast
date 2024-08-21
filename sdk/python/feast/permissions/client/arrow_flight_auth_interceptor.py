import pyarrow.flight as fl

from feast.permissions.auth.auth_type import AuthType
from feast.permissions.auth_model import AuthConfig
from feast.permissions.client.auth_client_manager_factory import get_auth_token


class FlightBearerTokenInterceptor(fl.ClientMiddleware):
    def __init__(self, auth_config: AuthConfig):
        super().__init__()
        self.auth_config = auth_config

    def call_completed(self, exception):
        pass

    def received_headers(self, headers):
        pass

    def sending_headers(self):
        access_token = get_auth_token(self.auth_config)
        return {b"authorization": b"Bearer " + access_token.encode("utf-8")}


class FlightAuthInterceptorFactory(fl.ClientMiddlewareFactory):
    def __init__(self, auth_config: AuthConfig):
        super().__init__()
        self.auth_config = auth_config

    def start_call(self, info):
        return FlightBearerTokenInterceptor(self.auth_config)


def build_arrow_flight_client(host: str, port, auth_config: AuthConfig):
    if auth_config.type != AuthType.NONE.value:
        middleware_factory = FlightAuthInterceptorFactory(auth_config)
        return fl.FlightClient(f"grpc://{host}:{port}", middleware=[middleware_factory])
    else:
        return fl.FlightClient(f"grpc://{host}:{port}")
