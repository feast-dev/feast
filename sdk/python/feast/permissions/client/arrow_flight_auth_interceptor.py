import pyarrow.flight as fl

from feast.permissions.auth_model import AuthConfig
from feast.permissions.client.client_auth_token import get_auth_token


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
