import pyarrow.flight as fl

from feast.permissions.auth.auth_type import AuthType
from feast.permissions.auth_model import AuthConfig
from feast.permissions.client.auth_client_manager import get_auth_client_manager


def create_auth_header(
    auth_config: AuthConfig,
) -> list[tuple[bytes, bytes]]:
    auth_client_manager = get_auth_client_manager(auth_config)
    token = auth_client_manager.get_token()

    return [(b"authorization", b"Bearer " + token.encode("utf-8"))]


def create_flight_call_options(auth_config: AuthConfig) -> fl.FlightCallOptions:
    if auth_config.type != AuthType.NONE:
        auth_client_manager = get_auth_client_manager(auth_config)
        token = auth_client_manager.get_token()
        headers = [(b"authorization", b"Bearer " + token.encode('utf-8'))]
        return fl.FlightCallOptions(headers=headers)
    return fl.FlightCallOptions()
