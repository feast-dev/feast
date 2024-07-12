import pyarrow.flight as fl

from feast.permissions.auth.auth_type import AuthType
from feast.permissions.auth_model import AuthConfig
from feast.permissions.client.auth_client_manager import get_auth_client_manager


def create_auth_header(
    auth_config: AuthConfig,
) -> tuple[tuple[str, str]]:
    auth_client_manager = get_auth_client_manager(auth_config)
    token = auth_client_manager.get_token()

    return (("authorization", "Bearer " + token),)


def create_flight_call_options(auth_config: AuthConfig) -> fl.FlightCallOptions:
    if auth_config.type != AuthType.NONE.value:
        headers = create_auth_header(auth_config)
        return fl.FlightCallOptions(headers=headers)
    return fl.FlightCallOptions()
