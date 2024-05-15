import assertpy
import pyarrow.flight as flight
import pytest

from feast import FeatureStore
from feast.offline_server import OfflineServer


@pytest.fixture
def offline_server(environment):
    store: FeatureStore = environment.feature_store

    location = "grpc+tcp://localhost:0"
    return OfflineServer(store=store, location=location)


@pytest.fixture
def arrow_client(offline_server):
    return flight.FlightClient(f"grpc://localhost:{offline_server.port}")


def test_offline_server_is_alive(environment, offline_server, arrow_client):
    server = offline_server
    client = arrow_client

    assertpy.assert_that(server).is_not_none
    assertpy.assert_that(server.port).is_not_equal_to(0)

    actions = list(client.list_actions())
    flights = list(client.list_flights())

    assertpy.assert_that(actions).is_empty()
    assertpy.assert_that(flights).is_empty()
