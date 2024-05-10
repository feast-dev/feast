import ast
import json
import traceback
from typing import Any, Dict

import pyarrow as pa
import pyarrow.flight as fl

from feast import FeatureStore


class OfflineServer(fl.FlightServerBase):
    def __init__(self, store: FeatureStore, location: str, **kwargs):
        super(OfflineServer, self).__init__(location, **kwargs)
        self._location = location
        # A dictionary of configured flights, e.g. API calls received and not yet served
        self.flights: Dict[str, Any] = {}
        self.store = store

    @classmethod
    def descriptor_to_key(self, descriptor):
        return (
            descriptor.descriptor_type.value,
            descriptor.command,
            tuple(descriptor.path or tuple()),
        )

    def _make_flight_info(self, key, descriptor, params):
        endpoints = [fl.FlightEndpoint(repr(key), [self._location])]
        # TODO calculate actual schema from the given features
        schema = pa.schema([])

        return fl.FlightInfo(schema, descriptor, endpoints, -1, -1)

    def get_flight_info(self, context, descriptor):
        key = OfflineServer.descriptor_to_key(descriptor)
        if key in self.flights:
            params = self.flights[key]
            return self._make_flight_info(key, descriptor, params)
        raise KeyError("Flight not found.")

    def list_flights(self, context, criteria):
        for key, table in self.flights.items():
            if key[1] is not None:
                descriptor = fl.FlightDescriptor.for_command(key[1])
            else:
                descriptor = fl.FlightDescriptor.for_path(*key[2])

            yield self._make_flight_info(key, descriptor, table)

    # Expects to receive request parameters and stores them in the flights dictionary
    # Indexed by the unique command
    def do_put(self, context, descriptor, reader, writer):
        key = OfflineServer.descriptor_to_key(descriptor)

        command = json.loads(key[1])
        if "api" in command:
            data = reader.read_all()
            self.flights[key] = data
        else:
            print(f"No 'api' field in command: {command}")

    # Extracts the API parameters from the flights dictionary, delegates the execution to the FeatureStore instance
    # and returns the stream of data
    def do_get(self, context, ticket):
        key = ast.literal_eval(ticket.ticket.decode())
        if key not in self.flights:
            print(f"Unknown key {key}")
            return None

        command = json.loads(key[1])
        api = command["api"]
        # print(f"get command is {command}")
        # print(f"requested api is {api}")
        if api == "get_historical_features":
            # Extract parameters from the internal flights dictionary
            entity_df_value = self.flights[key]
            entity_df = pa.Table.to_pandas(entity_df_value)
            # print(f"entity_df is {entity_df}")

            features = command["features"]
            # print(f"features is {features}")

            print(
                f"get_historical_features for: entity_df from {entity_df.index[0]} to {entity_df.index[len(entity_df)-1]}, "
                f"features from {features[0]} to {features[len(features)-1]}"
            )

            # TODO define error handling
            try:
                training_df = self.store.get_historical_features(
                    entity_df, features
                ).to_df()
            except Exception:
                traceback.print_exc()
            table = pa.Table.from_pandas(training_df)

            # Get service is consumed, so we clear the corresponding flight and data
            del self.flights[key]

            return fl.RecordBatchStream(table)
        else:
            raise NotImplementedError

    def list_actions(self, context):
        return []

    def do_action(self, context, action):
        raise NotImplementedError

    def do_drop_dataset(self, dataset):
        pass


def start_server(
    store: FeatureStore,
    host: str,
    port: int,
):
    location = "grpc+tcp://{}:{}".format(host, port)
    server = OfflineServer(store, location)
    print("Serving on", location)
    server.serve()
