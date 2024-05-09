import ast
import traceback
from typing import Dict

import pyarrow as pa
import pyarrow.flight as flight

from feast import FeatureStore


class OfflineServer(flight.FlightServerBase):
    def __init__(self, store: FeatureStore, location: str, **kwargs):
        super(OfflineServer, self).__init__(location, **kwargs)
        self._location = location
        self.flights: Dict[str, Dict[str, str]] = {}
        self.store = store

    @classmethod
    def descriptor_to_key(self, descriptor):
        return (
            descriptor.descriptor_type.value,
            descriptor.command,
            tuple(descriptor.path or tuple()),
        )

    # TODO: since we cannot anticipate here the call to get_historical_features call, what data should we return?
    # ATM it returns the metadata of the "entity_df" table
    def _make_flight_info(self, key, descriptor, params):
        table = params["entity_df"]
        endpoints = [flight.FlightEndpoint(repr(key), [self._location])]
        mock_sink = pa.MockOutputStream()
        stream_writer = pa.RecordBatchStreamWriter(mock_sink, table.schema)
        stream_writer.write_table(table)
        stream_writer.close()
        data_size = mock_sink.size()

        return flight.FlightInfo(
            table.schema, descriptor, endpoints, table.num_rows, data_size
        )

    def get_flight_info(self, context, descriptor):
        key = OfflineServer.descriptor_to_key(descriptor)
        if key in self.flights:
            params = self.flights[key]
            return self._make_flight_info(key, descriptor, params)
        raise KeyError("Flight not found.")

    def list_flights(self, context, criteria):
        for key, table in self.flights.items():
            if key[1] is not None:
                descriptor = flight.FlightDescriptor.for_command(key[1])
            else:
                descriptor = flight.FlightDescriptor.for_path(*key[2])

            yield self._make_flight_info(key, descriptor, table)

    # Expects to receive request parameters and stores them in the flights dictionary
    # Indexed by the unique command
    def do_put(self, context, descriptor, reader, writer):
        key = OfflineServer.descriptor_to_key(descriptor)

        if key in self.flights:
            params = self.flights[key]
        else:
            params = {}
        decoded_metadata = {
            key.decode(): value.decode()
            for key, value in reader.schema.metadata.items()
        }
        if "command" in decoded_metadata:
            command = decoded_metadata["command"]
            api = decoded_metadata["api"]
            param = decoded_metadata["param"]
            value = reader.read_all()
            # Merge the existing dictionary for the same key, as we have multiple calls to do_put for the same key
            params.update({"command": command, "api": api, param: value})

        self.flights[key] = params

    # Extracts the API parameters from the flights dictionary, delegates the execution to the FeatureStore instance
    # and returns the stream of data
    def do_get(self, context, ticket):
        key = ast.literal_eval(ticket.ticket.decode())
        if key not in self.flights:
            print(f"Unknown key {key}")
            return None

        api = self.flights[key]["api"]
        # print(f"get key is {key}")
        # print(f"requested api is {api}")
        if api == "get_historical_features":
            # Extract parameters from the internal flight descriptor
            entity_df_value = self.flights[key]["entity_df"]
            entity_df = pa.Table.to_pandas(entity_df_value)
            # print(f"entity_df is {entity_df}")

            features_value = self.flights[key]["features"]
            features = pa.RecordBatch.to_pylist(features_value)
            features = [item["features"] for item in features]
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

            # Get service is consumed, so we clear the corresponding flight
            del self.flights[key]

            return flight.RecordBatchStream(table)
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
