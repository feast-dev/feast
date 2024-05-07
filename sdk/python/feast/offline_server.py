import ast

import pyarrow as pa
import pyarrow.flight

from feast import FeatureStore


class OfflineServer(pa.flight.FlightServerBase):
    def __init__(self, location=None):
        super(OfflineServer, self).__init__(location)
        self._location = location
        self.flights = {}
        self.store = FeatureStore

    @classmethod
    def descriptor_to_key(self, descriptor):
        return (
            descriptor.descriptor_type.value,
            descriptor.command,
            tuple(descriptor.path or tuple()),
        )

    def _make_flight_info(self, key, descriptor, table):
        endpoints = [pyarrow.flight.FlightEndpoint(repr(key), [self._location])]
        mock_sink = pyarrow.MockOutputStream()
        stream_writer = pyarrow.RecordBatchStreamWriter(mock_sink, table.schema)
        stream_writer.write_table(table)
        stream_writer.close()
        data_size = mock_sink.size()

        return pyarrow.flight.FlightInfo(
            table.schema, descriptor, endpoints, table.num_rows, data_size
        )

    def get_flight_info(self, context, descriptor):
        key = OfflineServer.descriptor_to_key(descriptor)
        if key in self.flights:
            table = self.flights[key]
            return self._make_flight_info(key, descriptor, table)
        raise KeyError("Flight not found.")

    def list_flights(self, context, criteria):
        for key, table in self.flights.items():
            if key[1] is not None:
                descriptor = pyarrow.flight.FlightDescriptor.for_command(key[1])
            else:
                descriptor = pyarrow.flight.FlightDescriptor.for_path(*key[2])

            yield self._make_flight_info(key, descriptor, table)

    def do_put(self, context, descriptor, reader, writer):
        key = OfflineServer.descriptor_to_key(descriptor)
        self.flights[key] = reader.read_all()

    def do_get(self, context, ticket):
        key = ast.literal_eval(ticket.ticket.decode())
        if key not in self.flights:
            return None

        entity_df_key = self.flights[key]
        entity_df = pa.Table.to_pandas(entity_df_key)
        # Get feature data
        features_key = (2, b"features_descriptor", ())
        if features_key in self.flights:
            features_data = self.flights[features_key]
            features = pa.RecordBatch.to_pylist(features_data)
            features = [item["features"] for item in features]
        else:
            features = None

        training_df = self.store.get_historical_features(entity_df, features).to_df()
        table = pa.Table.from_pandas(training_df)

        return pa.flight.RecordBatchStream(table)


def start_server(
    store: FeatureStore,
    host: str,
    port: int,
):
    location = "grpc+tcp://{}:{}".format(host, port)
    server = OfflineServer(location)
    print("Serving on", location)
    server.serve()
