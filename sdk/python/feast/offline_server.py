import ast
import json
import logging
import traceback
from typing import Any, Dict, List

import pyarrow as pa
import pyarrow.flight as fl

from feast import FeatureStore, FeatureView
from feast.feature_view import DUMMY_ENTITY_NAME

logger = logging.getLogger(__name__)


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
            logger.debug(f"do_put: command is{command}, data is {data}")
            self.flights[key] = data
        else:
            logger.warning(f"No 'api' field in command: {command}")

    def get_feature_view_by_name(
        self, fv_name: str, name_alias: str, project: str
    ) -> FeatureView:
        """
        Retrieves a feature view by name, including all subclasses of FeatureView.

        Args:
            fv_name: Name of feature view
            name_alias: Alias to be applied to the projection of the registered view
            project: Feast project that this feature view belongs to

        Returns:
            Returns either the specified feature view, or raises an exception if
            none is found
        """
        try:
            fv = self.store.registry.get_feature_view(name=fv_name, project=project)
            if name_alias is not None:
                for fs in self.store.registry.list_feature_services(project=project):
                    for p in fs.feature_view_projections:
                        if p.name_alias == name_alias:
                            logger.debug(
                                f"Found matching FeatureService {fs.name} with projection {p}"
                            )
                            fv = fv.with_projection(p)
            return fv
        except Exception:
            try:
                return self.store.registry.get_stream_feature_view(
                    name=fv_name, project=project
                )
            except Exception as e:
                logger.error(
                    f"Cannot find any FeatureView by name {fv_name} in project {project}"
                )
                raise e

    def list_feature_views_by_name(
        self, feature_view_names: List[str], name_aliases: List[str], project: str
    ) -> List[FeatureView]:
        return [
            remove_dummies(
                self.get_feature_view_by_name(
                    fv_name=fv_name, name_alias=name_aliases[index], project=project
                )
            )
            for index, fv_name in enumerate(feature_view_names)
        ]

    # Extracts the API parameters from the flights dictionary, delegates the execution to the FeatureStore instance
    # and returns the stream of data
    def do_get(self, context, ticket):
        key = ast.literal_eval(ticket.ticket.decode())
        if key not in self.flights:
            logger.error(f"Unknown key {key}")
            return None

        command = json.loads(key[1])
        api = command["api"]
        logger.debug(f"get command is {command}")
        logger.debug(f"requested api is {api}")
        if api == "get_historical_features":
            # Extract parameters from the internal flights dictionary
            entity_df_value = self.flights[key]
            entity_df = pa.Table.to_pandas(entity_df_value)
            logger.debug(f"do_get: entity_df is {entity_df}")

            feature_view_names = command["feature_view_names"]
            logger.debug(f"do_get: feature_view_names is {feature_view_names}")
            name_aliases = command["name_aliases"]
            logger.debug(f"do_get: name_aliases is {name_aliases}")
            feature_refs = command["feature_refs"]
            logger.debug(f"do_get: feature_refs is {feature_refs}")
            project = command["project"]
            logger.debug(f"do_get: project is {project}")
            full_feature_names = command["full_feature_names"]
            feature_views = self.list_feature_views_by_name(
                feature_view_names=feature_view_names,
                name_aliases=name_aliases,
                project=project,
            )
            logger.debug(f"do_get: feature_views is {feature_views}")

            logger.info(
                f"get_historical_features for: entity_df from {entity_df.index[0]} to {entity_df.index[len(entity_df)-1]}, "
                f"feature_views is {[(fv.name, fv.entities) for fv in feature_views]}"
                f"feature_refs is {feature_refs}"
            )

            try:
                training_df = (
                    self.store._get_provider()
                    .get_historical_features(
                        config=self.store.config,
                        feature_views=feature_views,
                        feature_refs=feature_refs,
                        entity_df=entity_df,
                        registry=self.store._registry,
                        project=project,
                        full_feature_names=full_feature_names,
                    )
                    .to_df()
                )
                logger.debug(f"Len of training_df is {len(training_df)}")
                table = pa.Table.from_pandas(training_df)
            except Exception as e:
                logger.exception(e)
                traceback.print_exc()
                raise e

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


def remove_dummies(fv: FeatureView) -> FeatureView:
    """
    Removes dummmy IDs from FeatureView instances created with FeatureView.from_proto
    """
    if DUMMY_ENTITY_NAME in fv.entities:
        fv.entities = []
        fv.entity_columns = []
    return fv


def start_server(
    store: FeatureStore,
    host: str,
    port: int,
):
    location = "grpc+tcp://{}:{}".format(host, port)
    server = OfflineServer(store, location)
    logger.info(f"Offline store server serving on {location}")
    server.serve()
