import ast
import json
import logging
import traceback
from datetime import datetime
from typing import Any, Dict, List

import pyarrow as pa
import pyarrow.flight as fl

from feast import FeatureStore, FeatureView, utils
from feast.feature_logging import FeatureServiceLoggingSource
from feast.feature_view import DUMMY_ENTITY_NAME
from feast.infra.offline_stores.offline_utils import get_offline_store_from_config
from feast.saved_dataset import SavedDatasetStorage

logger = logging.getLogger(__name__)


class OfflineServer(fl.FlightServerBase):
    def __init__(self, store: FeatureStore, location: str, **kwargs):
        super(OfflineServer, self).__init__(location, **kwargs)
        self._location = location
        # A dictionary of configured flights, e.g. API calls received and not yet served
        self.flights: Dict[str, Any] = {}
        self.store = store
        self.offline_store = get_offline_store_from_config(store.config.offline_store)

    @classmethod
    def descriptor_to_key(self, descriptor: fl.FlightDescriptor):
        return (
            descriptor.descriptor_type.value,
            descriptor.command,
            tuple(descriptor.path or tuple()),
        )

    def _make_flight_info(self, key: Any, descriptor: fl.FlightDescriptor):
        endpoints = [fl.FlightEndpoint(repr(key), [self._location])]
        # TODO calculate actual schema from the given features
        schema = pa.schema([])

        return fl.FlightInfo(schema, descriptor, endpoints, -1, -1)

    def get_flight_info(
        self, context: fl.ServerCallContext, descriptor: fl.FlightDescriptor
    ):
        key = OfflineServer.descriptor_to_key(descriptor)
        if key in self.flights:
            return self._make_flight_info(key, descriptor)
        raise KeyError("Flight not found.")

    def list_flights(self, context: fl.ServerCallContext, criteria: bytes):
        for key, table in self.flights.items():
            if key[1] is not None:
                descriptor = fl.FlightDescriptor.for_command(key[1])
            else:
                descriptor = fl.FlightDescriptor.for_path(*key[2])

            yield self._make_flight_info(key, descriptor)

    # Expects to receive request parameters and stores them in the flights dictionary
    # Indexed by the unique command
    def do_put(
        self,
        context: fl.ServerCallContext,
        descriptor: fl.FlightDescriptor,
        reader: fl.MetadataRecordBatchReader,
        writer: fl.FlightMetadataWriter,
    ):
        key = OfflineServer.descriptor_to_key(descriptor)
        command = json.loads(key[1])
        if "api" in command:
            data = reader.read_all()
            logger.debug(f"do_put: command is{command}, data is {data}")
            self.flights[key] = data

            self._call_api(command["api"], command, key)
        else:
            logger.warning(f"No 'api' field in command: {command}")

    def _call_api(self, api: str, command: dict, key: str):
        assert api is not None, "api can not be empty"

        remove_data = False
        try:
            if api == OfflineServer.offline_write_batch.__name__:
                self.offline_write_batch(command, key)
                remove_data = True
            elif api == OfflineServer.write_logged_features.__name__:
                self.write_logged_features(command, key)
                remove_data = True
            elif api == OfflineServer.persist.__name__:
                self.persist(command, key)
                remove_data = True
        except Exception as e:
            remove_data = True
            logger.exception(e)
            traceback.print_exc()
            raise e
        finally:
            if remove_data:
                # Get service is consumed, so we clear the corresponding flight and data
                del self.flights[key]

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

    def _validate_do_get_parameters(self, command: dict):
        assert "api" in command, "api parameter is mandatory"

    # Extracts the API parameters from the flights dictionary, delegates the execution to the FeatureStore instance
    # and returns the stream of data
    def do_get(self, context: fl.ServerCallContext, ticket: fl.Ticket):
        key = ast.literal_eval(ticket.ticket.decode())
        if key not in self.flights:
            logger.error(f"Unknown key {key}")
            return None

        command = json.loads(key[1])

        self._validate_do_get_parameters(command)

        api = command["api"]
        logger.debug(f"get command is {command}")
        logger.debug(f"requested api is {api}")
        try:
            if api == OfflineServer.get_historical_features.__name__:
                table = self.get_historical_features(command, key).to_arrow()
            elif api == OfflineServer.pull_all_from_table_or_query.__name__:
                table = self.pull_all_from_table_or_query(command).to_arrow()
            elif api == OfflineServer.pull_latest_from_table_or_query.__name__:
                table = self.pull_latest_from_table_or_query(command).to_arrow()
            else:
                raise NotImplementedError
        except Exception as e:
            logger.exception(e)
            traceback.print_exc()
            raise e

        # Get service is consumed, so we clear the corresponding flight and data
        del self.flights[key]
        return fl.RecordBatchStream(table)

    def _validate_offline_write_batch_parameters(self, command: dict):
        assert (
            "feature_view_names" in command
        ), "feature_view_names is a mandatory parameter"
        assert "name_aliases" in command, "name_aliases is a mandatory parameter"

        feature_view_names = command["feature_view_names"]
        assert (
            len(feature_view_names) == 1
        ), "feature_view_names list should only have one item"

        name_aliases = command["name_aliases"]
        assert len(name_aliases) == 1, "name_aliases list should only have one item"

    def offline_write_batch(self, command: dict, key: str):
        self._validate_offline_write_batch_parameters(command)

        feature_view_names = command["feature_view_names"]
        name_aliases = command["name_aliases"]

        project = self.store.config.project
        feature_views = self.list_feature_views_by_name(
            feature_view_names=feature_view_names,
            name_aliases=name_aliases,
            project=project,
        )

        assert len(feature_views) == 1, "incorrect feature view"
        table = self.flights[key]
        self.offline_store.offline_write_batch(
            self.store.config, feature_views[0], table, command["progress"]
        )

    def _validate_write_logged_features_parameters(self, command: dict):
        assert "feature_service_name" in command

    def write_logged_features(self, command: dict, key: str):
        self._validate_write_logged_features_parameters(command)
        table = self.flights[key]
        feature_service = self.store.get_feature_service(
            command["feature_service_name"]
        )

        assert (
            feature_service.logging_config is not None
        ), "feature service must have logging_config set"

        self.offline_store.write_logged_features(
            config=self.store.config,
            data=table,
            source=FeatureServiceLoggingSource(
                feature_service, self.store.config.project
            ),
            logging_config=feature_service.logging_config,
            registry=self.store.registry,
        )

    def _validate_pull_all_from_table_or_query_parameters(self, command: dict):
        assert (
            "data_source_name" in command
        ), "data_source_name is a mandatory parameter"
        assert (
            "join_key_columns" in command
        ), "join_key_columns is a mandatory parameter"
        assert (
            "feature_name_columns" in command
        ), "feature_name_columns is a mandatory parameter"
        assert "timestamp_field" in command, "timestamp_field is a mandatory parameter"
        assert "start_date" in command, "start_date is a mandatory parameter"
        assert "end_date" in command, "end_date is a mandatory parameter"

    def pull_all_from_table_or_query(self, command: dict):
        self._validate_pull_all_from_table_or_query_parameters(command)

        return self.offline_store.pull_all_from_table_or_query(
            self.store.config,
            self.store.get_data_source(command["data_source_name"]),
            command["join_key_columns"],
            command["feature_name_columns"],
            command["timestamp_field"],
            utils.make_tzaware(datetime.fromisoformat(command["start_date"])),
            utils.make_tzaware(datetime.fromisoformat(command["end_date"])),
        )

    def _validate_pull_latest_from_table_or_query_parameters(self, command: dict):
        assert (
            "data_source_name" in command
        ), "data_source_name is a mandatory parameter"
        assert (
            "join_key_columns" in command
        ), "join_key_columns is a mandatory parameter"
        assert (
            "feature_name_columns" in command
        ), "feature_name_columns is a mandatory parameter"
        assert "timestamp_field" in command, "timestamp_field is a mandatory parameter"
        assert "start_date" in command, "start_date is a mandatory parameter"
        assert "end_date" in command, "end_date is a mandatory parameter"

    def pull_latest_from_table_or_query(self, command: dict):
        self._validate_pull_latest_from_table_or_query_parameters(command)

        return self.offline_store.pull_latest_from_table_or_query(
            self.store.config,
            self.store.get_data_source(command["data_source_name"]),
            command["join_key_columns"],
            command["feature_name_columns"],
            command["timestamp_field"],
            command["created_timestamp_column"],
            utils.make_tzaware(datetime.fromisoformat(command["start_date"])),
            utils.make_tzaware(datetime.fromisoformat(command["end_date"])),
        )

    def list_actions(self, context):
        return [
            (
                OfflineServer.offline_write_batch.__name__,
                "Writes the specified arrow table to the data source underlying the specified feature view.",
            ),
            (
                OfflineServer.write_logged_features.__name__,
                "Writes logged features to a specified destination in the offline store.",
            ),
            (
                OfflineServer.persist.__name__,
                "Synchronously executes the underlying query and persists the result in the same offline store at the "
                "specified destination.",
            ),
        ]

    def _validate_get_historical_features_parameters(self, command: dict, key: str):
        assert key in self.flights, f"missing key={key}"
        assert "feature_view_names" in command, "feature_view_names is mandatory"
        assert "name_aliases" in command, "name_aliases is mandatory"
        assert "feature_refs" in command, "feature_refs is mandatory"
        assert "project" in command, "project is mandatory"
        assert "full_feature_names" in command, "full_feature_names is mandatory"

    def get_historical_features(self, command: dict, key: str):
        self._validate_get_historical_features_parameters(command, key)

        # Extract parameters from the internal flights dictionary
        entity_df_value = self.flights[key]
        entity_df = pa.Table.to_pandas(entity_df_value)

        feature_view_names = command["feature_view_names"]
        name_aliases = command["name_aliases"]
        feature_refs = command["feature_refs"]
        project = command["project"]
        full_feature_names = command["full_feature_names"]

        feature_views = self.list_feature_views_by_name(
            feature_view_names=feature_view_names,
            name_aliases=name_aliases,
            project=project,
        )

        retJob = self.offline_store.get_historical_features(
            config=self.store.config,
            feature_views=feature_views,
            feature_refs=feature_refs,
            entity_df=entity_df,
            registry=self.store.registry,
            project=project,
            full_feature_names=full_feature_names,
        )

        return retJob

    def _validate_persist_parameters(self, command: dict):
        assert "retrieve_func" in command, "retrieve_func is mandatory"
        assert "data_source_name" in command, "data_source_name is mandatory"
        assert "allow_overwrite" in command, "allow_overwrite is mandatory"

    def persist(self, command: dict, key: str):
        self._validate_persist_parameters(command)

        try:
            retrieve_func = command["retrieve_func"]
            if retrieve_func == OfflineServer.get_historical_features.__name__:
                ret_job = self.get_historical_features(command, key)
            elif (
                retrieve_func == OfflineServer.pull_latest_from_table_or_query.__name__
            ):
                ret_job = self.pull_latest_from_table_or_query(command)
            elif retrieve_func == OfflineServer.pull_all_from_table_or_query.__name__:
                ret_job = self.pull_all_from_table_or_query(command)
            else:
                raise NotImplementedError

            data_source = self.store.get_data_source(command["data_source_name"])
            storage = SavedDatasetStorage.from_data_source(data_source)
            ret_job.persist(storage, command["allow_overwrite"], command["timeout"])
        except Exception as e:
            logger.exception(e)
            traceback.print_exc()
            raise e

    def do_action(self, context: fl.ServerCallContext, action: fl.Action):
        pass

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
