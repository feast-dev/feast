import json
import logging
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Literal, Optional, Tuple, Union

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.flight as fl
import pyarrow.parquet
from pyarrow import Schema
from pyarrow._flight import FlightCallOptions, FlightDescriptor, Ticket
from pydantic import StrictInt, StrictStr

from feast import OnDemandFeatureView
from feast.arrow_error_handler import arrow_client_error_handling_decorator
from feast.data_source import DataSource
from feast.feature_logging import (
    FeatureServiceLoggingSource,
    LoggingConfig,
    LoggingSource,
)
from feast.feature_view import FeatureView
from feast.infra.offline_stores import offline_utils
from feast.infra.offline_stores.offline_store import (
    OfflineStore,
    RetrievalJob,
    RetrievalMetadata,
)
from feast.infra.registry.base_registry import BaseRegistry
from feast.permissions.auth.auth_type import AuthType
from feast.permissions.auth_model import AuthConfig
from feast.permissions.client.arrow_flight_auth_interceptor import (
    FlightAuthInterceptorFactory,
)
from feast.repo_config import FeastConfigBaseModel, RepoConfig
from feast.saved_dataset import SavedDatasetStorage

logger = logging.getLogger(__name__)


class FeastFlightClient(fl.FlightClient):
    @arrow_client_error_handling_decorator
    def get_flight_info(
        self, descriptor: FlightDescriptor, options: FlightCallOptions = None
    ):
        return super().get_flight_info(descriptor, options)

    @arrow_client_error_handling_decorator
    def do_get(self, ticket: Ticket, options: FlightCallOptions = None):
        return super().do_get(ticket, options)

    @arrow_client_error_handling_decorator
    def do_put(
        self,
        descriptor: FlightDescriptor,
        schema: Schema,
        options: FlightCallOptions = None,
    ):
        return super().do_put(descriptor, schema, options)

    @arrow_client_error_handling_decorator
    def list_flights(self, criteria: bytes = b"", options: FlightCallOptions = None):
        return super().list_flights(criteria, options)

    @arrow_client_error_handling_decorator
    def list_actions(self, options: FlightCallOptions = None):
        return super().list_actions(options)


def build_arrow_flight_client(
    scheme: str, host: str, port, auth_config: AuthConfig, cert: str = ""
):
    arrow_scheme = "grpc+tcp"
    if scheme == "https":
        logger.info(
            "Scheme is https so going to connect offline server in SSL(TLS) mode."
        )
        arrow_scheme = "grpc+tls"

    kwargs = {}
    if cert:
        with open(cert, "rb") as root_certs:
            kwargs["tls_root_certs"] = root_certs.read()

    if auth_config.type != AuthType.NONE.value:
        middlewares = [FlightAuthInterceptorFactory(auth_config)]
        return FeastFlightClient(
            f"{arrow_scheme}://{host}:{port}", middleware=middlewares, **kwargs
        )

    return FeastFlightClient(f"{arrow_scheme}://{host}:{port}", **kwargs)


class RemoteOfflineStoreConfig(FeastConfigBaseModel):
    type: Literal["remote"] = "remote"

    scheme: Literal["http", "https"] = "http"

    host: StrictStr
    """ str: remote offline store server port, e.g. the host URL for offline store  of arrow flight server. """

    port: Optional[StrictInt] = None
    """ str: remote offline store server port."""

    cert: StrictStr = ""
    """ str: Path to the public certificate when the offline server starts in TLS(SSL) mode. This may be needed if the offline server started with a self-signed certificate, typically this file ends with `*.crt`, `*.cer`, or `*.pem`.
    If type is 'remote', then this configuration is needed to connect to remote offline server in TLS mode. """


class RemoteRetrievalJob(RetrievalJob):
    def __init__(
        self,
        client: FeastFlightClient,
        api: str,
        api_parameters: Dict[str, Any],
        entity_df: Optional[Union[pd.DataFrame, str]] = None,
        table: pa.Table = None,
        metadata: Optional[RetrievalMetadata] = None,
    ):
        # Initialize the client connection
        self.client = client
        self.api = api
        self.api_parameters = api_parameters
        self.entity_df = entity_df
        self.table = table
        self._metadata = metadata

    # Invoked to realize the Pandas DataFrame
    def _to_df_internal(self, timeout: Optional[int] = None) -> pd.DataFrame:
        # We use arrow format because it gives better control of the table schema
        return self._to_arrow_internal().to_pandas()

    # Invoked to synchronously execute the underlying query and return the result as an arrow table
    # This is where do_get service is invoked
    def _to_arrow_internal(self, timeout: Optional[int] = None) -> pa.Table:
        return _send_retrieve_remote(
            self.api,
            self.api_parameters,
            self.entity_df,
            self.table,
            self.client,
        )

    @property
    def on_demand_feature_views(self) -> List[OnDemandFeatureView]:
        return []

    @property
    def metadata(self) -> Optional[RetrievalMetadata]:
        return self._metadata

    @property
    def full_feature_names(self) -> bool:
        return self.api_parameters["full_feature_names"]

    def persist(
        self,
        storage: SavedDatasetStorage,
        allow_overwrite: bool = False,
        timeout: Optional[int] = None,
    ):
        """
        Arrow flight action is being used to perform the persist action remotely
        """

        api_parameters = {
            "data_source_name": storage.to_data_source().name,
            "allow_overwrite": allow_overwrite,
            "timeout": timeout,
        }

        # Add api parameters to command
        for key, value in self.api_parameters.items():
            api_parameters[key] = value

        api_parameters["retrieve_func"] = self.api

        _call_put(
            api=RemoteRetrievalJob.persist.__name__,
            api_parameters=api_parameters,
            client=self.client,
            table=self.table,
            entity_df=self.entity_df,
        )


class RemoteOfflineStore(OfflineStore):
    @staticmethod
    def get_historical_features(
        config: RepoConfig,
        feature_views: List[FeatureView],
        feature_refs: List[str],
        entity_df: Optional[Union[pd.DataFrame, str]],
        registry: BaseRegistry,
        project: str,
        full_feature_names: bool = False,
        **kwargs,
    ) -> RemoteRetrievalJob:
        assert isinstance(config.offline_store, RemoteOfflineStoreConfig)

        client = build_arrow_flight_client(
            scheme=config.offline_store.scheme,
            host=config.offline_store.host,
            port=config.offline_store.port,
            auth_config=config.auth_config,
            cert=config.offline_store.cert,
        )

        feature_view_names = [fv.name for fv in feature_views]
        name_aliases = [fv.projection.name_alias for fv in feature_views]

        api_parameters = {
            "feature_view_names": feature_view_names,
            "feature_refs": feature_refs,
            "project": project,
            "full_feature_names": full_feature_names,
            "name_aliases": name_aliases,
        }

        # Extract and serialize start_date/end_date for remote transmission
        start_date = kwargs.get("start_date", None)
        end_date = kwargs.get("end_date", None)

        if start_date is not None:
            api_parameters["start_date"] = start_date.isoformat()
        if end_date is not None:
            api_parameters["end_date"] = end_date.isoformat()

        return RemoteRetrievalJob(
            client=client,
            api=OfflineStore.get_historical_features.__name__,
            api_parameters=api_parameters,
            entity_df=entity_df,
            metadata=_create_retrieval_metadata(feature_refs, entity_df),
        )

    @staticmethod
    def pull_all_from_table_or_query(
        config: RepoConfig,
        data_source: DataSource,
        join_key_columns: List[str],
        feature_name_columns: List[str],
        timestamp_field: str,
        created_timestamp_column: Optional[str] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
    ) -> RetrievalJob:
        assert isinstance(config.offline_store, RemoteOfflineStoreConfig)

        # Initialize the client connection
        client = build_arrow_flight_client(
            scheme=config.offline_store.scheme,
            host=config.offline_store.host,
            port=config.offline_store.port,
            auth_config=config.auth_config,
            cert=config.offline_store.cert,
        )

        api_parameters = {
            "data_source_name": data_source.name,
            "join_key_columns": join_key_columns,
            "feature_name_columns": feature_name_columns,
            "timestamp_field": timestamp_field,
            "created_timestamp_column": created_timestamp_column,
            "start_date": start_date.isoformat() if start_date else None,
            "end_date": end_date.isoformat() if end_date else None,
        }

        return RemoteRetrievalJob(
            client=client,
            api=OfflineStore.pull_all_from_table_or_query.__name__,
            api_parameters=api_parameters,
        )

    @staticmethod
    def pull_latest_from_table_or_query(
        config: RepoConfig,
        data_source: DataSource,
        join_key_columns: List[str],
        feature_name_columns: List[str],
        timestamp_field: str,
        created_timestamp_column: Optional[str],
        start_date: datetime,
        end_date: datetime,
    ) -> RetrievalJob:
        assert isinstance(config.offline_store, RemoteOfflineStoreConfig)

        # Initialize the client connection
        client = build_arrow_flight_client(
            config.offline_store.scheme,
            config.offline_store.host,
            config.offline_store.port,
            config.auth_config,
            cert=config.offline_store.cert,
        )

        api_parameters = {
            "data_source_name": data_source.name,
            "join_key_columns": join_key_columns,
            "feature_name_columns": feature_name_columns,
            "timestamp_field": timestamp_field,
            "created_timestamp_column": created_timestamp_column,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
        }

        return RemoteRetrievalJob(
            client=client,
            api=OfflineStore.pull_latest_from_table_or_query.__name__,
            api_parameters=api_parameters,
        )

    @staticmethod
    def write_logged_features(
        config: RepoConfig,
        data: Union[pyarrow.Table, Path],
        source: LoggingSource,
        logging_config: LoggingConfig,
        registry: BaseRegistry,
    ):
        assert isinstance(config.offline_store, RemoteOfflineStoreConfig)
        assert isinstance(source, FeatureServiceLoggingSource)

        if isinstance(data, Path):
            data = pyarrow.parquet.read_table(data, use_threads=False, pre_buffer=False)

        # Initialize the client connection
        client = build_arrow_flight_client(
            config.offline_store.scheme,
            config.offline_store.host,
            config.offline_store.port,
            config.auth_config,
            config.offline_store.cert,
        )

        api_parameters = {
            "feature_service_name": source._feature_service.name,
        }

        _call_put(
            api=OfflineStore.write_logged_features.__name__,
            api_parameters=api_parameters,
            client=client,
            table=data,
            entity_df=None,
        )

    @staticmethod
    def offline_write_batch(
        config: RepoConfig,
        feature_view: FeatureView,
        table: pyarrow.Table,
        progress: Optional[Callable[[int], Any]],
    ):
        assert isinstance(config.offline_store, RemoteOfflineStoreConfig)

        # Initialize the client connection
        client = build_arrow_flight_client(
            config.offline_store.scheme,
            config.offline_store.host,
            config.offline_store.port,
            config.auth_config,
            config.offline_store.cert,
        )

        feature_view_names = [feature_view.name]
        name_aliases = [feature_view.projection.name_alias]

        api_parameters = {
            "feature_view_names": feature_view_names,
            "progress": progress,
            "name_aliases": name_aliases,
        }

        _call_put(
            api=OfflineStore.offline_write_batch.__name__,
            api_parameters=api_parameters,
            client=client,
            table=table,
            entity_df=None,
        )

    def validate_data_source(
        self,
        config: RepoConfig,
        data_source: DataSource,
    ):
        assert isinstance(config.offline_store, RemoteOfflineStoreConfig)

        client = build_arrow_flight_client(
            config.offline_store.scheme,
            config.offline_store.host,
            config.offline_store.port,
            config.auth_config,
            config.offline_store.cert,
        )

        api_parameters = {
            "data_source_proto": str(data_source),
        }
        logger.debug(f"validating DataSource {data_source.name}")
        _call_put(
            api=OfflineStore.validate_data_source.__name__,
            api_parameters=api_parameters,
            client=client,
            table=None,
            entity_df=None,
        )

    def get_table_column_names_and_types_from_data_source(
        self, config: RepoConfig, data_source: DataSource
    ) -> Iterable[Tuple[str, str]]:
        assert isinstance(config.offline_store, RemoteOfflineStoreConfig)

        client = build_arrow_flight_client(
            config.offline_store.scheme,
            config.offline_store.host,
            config.offline_store.port,
            config.auth_config,
            config.offline_store.cert,
        )

        api_parameters = {
            "data_source_proto": str(data_source),
        }
        logger.debug(
            f"Calling {OfflineStore.get_table_column_names_and_types_from_data_source.__name__} with {api_parameters}"
        )
        table = _send_retrieve_remote(
            api=OfflineStore.get_table_column_names_and_types_from_data_source.__name__,
            api_parameters=api_parameters,
            client=client,
            table=None,
            entity_df=None,
        )

        logger.debug(
            f"get_table_column_names_and_types_from_data_source for {data_source.name}: {table}"
        )
        return zip(table.column("name").to_pylist(), table.column("type").to_pylist())


def _create_retrieval_metadata(
    feature_refs: List[str], entity_df: Optional[pd.DataFrame] = None
):
    if entity_df is None:
        return RetrievalMetadata(
            features=feature_refs,
            keys=[],  # No entity keys when no entity_df provided
            min_event_timestamp=None,
            max_event_timestamp=None,
        )
    entity_schema = _get_entity_schema(
        entity_df=entity_df,
    )

    event_timestamp_col = offline_utils.infer_event_timestamp_from_entity_df(
        entity_schema=entity_schema,
    )

    timestamp_range = _get_entity_df_event_timestamp_range(
        entity_df, event_timestamp_col
    )

    return RetrievalMetadata(
        features=feature_refs,
        keys=list(set(entity_df.columns) - {event_timestamp_col}),
        min_event_timestamp=timestamp_range[0],
        max_event_timestamp=timestamp_range[1],
    )


def _get_entity_schema(entity_df: pd.DataFrame) -> Dict[str, np.dtype]:
    return dict(zip(entity_df.columns, entity_df.dtypes))


def _get_entity_df_event_timestamp_range(
    entity_df: Union[pd.DataFrame, str],
    entity_df_event_timestamp_col: str,
) -> Tuple[datetime, datetime]:
    if not isinstance(entity_df, pd.DataFrame):
        raise ValueError(
            f"Please provide an entity_df of type {type(pd.DataFrame)} instead of type {type(entity_df)}"
        )

    entity_df_event_timestamp = entity_df.loc[
        :, entity_df_event_timestamp_col
    ].infer_objects()
    if pd.api.types.is_string_dtype(entity_df_event_timestamp):
        entity_df_event_timestamp = pd.to_datetime(entity_df_event_timestamp, utc=True)

    return (
        entity_df_event_timestamp.min().to_pydatetime(),
        entity_df_event_timestamp.max().to_pydatetime(),
    )


def _send_retrieve_remote(
    api: str,
    api_parameters: Dict[str, Any],
    entity_df: Optional[Union[pd.DataFrame, str]],
    table: Optional[pa.Table],
    client: FeastFlightClient,
):
    command_descriptor = _call_put(
        api,
        api_parameters,
        client,
        entity_df,
        table,
    )
    return _call_get(client, command_descriptor)


def _call_get(
    client: FeastFlightClient,
    command_descriptor: fl.FlightDescriptor,
):
    flight = client.get_flight_info(command_descriptor)
    ticket = flight.endpoints[0].ticket
    reader = client.do_get(ticket)
    return read_all(reader)


def _call_put(
    api: str,
    api_parameters: Dict[str, Any],
    client: FeastFlightClient,
    entity_df: Optional[Union[pd.DataFrame, str]],
    table: pa.Table,
):
    # Generate unique command identifier
    command_id = str(uuid.uuid4())
    command = {
        "command_id": command_id,
        "api": api,
    }
    # Add api parameters to command
    for key, value in api_parameters.items():
        command[key] = value

    command_descriptor = fl.FlightDescriptor.for_command(
        json.dumps(
            command,
        )
    )

    _put_parameters(command_descriptor, entity_df, table, client)
    return command_descriptor


def _put_parameters(
    command_descriptor: fl.FlightDescriptor,
    entity_df: Optional[Union[pd.DataFrame, str]],
    table: pa.Table,
    client: FeastFlightClient,
):
    updatedTable: pa.Table

    if entity_df is not None:
        updatedTable = pa.Table.from_pandas(entity_df)
    elif table is not None:
        updatedTable = table
    else:
        updatedTable = _create_empty_table()

    writer, _ = client.do_put(command_descriptor, updatedTable.schema)

    write_table(writer, updatedTable)


@arrow_client_error_handling_decorator
def write_table(writer, updated_table: pa.Table):
    writer.write_table(updated_table)
    writer.close()


@arrow_client_error_handling_decorator
def read_all(reader):
    return reader.read_all()


def _create_empty_table():
    schema = pa.schema(
        {
            "key": pa.string(),
        }
    )

    keys = ["mock_key"]

    table = pa.Table.from_pydict(dict(zip(schema.names, keys)), schema=schema)

    return table
