import json
import logging
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, List, Literal, Optional, Tuple, Union

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.flight as fl
import pyarrow.parquet
from pydantic import StrictInt, StrictStr

from feast import OnDemandFeatureView
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
from feast.permissions.client.utils import create_flight_call_options
from feast.repo_config import FeastConfigBaseModel, RepoConfig
from feast.saved_dataset import SavedDatasetStorage

logger = logging.getLogger(__name__)


class RemoteOfflineStoreConfig(FeastConfigBaseModel):
    type: Literal["remote"] = "remote"
    host: StrictStr
    """ str: remote offline store server port, e.g. the host URL for offline store  of arrow flight server. """

    port: Optional[StrictInt] = None
    """ str: remote offline store server port."""


class RemoteRetrievalJob(RetrievalJob):
    def __init__(
        self,
        client: fl.FlightClient,
        options: pa.flight.FlightCallOptions,
        api: str,
        api_parameters: Dict[str, Any],
        entity_df: Union[pd.DataFrame, str] = None,
        table: pa.Table = None,
        metadata: Optional[RetrievalMetadata] = None,
    ):
        # Initialize the client connection
        self.client = client
        self.options = options
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
            self.options,
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
            options=self.options,
            table=self.table,
            entity_df=self.entity_df,
        )


class RemoteOfflineStore(OfflineStore):
    @staticmethod
    def get_historical_features(
        config: RepoConfig,
        feature_views: List[FeatureView],
        feature_refs: List[str],
        entity_df: Union[pd.DataFrame, str],
        registry: BaseRegistry,
        project: str,
        full_feature_names: bool = False,
    ) -> RemoteRetrievalJob:
        assert isinstance(config.offline_store, RemoteOfflineStoreConfig)

        # Initialize the client connection
        client = RemoteOfflineStore.init_client(config)
        options = create_flight_call_options(config.auth_config)

        feature_view_names = [fv.name for fv in feature_views]
        name_aliases = [fv.projection.name_alias for fv in feature_views]

        api_parameters = {
            "feature_view_names": feature_view_names,
            "feature_refs": feature_refs,
            "project": project,
            "full_feature_names": full_feature_names,
            "name_aliases": name_aliases,
        }

        return RemoteRetrievalJob(
            client=client,
            options=options,
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
        start_date: datetime,
        end_date: datetime,
    ) -> RetrievalJob:
        assert isinstance(config.offline_store, RemoteOfflineStoreConfig)

        # Initialize the client connection
        client = RemoteOfflineStore.init_client(config)
        options = create_flight_call_options(config.auth_config)

        api_parameters = {
            "data_source_name": data_source.name,
            "join_key_columns": join_key_columns,
            "feature_name_columns": feature_name_columns,
            "timestamp_field": timestamp_field,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
        }

        return RemoteRetrievalJob(
            client=client,
            options=options,
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
        client = RemoteOfflineStore.init_client(config)
        options = create_flight_call_options(config.auth_config)

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
            options=options,
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
        client = RemoteOfflineStore.init_client(config)
        options = create_flight_call_options(config.auth_config)

        api_parameters = {
            "feature_service_name": source._feature_service.name,
        }

        _call_put(
            api=OfflineStore.write_logged_features.__name__,
            api_parameters=api_parameters,
            client=client,
            options=options,
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
        client = RemoteOfflineStore.init_client(config)
        options = create_flight_call_options(config.auth_config)

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
            options=options,
            table=table,
            entity_df=None,
        )

    @staticmethod
    def init_client(config):
        location = f"grpc://{config.offline_store.host}:{config.offline_store.port}"
        client = fl.connect(location=location)
        logger.info(f"Connecting FlightClient at {location}")
        return client


def _create_retrieval_metadata(feature_refs: List[str], entity_df: pd.DataFrame):
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
    entity_df: Union[pd.DataFrame, str],
    table: pa.Table,
    client: fl.FlightClient,
    options: pa.flight.FlightCallOptions,
):
    command_descriptor = _call_put(
        api,
        api_parameters,
        client,
        options,
        entity_df,
        table,
    )
    return _call_get(client, options, command_descriptor)


def _call_get(
    client: fl.FlightClient,
    options: pa.flight.FlightCallOptions,
    command_descriptor: fl.FlightDescriptor,
):
    flight = client.get_flight_info(command_descriptor, options)
    ticket = flight.endpoints[0].ticket
    reader = client.do_get(ticket, options)
    return reader.read_all()


def _call_put(
    api: str,
    api_parameters: Dict[str, Any],
    client: fl.FlightClient,
    options: pa.flight.FlightCallOptions,
    entity_df: Union[pd.DataFrame, str],
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

    _put_parameters(command_descriptor, entity_df, table, client, options)
    return command_descriptor


def _put_parameters(
    command_descriptor: fl.FlightDescriptor,
    entity_df: Union[pd.DataFrame, str],
    table: pa.Table,
    client: fl.FlightClient,
    options: pa.flight.FlightCallOptions,
):
    updatedTable: pa.Table

    if entity_df is not None:
        updatedTable = pa.Table.from_pandas(entity_df)
    elif table is not None:
        updatedTable = table
    else:
        updatedTable = _create_empty_table()

    writer, _ = client.do_put(command_descriptor, updatedTable.schema, options)

    writer.write_table(updatedTable)
    writer.close()


def _create_empty_table():
    schema = pa.schema(
        {
            "key": pa.string(),
        }
    )

    keys = ["mock_key"]

    table = pa.Table.from_pydict(dict(zip(schema.names, keys)), schema=schema)

    return table
