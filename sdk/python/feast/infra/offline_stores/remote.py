import json
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, List, Literal, Optional, Union

import pandas as pd
import pyarrow as pa
import pyarrow.flight as fl
import pyarrow.parquet
from pydantic import StrictInt, StrictStr

from feast import OnDemandFeatureView
from feast.data_source import DataSource
from feast.feature_logging import LoggingConfig, LoggingSource
from feast.feature_view import FeatureView
from feast.infra.offline_stores.offline_store import (
    OfflineStore,
    RetrievalJob,
)
from feast.infra.registry.base_registry import BaseRegistry
from feast.repo_config import FeastConfigBaseModel, RepoConfig
from feast.usage import log_exceptions_and_usage


class RemoteOfflineStoreConfig(FeastConfigBaseModel):
    type: Literal["remote"] = "remote"
    host: StrictStr
    """ str: remote offline store server port, e.g. the host URL for offline store  of arrow flight server. """

    port: Optional[StrictInt] = None
    """ str: remote offline store server port."""


class RemoteRetrievalJob(RetrievalJob):
    def __init__(
        self,
        config: RepoConfig,
        feature_refs: List[str],
        entity_df: Union[pd.DataFrame, str],
        # TODO add missing parameters from the OfflineStore API
    ):
        # Initialize the client connection
        self.client = fl.connect(
            f"grpc://{config.offline_store.host}:{config.offline_store.port}"
        )
        self.feature_refs = feature_refs
        self.entity_df = entity_df

    # TODO add one specialized implementation for each OfflineStore API
    # This can result in a dictionary of functions indexed by api (e.g., "get_historical_features")
    def _put_parameters(self, command_descriptor):
        entity_df_table = pa.Table.from_pandas(self.entity_df)

        writer, _ = self.client.do_put(
            command_descriptor,
            entity_df_table.schema,
        )

        writer.write_table(entity_df_table)
        writer.close()

    # Invoked to realize the Pandas DataFrame
    def _to_df_internal(self, timeout: Optional[int] = None) -> pd.DataFrame:
        # We use arrow format because it gives better control of the table schema
        return self._to_arrow_internal().to_pandas()

    # Invoked to synchronously execute the underlying query and return the result as an arrow table
    # This is where do_get service is invoked
    def _to_arrow_internal(self, timeout: Optional[int] = None) -> pa.Table:
        # Generate unique command identifier
        command_id = str(uuid.uuid4())
        command = {
            "command_id": command_id,
            "api": "get_historical_features",
            "features": self.feature_refs,
        }
        command_descriptor = fl.FlightDescriptor.for_command(
            json.dumps(
                command,
            )
        )

        self._put_parameters(command_descriptor)
        flight = self.client.get_flight_info(command_descriptor)
        ticket = flight.endpoints[0].ticket

        reader = self.client.do_get(ticket)
        return reader.read_all()

    @property
    def on_demand_feature_views(self) -> List[OnDemandFeatureView]:
        return []


class RemoteOfflineStore(OfflineStore):
    @staticmethod
    @log_exceptions_and_usage(offline_store="remote")
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

        # TODO: extend RemoteRetrievalJob API with all method parameters
        return RemoteRetrievalJob(
            config=config, feature_refs=feature_refs, entity_df=entity_df
        )

    @staticmethod
    @log_exceptions_and_usage(offline_store="remote")
    def pull_all_from_table_or_query(
        config: RepoConfig,
        data_source: DataSource,
        join_key_columns: List[str],
        feature_name_columns: List[str],
        timestamp_field: str,
        start_date: datetime,
        end_date: datetime,
    ) -> RetrievalJob:
        # TODO Implementation here.
        raise NotImplementedError

    @staticmethod
    @log_exceptions_and_usage(offline_store="remote")
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
        # TODO Implementation here.
        raise NotImplementedError

    @staticmethod
    @log_exceptions_and_usage(offline_store="remote")
    def write_logged_features(
        config: RepoConfig,
        data: Union[pyarrow.Table, Path],
        source: LoggingSource,
        logging_config: LoggingConfig,
        registry: BaseRegistry,
    ):
        # TODO Implementation here.
        raise NotImplementedError

    @staticmethod
    @log_exceptions_and_usage(offline_store="remote")
    def offline_write_batch(
        config: RepoConfig,
        feature_view: FeatureView,
        table: pyarrow.Table,
        progress: Optional[Callable[[int], Any]],
    ):
        # TODO Implementation here.
        raise NotImplementedError
