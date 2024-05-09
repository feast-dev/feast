import uuid
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, List, Literal, Optional, Union

import pandas as pd
import pyarrow as pa
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
        # Generate unique command identifier
        self.command = str(uuid.uuid4())
        # Initialize the client connection
        self.client = pa.flight.connect(
            f"grpc://{config.offline_store.host}:{config.offline_store.port}"
        )
        # Put API parameters
        self._put_parameters(feature_refs, entity_df)

    def _put_parameters(self, feature_refs, entity_df):
        historical_flight_descriptor = pa.flight.FlightDescriptor.for_command(
            self.command
        )

        entity_df_table = pa.Table.from_pandas(entity_df)
        writer, _ = self.client.do_put(
            historical_flight_descriptor,
            entity_df_table.schema.with_metadata(
                {
                    "command": self.command,
                    "api": "get_historical_features",
                    "param": "entity_df",
                }
            ),
        )
        writer.write_table(entity_df_table)
        writer.close()

        features_array = pa.array(feature_refs)
        features_batch = pa.RecordBatch.from_arrays([features_array], ["features"])
        writer, _ = self.client.do_put(
            historical_flight_descriptor,
            features_batch.schema.with_metadata(
                {
                    "command": self.command,
                    "api": "get_historical_features",
                    "param": "features",
                }
            ),
        )
        writer.write_batch(features_batch)
        writer.close()

    # Invoked to realize the Pandas DataFrame
    def _to_df_internal(self, timeout: Optional[int] = None) -> pd.DataFrame:
        # We use arrow format because it gives better control of the table schema
        return self._to_arrow_internal().to_pandas()

    # Invoked to synchronously execute the underlying query and return the result as an arrow table
    # This is where do_get service is invoked
    def _to_arrow_internal(self, timeout: Optional[int] = None) -> pa.Table:
        upload_descriptor = pa.flight.FlightDescriptor.for_command(self.command)
        flight = self.client.get_flight_info(upload_descriptor)
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
        print(f"config.offline_store is {type(config.offline_store)}")
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
