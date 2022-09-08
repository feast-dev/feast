from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple, Union

import pandas as pd
import pyarrow
from tqdm import tqdm

from feast import FeatureService, errors
from feast.entity import Entity
from feast.feature_view import FeatureView
from feast.importer import import_class
from feast.infra.infra_object import Infra
from feast.infra.offline_stores.offline_store import RetrievalJob
from feast.infra.registry.base_registry import BaseRegistry
from feast.protos.feast.core.Registry_pb2 import Registry as RegistryProto
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from feast.repo_config import RepoConfig
from feast.saved_dataset import SavedDataset

PROVIDERS_CLASS_FOR_TYPE = {
    "gcp": "feast.infra.gcp.GcpProvider",
    "aws": "feast.infra.aws.AwsProvider",
    "local": "feast.infra.local.LocalProvider",
    "azure": "feast.infra.contrib.azure_provider.AzureProvider",
}


class Provider(ABC):
    """
    A provider defines an implementation of a feature store object. It orchestrates the various
    components of a feature store, such as the offline store, online store, and materialization
    engine. It is configured through a RepoConfig object.
    """

    @abstractmethod
    def __init__(self, config: RepoConfig):
        pass

    @abstractmethod
    def update_infra(
        self,
        project: str,
        tables_to_delete: Sequence[FeatureView],
        tables_to_keep: Sequence[FeatureView],
        entities_to_delete: Sequence[Entity],
        entities_to_keep: Sequence[Entity],
        partial: bool,
    ):
        """
        Reconciles cloud resources with the specified set of Feast objects.

        Args:
            project: Feast project to which the objects belong.
            tables_to_delete: Feature views whose corresponding infrastructure should be deleted.
            tables_to_keep: Feature views whose corresponding infrastructure should not be deleted, and
                may need to be updated.
            entities_to_delete: Entities whose corresponding infrastructure should be deleted.
            entities_to_keep: Entities whose corresponding infrastructure should not be deleted, and
                may need to be updated.
            partial: If true, tables_to_delete and tables_to_keep are not exhaustive lists, so
                infrastructure corresponding to other feature views should be not be touched.
        """
        pass

    def plan_infra(
        self, config: RepoConfig, desired_registry_proto: RegistryProto
    ) -> Infra:
        """
        Returns the Infra required to support the desired registry.

        Args:
            config: The RepoConfig for the current FeatureStore.
            desired_registry_proto: The desired registry, in proto form.
        """
        return Infra()

    @abstractmethod
    def teardown_infra(
        self,
        project: str,
        tables: Sequence[FeatureView],
        entities: Sequence[Entity],
    ):
        """
        Tears down all cloud resources for the specified set of Feast objects.

        Args:
            project: Feast project to which the objects belong.
            tables: Feature views whose corresponding infrastructure should be deleted.
            entities: Entities whose corresponding infrastructure should be deleted.
        """
        pass

    @abstractmethod
    def online_write_batch(
        self,
        config: RepoConfig,
        table: FeatureView,
        data: List[
            Tuple[EntityKeyProto, Dict[str, ValueProto], datetime, Optional[datetime]]
        ],
        progress: Optional[Callable[[int], Any]],
    ) -> None:
        """
        Writes a batch of feature rows to the online store.

        If a tz-naive timestamp is passed to this method, it is assumed to be UTC.

        Args:
            config: The config for the current feature store.
            table: Feature view to which these feature rows correspond.
            data: A list of quadruplets containing feature data. Each quadruplet contains an entity
                key, a dict containing feature values, an event timestamp for the row, and the created
                timestamp for the row if it exists.
            progress: Function to be called once a batch of rows is written to the online store, used
                to show progress.
        """
        pass

    def ingest_df(
        self,
        feature_view: FeatureView,
        df: pd.DataFrame,
    ):
        """
        Persists a dataframe to the online store.

        Args:
            feature_view: The feature view to which the dataframe corresponds.
            df: The dataframe to be persisted.
        """
        pass

    def ingest_df_to_offline_store(
        self,
        feature_view: FeatureView,
        df: pyarrow.Table,
    ):
        """
        Persists a dataframe to the offline store.

        Args:
            feature_view: The feature view to which the dataframe corresponds.
            df: The dataframe to be persisted.
        """
        pass

    @abstractmethod
    def materialize_single_feature_view(
        self,
        config: RepoConfig,
        feature_view: FeatureView,
        start_date: datetime,
        end_date: datetime,
        registry: BaseRegistry,
        project: str,
        tqdm_builder: Callable[[int], tqdm],
    ) -> None:
        """
        Writes latest feature values in the specified time range to the online store.

        Args:
            config: The config for the current feature store.
            feature_view: The feature view to materialize.
            start_date: The start of the time range.
            end_date: The end of the time range.
            registry: The registry for the current feature store.
            project: Feast project to which the objects belong.
            tqdm_builder: A function to monitor the progress of materialization.
        """
        pass

    @abstractmethod
    def get_historical_features(
        self,
        config: RepoConfig,
        feature_views: List[FeatureView],
        feature_refs: List[str],
        entity_df: Union[pd.DataFrame, str],
        registry: BaseRegistry,
        project: str,
        full_feature_names: bool,
    ) -> RetrievalJob:
        """
        Retrieves the point-in-time correct historical feature values for the specified entity rows.

        Args:
            config: The config for the current feature store.
            feature_views: A list containing all feature views that are referenced in the entity rows.
            feature_refs: The features to be retrieved.
            entity_df: A collection of rows containing all entity columns on which features need to be joined,
                as well as the timestamp column used for point-in-time joins. Either a pandas dataframe can be
                provided or a SQL query.
            registry: The registry for the current feature store.
            project: Feast project to which the feature views belong.
            full_feature_names: If True, feature names will be prefixed with the corresponding feature view name,
                changing them from the format "feature" to "feature_view__feature" (e.g. "daily_transactions"
                changes to "customer_fv__daily_transactions").

        Returns:
            A RetrievalJob that can be executed to get the features.
        """
        pass

    @abstractmethod
    def online_read(
        self,
        config: RepoConfig,
        table: FeatureView,
        entity_keys: List[EntityKeyProto],
        requested_features: List[str] = None,
    ) -> List[Tuple[Optional[datetime], Optional[Dict[str, ValueProto]]]]:
        """
        Reads features values for the given entity keys.

        Args:
            config: The config for the current feature store.
            table: The feature view whose feature values should be read.
            entity_keys: The list of entity keys for which feature values should be read.
            requested_features: The list of features that should be read.

        Returns:
            A list of the same length as entity_keys. Each item in the list is a tuple where the first
            item is the event timestamp for the row, and the second item is a dict mapping feature names
            to values, which are returned in proto format.
        """
        pass

    @abstractmethod
    def retrieve_saved_dataset(
        self, config: RepoConfig, dataset: SavedDataset
    ) -> RetrievalJob:
        """
        Reads a saved dataset.

        Args:
            config: The config for the current feature store.
            dataset: A SavedDataset object containing all parameters necessary for retrieving the dataset.

        Returns:
            A RetrievalJob that can be executed to get the saved dataset.
        """
        pass

    @abstractmethod
    def write_feature_service_logs(
        self,
        feature_service: FeatureService,
        logs: Union[pyarrow.Table, Path],
        config: RepoConfig,
        registry: BaseRegistry,
    ):
        """
        Writes features and entities logged by a feature server to the offline store.

        The schema of the logs table is inferred from the specified feature service. Only feature
        services with configured logging are accepted.

        Args:
            feature_service: The feature service to be logged.
            logs: The logs, either as an arrow table or as a path to a parquet directory.
            config: The config for the current feature store.
            registry: The registry for the current feature store.
        """
        pass

    @abstractmethod
    def retrieve_feature_service_logs(
        self,
        feature_service: FeatureService,
        start_date: datetime,
        end_date: datetime,
        config: RepoConfig,
        registry: BaseRegistry,
    ) -> RetrievalJob:
        """
        Reads logged features for the specified time window.

        Args:
            feature_service: The feature service whose logs should be retrieved.
            start_date: The start of the window.
            end_date: The end of the window.
            config: The config for the current feature store.
            registry: The registry for the current feature store.

        Returns:
            A RetrievalJob that can be executed to get the feature service logs.
        """
        pass

    def get_feature_server_endpoint(self) -> Optional[str]:
        """Returns endpoint for the feature server, if it exists."""
        return None


def get_provider(config: RepoConfig) -> Provider:
    if "." not in config.provider:
        if config.provider not in PROVIDERS_CLASS_FOR_TYPE:
            raise errors.FeastProviderNotImplementedError(config.provider)

        provider = PROVIDERS_CLASS_FOR_TYPE[config.provider]
    else:
        provider = config.provider

    # Split provider into module and class names by finding the right-most dot.
    # For example, provider 'foo.bar.MyProvider' will be parsed into 'foo.bar' and 'MyProvider'
    module_name, class_name = provider.rsplit(".", 1)

    cls = import_class(module_name, class_name, "Provider")

    return cls(config)
