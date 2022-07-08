import abc
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
from feast.protos.feast.core.Registry_pb2 import Registry as RegistryProto
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from feast.registry import BaseRegistry
from feast.repo_config import RepoConfig
from feast.saved_dataset import SavedDataset

PROVIDERS_CLASS_FOR_TYPE = {
    "gcp": "feast.infra.gcp.GcpProvider",
    "aws": "feast.infra.aws.AwsProvider",
    "local": "feast.infra.local.LocalProvider",
}


class Provider(abc.ABC):
    @abc.abstractmethod
    def __init__(self, config: RepoConfig):
        ...

    @abc.abstractmethod
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
        Reconcile cloud resources with the objects declared in the feature repo.

        Args:
            project: Project to which tables belong
            tables_to_delete: Tables that were deleted from the feature repo, so provider needs to
                clean up the corresponding cloud resources.
            tables_to_keep: Tables that are still in the feature repo. Depending on implementation,
                provider may or may not need to update the corresponding resources.
            entities_to_delete: Entities that were deleted from the feature repo, so provider needs to
                clean up the corresponding cloud resources.
            entities_to_keep: Entities that are still in the feature repo. Depending on implementation,
                provider may or may not need to update the corresponding resources.
            partial: if true, then tables_to_delete and tables_to_keep are *not* exhaustive lists.
                There may be other tables that are not touched by this update.
        """
        ...

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

    @abc.abstractmethod
    def teardown_infra(
        self,
        project: str,
        tables: Sequence[FeatureView],
        entities: Sequence[Entity],
    ):
        """
        Tear down all cloud resources for a repo.

        Args:
            project: Feast project to which tables belong
            tables: Tables that are declared in the feature repo.
            entities: Entities that are declared in the feature repo.
        """
        ...

    @abc.abstractmethod
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
        Write a batch of feature rows to the online store. This is a low level interface, not
        expected to be used by the users directly.

        If a tz-naive timestamp is passed to this method, it is assumed to be UTC.

        Args:
            config: The RepoConfig for the current FeatureStore.
            table: Feast FeatureView
            data: a list of quadruplets containing Feature data. Each quadruplet contains an Entity Key,
                a dict containing feature values, an event timestamp for the row, and
                the created timestamp for the row if it exists.
            progress: Optional function to be called once every mini-batch of rows is written to
                the online store. Can be used to display progress.
        """
        ...

    def ingest_df(
        self,
        feature_view: FeatureView,
        entities: List[Entity],
        df: pd.DataFrame,
    ):
        """
        Ingests a DataFrame directly into the online store
        """
        pass

    def ingest_df_to_offline_store(
        self,
        feature_view: FeatureView,
        df: pyarrow.Table,
    ):
        """
        Ingests a DataFrame directly into the offline store
        """
        pass

    @abc.abstractmethod
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
        pass

    @abc.abstractmethod
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
        pass

    @abc.abstractmethod
    def online_read(
        self,
        config: RepoConfig,
        table: FeatureView,
        entity_keys: List[EntityKeyProto],
        requested_features: List[str] = None,
    ) -> List[Tuple[Optional[datetime], Optional[Dict[str, ValueProto]]]]:
        """
        Read feature values given an Entity Key. This is a low level interface, not
        expected to be used by the users directly.

        Returns:
            Data is returned as a list, one item per entity key. Each item in the list is a tuple
            of event_ts for the row, and the feature data as a dict from feature names to values.
            Values are returned as Value proto message.
        """
        ...

    @abc.abstractmethod
    def retrieve_saved_dataset(
        self, config: RepoConfig, dataset: SavedDataset
    ) -> RetrievalJob:
        """
        Read saved dataset from offline store.
        All parameters for retrieval (like path, datetime boundaries, column names for both keys and features, etc)
        are determined from SavedDataset object.

        Returns:
             RetrievalJob object, which is lazy wrapper for actual query performed under the hood.

        """
        ...

    @abc.abstractmethod
    def write_feature_service_logs(
        self,
        feature_service: FeatureService,
        logs: Union[pyarrow.Table, Path],
        config: RepoConfig,
        registry: BaseRegistry,
    ):
        """
        Write features and entities logged by a feature server to an offline store.

        Schema of logs table is being inferred from the provided feature service.
        Only feature services with configured logging are accepted.

        Logs dataset can be passed as Arrow Table or path to parquet directory.
        """
        ...

    @abc.abstractmethod
    def retrieve_feature_service_logs(
        self,
        feature_service: FeatureService,
        start_date: datetime,
        end_date: datetime,
        config: RepoConfig,
        registry: BaseRegistry,
    ) -> RetrievalJob:
        """
        Read logged features from an offline store for a given time window [from, to).
        Target table is determined based on logging configuration from the feature service.

        Returns:
             RetrievalJob object, which wraps the query to the offline store.

        """
        ...

    def get_feature_server_endpoint(self) -> Optional[str]:
        """Returns endpoint for the feature server, if it exists."""
        return None


def get_provider(config: RepoConfig, repo_path: Path) -> Provider:
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
