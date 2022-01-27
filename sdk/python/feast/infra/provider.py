import abc
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple, Union

import pandas
import pyarrow
from tqdm import tqdm

from feast import errors
from feast.entity import Entity
from feast.feature_view import DUMMY_ENTITY_ID, FeatureView
from feast.importer import import_class
from feast.infra.infra_object import Infra
from feast.infra.offline_stores.offline_store import RetrievalJob
from feast.on_demand_feature_view import OnDemandFeatureView
from feast.protos.feast.core.Registry_pb2 import Registry as RegistryProto
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from feast.registry import Registry
from feast.repo_config import RepoConfig
from feast.saved_dataset import SavedDataset
from feast.type_map import python_values_to_proto_values
from feast.value_type import ValueType

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
        self, project: str, tables: Sequence[FeatureView], entities: Sequence[Entity],
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
        self, feature_view: FeatureView, entities: List[Entity], df: pandas.DataFrame,
    ):
        """
        Ingests a DataFrame directly into the online store
        """
        pass

    @abc.abstractmethod
    def materialize_single_feature_view(
        self,
        config: RepoConfig,
        feature_view: FeatureView,
        start_date: datetime,
        end_date: datetime,
        registry: Registry,
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
        entity_df: Union[pandas.DataFrame, str],
        registry: Registry,
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


def _get_requested_feature_views_to_features_dict(
    feature_refs: List[str],
    feature_views: List[FeatureView],
    on_demand_feature_views: List[OnDemandFeatureView],
) -> Tuple[Dict[FeatureView, List[str]], Dict[OnDemandFeatureView, List[str]]]:
    """Create a dict of FeatureView -> List[Feature] for all requested features.
    Set full_feature_names to True to have feature names prefixed by their feature view name."""

    feature_views_to_feature_map: Dict[FeatureView, List[str]] = defaultdict(list)
    on_demand_feature_views_to_feature_map: Dict[
        OnDemandFeatureView, List[str]
    ] = defaultdict(list)

    for ref in feature_refs:
        ref_parts = ref.split(":")
        feature_view_from_ref = ref_parts[0]
        feature_from_ref = ref_parts[1]

        found = False
        for fv in feature_views:
            if fv.projection.name_to_use() == feature_view_from_ref:
                found = True
                feature_views_to_feature_map[fv].append(feature_from_ref)
        for odfv in on_demand_feature_views:
            if odfv.projection.name_to_use() == feature_view_from_ref:
                found = True
                on_demand_feature_views_to_feature_map[odfv].append(feature_from_ref)

        if not found:
            raise ValueError(f"Could not find feature view from reference {ref}")

    return feature_views_to_feature_map, on_demand_feature_views_to_feature_map


def _get_column_names(
    feature_view: FeatureView, entities: List[Entity]
) -> Tuple[List[str], List[str], str, Optional[str]]:
    """
    If a field mapping exists, run it in reverse on the join keys,
    feature names, event timestamp column, and created timestamp column
    to get the names of the relevant columns in the offline feature store table.

    Returns:
        Tuple containing the list of reverse-mapped join_keys,
        reverse-mapped feature names, reverse-mapped event timestamp column,
        and reverse-mapped created timestamp column that will be passed into
        the query to the offline store.
    """
    # if we have mapped fields, use the original field names in the call to the offline store
    event_timestamp_column = feature_view.batch_source.event_timestamp_column
    feature_names = [feature.name for feature in feature_view.features]
    created_timestamp_column = feature_view.batch_source.created_timestamp_column
    join_keys = [
        entity.join_key for entity in entities if entity.join_key != DUMMY_ENTITY_ID
    ]
    if feature_view.batch_source.field_mapping is not None:
        reverse_field_mapping = {
            v: k for k, v in feature_view.batch_source.field_mapping.items()
        }
        event_timestamp_column = (
            reverse_field_mapping[event_timestamp_column]
            if event_timestamp_column in reverse_field_mapping.keys()
            else event_timestamp_column
        )
        created_timestamp_column = (
            reverse_field_mapping[created_timestamp_column]
            if created_timestamp_column
            and created_timestamp_column in reverse_field_mapping.keys()
            else created_timestamp_column
        )
        join_keys = [
            reverse_field_mapping[col] if col in reverse_field_mapping.keys() else col
            for col in join_keys
        ]
        feature_names = [
            reverse_field_mapping[col] if col in reverse_field_mapping.keys() else col
            for col in feature_names
        ]

    # We need to exclude join keys and timestamp columns from the list of features, after they are mapped to
    # their final column names via the `field_mapping` field of the source.
    _feature_names = set(feature_names) - set(join_keys)
    _feature_names = _feature_names - {event_timestamp_column, created_timestamp_column}
    feature_names = list(_feature_names)
    return (
        join_keys,
        feature_names,
        event_timestamp_column,
        created_timestamp_column,
    )


def _run_field_mapping(
    table: pyarrow.Table, field_mapping: Dict[str, str],
) -> pyarrow.Table:
    # run field mapping in the forward direction
    cols = table.column_names
    mapped_cols = [
        field_mapping[col] if col in field_mapping.keys() else col for col in cols
    ]
    table = table.rename_columns(mapped_cols)
    return table


def _coerce_datetime(ts):
    """
    Depending on underlying time resolution, arrow to_pydict() sometimes returns pandas
    timestamp type (for nanosecond resolution), and sometimes you get standard python datetime
    (for microsecond resolution).
    While pandas timestamp class is a subclass of python datetime, it doesn't always behave the
    same way. We convert it to normal datetime so that consumers downstream don't have to deal
    with these quirks.
    """

    if isinstance(ts, pandas.Timestamp):
        return ts.to_pydatetime()
    else:
        return ts


def _convert_arrow_to_proto(
    table: Union[pyarrow.Table, pyarrow.RecordBatch],
    feature_view: FeatureView,
    join_keys: Dict[str, ValueType],
) -> List[Tuple[EntityKeyProto, Dict[str, ValueProto], datetime, Optional[datetime]]]:
    # Avoid ChunkedArrays which guarentees `zero_copy_only` availiable.
    if isinstance(table, pyarrow.Table):
        table = table.to_batches()[0]

    columns = [(f.name, f.dtype) for f in feature_view.features] + list(
        join_keys.items()
    )

    proto_values_by_column = {
        column: python_values_to_proto_values(
            table.column(column).to_numpy(zero_copy_only=False), value_type
        )
        for column, value_type in columns
    }

    entity_keys = [
        EntityKeyProto(
            join_keys=join_keys,
            entity_values=[proto_values_by_column[k][idx] for k in join_keys],
        )
        for idx in range(table.num_rows)
    ]

    # Serialize the features per row
    feature_dict = {
        feature.name: proto_values_by_column[feature.name]
        for feature in feature_view.features
    }
    features = [dict(zip(feature_dict, vars)) for vars in zip(*feature_dict.values())]

    # Convert event_timestamps
    event_timestamps = [
        _coerce_datetime(val)
        for val in pandas.to_datetime(
            table.column(feature_view.batch_source.event_timestamp_column).to_numpy(
                zero_copy_only=False
            )
        )
    ]

    # Convert created_timestamps if they exist
    if feature_view.batch_source.created_timestamp_column:
        created_timestamps = [
            _coerce_datetime(val)
            for val in pandas.to_datetime(
                table.column(
                    feature_view.batch_source.created_timestamp_column
                ).to_numpy(zero_copy_only=False)
            )
        ]
    else:
        created_timestamps = [None] * table.num_rows

    return list(zip(entity_keys, features, event_timestamps, created_timestamps))
