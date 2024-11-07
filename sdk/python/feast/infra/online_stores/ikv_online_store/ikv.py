from datetime import datetime, timezone
from typing import (
    Any,
    Callable,
    Dict,
    Iterator,
    List,
    Literal,
    Optional,
    Sequence,
    Tuple,
)

from google.protobuf.timestamp_pb2 import Timestamp
from ikvpy.client import IKVReader, IKVWriter
from ikvpy.clientoptions import ClientOptions, ClientOptionsBuilder
from ikvpy.document import IKVDocument, IKVDocumentBuilder
from ikvpy.factory import create_new_reader, create_new_writer
from pydantic import StrictStr

from feast import Entity, FeatureView, utils
from feast.infra.online_stores.helpers import compute_entity_id
from feast.infra.online_stores.online_store import OnlineStore
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from feast.repo_config import FeastConfigBaseModel, RepoConfig

PRIMARY_KEY_FIELD_NAME: str = "_entity_key"
EVENT_CREATION_TIMESTAMP_FIELD_NAME: str = "_event_timestamp"
CREATION_TIMESTAMP_FIELD_NAME: str = "_created_timestamp"


class IKVOnlineStoreConfig(FeastConfigBaseModel):
    """Online store config for IKV store"""

    type: Literal["ikv"] = "ikv"
    """Online store type selector"""

    account_id: StrictStr
    """(Required) IKV account id"""

    account_passkey: StrictStr
    """(Required) IKV account passkey"""

    store_name: StrictStr
    """(Required) IKV store name"""

    mount_directory: Optional[StrictStr] = None
    """(Required only for reader) IKV mount point i.e. directory for storing IKV data locally."""


class IKVOnlineStore(OnlineStore):
    """
    IKV (inlined.io key value) store implementation of the online store interface.
    """

    # lazy initialization
    _reader: Optional[IKVReader] = None
    _writer: Optional[IKVWriter] = None

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
        self._init_writer(config=config)
        assert self._writer is not None

        for entity_key, features, event_timestamp, _ in data:
            entity_id: str = compute_entity_id(
                entity_key,
                entity_key_serialization_version=config.entity_key_serialization_version,
            )
            document: IKVDocument = IKVOnlineStore._create_document(
                entity_id, table, features, event_timestamp
            )
            self._writer.upsert_fields(document)
            if progress:
                progress(1)

    def online_read(
        self,
        config: RepoConfig,
        table: FeatureView,
        entity_keys: List[EntityKeyProto],
        requested_features: Optional[List[str]] = None,
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
        self._init_reader(config=config)

        if not len(entity_keys):
            return []

        # create IKV primary keys
        primary_keys = [
            compute_entity_id(ek, config.entity_key_serialization_version)
            for ek in entity_keys
        ]

        # create IKV field names
        if requested_features is None:
            requested_features = []

        field_names: List[Optional[str]] = [None] * (1 + len(requested_features))
        field_names[0] = EVENT_CREATION_TIMESTAMP_FIELD_NAME
        for i, fn in enumerate(requested_features):
            field_names[i + 1] = IKVOnlineStore._create_ikv_field_name(table, fn)

        assert self._reader is not None
        value_iter = self._reader.multiget_bytes_values(
            bytes_primary_keys=[],
            str_primary_keys=primary_keys,
            field_names=field_names,
        )

        # decode results
        return [
            IKVOnlineStore._decode_fields_for_primary_key(
                requested_features, value_iter
            )
            for _ in range(0, len(primary_keys))
        ]

    @staticmethod
    def _decode_fields_for_primary_key(
        requested_features: List[str], value_iter: Iterator[Optional[bytes]]
    ) -> Tuple[Optional[datetime], Optional[Dict[str, ValueProto]]]:
        # decode timestamp
        dt: Optional[datetime] = None
        dt_bytes = next(value_iter)
        if dt_bytes:
            proto_timestamp = Timestamp()
            proto_timestamp.ParseFromString(dt_bytes)
            dt = datetime.fromtimestamp(proto_timestamp.seconds, tz=timezone.utc)

        # decode other features
        features = {}
        for requested_feature in requested_features:
            value_proto_bytes: Optional[bytes] = next(value_iter)
            if value_proto_bytes:
                value_proto = ValueProto()
                value_proto.ParseFromString(value_proto_bytes)
                features[requested_feature] = value_proto

        return dt, features

    def update(
        self,
        config: RepoConfig,
        tables_to_delete: Sequence[FeatureView],
        tables_to_keep: Sequence[FeatureView],
        entities_to_delete: Sequence[Entity],
        entities_to_keep: Sequence[Entity],
        partial: bool,
    ):
        """
        Reconciles cloud resources with the specified set of Feast objects.

        Args:
            config: The config for the current feature store.
            tables_to_delete: Feature views whose corresponding infrastructure should be deleted.
            tables_to_keep: Feature views whose corresponding infrastructure should not be deleted, and
                may need to be updated.
            entities_to_delete: Entities whose corresponding infrastructure should be deleted.
            entities_to_keep: Entities whose corresponding infrastructure should not be deleted, and
                may need to be updated.
            partial: If true, tables_to_delete and tables_to_keep are not exhaustive lists, so
                infrastructure corresponding to other feature views should be not be touched.
        """
        self._init_writer(config=config)
        assert self._writer is not None

        # note: we assume tables_to_keep does not overlap with tables_to_delete

        for feature_view in tables_to_delete:
            # each field in an IKV document is prefixed by the feature-view's name
            self._writer.drop_fields_by_name_prefix([feature_view.name])

    def teardown(
        self,
        config: RepoConfig,
        tables: Sequence[FeatureView],
        entities: Sequence[Entity],
    ):
        """
        Tears down all cloud resources for the specified set of Feast objects.

        Args:
            config: The config for the current feature store.
            tables: Feature views whose corresponding infrastructure should be deleted.
            entities: Entities whose corresponding infrastructure should be deleted.
        """
        self._init_writer(config=config)
        assert self._writer is not None

        # drop fields corresponding to this feature-view
        for feature_view in tables:
            self._writer.drop_fields_by_name_prefix([feature_view.name])

        # shutdown clients
        self._writer.shutdown()
        self._writer = None

        if self._reader is not None:
            self._reader.shutdown()
            self._reader = None

    @staticmethod
    def _create_ikv_field_name(feature_view: FeatureView, feature_name: str) -> str:
        return "{}_{}".format(feature_view.name, feature_name)

    @staticmethod
    def _create_document(
        entity_id: str,
        feature_view: FeatureView,
        values: Dict[str, ValueProto],
        event_timestamp: datetime,
    ) -> IKVDocument:
        """Converts feast key-value pairs into an IKV document."""

        # initialie builder by inserting primary key and row creation timestamp
        event_timestamp_seconds = int(utils.make_tzaware(event_timestamp).timestamp())
        event_timestamp_seconds_proto = Timestamp()
        event_timestamp_seconds_proto.seconds = event_timestamp_seconds

        # event_timestamp_str: str = utils.make_tzaware(event_timestamp).isoformat()
        builder = (
            IKVDocumentBuilder()
            .put_string_field(PRIMARY_KEY_FIELD_NAME, entity_id)
            .put_bytes_field(
                EVENT_CREATION_TIMESTAMP_FIELD_NAME,
                event_timestamp_seconds_proto.SerializeToString(),
            )
        )

        for feature_name, feature_value in values.items():
            field_name = IKVOnlineStore._create_ikv_field_name(
                feature_view, feature_name
            )
            builder.put_bytes_field(field_name, feature_value.SerializeToString())

        return builder.build()

    def _init_writer(self, config: RepoConfig):
        """Initializes ikv writer client."""
        # initialize writer
        if self._writer is None:
            online_config = config.online_store
            assert isinstance(online_config, IKVOnlineStoreConfig)
            client_options = IKVOnlineStore._config_to_client_options(online_config)

            self._writer = create_new_writer(client_options)
            self._writer.startup()  # blocking operation

    def _init_reader(self, config: RepoConfig):
        """Initializes ikv reader client."""
        # initialize reader
        if self._reader is None:
            online_config = config.online_store
            assert isinstance(online_config, IKVOnlineStoreConfig)
            client_options = IKVOnlineStore._config_to_client_options(online_config)

            if online_config.mount_directory and len(online_config.mount_directory) > 0:
                self._reader = create_new_reader(client_options)
                self._reader.startup()  # blocking operation

    @staticmethod
    def _config_to_client_options(config: IKVOnlineStoreConfig) -> ClientOptions:
        """Utility for IKVOnlineStoreConfig to IKV ClientOptions conversion."""
        builder = (
            ClientOptionsBuilder()
            .with_account_id(config.account_id)
            .with_account_passkey(config.account_passkey)
            .with_store_name(config.store_name)
        )

        if config.mount_directory and len(config.mount_directory) > 0:
            builder = builder.with_mount_directory(config.mount_directory)

        return builder.build()
