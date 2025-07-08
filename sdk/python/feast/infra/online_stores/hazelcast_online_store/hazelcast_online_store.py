#
#  Copyright 2019 The Feast Authors
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      https://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#

"""
Hazelcast online store for Feast.
"""

import base64
import threading
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Literal, Optional, Sequence, Tuple

from hazelcast.client import HazelcastClient
from hazelcast.core import HazelcastJsonValue
from hazelcast.discovery import HazelcastCloudDiscovery
from pydantic import StrictStr

from feast import Entity, FeatureView, RepoConfig
from feast.infra.key_encoding_utils import serialize_entity_key
from feast.infra.online_stores.online_store import OnlineStore
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from feast.repo_config import FeastConfigBaseModel

# Exception messages
EXCEPTION_HAZELCAST_UNEXPECTED_CONFIGURATION_CLASS = (
    "Unexpected configuration object (not a HazelcastOnlineStoreConfig instance)"
)

# Hazelcast schema names for each field
D_FEATURE_NAME = "feature_name"
D_FEATURE_VALUE = "feature_value"
D_ENTITY_KEY = "entity_key"
D_EVENT_TS = "event_ts"
D_CREATED_TS = "created_ts"


class HazelcastInvalidConfig(Exception):
    def __init__(self, msg: str):
        super().__init__(msg)


class HazelcastOnlineStoreConfig(FeastConfigBaseModel):
    """Online store config for Hazelcast store"""

    type: Literal["hazelcast"] = "hazelcast"
    """Online store type selector"""

    cluster_name: StrictStr = "dev"
    """Name of the cluster you want to connect. The default cluster name is `dev`"""

    cluster_members: Optional[List[str]] = ["localhost:5701"]
    """List of member addresses which is connected to your cluster"""

    discovery_token: Optional[StrictStr] = ""
    """The discovery token of your Hazelcast Viridian cluster"""

    ssl_cafile_path: Optional[StrictStr] = ""
    """Absolute path of CA certificates in PEM format."""

    ssl_certfile_path: Optional[StrictStr] = ""
    """Absolute path of the client certificate in PEM format."""

    ssl_keyfile_path: Optional[StrictStr] = ""
    """Absolute path of the private key file for the client certificate in the PEM format."""

    ssl_password: Optional[StrictStr] = ""
    """Password for decrypting the keyfile if it is encrypted."""

    key_ttl_seconds: Optional[int] = 0
    """Hazelcast key bin TTL (in seconds) for expiring entities"""


class HazelcastOnlineStore(OnlineStore):
    """
    Hazelcast online store implementation for Feast

    Attributes:
        _client: Hazelcast client connection.
        _lock: Prevent race condition while creating the client connection
    """

    _client: Optional[HazelcastClient] = None
    _lock = threading.Lock()

    def _get_client(self, config: HazelcastOnlineStoreConfig):
        """
        Establish the client connection to Hazelcast cluster, if not yet created,
        and return it.

        The established client connection could be Hazelcast Viridian and SSL enabled based on user config.

        Args:
            config: The HazelcastOnlineStoreConfig for the online store.
        """
        if self._client is None:
            with self._lock:
                if self._client is None:
                    if config.discovery_token != "":
                        HazelcastCloudDiscovery._CLOUD_URL_BASE = (
                            "api.viridian.hazelcast.com"
                        )
                        self._client = HazelcastClient(
                            cluster_name=config.cluster_name,
                            cloud_discovery_token=config.discovery_token,
                            statistics_enabled=True,
                            ssl_enabled=True,
                            ssl_cafile=config.ssl_cafile_path,
                            ssl_certfile=config.ssl_certfile_path,
                            ssl_keyfile=config.ssl_keyfile_path,
                            ssl_password=config.ssl_password,
                        )
                    elif config.ssl_cafile_path != "":
                        self._client = HazelcastClient(
                            cluster_name=config.cluster_name,
                            statistics_enabled=True,
                            ssl_enabled=True,
                            ssl_cafile=config.ssl_cafile_path,
                            ssl_certfile=config.ssl_certfile_path,
                            ssl_keyfile=config.ssl_keyfile_path,
                            ssl_password=config.ssl_password,
                        )
                    else:
                        self._client = HazelcastClient(
                            statistics_enabled=True,
                            cluster_members=config.cluster_members,
                            cluster_name=config.cluster_name,
                        )
        return self._client

    def online_write_batch(
        self,
        config: RepoConfig,
        table: FeatureView,
        data: List[
            Tuple[EntityKeyProto, Dict[str, ValueProto], datetime, Optional[datetime]]
        ],
        progress: Optional[Callable[[int], Any]],
    ) -> None:
        online_store_config = config.online_store
        if not isinstance(online_store_config, HazelcastOnlineStoreConfig):
            raise HazelcastInvalidConfig(
                EXCEPTION_HAZELCAST_UNEXPECTED_CONFIGURATION_CLASS
            )

        client = self._get_client(online_store_config)
        fv_map = client.get_map(_map_name(config.project, table))

        for entity_key, values, event_ts, created_ts in data:
            entity_key_str = base64.b64encode(
                serialize_entity_key(
                    entity_key,
                    entity_key_serialization_version=3,
                )
            ).decode("utf-8")
            event_ts_utc = event_ts.astimezone(tz=timezone.utc).timestamp()
            created_ts_utc = 0.0
            if created_ts is not None:
                created_ts_utc = created_ts.astimezone(tz=timezone.utc).timestamp()
            for feature_name, value in values.items():
                feature_value = base64.b64encode(value.SerializeToString()).decode(
                    "utf-8"
                )
                hz_combined_key = entity_key_str + feature_name
                fv_map.put(
                    hz_combined_key,
                    HazelcastJsonValue(
                        {
                            D_ENTITY_KEY: entity_key_str,
                            D_FEATURE_NAME: feature_name,
                            D_FEATURE_VALUE: feature_value,
                            D_EVENT_TS: event_ts_utc,
                            D_CREATED_TS: created_ts_utc,
                        }
                    ),
                    online_store_config.key_ttl_seconds,
                )
                if progress:
                    progress(1)

    def online_read(
        self,
        config: RepoConfig,
        table: FeatureView,
        entity_keys: List[EntityKeyProto],
        requested_features: Optional[List[str]] = None,
    ) -> List[Tuple[Optional[datetime], Optional[Dict[str, ValueProto]]]]:
        online_store_config = config.online_store
        if not isinstance(online_store_config, HazelcastOnlineStoreConfig):
            raise HazelcastInvalidConfig(
                EXCEPTION_HAZELCAST_UNEXPECTED_CONFIGURATION_CLASS
            )

        client = self._get_client(online_store_config)
        entries: List[Tuple[Optional[datetime], Optional[Dict[str, ValueProto]]]] = []
        fv_map = client.get_map(_map_name(config.project, table))

        hz_keys = []
        entity_keys_str = {}
        for entity_key in entity_keys:
            entity_key_str = base64.b64encode(
                serialize_entity_key(
                    entity_key,
                    entity_key_serialization_version=3,
                )
            ).decode("utf-8")
            if requested_features:
                feature_keys = [
                    entity_key_str + feature for feature in requested_features
                ]
            else:
                feature_keys = [entity_key_str + f.name for f in table.features]
            hz_keys.extend(feature_keys)
            entity_keys_str[entity_key_str] = feature_keys

        data = fv_map.get_all(hz_keys).result()
        entities = []
        for key in hz_keys:
            try:
                data[key] = data[key].loads()
                entities.append(data[key][D_ENTITY_KEY])
            except KeyError:
                continue

        for key in entity_keys_str:
            if key in entities:
                entry = {}
                event_ts = None
                for f_key in entity_keys_str[key]:
                    row = data[f_key]
                    value = ValueProto()
                    value.ParseFromString(base64.b64decode(row[D_FEATURE_VALUE]))
                    entry[row[D_FEATURE_NAME]] = value
                    event_ts = datetime.fromtimestamp(row[D_EVENT_TS], tz=timezone.utc)
                entries.append((event_ts, entry))
            else:
                entries.append((None, None))
        return entries

    def update(
        self,
        config: RepoConfig,
        tables_to_delete: Sequence[FeatureView],
        tables_to_keep: Sequence[FeatureView],
        entities_to_delete: Sequence[Entity],
        entities_to_keep: Sequence[Entity],
        partial: bool,
    ):
        online_store_config = config.online_store
        if not isinstance(online_store_config, HazelcastOnlineStoreConfig):
            raise HazelcastInvalidConfig(
                EXCEPTION_HAZELCAST_UNEXPECTED_CONFIGURATION_CLASS
            )

        client = self._get_client(online_store_config)
        project = config.project

        for table in tables_to_keep:
            client.sql.execute(
                f"""CREATE OR REPLACE MAPPING {_map_name(project, table)} (
                        __key VARCHAR,
                        {D_ENTITY_KEY} VARCHAR,
                        {D_FEATURE_NAME} VARCHAR,
                        {D_FEATURE_VALUE} VARCHAR,
                        {D_EVENT_TS} DECIMAL,
                        {D_CREATED_TS} DECIMAL
                    )
                    TYPE IMap
                    OPTIONS (
                        'keyFormat' = 'varchar',
                        'valueFormat' = 'json-flat'
                    )
                """
            ).result()

        for table in tables_to_delete:
            client.sql.execute(
                f"DELETE FROM {_map_name(config.project, table)}"
            ).result()
            client.sql.execute(
                f"DROP MAPPING IF EXISTS {_map_name(config.project, table)}"
            ).result()

    def teardown(
        self,
        config: RepoConfig,
        tables: Sequence[FeatureView],
        entities: Sequence[Entity],
    ):
        online_store_config = config.online_store
        if not isinstance(online_store_config, HazelcastOnlineStoreConfig):
            raise HazelcastInvalidConfig(
                EXCEPTION_HAZELCAST_UNEXPECTED_CONFIGURATION_CLASS
            )

        client = self._get_client(online_store_config)
        project = config.project

        for table in tables:
            client.sql.execute(f"DELETE FROM {_map_name(config.project, table)}")
            client.sql.execute(f"DROP MAPPING IF EXISTS {_map_name(project, table)}")


def _map_name(project: str, table: FeatureView) -> str:
    return f"{project}_{table.name}"
