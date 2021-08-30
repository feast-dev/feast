# Copyright 2021 The Feast Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple, Union

import pytz
from google.protobuf.timestamp_pb2 import Timestamp
from pydantic import StrictStr
from pydantic.schema import Literal

# TODO: release https://github.com/tikv/client-py
from tikv_client import RawClient

from feast import Entity, FeatureTable, utils
from feast.feature_view import FeatureView
from feast.infra.key_encoding_utils import serialize_entity_key
from feast.infra.online_stores.online_store import OnlineStore
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from feast.repo_config import FeastConfigBaseModel, RepoConfig


class TiKVOnlineStoreConfig(FeastConfigBaseModel):
    """ Online store config for local (TiKV-based) store """

    type: Literal["tikv", "feast.infra.online_stores.tikv.TiKVOnlineStore"] = "tikv"
    """ Online store type selector"""

    mode: StrictStr = "rawkv"
    """KV mode for TiKV
     format: rawkv or txnkv """

    pd_addresses: StrictStr = "127.0.0.1:2379"
    """PD Addresses for the TiKV Cluster
     format: host1:port,host2:port,host3:port eg. pd1:2379,pd2:2379,pd3:2379 """


class TiKVOnlineStore(OnlineStore):
    """
    OnlineStore is an object used for all interaction between Feast and the service used for offline storage of
    features.
    """

    _rawkv_client: Optional[RawClient] = None

    @staticmethod
    def _is_rawkv(online_store_config: TiKVOnlineStoreConfig) -> bool:
        return online_store_config.mode.lower() == "rawkv"

    @staticmethod
    def _encode_tikv_key(
        project: str, entity_key: EntityKeyProto, feature_view: str, feature_name: str
    ):
        entity_key_bin = serialize_entity_key(entity_key)
        tikv_key = (
            f"{project}:".encode("utf8")
            + entity_key_bin
            + f":{feature_view}:{feature_name}".encode("utf8")
        )
        return tikv_key

    def _get_rawkv_client(self, online_store_config: TiKVOnlineStoreConfig):
        if not self._rawkv_client:
            _rawkv_client = RawClient.connect(online_store_config.pd_addresses)
        return _rawkv_client

    def update(
        self,
        config: RepoConfig,
        tables_to_delete: Sequence[Union[FeatureTable, FeatureView]],
        tables_to_keep: Sequence[Union[FeatureTable, FeatureView]],
        entities_to_delete: Sequence[Entity],
        entities_to_keep: Sequence[Entity],
        partial: bool,
    ):
        """
        There's currently no setup done for TiKV.
        """
        pass

    def teardown(
        self,
        config: RepoConfig,
        tables: Sequence[Union[FeatureTable, FeatureView]],
        entities: Sequence[Entity],
    ):
        """
        There's currently no teardown done for TiKV.
        """
        pass

    def online_write_batch(
        self,
        config: RepoConfig,
        table: Union[FeatureTable, FeatureView],
        data: List[
            Tuple[EntityKeyProto, Dict[str, ValueProto], datetime, Optional[datetime]]
        ],
        progress: Optional[Callable[[int], Any]],
    ) -> None:
        online_store_config = config.online_store
        assert isinstance(online_store_config, TiKVOnlineStoreConfig)
        assert self._is_rawkv(online_store_config)

        client = self._get_rawkv_client(online_store_config)
        project = config.project
        feature_view = table.name

        for entity_key, values, timestamp, created_ts in data:
            ts = Timestamp()
            ts.seconds = int(utils.make_tzaware(timestamp).timestamp())

            if created_ts is not None:
                ex = Timestamp()
                ex.seconds = int(utils.make_tzaware(created_ts).timestamp())
            else:
                ex = ts

            ts_key = self._encode_tikv_key(project, entity_key, feature_view, "_ts")
            ts_value = ts.SerializeToString()
            client.put(ts_key, ts_value)

            ex_key = self._encode_tikv_key(project, entity_key, feature_view, "_ex")
            ex_value = ex.SerializeToString()
            client.put(ex_key, ex_value)

            for feature_name, val in values.items():
                tikv_key = self._encode_tikv_key(
                    project, entity_key, feature_view, feature_name
                )
                tikv_value = val.SerializeToString()
                client.put(tikv_key, tikv_value)
            if progress:
                progress(1)

    def online_read(
        self,
        config: RepoConfig,
        table: Union[FeatureTable, FeatureView],
        entity_keys: List[EntityKeyProto],
        requested_features: Optional[List[str]] = None,
    ) -> List[Tuple[Optional[datetime], Optional[Dict[str, ValueProto]]]]:
        online_store_config = config.online_store
        assert isinstance(online_store_config, TiKVOnlineStoreConfig)
        assert self._is_rawkv(online_store_config)

        client = self._get_rawkv_client(online_store_config)
        feature_view = table.name
        project = config.project

        result: List[Tuple[Optional[datetime], Optional[Dict[str, ValueProto]]]] = []

        if not requested_features:
            requested_features = [f.name for f in table.features]

        for entity_key in entity_keys:
            res = {}
            for feature_name in requested_features:
                tikv_key = self._encode_tikv_key(
                    project, entity_key, feature_view, feature_name
                )
                val_bin = client.get(tikv_key)
                val = ValueProto()
                if val_bin:
                    val.ParseFromString(val_bin)
                res[feature_name] = val

            ts_key = self._encode_tikv_key(project, entity_key, feature_view, "_ts")
            ts_value = client.get(ts_key)
            res_ts = Timestamp()
            if ts_value:
                res_ts.ParseFromString(ts_value)

            if not res:
                result.append((None, None))
            else:
                timestamp = datetime.fromtimestamp(res_ts.seconds)
                result.append((timestamp, res))
                print(result)
        return result
