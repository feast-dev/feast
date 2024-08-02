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
import json
import logging
from datetime import datetime
from typing import (Any, Callable, Dict, List, Literal, Optional, Sequence,
                    Tuple)

from pydantic import StrictStr

from feast import Entity, FeatureView, RepoConfig
from feast.infra.online_stores.online_store import OnlineStore
from feast.permissions.client.http_auth_requests_wrapper import \
    get_http_auth_requests_session
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from feast.repo_config import FeastConfigBaseModel
from feast.type_map import python_values_to_proto_values
from feast.value_type import ValueType

logger = logging.getLogger(__name__)


class RemoteOnlineStoreConfig(FeastConfigBaseModel):
    """Remote Online store config for remote online store"""

    type: Literal["remote"] = "remote"
    """Online store type selector"""

    path: StrictStr = "http://localhost:6566"
    """ str: Path to metadata store.
    If type is 'remote', then this is a URL for registry server """


class RemoteOnlineStore(OnlineStore):
    """
    remote online store implementation wrapper to communicate with feast online server.
    """

    def online_write_batch(
        self,
        config: RepoConfig,
        table: FeatureView,
        data: List[
            Tuple[EntityKeyProto, Dict[str, ValueProto], datetime, Optional[datetime]]
        ],
        progress: Optional[Callable[[int], Any]],
    ) -> None:
        raise NotImplementedError

    def online_read(
        self,
        config: RepoConfig,
        table: FeatureView,
        entity_keys: List[EntityKeyProto],
        requested_features: Optional[List[str]] = None,
    ) -> List[Tuple[Optional[datetime], Optional[Dict[str, ValueProto]]]]:
        assert isinstance(config.online_store, RemoteOnlineStoreConfig)
        config.online_store.__class__ = RemoteOnlineStoreConfig

        req_body = self._construct_online_read_api_json_request(
            entity_keys, table, requested_features
        )
        response = get_http_auth_requests_session(config.auth_config).post(
            f"{config.online_store.path}/get-online-features", data=req_body
        )
        if response.status_code == 200:
            logger.debug("Able to retrieve the online features from feature server.")
            response_json = json.loads(response.text)
            event_ts = self._get_event_ts(response_json)
            # Iterating over results and converting the API results in column format to row format.
            result_tuples: List[
                Tuple[Optional[datetime], Optional[Dict[str, ValueProto]]]
            ] = []
            for feature_value_index in range(len(entity_keys)):
                feature_values_dict: Dict[str, ValueProto] = dict()
                for index, feature_name in enumerate(
                    response_json["metadata"]["feature_names"]
                ):
                    if (
                        requested_features is not None
                        and feature_name in requested_features
                    ):
                        if (
                            response_json["results"][index]["statuses"][
                                feature_value_index
                            ]
                            == "PRESENT"
                        ):
                            message = python_values_to_proto_values(
                                [
                                    response_json["results"][index]["values"][
                                        feature_value_index
                                    ]
                                ],
                                ValueType.UNKNOWN,
                            )
                            feature_values_dict[feature_name] = message[0]
                        else:
                            feature_values_dict[feature_name] = ValueProto()
                result_tuples.append((event_ts, feature_values_dict))
            return result_tuples
        else:
            error_msg = f"Unable to retrieve the online store data using feature server API. Error_code={response.status_code}, error_message={response.reason}"
            logger.error(error_msg)
            raise RuntimeError(error_msg)

    def _construct_online_read_api_json_request(
        self,
        entity_keys: List[EntityKeyProto],
        table: FeatureView,
        requested_features: Optional[List[str]] = None,
    ) -> str:
        api_requested_features = []
        if requested_features is not None:
            for requested_feature in requested_features:
                api_requested_features.append(f"{table.name}:{requested_feature}")

        entity_values = []
        entity_key = ""
        for row in entity_keys:
            entity_key = row.join_keys[0]
            entity_values.append(
                getattr(row.entity_values[0], row.entity_values[0].WhichOneof("val"))
            )

        req_body = json.dumps(
            {
                "features": api_requested_features,
                "entities": {entity_key: entity_values},
            }
        )
        return req_body

    def _get_event_ts(self, response_json) -> datetime:
        event_ts = ""
        if len(response_json["results"]) > 1:
            event_ts = response_json["results"][1]["event_timestamps"][0]
        return datetime.fromisoformat(event_ts.replace("Z", "+00:00"))

    def update(
        self,
        config: RepoConfig,
        tables_to_delete: Sequence[FeatureView],
        tables_to_keep: Sequence[FeatureView],
        entities_to_delete: Sequence[Entity],
        entities_to_keep: Sequence[Entity],
        partial: bool,
    ):
        pass

    def teardown(
        self,
        config: RepoConfig,
        tables: Sequence[FeatureView],
        entities: Sequence[Entity],
    ):
        pass
