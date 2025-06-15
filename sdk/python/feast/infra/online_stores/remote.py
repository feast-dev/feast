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
from collections import defaultdict
from datetime import datetime
from typing import Any, Callable, Dict, List, Literal, Optional, Sequence, Tuple

import requests
from pydantic import StrictStr

from feast import Entity, FeatureView, RepoConfig
from feast.infra.online_stores.helpers import _to_naive_utc
from feast.infra.online_stores.online_store import OnlineStore
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from feast.repo_config import FeastConfigBaseModel
from feast.rest_error_handler import rest_error_handling_decorator
from feast.type_map import (
    feast_value_type_to_python_type,
    python_values_to_proto_values,
)
from feast.value_type import ValueType

logger = logging.getLogger(__name__)


class RemoteOnlineStoreConfig(FeastConfigBaseModel):
    """Remote Online store config for remote online store"""

    type: Literal["remote"] = "remote"
    """Online store type selector"""

    path: StrictStr = "http://localhost:6566"
    """ str: Path to metadata store.
    If type is 'remote', then this is a URL for registry server """

    cert: StrictStr = ""
    """ str: Path to the public certificate when the online server starts in TLS(SSL) mode. This may be needed if the online server started with a self-signed certificate, typically this file ends with `*.crt`, `*.cer`, or `*.pem`.
    If type is 'remote', then this configuration is needed to connect to remote online server in TLS mode. """


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
        """
        Writes a batch of feature rows to the remote online store via the remote API.
        """
        assert isinstance(config.online_store, RemoteOnlineStoreConfig)
        config.online_store.__class__ = RemoteOnlineStoreConfig

        columnar_data: Dict[str, List[Any]] = defaultdict(list)

        # Iterate through each row to populate columnar data directly
        for entity_key_proto, feature_values_proto, event_ts, created_ts in data:
            # Populate entity key values
            for join_key, entity_value_proto in zip(
                entity_key_proto.join_keys, entity_key_proto.entity_values
            ):
                columnar_data[join_key].append(
                    feast_value_type_to_python_type(entity_value_proto)
                )

            # Populate feature values
            for feature_name, feature_value_proto in feature_values_proto.items():
                columnar_data[feature_name].append(
                    feast_value_type_to_python_type(feature_value_proto)
                )

            # Populate timestamps
            columnar_data["event_timestamp"].append(_to_naive_utc(event_ts).isoformat())
            columnar_data["created"].append(
                _to_naive_utc(created_ts).isoformat() if created_ts else None
            )

        req_body = {
            "feature_view_name": table.name,
            "df": columnar_data,
            "allow_registry_cache": False,
        }

        response = post_remote_online_write(config=config, req_body=req_body)

        if response.status_code != 200:
            error_msg = f"Unable to write online store data using feature server API. Error_code={response.status_code}, error_message={response.text}"
            logger.error(error_msg)
            raise RuntimeError(error_msg)

        if progress:
            data_length = len(data)
            logger.info(
                f"Writing {data_length} rows to the remote store for feature view {table.name}."
            )
            progress(data_length)

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
        response = get_remote_online_features(config=config, req_body=req_body)
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
            error_msg = f"Unable to retrieve the online store data using feature server API. Error_code={response.status_code}, error_message={response.text}"
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


@rest_error_handling_decorator
def get_remote_online_features(
    session: requests.Session, config: RepoConfig, req_body: str
) -> requests.Response:
    if config.online_store.cert:
        return session.post(
            f"{config.online_store.path}/get-online-features",
            data=req_body,
            verify=config.online_store.cert,
        )
    else:
        return session.post(
            f"{config.online_store.path}/get-online-features", data=req_body
        )


@rest_error_handling_decorator
def post_remote_online_write(
    session: requests.Session, config: RepoConfig, req_body: dict
) -> requests.Response:
    url = f"{config.online_store.path}/write-to-online-store"
    if config.online_store.cert:
        return session.post(url, json=req_body, verify=config.online_store.cert)
    else:
        return session.post(url, json=req_body)
