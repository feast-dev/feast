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
from feast.utils import _get_feature_view_vector_field_metadata
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

    def retrieve_online_documents(
        self,
        config: RepoConfig,
        table: FeatureView,
        requested_features: Optional[List[str]],
        embedding: Optional[List[float]],
        top_k: int,
        distance_metric: Optional[str] = "L2",
    ) -> List[
        Tuple[
            Optional[datetime],
            Optional[EntityKeyProto],
            Optional[ValueProto],
            Optional[ValueProto],
            Optional[ValueProto],
        ]
    ]:
        assert isinstance(config.online_store, RemoteOnlineStoreConfig)
        config.online_store.__class__ = RemoteOnlineStoreConfig

        req_body = self._construct_online_documents_api_json_request(
            table, requested_features, embedding, top_k, distance_metric
        )
        response = get_remote_online_documents(config=config, req_body=req_body)
        if response.status_code == 200:
            logger.debug("Able to retrieve the online documents from feature server.")
            response_json = json.loads(response.text)
            event_ts: Optional[datetime] = self._get_event_ts(response_json)

            # Create feature name to index mapping for efficient lookup
            feature_name_to_index = {
                name: idx
                for idx, name in enumerate(response_json["metadata"]["feature_names"])
            }

            vector_field_metadata = _get_feature_view_vector_field_metadata(table)

            # Process each result row
            num_results = len(response_json["results"][0]["values"])
            result_tuples = []

            for row_idx in range(num_results):
                # Extract values using helper methods
                feature_val = self._extract_requested_feature_value(
                    response_json, feature_name_to_index, requested_features, row_idx
                )
                vector_value = self._extract_vector_field_value(
                    response_json, feature_name_to_index, vector_field_metadata, row_idx
                )
                distance_val = self._extract_distance_value(
                    response_json, feature_name_to_index, "distance", row_idx
                )
                entity_key_proto = self._construct_entity_key_from_response(
                    response_json, row_idx, feature_name_to_index, table
                )

                result_tuples.append(
                    (
                        event_ts,
                        entity_key_proto,
                        feature_val,
                        vector_value,
                        distance_val,
                    )
                )

            return result_tuples
        else:
            error_msg = f"Unable to retrieve the online documents using feature server API. Error_code={response.status_code}, error_message={response.text}"
            logger.error(error_msg)
            raise RuntimeError(error_msg)

    def retrieve_online_documents_v2(
        self,
        config: RepoConfig,
        table: FeatureView,
        requested_features: Optional[List[str]],
        embedding: Optional[List[float]],
        top_k: int,
        distance_metric: Optional[str] = None,
        query_string: Optional[str] = None,
    ) -> List[
        Tuple[
            Optional[datetime],
            Optional[EntityKeyProto],
            Optional[Dict[str, ValueProto]],
        ]
    ]:
        assert isinstance(config.online_store, RemoteOnlineStoreConfig)
        config.online_store.__class__ = RemoteOnlineStoreConfig

        req_body = self._construct_online_documents_v2_api_json_request(
            table,
            requested_features,
            embedding,
            top_k,
            distance_metric,
            query_string,
            api_version=2,
        )
        response = get_remote_online_documents(config=config, req_body=req_body)
        if response.status_code == 200:
            logger.debug("Able to retrieve the online documents from feature server.")
            response_json = json.loads(response.text)
            event_ts: Optional[datetime] = self._get_event_ts(response_json)

            # Create feature name to index mapping for efficient lookup
            feature_name_to_index = {
                name: idx
                for idx, name in enumerate(response_json["metadata"]["feature_names"])
            }

            # Process each result row
            num_results = (
                len(response_json["results"][0]["values"])
                if response_json["results"]
                else 0
            )
            result_tuples = []

            for row_idx in range(num_results):
                # Build feature values dictionary for requested features
                feature_values_dict = {}

                if requested_features:
                    for feature_name in requested_features:
                        if feature_name in feature_name_to_index:
                            feature_idx = feature_name_to_index[feature_name]
                            if self._is_feature_present(
                                response_json, feature_idx, row_idx
                            ):
                                feature_values_dict[feature_name] = (
                                    self._extract_feature_value(
                                        response_json, feature_idx, row_idx
                                    )
                                )
                            else:
                                feature_values_dict[feature_name] = ValueProto()

                # Construct entity key proto using existing helper method
                entity_key_proto = self._construct_entity_key_from_response(
                    response_json, row_idx, feature_name_to_index, table
                )

                result_tuples.append(
                    (
                        event_ts,
                        entity_key_proto,
                        feature_values_dict if feature_values_dict else None,
                    )
                )

            return result_tuples
        else:
            error_msg = f"Unable to retrieve the online documents using feature server API. Error_code={response.status_code}, error_message={response.text}"
            logger.error(error_msg)
            raise RuntimeError(error_msg)

    def _extract_requested_feature_value(
        self,
        response_json: dict,
        feature_name_to_index: dict,
        requested_features: Optional[List[str]],
        row_idx: int,
    ) -> Optional[ValueProto]:
        """Extract the first available requested feature value."""
        if not requested_features:
            return ValueProto()

        for feature_name in requested_features:
            if feature_name in feature_name_to_index:
                feature_idx = feature_name_to_index[feature_name]
                if self._is_feature_present(response_json, feature_idx, row_idx):
                    return self._extract_feature_value(
                        response_json, feature_idx, row_idx
                    )

        return ValueProto()

    def _extract_vector_field_value(
        self,
        response_json: dict,
        feature_name_to_index: dict,
        vector_field_metadata,
        row_idx: int,
    ) -> Optional[ValueProto]:
        """Extract vector field value from response."""
        if (
            not vector_field_metadata
            or vector_field_metadata.name not in feature_name_to_index
        ):
            return ValueProto()

        vector_feature_idx = feature_name_to_index[vector_field_metadata.name]
        if self._is_feature_present(response_json, vector_feature_idx, row_idx):
            return self._extract_feature_value(
                response_json, vector_feature_idx, row_idx
            )

        return ValueProto()

    def _extract_distance_value(
        self,
        response_json: dict,
        feature_name_to_index: dict,
        distance_feature_name: str,
        row_idx: int,
    ) -> Optional[ValueProto]:
        """Extract distance/score value from response."""
        if not distance_feature_name:
            return ValueProto()

        distance_feature_idx = feature_name_to_index[distance_feature_name]
        if self._is_feature_present(response_json, distance_feature_idx, row_idx):
            distance_value = response_json["results"][distance_feature_idx]["values"][
                row_idx
            ]
            distance_val = ValueProto()
            distance_val.float_val = float(distance_value)
            return distance_val

        return ValueProto()

    def _is_feature_present(
        self, response_json: dict, feature_idx: int, row_idx: int
    ) -> bool:
        """Check if a feature is present in the response."""
        return response_json["results"][feature_idx]["statuses"][row_idx] == "PRESENT"

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

    def _construct_online_documents_api_json_request(
        self,
        table: FeatureView,
        requested_features: Optional[List[str]] = None,
        embedding: Optional[List[float]] = None,
        top_k: Optional[int] = None,
        distance_metric: Optional[str] = "L2",
    ) -> str:
        api_requested_features = []
        if requested_features is not None:
            for requested_feature in requested_features:
                api_requested_features.append(f"{table.name}:{requested_feature}")

        req_body = json.dumps(
            {
                "features": api_requested_features,
                "query": embedding,
                "top_k": top_k,
                "distance_metric": distance_metric,
            }
        )
        return req_body

    def _construct_online_documents_v2_api_json_request(
        self,
        table: FeatureView,
        requested_features: Optional[List[str]],
        embedding: Optional[List[float]],
        top_k: int,
        distance_metric: Optional[str] = None,
        query_string: Optional[str] = None,
        api_version: Optional[int] = 2,
    ) -> str:
        api_requested_features = []
        if requested_features is not None:
            for requested_feature in requested_features:
                api_requested_features.append(f"{table.name}:{requested_feature}")

        req_body = json.dumps(
            {
                "features": api_requested_features,
                "query": embedding,
                "top_k": top_k,
                "distance_metric": distance_metric,
                "query_string": query_string,
                "api_version": api_version,
            }
        )
        return req_body

    def _get_event_ts(self, response_json) -> datetime:
        event_ts = ""
        if len(response_json["results"]) > 1:
            event_ts = response_json["results"][1]["event_timestamps"][0]
        return datetime.fromisoformat(event_ts.replace("Z", "+00:00"))

    def _construct_entity_key_from_response(
        self,
        response_json: dict,
        row_idx: int,
        feature_name_to_index: dict,
        table: FeatureView,
    ) -> Optional[EntityKeyProto]:
        """Construct EntityKeyProto from response data."""
        # Use the feature view's join_keys to identify entity fields
        entity_fields = [
            join_key
            for join_key in table.join_keys
            if join_key in feature_name_to_index
        ]

        if not entity_fields:
            return None

        entity_key_proto = EntityKeyProto()
        entity_key_proto.join_keys.extend(entity_fields)

        for entity_field in entity_fields:
            if entity_field in feature_name_to_index:
                feature_idx = feature_name_to_index[entity_field]
                if self._is_feature_present(response_json, feature_idx, row_idx):
                    entity_value = self._extract_feature_value(
                        response_json, feature_idx, row_idx
                    )
                    entity_key_proto.entity_values.append(entity_value)

        return entity_key_proto if entity_key_proto.entity_values else None

    def _extract_feature_value(
        self, response_json: dict, feature_idx: int, row_idx: int
    ) -> ValueProto:
        """Extract and convert a feature value to ValueProto."""
        raw_value = response_json["results"][feature_idx]["values"][row_idx]
        if raw_value is None:
            return ValueProto()
        proto_values = python_values_to_proto_values([raw_value])
        return proto_values[0]

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
def get_remote_online_documents(
    session: requests.Session, config: RepoConfig, req_body: str
) -> requests.Response:
    if config.online_store.cert:
        return session.post(
            f"{config.online_store.path}/retrieve-online-documents",
            data=req_body,
            verify=config.online_store.cert,
        )
    else:
        return session.post(
            f"{config.online_store.path}/retrieve-online-documents", data=req_body
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
