# Copyright 2022 The Feast Authors
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
import os
import random
import time
from datetime import datetime
from typing import Any, Callable, Dict, List, Literal, Optional, Sequence, Tuple, cast

import requests
from rockset.exceptions import BadRequestException, RocksetException
from rockset.models import QueryRequestSql
from rockset.query_paginator import QueryPaginator
from rockset.rockset_client import RocksetClient

from feast.entity import Entity
from feast.feature_view import FeatureView
from feast.infra.online_stores.helpers import compute_entity_id
from feast.infra.online_stores.online_store import OnlineStore
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from feast.repo_config import FeastConfigBaseModel, RepoConfig
from feast.usage import log_exceptions_and_usage

logger = logging.getLogger(__name__)


class RocksetOnlineStoreConfig(FeastConfigBaseModel):
    """Online store config for Rockset store"""

    type: Literal["rockset"] = "rockset"
    """Online store type selector"""

    api_key: Optional[str] = None
    """Api Key to be used for Rockset Account. If not set the env var ROCKSET_APIKEY will be used."""

    host: Optional[str] = None
    """The Host Url for Rockset requests. If not set the env var ROCKSET_APISERVER will be used."""

    read_pagination_batch_size: int = 100
    """Batch size of records that will be turned per page when paginating a batched read"""

    collection_created_timeout_secs: int = 60
    """The amount of time, in seconds, we will wait for the collection to become visible to the API"""

    collection_ready_timeout_secs: int = 30 * 60
    """The amount of time, in seconds, we will wait for the collection to enter READY state"""

    fence_all_writes: bool = True
    """Whether to wait for all writes to be flushed from log and queryable. If False, documents that are written may not be seen immediately in subsequent reads"""

    fence_timeout_secs: int = 10 * 60
    """The amount of time we will wait, in seconds, for the write fence to be passed"""

    initial_request_backoff_secs: int = 2
    """Initial backoff, in seconds, we will wait between requests when polling for a response"""

    max_request_backoff_secs: int = 30
    """Initial backoff, in seconds, we will wait between requests when polling for a response"""

    max_request_attempts: int = 10 * 1000
    """The max amount of times we will retry a failed request"""


class RocksetOnlineStore(OnlineStore):
    """
    Rockset implementation of the online store interface.

    Attributes:
        _rockset_client: Rockset openapi client.
    """

    _rockset_client = None

    @log_exceptions_and_usage(online_store="rockset")
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
        Write a batch of feature rows to online Rockset store.

        Args:
            config: The RepoConfig for the current FeatureStore.
            table: Feast FeatureView.
            data: a list of quadruplets containing Feature data. Each quadruplet contains an Entity Key,
            a dict containing feature values, an event timestamp for the row, and
            the created timestamp for the row if it exists.
            progress: Optional function to be called once every mini-batch of rows is written to
            the online store. Can be used to display progress.
        """

        online_config = config.online_store
        assert isinstance(online_config, RocksetOnlineStoreConfig)

        rs = self.get_rockset_client(online_config)
        collection_name = self.get_collection_name(config, table)

        # We need to deduplicate on entity_id and we will save the latest timestamp version.
        dedup_dict = {}
        for feature_vals in data:
            entity_key, features, timestamp, created_ts = feature_vals
            serialized_key = compute_entity_id(
                entity_key=entity_key,
                entity_key_serialization_version=config.entity_key_serialization_version,
            )

            if serialized_key not in dedup_dict:
                dedup_dict[serialized_key] = feature_vals
                continue

            # If the entity already existings in the dictionary ignore the entry if it has a lower timestamp.
            if timestamp <= dedup_dict[serialized_key][2]:
                continue

            dedup_dict[serialized_key] = feature_vals

        request_batch = []
        for serialized_key, feature_vals in dedup_dict.items():
            document = {}
            entity_key, features, timestamp, created_ts = feature_vals
            document["_id"] = serialized_key

            # Rockset python client currently does not handle datetime correctly and will convert
            # to string instead of native Rockset DATETIME. This will be fixed, but until then we
            # use isoformat.
            document["event_ts"] = timestamp.isoformat()
            document["created_ts"] = (
                "" if created_ts is None else created_ts.isoformat()
            )
            for k, v in features.items():
                # Rockset client currently does not support bytes type.
                document[k] = v.SerializeToString().hex()

            # TODO: Implement async batching with retries.
            request_batch.append(document)

            if progress:
                progress(1)

        resp = rs.Documents.add_documents(
            collection=collection_name, data=request_batch
        )
        if online_config.fence_all_writes:
            self.wait_for_fence(rs, collection_name, resp["last_offset"], online_config)

        return None

    @log_exceptions_and_usage(online_store="rockset")
    def online_read(
        self,
        config: RepoConfig,
        table: FeatureView,
        entity_keys: List[EntityKeyProto],
        requested_features: Optional[List[str]] = None,
    ) -> List[Tuple[Optional[datetime], Optional[Dict[str, ValueProto]]]]:
        """
        Retrieve feature values from the online Rockset store.

        Args:
            config: The RepoConfig for the current FeatureStore.
            table: Feast FeatureView.
            entity_keys: a list of entity keys that should be read from the FeatureStore.
        """
        online_config = config.online_store
        assert isinstance(online_config, RocksetOnlineStoreConfig)

        rs = self.get_rockset_client(online_config)
        collection_name = self.get_collection_name(config, table)

        feature_list = ""
        if requested_features is not None:
            feature_list = ",".join(requested_features)

        entity_serialized_key_list = [
            compute_entity_id(
                k,
                entity_key_serialization_version=config.entity_key_serialization_version,
            )
            for k in entity_keys
        ]

        entity_query_str = ",".join(
            "'{id}'".format(id=s) for s in entity_serialized_key_list
        )

        query_str = f"""
            SELECT
                "_id",
                "event_ts",
                {feature_list}
            FROM
                {collection_name}
            WHERE
                "_id" IN ({entity_query_str})
        """

        feature_set = set()
        if requested_features:
            feature_set.update(requested_features)

        result_map = {}
        for page in QueryPaginator(
            rs,
            rs.Queries.query(
                sql=QueryRequestSql(
                    query=query_str,
                    paginate=True,
                    initial_paginate_response_doc_count=online_config.read_pagination_batch_size,
                )
            ),
        ):
            for doc in page:
                result = {}
                for k, v in doc.items():
                    if k not in feature_set:
                        # We want to skip deserializing values that are not feature values like bookeeping values.
                        continue

                    val = ValueProto()

                    # TODO: Remove bytes <-> string parsing once client supports bytes.
                    val.ParseFromString(bytes.fromhex(v))
                    result[k] = val
                result_map[doc["_id"]] = (
                    datetime.fromisoformat(doc["event_ts"]),
                    result,
                )

        results_list: List[
            Tuple[Optional[datetime], Optional[Dict[str, ValueProto]]]
        ] = []
        for key in entity_serialized_key_list:
            if key not in result_map:
                # If not found, we add a gap to let the client know.
                results_list.append((None, None))
                continue

            results_list.append(result_map[key])

        return results_list

    @log_exceptions_and_usage(online_store="rockset")
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
        Update tables from the Rockset Online Store.

        Args:
            config: The RepoConfig for the current FeatureStore.
            tables_to_delete: Tables to delete from the Rockset Online Store.
            tables_to_keep: Tables to keep in the Rockset Online Store.
        """
        online_config = config.online_store
        assert isinstance(online_config, RocksetOnlineStoreConfig)
        rs = self.get_rockset_client(online_config)

        created_collections = []
        for table_instance in tables_to_keep:
            try:
                collection_name = self.get_collection_name(config, table_instance)
                rs.Collections.create_file_upload_collection(name=collection_name)
                created_collections.append(collection_name)
            except BadRequestException as e:
                if self.parse_request_error_type(e) == "AlreadyExists":
                    # Table already exists nothing to do. We should still make sure it is ready though.
                    created_collections.append(collection_name)
                    continue
                raise

        for table_to_delete in tables_to_delete:
            self.delete_collection(
                rs, collection_name=self.get_collection_name(config, table_to_delete)
            )

        # Now wait for all collections to be READY.
        self.wait_for_ready_collections(
            rs, created_collections, online_config=online_config
        )

    @log_exceptions_and_usage(online_store="rockset")
    def teardown(
        self,
        config: RepoConfig,
        tables: Sequence[FeatureView],
        entities: Sequence[Entity],
    ):
        """
        Delete all collections from the Rockset Online Store.

        Args:
            config: The RepoConfig for the current FeatureStore.
            tables: Tables to delete from the feature repo.
        """
        online_config = config.online_store
        assert isinstance(online_config, RocksetOnlineStoreConfig)
        rs = self.get_rockset_client(online_config)
        for table in tables:
            self.delete_collection(
                rs, collection_name=self.get_collection_name(config, table)
            )

    def get_rockset_client(
        self, onlineConfig: RocksetOnlineStoreConfig
    ) -> RocksetClient:
        """
        Fetches the RocksetClient to be used for all requests for this online store based on the api
        configuration in the provided config. If no configuration provided local ENV vars will be used.

        Args:
            onlineConfig: The RocksetOnlineStoreConfig associated with this online store.
        """
        if self._rockset_client is not None:
            return self._rockset_client

        _api_key = (
            os.getenv("ROCKSET_APIKEY")
            if isinstance(onlineConfig.api_key, type(None))
            else onlineConfig.api_key
        )
        _host = (
            os.getenv("ROCKSET_APISERVER")
            if isinstance(onlineConfig.host, type(None))
            else onlineConfig.host
        )
        self._rockset_client = RocksetClient(host=_host, api_key=_api_key)
        return self._rockset_client

    @staticmethod
    def delete_collection(rs: RocksetClient, collection_name: str):
        """
        Deletes the collection  whose name was provided

        Args:
            rs: The RocksetClient to be used for the deletion.
            collection_name: The name of the collection to be deleted.
        """

        try:
            rs.Collections.delete(collection=collection_name)
        except RocksetException as e:
            if RocksetOnlineStore.parse_request_error_type(e) == "NotFound":
                logger.warning(
                    f"Trying to delete collection that does not exist {collection_name}"
                )
                return
            raise

    @staticmethod
    def get_collection_name(config: RepoConfig, feature_view: FeatureView) -> str:
        """
        Returns the collection name based on the provided config and FeatureView.

        Args:
            config: RepoConfig for the online store.
            feature_view: FeatureView that is backed by the returned collection name.

        Returns:
            The collection name as a string.
        """
        project_val = config.project if config.project else "feast"
        table_name = feature_view.name if feature_view.name else "feature_store"
        return f"{project_val}_{table_name}"

    @staticmethod
    def parse_request_error_type(e: RocksetException) -> str:
        """
        Parse a throw RocksetException. Will return a string representing the type of error that was thrown.

        Args:
            e: The RockException that is being parsed.

        Returns:
            Error type parsed as a string.
        """

        body_dict = json.loads(e.body)
        return body_dict["type"]

    @staticmethod
    def wait_for_fence(
        rs: RocksetClient,
        collection_name: str,
        last_offset: str,
        online_config: RocksetOnlineStoreConfig,
    ):
        """
        Waits until 'last_offset' is flushed and values are ready to be read. If wait lasts longer than the timeout specified in config
        a timeout exception will be throw.

        Args:
            rs: Rockset client that will be used to make all requests.
            collection_name: Collection associated with the offsets we are waiting for.
            last_offset: The actual offsets we are waiting to be flushed.
            online_config: The config that will be used to determine timeouts and backout configurations.
        """

        resource_path = (
            f"/v1/orgs/self/ws/commons/collections/{collection_name}/offsets/commit"
        )
        request = {"name": [last_offset]}

        headers = {}
        headers["Content-Type"] = "application/json"
        headers["Authorization"] = f"ApiKey {rs.api_client.configuration.api_key}"

        t_start = time.time()
        for num_attempts in range(online_config.max_request_attempts):
            delay = time.time() - t_start
            resp = requests.post(
                url=f"{rs.api_client.configuration.host}{resource_path}",
                json=request,
                headers=headers,
            )

            if resp.status_code == 200 and resp.json()["data"]["passed"] is True:
                break

            if delay > online_config.fence_timeout_secs:
                raise TimeoutError(
                    f"Write to collection {collection_name} at offset {last_offset} was not available for read after {delay} secs"
                )

            if resp.status_code == 429:
                RocksetOnlineStore.backoff_sleep(num_attempts, online_config)
                continue
            elif resp.status_code != 200:
                raise Exception(f"[{resp.status_code}]: {resp.reason}")

            RocksetOnlineStore.backoff_sleep(num_attempts, online_config)

    @staticmethod
    def wait_for_ready_collections(
        rs: RocksetClient,
        collection_names: List[str],
        online_config: RocksetOnlineStoreConfig,
    ):
        """
        Waits until all collections provided have entered READY state and can accept new documents. If wait
        lasts longer than timeout a TimeoutError exception will be thrown.

        Args:
            rs: Rockset client that will be used to make all requests.
            collection_names: All collections that we will wait for.
            timeout: The max amount of time we will wait for the collections to become READY.
        """

        t_start = time.time()
        for cname in collection_names:
            # We will wait until the provided timeout for all collections to become READY.
            for num_attempts in range(online_config.max_request_attempts):
                resp = None
                delay = time.time() - t_start
                try:
                    resp = rs.Collections.get(collection=cname)
                except RocksetException as e:
                    error_type = RocksetOnlineStore.parse_request_error_type(e)
                    if error_type == "NotFound":
                        if delay > online_config.collection_created_timeout_secs:
                            raise TimeoutError(
                                f"Collection {cname} failed to become visible after {delay} seconds"
                            )
                    elif error_type == "RateLimitExceeded":
                        RocksetOnlineStore.backoff_sleep(num_attempts, online_config)
                        continue
                    else:
                        raise

                if (
                    resp is not None
                    and cast(Dict[str, dict], resp)["data"]["status"] == "READY"
                ):
                    break

                if delay > online_config.collection_ready_timeout_secs:
                    raise TimeoutError(
                        f"Collection {cname} failed to become ready after {delay} seconds"
                    )

                RocksetOnlineStore.backoff_sleep(num_attempts, online_config)

    @staticmethod
    def backoff_sleep(attempts: int, online_config: RocksetOnlineStoreConfig):
        """
        Sleep for the needed amount of time based on the number of request attempts.

        Args:
            backoff: The amount of time we will sleep for
            max_backoff: The max amount of time we should ever backoff for.
            rate_limited: Whether this method is being called as part of a rate limited request.
        """

        default_backoff = online_config.initial_request_backoff_secs

        # Full jitter, exponential backoff.
        backoff = random.uniform(
            default_backoff,
            min(default_backoff << attempts, online_config.max_request_backoff_secs),
        )
        time.sleep(backoff)
