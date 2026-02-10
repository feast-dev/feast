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
import asyncio
import contextlib
import itertools
import logging
from collections import OrderedDict, defaultdict
from datetime import datetime
from typing import Any, Callable, Dict, List, Literal, Optional, Sequence, Tuple, Union

from aiobotocore.config import AioConfig
from pydantic import StrictBool, StrictStr

from feast import Entity, FeatureView, utils
from feast.infra.online_stores.helpers import compute_entity_id
from feast.infra.online_stores.online_store import OnlineStore
from feast.infra.supported_async_methods import SupportedAsyncMethods
from feast.infra.utils.aws_utils import dynamo_write_items_async
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from feast.repo_config import FeastConfigBaseModel, RepoConfig
from feast.utils import get_user_agent

try:
    import boto3
    from aiobotocore import session
    from boto3.dynamodb.types import TypeDeserializer
    from botocore.config import Config
    from botocore.exceptions import ClientError
except ImportError as e:
    from feast.errors import FeastExtrasDependencyImportError

    raise FeastExtrasDependencyImportError("aws", str(e))


logger = logging.getLogger(__name__)


class DynamoDBOnlineStoreConfig(FeastConfigBaseModel):
    """Online store config for DynamoDB store"""

    type: Literal["dynamodb"] = "dynamodb"
    """Online store type selector"""

    batch_size: int = 100
    """Number of items to retrieve in a DynamoDB BatchGetItem call.
    DynamoDB supports up to 100 items per BatchGetItem request."""

    endpoint_url: Union[str, None] = None
    """DynamoDB endpoint URL. Use for local development (e.g., http://localhost:8000)
    or VPC endpoints for improved latency."""

    region: StrictStr
    """AWS Region Name"""

    table_name_template: StrictStr = "{project}.{table_name}"
    """DynamoDB table name template"""

    consistent_reads: StrictBool = False
    """Whether to read from Dynamodb by forcing consistent reads"""

    tags: Union[Dict[str, str], None] = None
    """AWS resource tags added to each table"""

    session_based_auth: bool = False
    """AWS session based client authentication"""

    max_pool_connections: int = 50
    """Max number of connections for async Dynamodb operations.
    Increase for high-throughput workloads."""

    keepalive_timeout: float = 30.0
    """Keep-alive timeout in seconds for async Dynamodb connections.
    Higher values help reuse connections under sustained load."""

    connect_timeout: Union[int, float] = 5
    """The time in seconds until a timeout exception is thrown when attempting to make
    an async connection. Lower values enable faster failure detection."""

    read_timeout: Union[int, float] = 10
    """The time in seconds until a timeout exception is thrown when attempting to read
    from an async connection. Lower values enable faster failure detection."""

    total_max_retry_attempts: Union[int, None] = 3
    """Maximum number of total attempts that will be made on a single request.

    Maps to `retries.total_max_attempts` in botocore.config.Config.
    """

    retry_mode: Union[Literal["legacy", "standard", "adaptive"], None] = "adaptive"
    """The type of retry mode (aio)botocore should use.

    Maps to `retries.mode` in botocore.config.Config.
    'adaptive' mode provides intelligent retry with client-side rate limiting.
    """


class DynamoDBOnlineStore(OnlineStore):
    """
    AWS DynamoDB implementation of the online store interface.

    Attributes:
        _dynamodb_client: Boto3 DynamoDB client.
        _dynamodb_resource: Boto3 DynamoDB resource.
        _aioboto_session: Async boto session.
        _aioboto_client: Async boto client.
        _aioboto_context_stack: Async context stack.
        _type_deserializer: Cached TypeDeserializer instance for performance.
    """

    _dynamodb_client = None
    _dynamodb_resource = None
    # Class-level cached TypeDeserializer to avoid per-request instantiation
    _type_deserializer: Optional[TypeDeserializer] = None

    def __init__(self):
        super().__init__()
        self._aioboto_session = None
        self._aioboto_client = None
        self._aioboto_context_stack = None
        # Initialize cached TypeDeserializer if not already done
        if DynamoDBOnlineStore._type_deserializer is None:
            DynamoDBOnlineStore._type_deserializer = TypeDeserializer()

    async def initialize(self, config: RepoConfig):
        online_config = config.online_store

        await self._get_aiodynamodb_client(
            online_config.region,
            online_config.max_pool_connections,
            online_config.keepalive_timeout,
            online_config.connect_timeout,
            online_config.read_timeout,
            online_config.total_max_retry_attempts,
            online_config.retry_mode,
            online_config.endpoint_url,
        )

    async def close(self):
        await self._aiodynamodb_close()

    def _get_aioboto_session(self):
        if self._aioboto_session is None:
            logger.debug("initializing the aiobotocore session")
            self._aioboto_session = session.get_session()
        return self._aioboto_session

    async def _get_aiodynamodb_client(
        self,
        region: str,
        max_pool_connections: int,
        keepalive_timeout: float,
        connect_timeout: Union[int, float],
        read_timeout: Union[int, float],
        total_max_retry_attempts: Union[int, None],
        retry_mode: Union[Literal["legacy", "standard", "adaptive"], None],
        endpoint_url: Optional[str] = None,
    ):
        if self._aioboto_client is None:
            logger.debug("initializing the aiobotocore dynamodb client")

            retries: Dict[str, Any] = {}
            if total_max_retry_attempts is not None:
                retries["total_max_attempts"] = total_max_retry_attempts
            if retry_mode is not None:
                retries["mode"] = retry_mode

            # Build client kwargs, including endpoint_url for VPC endpoints or local testing
            client_kwargs: Dict[str, Any] = {
                "region_name": region,
                "config": AioConfig(
                    max_pool_connections=max_pool_connections,
                    connect_timeout=connect_timeout,
                    read_timeout=read_timeout,
                    retries=retries if retries else None,
                    connector_args={"keepalive_timeout": keepalive_timeout},
                ),
            }
            if endpoint_url:
                client_kwargs["endpoint_url"] = endpoint_url

            client_context = self._get_aioboto_session().create_client(
                "dynamodb",
                **client_kwargs,
            )
            self._aioboto_context_stack = contextlib.AsyncExitStack()
            self._aioboto_client = (
                await self._aioboto_context_stack.enter_async_context(client_context)
            )
        return self._aioboto_client

    async def _aiodynamodb_close(self):
        if self._aioboto_client:
            await self._aioboto_client.close()
            self._aioboto_client = None
        if self._aioboto_context_stack:
            await self._aioboto_context_stack.aclose()
            self._aioboto_context_stack = None
        if self._aioboto_session:
            self._aioboto_session = None

    @property
    def async_supported(self) -> SupportedAsyncMethods:
        return SupportedAsyncMethods(read=True, write=True)

    @staticmethod
    def _table_tags(online_config, table_instance) -> list[dict[str, str]]:
        table_instance_tags = table_instance.tags or {}
        online_tags = online_config.tags or {}

        common_tags = [
            {"Key": key, "Value": table_instance_tags.get(key) or value}
            for key, value in online_tags.items()
        ]
        table_tags = [
            {"Key": key, "Value": value}
            for key, value in table_instance_tags.items()
            if key not in online_tags
        ]

        return common_tags + table_tags

    @staticmethod
    def _update_tags(dynamodb_client, table_name: str, new_tags: list[dict[str, str]]):
        table_arn = dynamodb_client.describe_table(TableName=table_name)["Table"][
            "TableArn"
        ]
        current_tags = dynamodb_client.list_tags_of_resource(ResourceArn=table_arn)[
            "Tags"
        ]
        if current_tags:
            remove_keys = [tag["Key"] for tag in current_tags]
            dynamodb_client.untag_resource(ResourceArn=table_arn, TagKeys=remove_keys)

        if new_tags:
            dynamodb_client.tag_resource(ResourceArn=table_arn, Tags=new_tags)

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
        Update tables from the DynamoDB Online Store.

        Args:
            config: The RepoConfig for the current FeatureStore.
            tables_to_delete: Tables to delete from the DynamoDB Online Store.
            tables_to_keep: Tables to keep in the DynamoDB Online Store.
        """
        online_config = config.online_store
        assert isinstance(online_config, DynamoDBOnlineStoreConfig)
        dynamodb_client = self._get_dynamodb_client(
            online_config.region,
            online_config.endpoint_url,
            online_config.session_based_auth,
        )
        dynamodb_resource = self._get_dynamodb_resource(
            online_config.region,
            online_config.endpoint_url,
            online_config.session_based_auth,
        )

        do_tag_updates = defaultdict(bool)
        for table_instance in tables_to_keep:
            # Add Tags attribute to creation request only if configured to prevent
            # TagResource permission issues, even with an empty Tags array.
            table_tags = self._table_tags(online_config, table_instance)
            kwargs = {"Tags": table_tags} if table_tags else {}

            table_name = _get_table_name(online_config, config, table_instance)
            # Check if table already exists before attempting to create
            # This is required for environments where IAM roles don't have
            # dynamodb:CreateTable permissions (e.g., Terraform-managed tables)
            table_exists = False
            try:
                dynamodb_client.describe_table(TableName=table_name)
                table_exists = True
                do_tag_updates[table_name] = True
                logger.info(
                    f"DynamoDB table {table_name} already exists, skipping creation"
                )
            except ClientError as ce:
                if ce.response["Error"]["Code"] != "ResourceNotFoundException":
                    # If it's not a "table not found" error, re-raise
                    raise

            # Only attempt to create table if it doesn't exist
            if not table_exists:
                try:
                    dynamodb_resource.create_table(
                        TableName=table_name,
                        KeySchema=[{"AttributeName": "entity_id", "KeyType": "HASH"}],
                        AttributeDefinitions=[
                            {"AttributeName": "entity_id", "AttributeType": "S"}
                        ],
                        BillingMode="PAY_PER_REQUEST",
                        **kwargs,
                    )
                    logger.info(f"Created DynamoDB table {table_name}")

                except ClientError as ce:
                    do_tag_updates[table_name] = True

                    # If the table creation fails with ResourceInUseException,
                    # it means the table already exists or is being created.
                    # Otherwise, re-raise the exception
                    if ce.response["Error"]["Code"] != "ResourceInUseException":
                        raise

        for table_instance in tables_to_keep:
            table_name = _get_table_name(online_config, config, table_instance)
            dynamodb_client.get_waiter("table_exists").wait(TableName=table_name)
            # once table is confirmed to exist, update the tags.
            # tags won't be updated in the create_table call if the table already exists
            if do_tag_updates[table_name]:
                tags = self._table_tags(online_config, table_instance)
                try:
                    self._update_tags(dynamodb_client, table_name, tags)
                except ClientError as ce:
                    # If tag update fails with AccessDeniedException, log warning and continue
                    # This allows Feast to work in environments where IAM roles don't have
                    # dynamodb:TagResource and dynamodb:UntagResource permissions
                    if ce.response["Error"]["Code"] == "AccessDeniedException":
                        logger.warning(
                            f"Unable to update tags for table {table_name} due to insufficient permissions."
                        )
                    else:
                        raise

        for table_to_delete in tables_to_delete:
            _delete_table_idempotent(
                dynamodb_resource,
                _get_table_name(online_config, config, table_to_delete),
            )

    def teardown(
        self,
        config: RepoConfig,
        tables: Sequence[FeatureView],
        entities: Sequence[Entity],
    ):
        """
        Delete tables from the DynamoDB Online Store.

        Args:
            config: The RepoConfig for the current FeatureStore.
            tables: Tables to delete from the feature repo.
        """
        online_config = config.online_store
        assert isinstance(online_config, DynamoDBOnlineStoreConfig)
        dynamodb_resource = self._get_dynamodb_resource(
            online_config.region,
            online_config.endpoint_url,
            online_config.session_based_auth,
        )

        for table in tables:
            _delete_table_idempotent(
                dynamodb_resource, _get_table_name(online_config, config, table)
            )

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
        Write a batch of feature rows to online DynamoDB store.

        Note: This method applies a ``batch_writer`` to automatically handle any unprocessed items
        and resend them as needed, this is useful if you're loading a lot of data at a time.

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
        assert isinstance(online_config, DynamoDBOnlineStoreConfig)
        dynamodb_resource = self._get_dynamodb_resource(
            online_config.region,
            online_config.endpoint_url,
            online_config.session_based_auth,
        )

        table_instance = dynamodb_resource.Table(
            _get_table_name(online_config, config, table)
        )
        self._write_batch_non_duplicates(table_instance, data, progress, config)

    async def online_write_batch_async(
        self,
        config: RepoConfig,
        table: FeatureView,
        data: List[
            Tuple[EntityKeyProto, Dict[str, ValueProto], datetime, Optional[datetime]]
        ],
        progress: Optional[Callable[[int], Any]],
    ) -> None:
        """
        Writes a batch of feature rows to the online store asynchronously.

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
        online_config = config.online_store
        assert isinstance(online_config, DynamoDBOnlineStoreConfig)

        table_name = _get_table_name(online_config, config, table)
        items = [
            _to_client_write_item(config, entity_key, features, timestamp)
            for entity_key, features, timestamp, _ in _latest_data_to_write(data)
        ]
        client = await self._get_aiodynamodb_client(
            online_config.region,
            online_config.max_pool_connections,
            online_config.keepalive_timeout,
            online_config.connect_timeout,
            online_config.read_timeout,
            online_config.total_max_retry_attempts,
            online_config.retry_mode,
            online_config.endpoint_url,
        )
        await dynamo_write_items_async(client, table_name, items)

    def online_read(
        self,
        config: RepoConfig,
        table: FeatureView,
        entity_keys: List[EntityKeyProto],
        requested_features: Optional[List[str]] = None,
    ) -> List[Tuple[Optional[datetime], Optional[Dict[str, ValueProto]]]]:
        """
        Retrieve feature values from the online DynamoDB store.

        Args:
            config: The RepoConfig for the current FeatureStore.
            table: Feast FeatureView.
            entity_keys: a list of entity keys that should be read from the FeatureStore.
            requested_features: Optional list of feature names to retrieve.
        """
        online_config = config.online_store
        assert isinstance(online_config, DynamoDBOnlineStoreConfig)

        dynamodb_resource = self._get_dynamodb_resource(
            online_config.region,
            online_config.endpoint_url,
            online_config.session_based_auth,
        )
        table_instance = dynamodb_resource.Table(
            _get_table_name(online_config, config, table)
        )

        batch_size = online_config.batch_size
        entity_ids = self._to_entity_ids(config, entity_keys)
        entity_ids_iter = iter(entity_ids)
        result: List[Tuple[Optional[datetime], Optional[Dict[str, ValueProto]]]] = []

        while True:
            batch = list(itertools.islice(entity_ids_iter, batch_size))

            # No more items to insert
            if len(batch) == 0:
                break
            batch_entity_ids = self._to_resource_batch_get_payload(
                online_config, table_instance.name, batch
            )
            response = dynamodb_resource.batch_get_item(
                RequestItems=batch_entity_ids,
            )
            batch_result = self._process_batch_get_response(
                table_instance.name,
                response,
                batch,
            )
            result.extend(batch_result)
        return result

    async def online_read_async(
        self,
        config: RepoConfig,
        table: FeatureView,
        entity_keys: List[EntityKeyProto],
        requested_features: Optional[List[str]] = None,
    ) -> List[Tuple[Optional[datetime], Optional[Dict[str, ValueProto]]]]:
        """
        Reads features values for the given entity keys asynchronously.

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
        online_config = config.online_store
        assert isinstance(online_config, DynamoDBOnlineStoreConfig)

        batch_size = online_config.batch_size
        entity_ids = self._to_entity_ids(config, entity_keys)
        entity_ids_iter = iter(entity_ids)
        table_name = _get_table_name(online_config, config, table)

        # Use cached TypeDeserializer for better performance
        if self._type_deserializer is None:
            self._type_deserializer = TypeDeserializer()
        deserialize = self._type_deserializer.deserialize

        def to_tbl_resp(raw_client_response):
            return {
                "entity_id": deserialize(raw_client_response["entity_id"]),
                "event_ts": deserialize(raw_client_response["event_ts"]),
                "values": deserialize(raw_client_response["values"]),
            }

        batches = []
        entity_id_batches = []
        while True:
            batch = list(itertools.islice(entity_ids_iter, batch_size))
            if not batch:
                break
            entity_id_batch = self._to_client_batch_get_payload(
                online_config, table_name, batch
            )
            batches.append(batch)
            entity_id_batches.append(entity_id_batch)

        client = await self._get_aiodynamodb_client(
            online_config.region,
            online_config.max_pool_connections,
            online_config.keepalive_timeout,
            online_config.connect_timeout,
            online_config.read_timeout,
            online_config.total_max_retry_attempts,
            online_config.retry_mode,
            online_config.endpoint_url,
        )
        response_batches = await asyncio.gather(
            *[
                client.batch_get_item(
                    RequestItems=entity_id_batch,
                )
                for entity_id_batch in entity_id_batches
            ]
        )

        result_batches = []
        for batch, response in zip(batches, response_batches):
            result_batch = self._process_batch_get_response(
                table_name,
                response,
                batch,
                to_tbl_response=to_tbl_resp,
            )
            result_batches.append(result_batch)

        return list(itertools.chain(*result_batches))

    def _get_dynamodb_client(
        self,
        region: str,
        endpoint_url: Optional[str] = None,
        session_based_auth: Optional[bool] = False,
    ):
        if self._dynamodb_client is None:
            self._dynamodb_client = _initialize_dynamodb_client(
                region, endpoint_url, session_based_auth
            )
        return self._dynamodb_client

    def _get_dynamodb_resource(
        self,
        region: str,
        endpoint_url: Optional[str] = None,
        session_based_auth: Optional[bool] = False,
    ):
        if self._dynamodb_resource is None:
            self._dynamodb_resource = _initialize_dynamodb_resource(
                region, endpoint_url, session_based_auth
            )
        return self._dynamodb_resource

    def _write_batch_non_duplicates(
        self,
        table_instance,
        data: List[
            Tuple[EntityKeyProto, Dict[str, ValueProto], datetime, Optional[datetime]]
        ],
        progress: Optional[Callable[[int], Any]],
        config: RepoConfig,
    ):
        """Deduplicate write batch request items on ``entity_id`` primary key."""
        with table_instance.batch_writer(overwrite_by_pkeys=["entity_id"]) as batch:
            for entity_key, features, timestamp, created_ts in data:
                batch.put_item(
                    Item=_to_resource_write_item(
                        config, entity_key, features, timestamp
                    )
                )
                if progress:
                    progress(1)

    def _process_batch_get_response(
        self,
        table_name: str,
        response: Dict[str, Any],
        batch: List[str],
        to_tbl_response: Callable = lambda raw_dict: raw_dict,
    ) -> List[Tuple[Optional[datetime], Optional[Dict[str, ValueProto]]]]:
        """Process batch get response using O(1) dictionary lookup.

        DynamoDB BatchGetItem doesn't return items in a particular order,
        so we use a dictionary for O(1) lookup instead of O(n log n) sorting.

        This method:
        - Uses dictionary lookup instead of sorting for response ordering
        - Pre-allocates the result list with None values
        - Minimizes object creation in the hot path

        Args:
            table_name: Name of the DynamoDB table
            response: Raw response from DynamoDB batch_get_item
            batch: List of entity_ids in the order they should be returned
            to_tbl_response: Function to transform raw DynamoDB response items
                (used for async client responses that need deserialization)

        Returns:
            List of (timestamp, features) tuples in the same order as batch
        """
        responses_data = response.get("Responses")
        if not responses_data:
            # No responses at all, return all None tuples
            return [(None, None)] * len(batch)

        table_responses = responses_data.get(table_name)
        if not table_responses:
            # No responses for this table, return all None tuples
            return [(None, None)] * len(batch)

        # Build a dictionary for O(1) lookup instead of O(n log n) sorting
        response_dict: Dict[str, Any] = {
            tbl_res["entity_id"]: tbl_res
            for tbl_res in map(to_tbl_response, table_responses)
        }

        # Pre-allocate result list with None tuples (faster than appending)
        batch_size = len(batch)
        result: List[Tuple[Optional[datetime], Optional[Dict[str, ValueProto]]]] = [
            (None, None)
        ] * batch_size

        # Process each entity in batch order using O(1) dict lookup
        for idx, entity_id in enumerate(batch):
            tbl_res = response_dict.get(entity_id)
            if tbl_res is not None:
                # Parse feature values
                features: Dict[str, ValueProto] = {}
                values_data = tbl_res["values"]
                for feature_name, value_bin in values_data.items():
                    val = ValueProto()
                    val.ParseFromString(value_bin.value)
                    features[feature_name] = val

                # Parse timestamp and set result
                result[idx] = (
                    datetime.fromisoformat(tbl_res["event_ts"]),
                    features,
                )

        return result

    @staticmethod
    def _to_entity_ids(config: RepoConfig, entity_keys: List[EntityKeyProto]):
        """Convert entity keys to entity IDs."""
        return [
            compute_entity_id(
                entity_key,
                entity_key_serialization_version=config.entity_key_serialization_version,
            )
            for entity_key in entity_keys
        ]

    @staticmethod
    def _to_resource_batch_get_payload(online_config, table_name, batch):
        return {
            table_name: {
                "Keys": [{"entity_id": entity_id} for entity_id in batch],
                "ConsistentRead": online_config.consistent_reads,
            }
        }

    @staticmethod
    def _to_client_batch_get_payload(online_config, table_name, batch):
        return {
            table_name: {
                "Keys": [{"entity_id": {"S": entity_id}} for entity_id in batch],
                "ConsistentRead": online_config.consistent_reads,
            }
        }

    def update_online_store(
        self,
        config: RepoConfig,
        table: FeatureView,
        data: List[
            Tuple[EntityKeyProto, Dict[str, ValueProto], datetime, Optional[datetime]]
        ],
        update_expressions: Dict[str, str],
        progress: Optional[Callable[[int], Any]] = None,
    ) -> None:
        """
        Update features in DynamoDB using UpdateItem with custom UpdateExpression.

        This method provides DynamoDB-specific list update functionality using
        native UpdateItem operations with list_append and other expressions.

        Args:
            config: The RepoConfig for the current FeatureStore.
            table: Feast FeatureView.
            data: Feature data to update. Each tuple contains an entity key,
                  feature values, event timestamp, and optional created timestamp.
            update_expressions: Dict mapping feature names to DynamoDB update expressions.
                Examples:
                - "transactions": "list_append(transactions, :new_val)"
                - "recent_items": "list_append(:new_val, recent_items)"  # prepend
            progress: Optional progress callback function.
        """
        online_config = config.online_store
        assert isinstance(online_config, DynamoDBOnlineStoreConfig)

        dynamodb_resource = self._get_dynamodb_resource(
            online_config.region,
            online_config.endpoint_url,
            online_config.session_based_auth,
        )

        table_instance = dynamodb_resource.Table(
            _get_table_name(online_config, config, table)
        )

        # Process each entity update
        for entity_key, features, timestamp, _ in _latest_data_to_write(data):
            entity_id = compute_entity_id(
                entity_key,
                entity_key_serialization_version=config.entity_key_serialization_version,
            )

            self._update_item_with_expression(
                table_instance,
                entity_id,
                features,
                timestamp,
                update_expressions,
                config,
            )

            if progress:
                progress(1)

    async def update_online_store_async(
        self,
        config: RepoConfig,
        table: FeatureView,
        data: List[
            Tuple[EntityKeyProto, Dict[str, ValueProto], datetime, Optional[datetime]]
        ],
        update_expressions: Dict[str, str],
        progress: Optional[Callable[[int], Any]] = None,
    ) -> None:
        """
        Async version of update_online_store.
        """
        online_config = config.online_store
        assert isinstance(online_config, DynamoDBOnlineStoreConfig)

        table_name = _get_table_name(online_config, config, table)
        client = await self._get_aiodynamodb_client(
            online_config.region,
            online_config.max_pool_connections,
            online_config.keepalive_timeout,
            online_config.connect_timeout,
            online_config.read_timeout,
            online_config.total_max_retry_attempts,
            online_config.retry_mode,
            online_config.endpoint_url,
        )

        # Process each entity update
        for entity_key, features, timestamp, _ in _latest_data_to_write(data):
            entity_id = compute_entity_id(
                entity_key,
                entity_key_serialization_version=config.entity_key_serialization_version,
            )

            await self._update_item_with_expression_async(
                client,
                table_name,
                entity_id,
                features,
                timestamp,
                update_expressions,
                config,
            )

            if progress:
                progress(1)

    def _update_item_with_expression(
        self,
        table_instance,
        entity_id: str,
        features: Dict[str, ValueProto],
        timestamp: datetime,
        update_expressions: Dict[str, str],
        config: RepoConfig,
    ):
        """Execute DynamoDB UpdateItem with list operations via read-modify-write."""
        # Read existing item to get current values for list operations
        existing_values: Dict[str, ValueProto] = {}
        item_exists = False
        try:
            response = table_instance.get_item(Key={"entity_id": entity_id})
            if "Item" in response:
                item_exists = True
                if "values" in response["Item"]:
                    for feat_name, val_bin in response["Item"]["values"].items():
                        val = ValueProto()
                        val.ParseFromString(val_bin.value)
                        existing_values[feat_name] = val
        except ClientError:
            pass

        # Build final feature values by applying list operations
        final_features: Dict[str, ValueProto] = {}
        for feature_name, value_proto in features.items():
            if feature_name in update_expressions:
                final_features[feature_name] = self._apply_list_operation(
                    existing_values.get(feature_name),
                    value_proto,
                    update_expressions[feature_name],
                )
            else:
                final_features[feature_name] = value_proto

        # For new items, use put_item
        if not item_exists:
            item = {
                "entity_id": entity_id,
                "event_ts": str(utils.make_tzaware(timestamp)),
                "values": {k: v.SerializeToString() for k, v in final_features.items()},
            }
            table_instance.put_item(Item=item)
            return

        # Build UpdateExpression for existing items
        update_expr_parts: list[str] = []
        expression_attribute_values: Dict[str, Any] = {}
        expression_attribute_names: Dict[str, str] = {
            "#values": "values",
            "#event_ts": "event_ts",
        }

        update_expr_parts.append("#event_ts = :event_ts")
        expression_attribute_values[":event_ts"] = str(utils.make_tzaware(timestamp))

        for feature_name, value_proto in final_features.items():
            feat_attr = f"#feat_{feature_name}"
            val_name = f":val_{feature_name}"
            expression_attribute_names[feat_attr] = feature_name
            expression_attribute_values[val_name] = value_proto.SerializeToString()  # type: ignore[assignment]
            update_expr_parts.append(f"#values.{feat_attr} = {val_name}")

        try:
            table_instance.update_item(
                Key={"entity_id": entity_id},
                UpdateExpression="SET " + ", ".join(update_expr_parts),
                ExpressionAttributeNames=expression_attribute_names,
                ExpressionAttributeValues=expression_attribute_values,
            )
        except ClientError as e:
            logger.error(f"Failed to update item {entity_id}: {e}")
            raise

    def _apply_list_operation(
        self, existing: Optional[ValueProto], new_value: ValueProto, update_expr: str
    ) -> ValueProto:
        """Apply list operation (append/prepend) and return merged ValueProto."""
        result = ValueProto()
        is_prepend = update_expr.strip().startswith("list_append(:new_val")
        existing_list = self._extract_list_values(existing) if existing else []
        new_list = self._extract_list_values(new_value)
        merged = new_list + existing_list if is_prepend else existing_list + new_list
        self._set_list_values(result, new_value, merged)
        return result

    def _extract_list_values(self, value_proto: ValueProto) -> list:
        """Extract list values from ValueProto."""
        if value_proto.HasField("string_list_val"):
            return list(value_proto.string_list_val.val)
        elif value_proto.HasField("int32_list_val"):
            return list(value_proto.int32_list_val.val)
        elif value_proto.HasField("int64_list_val"):
            return list(value_proto.int64_list_val.val)
        elif value_proto.HasField("float_list_val"):
            return list(value_proto.float_list_val.val)
        elif value_proto.HasField("double_list_val"):
            return list(value_proto.double_list_val.val)
        elif value_proto.HasField("bool_list_val"):
            return list(value_proto.bool_list_val.val)
        elif value_proto.HasField("bytes_list_val"):
            return list(value_proto.bytes_list_val.val)
        return []

    def _set_list_values(
        self, result: ValueProto, template: ValueProto, values: list
    ) -> None:
        """Set list values on result ValueProto based on template type."""
        if template.HasField("string_list_val"):
            result.string_list_val.val.extend(values)
        elif template.HasField("int32_list_val"):
            result.int32_list_val.val.extend(values)
        elif template.HasField("int64_list_val"):
            result.int64_list_val.val.extend(values)
        elif template.HasField("float_list_val"):
            result.float_list_val.val.extend(values)
        elif template.HasField("double_list_val"):
            result.double_list_val.val.extend(values)
        elif template.HasField("bool_list_val"):
            result.bool_list_val.val.extend(values)
        elif template.HasField("bytes_list_val"):
            result.bytes_list_val.val.extend(values)

    async def _update_item_with_expression_async(
        self,
        client,
        table_name: str,
        entity_id: str,
        features: Dict[str, ValueProto],
        timestamp: datetime,
        update_expressions: Dict[str, str],
        config: RepoConfig,
    ):
        """Async version of _update_item_with_expression."""
        # Read existing item
        existing_values: Dict[str, ValueProto] = {}
        item_exists = False
        try:
            response = await client.get_item(
                TableName=table_name, Key={"entity_id": {"S": entity_id}}
            )
            if "Item" in response:
                item_exists = True
                if "values" in response["Item"] and "M" in response["Item"]["values"]:
                    for feat_name, val_data in response["Item"]["values"]["M"].items():
                        if "B" in val_data:
                            val = ValueProto()
                            val.ParseFromString(val_data["B"])
                            existing_values[feat_name] = val
        except ClientError:
            pass

        # Build final feature values
        final_features: Dict[str, ValueProto] = {}
        for feature_name, value_proto in features.items():
            if feature_name in update_expressions:
                final_features[feature_name] = self._apply_list_operation(
                    existing_values.get(feature_name),
                    value_proto,
                    update_expressions[feature_name],
                )
            else:
                final_features[feature_name] = value_proto

        # For new items, use put_item
        if not item_exists:
            item = {
                "entity_id": {"S": entity_id},
                "event_ts": {"S": str(utils.make_tzaware(timestamp))},
                "values": {
                    "M": {
                        k: {"B": v.SerializeToString()}
                        for k, v in final_features.items()
                    }
                },
            }
            await client.put_item(TableName=table_name, Item=item)
            return

        # Build UpdateExpression for existing items
        update_expr_parts: list[str] = []
        expression_attribute_values: Dict[str, Any] = {}
        expression_attribute_names: Dict[str, str] = {
            "#values": "values",
            "#event_ts": "event_ts",
        }

        update_expr_parts.append("#event_ts = :event_ts")
        expression_attribute_values[":event_ts"] = {
            "S": str(utils.make_tzaware(timestamp))
        }

        for feature_name, value_proto in final_features.items():
            feat_attr = f"#feat_{feature_name}"
            val_name = f":val_{feature_name}"
            expression_attribute_names[feat_attr] = feature_name
            expression_attribute_values[val_name] = {
                "B": value_proto.SerializeToString()
            }
            update_expr_parts.append(f"#values.{feat_attr} = {val_name}")

        try:
            await client.update_item(
                TableName=table_name,
                Key={"entity_id": {"S": entity_id}},
                UpdateExpression="SET " + ", ".join(update_expr_parts),
                ExpressionAttributeNames=expression_attribute_names,
                ExpressionAttributeValues=expression_attribute_values,
            )
        except ClientError as e:
            logger.error(f"Failed to update item {entity_id}: {e}")
            raise


# Global async client functions removed - now using instance methods


def _initialize_dynamodb_client(
    region: str,
    endpoint_url: Optional[str] = None,
    session_based_auth: Optional[bool] = False,
):
    if session_based_auth:
        return boto3.Session().client(
            "dynamodb",
            region_name=region,
            endpoint_url=endpoint_url,
            config=Config(user_agent=get_user_agent()),
        )
    else:
        return boto3.client(
            "dynamodb",
            region_name=region,
            endpoint_url=endpoint_url,
            config=Config(user_agent=get_user_agent()),
        )


def _initialize_dynamodb_resource(
    region: str,
    endpoint_url: Optional[str] = None,
    session_based_auth: Optional[bool] = False,
):
    if session_based_auth:
        return boto3.Session().resource(
            "dynamodb", region_name=region, endpoint_url=endpoint_url
        )
    else:
        return boto3.resource("dynamodb", region_name=region, endpoint_url=endpoint_url)


# TODO(achals): This form of user-facing templating is experimental.
# Please refer to https://github.com/feast-dev/feast/issues/2438 before building on top of it,
def _get_table_name(
    online_config: DynamoDBOnlineStoreConfig, config: RepoConfig, table: FeatureView
) -> str:
    return online_config.table_name_template.format(
        project=config.project, table_name=table.name
    )


def _delete_table_idempotent(
    dynamodb_resource,
    table_name: str,
):
    try:
        table = dynamodb_resource.Table(table_name)
        table.delete()
        logger.info(f"Dynamo table {table_name} was deleted")
    except ClientError as ce:
        error_code = ce.response["Error"]["Code"]

        # If the table deletion fails with ResourceNotFoundException,
        # it means the table has already been deleted.
        if error_code == "ResourceNotFoundException":
            logger.warning(f"Trying to delete table that doesn't exist: {table_name}")
        # If it fails with AccessDeniedException, the IAM role doesn't have
        # dynamodb:DeleteTable permission (e.g., Terraform-managed tables)
        elif error_code == "AccessDeniedException":
            logger.warning(
                f"Unable to delete table {table_name} due to insufficient permissions. "
                f"The table may need to be deleted manually or via your infrastructure management tool (e.g., Terraform)."
            )
        else:
            # Some other error, re-raise
            raise


def _to_resource_write_item(config, entity_key, features, timestamp):
    entity_id = compute_entity_id(
        entity_key,
        entity_key_serialization_version=config.entity_key_serialization_version,
    )
    return {
        "entity_id": entity_id,  # PartitionKey
        "event_ts": str(utils.make_tzaware(timestamp)),
        "values": {
            k: v.SerializeToString()
            for k, v in features.items()  # Serialized Features
        },
    }


def _to_client_write_item(config, entity_key, features, timestamp):
    entity_id = compute_entity_id(
        entity_key,
        entity_key_serialization_version=config.entity_key_serialization_version,
    )
    return {
        "entity_id": {"S": entity_id},  # PartitionKey
        "event_ts": {"S": str(utils.make_tzaware(timestamp))},
        "values": {
            "M": {
                k: {"B": v.SerializeToString()}
                for k, v in features.items()  # Serialized Features
            }
        },
    }


def _latest_data_to_write(
    data: List[
        Tuple[EntityKeyProto, Dict[str, ValueProto], datetime, Optional[datetime]]
    ],
):
    as_hashable = ((d[0].SerializeToString(), d) for d in data)
    sorted_data = sorted(as_hashable, key=lambda ah: (ah[0], ah[1][2]))
    return (v for _, v in OrderedDict((ah[0], ah[1]) for ah in sorted_data).items())
