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

    batch_size: int = 40
    """Number of items to retrieve in a DynamoDB BatchGetItem call."""

    endpoint_url: Union[str, None] = None
    """DynamoDB local development endpoint Url, i.e. http://localhost:8000"""

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

    max_pool_connections: int = 10
    """Max number of connections for async Dynamodb operations"""

    keepalive_timeout: float = 12.0
    """Keep-alive timeout in seconds for async Dynamodb connections."""

    connect_timeout: Union[int, float] = 60
    """The time in seconds until a timeout exception is thrown when attempting to make
    an async connection."""

    read_timeout: Union[int, float] = 60
    """The time in seconds until a timeout exception is thrown when attempting to read
    from an async connection."""

    total_max_retry_attempts: Union[int, None] = None
    """Maximum number of total attempts that will be made on a single request.

    Maps to `retries.total_max_attempts` in botocore.config.Config.
    """

    retry_mode: Union[Literal["legacy", "standard", "adaptive"], None] = None
    """The type of retry mode (aio)botocore should use.

    Maps to `retries.mode` in botocore.config.Config.
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
    """

    _dynamodb_client = None
    _dynamodb_resource = None

    def __init__(self):
        super().__init__()
        self._aioboto_session = None
        self._aioboto_client = None
        self._aioboto_context_stack = None

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
    ):
        if self._aioboto_client is None:
            logger.debug("initializing the aiobotocore dynamodb client")

            retries: Dict[str, Any] = {}
            if total_max_retry_attempts is not None:
                retries["total_max_attempts"] = total_max_retry_attempts
            if retry_mode is not None:
                retries["mode"] = retry_mode

            client_context = self._get_aioboto_session().create_client(
                "dynamodb",
                region_name=region,
                config=AioConfig(
                    max_pool_connections=max_pool_connections,
                    connect_timeout=connect_timeout,
                    read_timeout=read_timeout,
                    retries=retries if retries else None,
                    connector_args={"keepalive_timeout": keepalive_timeout},
                ),
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
                table_instance.name, response, entity_ids, batch
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

        deserialize = TypeDeserializer().deserialize

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
                entity_ids,
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

    def _sort_dynamodb_response(
        self,
        responses: list,
        order: list,
        to_tbl_response: Callable = lambda raw_dict: raw_dict,
    ) -> Any:
        """DynamoDB Batch Get Item doesn't return items in a particular order."""
        # Assign an index to order
        order_with_index = {value: idx for idx, value in enumerate(order)}
        # Sort table responses by index
        table_responses_ordered: Any = [
            (order_with_index[tbl_res["entity_id"]], tbl_res)
            for tbl_res in map(to_tbl_response, responses)
        ]
        table_responses_ordered = sorted(
            table_responses_ordered, key=lambda tup: tup[0]
        )
        _, table_responses_ordered = zip(*table_responses_ordered)
        return table_responses_ordered

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
        self, table_name, response, entity_ids, batch, **sort_kwargs
    ):
        response = response.get("Responses")
        table_responses = response.get(table_name)

        batch_result = []
        if table_responses:
            table_responses = self._sort_dynamodb_response(
                table_responses, entity_ids, **sort_kwargs
            )
            entity_idx = 0
            for tbl_res in table_responses:
                entity_id = tbl_res["entity_id"]
                while entity_id != batch[entity_idx]:
                    batch_result.append((None, None))
                    entity_idx += 1
                res = {}
                for feature_name, value_bin in tbl_res["values"].items():
                    val = ValueProto()
                    val.ParseFromString(value_bin.value)
                    res[feature_name] = val
                batch_result.append((datetime.fromisoformat(tbl_res["event_ts"]), res))
                entity_idx += 1
        # Not all entities in a batch may have responses
        # Pad with remaining values in batch that were not found
        batch_size_nones = ((None, None),) * (len(batch) - len(batch_result))
        batch_result.extend(batch_size_nones)
        return batch_result

    @staticmethod
    def _to_entity_ids(config: RepoConfig, entity_keys: List[EntityKeyProto]):
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
