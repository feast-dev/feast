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
import itertools
import logging
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

from pydantic import StrictStr
from pydantic.typing import Literal, Union

from feast import Entity, FeatureView, utils
from feast.infra.infra_object import DYNAMODB_INFRA_OBJECT_CLASS_TYPE, InfraObject
from feast.infra.online_stores.helpers import compute_entity_id
from feast.infra.online_stores.online_store import OnlineStore
from feast.protos.feast.core.DynamoDBTable_pb2 import (
    DynamoDBTable as DynamoDBTableProto,
)
from feast.protos.feast.core.InfraObject_pb2 import InfraObject as InfraObjectProto
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from feast.repo_config import FeastConfigBaseModel, RepoConfig
from feast.usage import log_exceptions_and_usage, tracing_span

try:
    import boto3
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


class DynamoDBOnlineStore(OnlineStore):
    """
    Online feature store for AWS DynamoDB.

    Attributes:
        _dynamodb_client: Boto3 DynamoDB client.
        _dynamodb_resource: Boto3 DynamoDB resource.
    """

    _dynamodb_client = None
    _dynamodb_resource = None

    @log_exceptions_and_usage(online_store="dynamodb")
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
            online_config.region, online_config.endpoint_url
        )
        dynamodb_resource = self._get_dynamodb_resource(
            online_config.region, online_config.endpoint_url
        )

        for table_instance in tables_to_keep:
            try:
                dynamodb_resource.create_table(
                    TableName=_get_table_name(online_config, config, table_instance),
                    KeySchema=[{"AttributeName": "entity_id", "KeyType": "HASH"}],
                    AttributeDefinitions=[
                        {"AttributeName": "entity_id", "AttributeType": "S"}
                    ],
                    BillingMode="PAY_PER_REQUEST",
                )
            except ClientError as ce:
                # If the table creation fails with ResourceInUseException,
                # it means the table already exists or is being created.
                # Otherwise, re-raise the exception
                if ce.response["Error"]["Code"] != "ResourceInUseException":
                    raise

        for table_instance in tables_to_keep:
            dynamodb_client.get_waiter("table_exists").wait(
                TableName=_get_table_name(online_config, config, table_instance)
            )

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
            online_config.region, online_config.endpoint_url
        )

        for table in tables:
            _delete_table_idempotent(
                dynamodb_resource, _get_table_name(online_config, config, table)
            )

    @log_exceptions_and_usage(online_store="dynamodb")
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
            online_config.region, online_config.endpoint_url
        )

        table_instance = dynamodb_resource.Table(
            _get_table_name(online_config, config, table)
        )
        self._write_batch_non_duplicates(table_instance, data, progress)

    @log_exceptions_and_usage(online_store="dynamodb")
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
            online_config.region, online_config.endpoint_url
        )
        table_instance = dynamodb_resource.Table(
            _get_table_name(online_config, config, table)
        )

        result: List[Tuple[Optional[datetime], Optional[Dict[str, ValueProto]]]] = []
        entity_ids = [compute_entity_id(entity_key) for entity_key in entity_keys]
        batch_size = online_config.batch_size
        entity_ids_iter = iter(entity_ids)
        while True:
            batch = list(itertools.islice(entity_ids_iter, batch_size))
            # No more items to insert
            if len(batch) == 0:
                break
            batch_entity_ids = {
                table_instance.name: {
                    "Keys": [{"entity_id": entity_id} for entity_id in batch]
                }
            }
            with tracing_span(name="remote_call"):
                response = dynamodb_resource.batch_get_item(
                    RequestItems=batch_entity_ids
                )
            response = response.get("Responses")
            table_responses = response.get(table_instance.name)
            if table_responses:
                table_responses = self._sort_dynamodb_response(
                    table_responses, entity_ids
                )
                entity_idx = 0
                for tbl_res in table_responses:
                    entity_id = tbl_res["entity_id"]
                    while entity_id != batch[entity_idx]:
                        result.append((None, None))
                        entity_idx += 1
                    res = {}
                    for feature_name, value_bin in tbl_res["values"].items():
                        val = ValueProto()
                        val.ParseFromString(value_bin.value)
                        res[feature_name] = val
                    result.append((datetime.fromisoformat(tbl_res["event_ts"]), res))
                    entity_idx += 1

            # Not all entities in a batch may have responses
            # Pad with remaining values in batch that were not found
            batch_size_nones = ((None, None),) * (len(batch) - len(result))
            result.extend(batch_size_nones)
        return result

    def _get_dynamodb_client(self, region: str, endpoint_url: Optional[str] = None):
        if self._dynamodb_client is None:
            self._dynamodb_client = _initialize_dynamodb_client(region, endpoint_url)
        return self._dynamodb_client

    def _get_dynamodb_resource(self, region: str, endpoint_url: Optional[str] = None):
        if self._dynamodb_resource is None:
            self._dynamodb_resource = _initialize_dynamodb_resource(
                region, endpoint_url
            )
        return self._dynamodb_resource

    def _sort_dynamodb_response(self, responses: list, order: list):
        """DynamoDB Batch Get Item doesn't return items in a particular order."""
        # Assign an index to order
        order_with_index = {value: idx for idx, value in enumerate(order)}
        # Sort table responses by index
        table_responses_ordered = [
            (order_with_index[tbl_res["entity_id"]], tbl_res) for tbl_res in responses
        ]
        table_responses_ordered = sorted(
            table_responses_ordered, key=lambda tup: tup[0]
        )
        _, table_responses_ordered = zip(*table_responses_ordered)
        return table_responses_ordered

    @log_exceptions_and_usage(online_store="dynamodb")
    def _write_batch_non_duplicates(
        self,
        table_instance,
        data: List[
            Tuple[EntityKeyProto, Dict[str, ValueProto], datetime, Optional[datetime]]
        ],
        progress: Optional[Callable[[int], Any]],
    ):
        """Deduplicate write batch request items on ``entity_id`` primary key."""
        with table_instance.batch_writer(overwrite_by_pkeys=["entity_id"]) as batch:
            for entity_key, features, timestamp, created_ts in data:
                entity_id = compute_entity_id(entity_key)
                batch.put_item(
                    Item={
                        "entity_id": entity_id,  # PartitionKey
                        "event_ts": str(utils.make_tzaware(timestamp)),
                        "values": {
                            k: v.SerializeToString()
                            for k, v in features.items()  # Serialized Features
                        },
                    }
                )
                if progress:
                    progress(1)


def _initialize_dynamodb_client(region: str, endpoint_url: Optional[str] = None):
    return boto3.client("dynamodb", region_name=region, endpoint_url=endpoint_url)


def _initialize_dynamodb_resource(region: str, endpoint_url: Optional[str] = None):
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
    dynamodb_resource, table_name: str,
):
    try:
        table = dynamodb_resource.Table(table_name)
        table.delete()
        logger.info(f"Dynamo table {table_name} was deleted")
    except ClientError as ce:
        # If the table deletion fails with ResourceNotFoundException,
        # it means the table has already been deleted.
        # Otherwise, re-raise the exception
        if ce.response["Error"]["Code"] != "ResourceNotFoundException":
            raise
        else:
            logger.warning(f"Trying to delete table that doesn't exist: {table_name}")


class DynamoDBTable(InfraObject):
    """
    A DynamoDB table managed by Feast.

    Attributes:
        name: The name of the table.
        region: The region of the table.
        endpoint_url: Local DynamoDB Endpoint Url.
        _dynamodb_client: Boto3 DynamoDB client.
        _dynamodb_resource: Boto3 DynamoDB resource.
    """

    region: str
    endpoint_url = None
    _dynamodb_client = None
    _dynamodb_resource = None

    def __init__(self, name: str, region: str, endpoint_url: Optional[str] = None):
        super().__init__(name)
        self.region = region
        self.endpoint_url = endpoint_url

    def to_infra_object_proto(self) -> InfraObjectProto:
        dynamodb_table_proto = self.to_proto()
        return InfraObjectProto(
            infra_object_class_type=DYNAMODB_INFRA_OBJECT_CLASS_TYPE,
            dynamodb_table=dynamodb_table_proto,
        )

    def to_proto(self) -> Any:
        dynamodb_table_proto = DynamoDBTableProto()
        dynamodb_table_proto.name = self.name
        dynamodb_table_proto.region = self.region
        return dynamodb_table_proto

    @staticmethod
    def from_infra_object_proto(infra_object_proto: InfraObjectProto) -> Any:
        return DynamoDBTable(
            name=infra_object_proto.dynamodb_table.name,
            region=infra_object_proto.dynamodb_table.region,
        )

    @staticmethod
    def from_proto(dynamodb_table_proto: DynamoDBTableProto) -> Any:
        return DynamoDBTable(
            name=dynamodb_table_proto.name, region=dynamodb_table_proto.region,
        )

    def update(self):
        dynamodb_client = self._get_dynamodb_client(self.region, self.endpoint_url)
        dynamodb_resource = self._get_dynamodb_resource(self.region, self.endpoint_url)

        try:
            dynamodb_resource.create_table(
                TableName=f"{self.name}",
                KeySchema=[{"AttributeName": "entity_id", "KeyType": "HASH"}],
                AttributeDefinitions=[
                    {"AttributeName": "entity_id", "AttributeType": "S"}
                ],
                BillingMode="PAY_PER_REQUEST",
            )
        except ClientError as ce:
            # If the table creation fails with ResourceInUseException,
            # it means the table already exists or is being created.
            # Otherwise, re-raise the exception
            if ce.response["Error"]["Code"] != "ResourceInUseException":
                raise

        dynamodb_client.get_waiter("table_exists").wait(TableName=f"{self.name}")

    def teardown(self):
        dynamodb_resource = self._get_dynamodb_resource(self.region, self.endpoint_url)
        _delete_table_idempotent(dynamodb_resource, self.name)

    def _get_dynamodb_client(self, region: str, endpoint_url: Optional[str] = None):
        if self._dynamodb_client is None:
            self._dynamodb_client = _initialize_dynamodb_client(region, endpoint_url)
        return self._dynamodb_client

    def _get_dynamodb_resource(self, region: str, endpoint_url: Optional[str] = None):
        if self._dynamodb_resource is None:
            self._dynamodb_resource = _initialize_dynamodb_resource(
                region, endpoint_url
            )
        return self._dynamodb_resource
