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
import logging
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

from pydantic import StrictStr
from pydantic.typing import Literal

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

    region: StrictStr
    """ AWS Region Name """


class DynamoDBOnlineStore(OnlineStore):
    """
    Online feature store for AWS DynamoDB.
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
        online_config = config.online_store
        assert isinstance(online_config, DynamoDBOnlineStoreConfig)
        dynamodb_client = self._get_dynamodb_client(online_config.region)
        dynamodb_resource = self._get_dynamodb_resource(online_config.region)

        for table_instance in tables_to_keep:
            try:
                dynamodb_resource.create_table(
                    TableName=_get_table_name(config, table_instance),
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
                TableName=_get_table_name(config, table_instance)
            )

        for table_to_delete in tables_to_delete:
            _delete_table_idempotent(
                dynamodb_resource, _get_table_name(config, table_to_delete)
            )

    def teardown(
        self,
        config: RepoConfig,
        tables: Sequence[FeatureView],
        entities: Sequence[Entity],
    ):
        online_config = config.online_store
        assert isinstance(online_config, DynamoDBOnlineStoreConfig)
        dynamodb_resource = self._get_dynamodb_resource(online_config.region)

        for table in tables:
            _delete_table_idempotent(dynamodb_resource, _get_table_name(config, table))

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
        online_config = config.online_store
        assert isinstance(online_config, DynamoDBOnlineStoreConfig)
        dynamodb_resource = self._get_dynamodb_resource(online_config.region)

        table_instance = dynamodb_resource.Table(_get_table_name(config, table))
        with table_instance.batch_writer() as batch:
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

    @log_exceptions_and_usage(online_store="dynamodb")
    def online_read(
        self,
        config: RepoConfig,
        table: FeatureView,
        entity_keys: List[EntityKeyProto],
        requested_features: Optional[List[str]] = None,
    ) -> List[Tuple[Optional[datetime], Optional[Dict[str, ValueProto]]]]:
        online_config = config.online_store
        assert isinstance(online_config, DynamoDBOnlineStoreConfig)
        dynamodb_resource = self._get_dynamodb_resource(online_config.region)

        result: List[Tuple[Optional[datetime], Optional[Dict[str, ValueProto]]]] = []
        for entity_key in entity_keys:
            table_instance = dynamodb_resource.Table(_get_table_name(config, table))
            entity_id = compute_entity_id(entity_key)
            with tracing_span(name="remote_call"):
                response = table_instance.get_item(Key={"entity_id": entity_id})
            value = response.get("Item")

            if value is not None:
                res = {}
                for feature_name, value_bin in value["values"].items():
                    val = ValueProto()
                    val.ParseFromString(value_bin.value)
                    res[feature_name] = val
                result.append((datetime.fromisoformat(value["event_ts"]), res))
            else:
                result.append((None, None))
        return result

    def _get_dynamodb_client(self, region: str):
        if self._dynamodb_client is None:
            self._dynamodb_client = _initialize_dynamodb_client(region)
        return self._dynamodb_client

    def _get_dynamodb_resource(self, region: str):
        if self._dynamodb_resource is None:
            self._dynamodb_resource = _initialize_dynamodb_resource(region)
        return self._dynamodb_resource


def _initialize_dynamodb_client(region: str):
    return boto3.client("dynamodb", region_name=region)


def _initialize_dynamodb_resource(region: str):
    return boto3.resource("dynamodb", region_name=region)


def _get_table_name(config: RepoConfig, table: FeatureView) -> str:
    return f"{config.project}.{table.name}"


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
    """

    name: str
    region: str

    def __init__(self, name: str, region: str):
        self.name = name
        self.region = region

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
        dynamodb_client = _initialize_dynamodb_client(region=self.region)
        dynamodb_resource = _initialize_dynamodb_resource(region=self.region)

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
        dynamodb_resource = _initialize_dynamodb_resource(region=self.region)
        _delete_table_idempotent(dynamodb_resource, self.name)
