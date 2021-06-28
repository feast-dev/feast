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
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple, Union

from pydantic import StrictStr
from pydantic.typing import Literal

from feast import Entity, FeatureTable, FeatureView, utils
from feast.infra.online_stores.helpers import compute_entity_id
from feast.infra.online_stores.online_store import OnlineStore
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from feast.repo_config import FeastConfigBaseModel, RepoConfig

try:
    import boto3
    from botocore.exceptions import ClientError
except ImportError as e:
    from feast.errors import FeastExtrasDependencyImportError

    raise FeastExtrasDependencyImportError("aws", str(e))


class DynamoDBOnlineStoreConfig(FeastConfigBaseModel):
    """Online store config for DynamoDB store"""

    type: Literal["dynamodb"] = "dynamodb"
    """Online store type selector"""

    region: Optional[StrictStr] = None
    """ AWS Region Name """


class DynamoDBOnlineStore(OnlineStore):
    """
    Online feature store for AWS DynamoDB.
    """

    _client: Optional[Union[Redis, RedisCluster]] = None

    def _initialize_dynamodb(self, online_config: DynamoDBOnlineStoreConfig):
        return boto3.client("dynamodb", region_name=online_config.region), boto3.resource("dynamodb", region_name=online_config.region)

    def update(
        self,
        config: RepoConfig,
        tables_to_delete: Sequence[Union[FeatureTable, FeatureView]],
        tables_to_keep: Sequence[Union[FeatureTable, FeatureView]],
        entities_to_delete: Sequence[Entity],
        entities_to_keep: Sequence[Entity],
        partial: bool,
    ):
        online_config = config.online_store
        assert isinstance(online_config, DynamoDBOnlineStoreConfig)
        client, dynamodb = self._initialize_dynamodb(online_config)

        for table_instance in tables_to_keep:
            try:
                table = dynamodb.create_table(
                    TableName=f"{config.project}.{table_instance.name}",
                    KeySchema=[{"AttributeName": "entity_id", "KeyType": "HASH"}],
                    AttributeDefinitions=[
                        {"AttributeName": "entity_id", "AttributeType": "S"}
                    ],
                    BillingMode="PAY_PER_REQUEST",
                )
            except ClientError as ce:
                print(ce)
                if ce.response["Error"]["Code"] == "ResourceNotFoundException":
                    table = dynamodb.Table(f"{config.project}.{table_instance.name}")

        for table_instance in tables_to_keep:
            client.get_waiter("table_exists").wait(
                TableName=f"{config.project}.{table_instance.name}"
            )

        for table_instance in tables_to_delete:
            table = dynamodb.Table(f"{config.project}.{table_instance.name}")
            table.delete()

    def teardown(
        self,
        config: RepoConfig,
        tables: Sequence[Union[FeatureTable, FeatureView]],
        entities: Sequence[Entity],
    ):
        online_config = config.online_store
        assert isinstance(online_config, DynamoDBOnlineStoreConfig)
        _, dynamodb = self._initialize_dynamodb(online_config)

        for table_instance in tables:
            table = dynamodb.Table(f"{config.project}.{table_instance.name}")
            table.delete()

    def online_write_batch(
        self,
        config: RepoConfig,
        table: Union[FeatureTable, FeatureView],
        data: List[
            Tuple[EntityKeyProto, Dict[str, ValueProto], datetime, Optional[datetime]]
        ],
        progress: Optional[Callable[[int], Any]],
    ) -> None:
        online_config = config.online_store
        assert isinstance(online_config, DynamoDBOnlineStoreConfig)
        _, dynamodb = self._initialize_dynamodb(online_config)

        table_instance = dynamodb.Table(f"{config.project}.{table.name}")
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

    def online_read(
        self,
        config: RepoConfig,
        table: Union[FeatureTable, FeatureView],
        entity_keys: List[EntityKeyProto],
        requested_features: Optional[List[str]] = None,
    ) -> List[Tuple[Optional[datetime], Optional[Dict[str, ValueProto]]]]:
        online_config = config.online_store
        assert isinstance(online_config, DynamoDBOnlineStoreConfig)
        _, dynamodb = self._initialize_dynamodb(online_config)

        result: List[Tuple[Optional[datetime], Optional[Dict[str, ValueProto]]]] = []
        for entity_key in entity_keys:
            table_instance = dynamodb.Table(f"{config.project}.{table.name}")
            entity_id = compute_entity_id(entity_key)  # TODO check id
            response = table_instance.get_item(Key={"entity_id": entity_id})
            value = response.get("Item", None)

            if value is not None:
                res = {}
                for feature_name, value_bin in value["values"].items():
                    val = ValueProto()
                    val.ParseFromString(value_bin.value)
                    res[feature_name] = val
                result.append((value["event_ts"], res))
            else:
                result.append((None, None))
        return result
