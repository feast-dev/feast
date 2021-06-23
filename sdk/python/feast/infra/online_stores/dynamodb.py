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

import mmh3
from pydantic import PositiveInt, StrictStr
from pydantic.typing import Literal

from feast import Entity, FeatureTable, FeatureView, utils
from feast.infra.key_encoding_utils import serialize_entity_key
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


class DynamoDbOnlineStoreConfig(FeastConfigBaseModel):
    """Online store config for DynamoDB store"""

    type: Literal["dynamodb"] = "dynamodb"
    """Online store type selector"""

    rcu: Optional[PositiveInt] = 5
    """ Read capacity unit """

    wcu: Optional[PositiveInt] = 5
    """ Write capacity unit """

    region_name: Optional[StrictStr] = None
    """ AWS Region Name """


class DynamoDbOnlineStore(OnlineStore):
    def _initialize_dynamodb(self, online_config: DynamoDbOnlineStoreConfig):
        return boto3.resource("dynamodb", region_name=online_config.region_name)

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
        assert isinstance(online_config, DynamoDbOnlineStoreConfig)
        dynamodb = self._initialize_dynamodb(online_config)

        for table_name in tables_to_keep:
            table = None
            try:
                table = dynamodb.create_table(
                    TableName=table_name.name,
                    KeySchema=[
                        {"AttributeName": "Row", "KeyType": "HASH"},
                        {"AttributeName": "Project", "KeyType": "RANGE"},
                    ],
                    AttributeDefinitions=[
                        {"AttributeName": "Row", "AttributeType": "S"},
                        {"AttributeName": "Project", "AttributeType": "S"},
                    ],
                    ProvisionedThroughput={
                        "ReadCapacityUnits": online_config.rcu,
                        "WriteCapacityUnits": online_config.wcu,
                    },
                )
                table.meta.client.get_waiter("table_exists").wait(
                    TableName=table_name.name
                )
            except ClientError as ce:
                print(ce)
                if ce.response["Error"]["Code"] == "ResourceNotFoundException":
                    table = dynamodb.Table(table_name.name)

        for table_name in tables_to_delete:
            table = dynamodb.Table(table_name.name)
            table.delete()

    def teardown(
        self,
        config: RepoConfig,
        tables: Sequence[Union[FeatureTable, FeatureView]],
        entities: Sequence[Entity],
    ):
        online_config = config.online_store
        assert isinstance(online_config, DynamoDbOnlineStoreConfig)
        dynamodb = self._initialize_dynamodb(online_config)

        for table_name in tables:
            try:
                table = dynamodb.Table(table_name)
                table.delete()
            except Exception as e:
                print(str(e))

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
        assert isinstance(online_config, DynamoDbOnlineStoreConfig)
        dynamodb = self._initialize_dynamodb(online_config)

        table_instance = dynamodb.Table(table.name)
        with table_instance.batch_writer() as batch:
            for entity_key, features, timestamp, created_ts in data:
                document_id = compute_datastore_entity_id(entity_key)  # TODO check id
                # TODO compression encoding
                batch.put_item(
                    Item={
                        "Row": document_id,  # PartitionKey
                        "Project": config.project,  # SortKey
                        "event_ts": str(utils.make_tzaware(timestamp)),
                        "values": {
                            k: v.SerializeToString()
                            for k, v in features.items()  # Serialized Features
                        },
                    }
                )

    def online_read(
        self,
        config: RepoConfig,
        table: Union[FeatureTable, FeatureView],
        entity_keys: List[EntityKeyProto],
        requested_features: Optional[List[str]] = None,
    ) -> List[Tuple[Optional[datetime], Optional[Dict[str, ValueProto]]]]:
        online_config = config.online_store
        assert isinstance(online_config, DynamoDbOnlineStoreConfig)
        dynamodb = self._initialize_dynamodb(online_config)

        result: List[Tuple[Optional[datetime], Optional[Dict[str, ValueProto]]]] = []
        for entity_key in entity_keys:
            table_instace = dynamodb.Table(table.name)
            document_id = compute_datastore_entity_id(entity_key)  # TODO check id
            response = table_instace.get_item(
                Key={"Row": document_id, "Project": config.project}
            )
            value = response["Item"]

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


def compute_datastore_entity_id(entity_key: EntityKeyProto) -> str:
    """
    Compute Datastore Entity id given Feast Entity Key.

    Remember that Datastore Entity is a concept from the Datastore data model, that has nothing to
    do with the Entity concept we have in Feast.
    """
    return mmh3.hash_bytes(serialize_entity_key(entity_key)).hex()
