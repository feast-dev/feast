import os
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple, Union

import boto3
import mmh3
import pandas
from botocore.exceptions import ClientError
from tqdm import tqdm

from feast import FeatureTable, utils
from feast.entity import Entity
from feast.feature_view import FeatureView
from feast.infra.key_encoding_utils import serialize_entity_key
from feast.infra.offline_stores.helpers import get_offline_store_from_sources
from feast.infra.provider import (
    Provider,
    RetrievalJob,
    _convert_arrow_to_proto,
    _get_column_names,
    _run_field_mapping,
)
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from feast.registry import Registry
from feast.repo_config import DynamoDbOnlineStoreConfig, RepoConfig


class AwsDynamodbProvider(Provider):
    _wcu: int
    _rcu: int

    def __init__(self, config: RepoConfig):
        assert isinstance(config.online_store, DynamoDbOnlineStoreConfig)
        if config and config.online_store and config.online_store.rcu:
            self._rcu = config.online_store.rcu
        else:
            self._rcu = 5

        if config and config.online_store and config.online_store.wcu:
            self._wcu = config.online_store.wcu
        else:
            self._wcu = 5

        if config and config.online_store and config.online_store.region_name:
            self.region_name = config.online_store.region_name
            os.environ["AWS_DEFAULT_REGION"] = self.region_name
        else:
            self.region_name = os.environ.get("AWS_DEFAULT_REGION", "us-west-2")
            os.environ["AWS_DEFAULT_REGION"] = self.region_name

    def _initialize_dynamodb(self):
        return boto3.resource("dynamodb", region_name=self.region_name)

    def update_infra(
        self,
        project: str,
        tables_to_delete: Sequence[Union[FeatureTable, FeatureView]],
        tables_to_keep: Sequence[Union[FeatureTable, FeatureView]],
        entities_to_delete: Sequence[Entity],
        entities_to_keep: Sequence[Entity],
        partial: bool,
    ):
        dynamodb = self._initialize_dynamodb()

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
                        "ReadCapacityUnits": self._rcu,
                        "WriteCapacityUnits": self._wcu,
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

    def teardown_infra(
        self,
        project: str,
        tables: Sequence[Union[FeatureTable, FeatureView]],
        entities: Sequence[Entity],
    ) -> None:
        dynamodb = self._initialize_dynamodb()

        for table_name in tables:
            table = dynamodb.Table(table_name)
            table.delete()

    def online_write_batch(
        self,
        project: str,
        table: Union[FeatureTable, FeatureView],
        data: List[
            Tuple[EntityKeyProto, Dict[str, ValueProto], datetime, Optional[datetime]]
        ],
        progress: Optional[Callable[[int], Any]],
    ) -> None:
        dynamodb = self._initialize_dynamodb()

        table_instance = dynamodb.Table(table.name)
        with table_instance.batch_writer() as batch:
            for entity_key, features, timestamp, created_ts in data:
                document_id = compute_datastore_entity_id(entity_key)  # TODO check id
                # TODO compression encoding
                batch.put_item(
                    Item={
                        "Row": document_id,  # PartitionKey
                        "Project": project,  # SortKey
                        "event_ts": str(utils.make_tzaware(timestamp)),
                        "values": {
                            k: v.SerializeToString()
                            for k, v in features.items()  # Serialized Features
                        },
                    }
                )

    def online_read(
        self,
        project: str,
        table: Union[FeatureTable, FeatureView],
        entity_keys: List[EntityKeyProto],
    ) -> List[Tuple[Optional[datetime], Optional[Dict[str, ValueProto]]]]:
        dynamodb = self._initialize_dynamodb()

        result: List[Tuple[Optional[datetime], Optional[Dict[str, ValueProto]]]] = []
        for entity_key in entity_keys:
            table_instace = dynamodb.Table(table.name)
            document_id = compute_datastore_entity_id(entity_key)  # TODO check id
            response = table_instace.get_item(
                Key={"Row": document_id, "Project": project}
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

    def materialize_single_feature_view(
        self,
        feature_view: FeatureView,
        start_date: datetime,
        end_date: datetime,
        registry: Registry,
        project: str,
        tqdm_builder: Callable[[int], tqdm],
    ) -> None:
        entities = []
        for entity_name in feature_view.entities:
            entities.append(registry.get_entity(entity_name, project))

        (
            join_key_columns,
            feature_name_columns,
            event_timestamp_column,
            created_timestamp_column,
        ) = _get_column_names(feature_view, entities)

        start_date = utils.make_tzaware(start_date)
        end_date = utils.make_tzaware(end_date)

        offline_store = get_offline_store_from_sources([feature_view.input])
        table = offline_store.pull_latest_from_table_or_query(
            data_source=feature_view.input,
            join_key_columns=join_key_columns,
            feature_name_columns=feature_name_columns,
            event_timestamp_column=event_timestamp_column,
            created_timestamp_column=created_timestamp_column,
            start_date=start_date,
            end_date=end_date,
        )

        if feature_view.input.field_mapping is not None:
            table = _run_field_mapping(table, feature_view.input.field_mapping)

        join_keys = [entity.join_key for entity in entities]
        rows_to_write = _convert_arrow_to_proto(table, feature_view, join_keys)

        with tqdm_builder(len(rows_to_write)) as pbar:
            self.online_write_batch(
                project, feature_view, rows_to_write, lambda x: pbar.update(x)
            )

        feature_view.materialization_intervals.append((start_date, end_date))
        registry.apply_feature_view(feature_view, project)

    @staticmethod
    def get_historical_features(
        config: RepoConfig,
        feature_views: List[FeatureView],
        feature_refs: List[str],
        entity_df: Union[pandas.DataFrame, str],
        registry: Registry,
        project: str,
    ) -> RetrievalJob:
        # TODO implement me
        pass


def compute_datastore_entity_id(entity_key: EntityKeyProto) -> str:
    """
    Compute Datastore Entity id given Feast Entity Key.

    Remember that Datastore Entity is a concept from the Datastore data model, that has nothing to
    do with the Entity concept we have in Feast.
    """
    return mmh3.hash_bytes(serialize_entity_key(entity_key)).hex()
