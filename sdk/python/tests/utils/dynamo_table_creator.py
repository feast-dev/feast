from datetime import datetime

import boto3

from feast import utils
from feast.infra.online_stores.helpers import compute_entity_id
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import Value as ValueProto


def create_n_customer_test_samples(n=10):
    return [
        (
            EntityKeyProto(
                join_keys=["customer"], entity_values=[ValueProto(string_val=str(i))]
            ),
            {
                "avg_orders_day": ValueProto(float_val=1.0),
                "name": ValueProto(string_val="John"),
                "age": ValueProto(int64_val=3),
            },
            datetime.utcnow(),
            None,
        )
        for i in range(n)
    ]


def create_test_table(project, tbl_name, region):
    client = boto3.client("dynamodb", region_name=region)
    client.create_table(
        TableName=f"{project}.{tbl_name}",
        KeySchema=[{"AttributeName": "entity_id", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "entity_id", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )


def delete_test_table(project, tbl_name, region):
    client = boto3.client("dynamodb", region_name=region)
    client.delete_table(TableName=f"{project}.{tbl_name}")


def insert_data_test_table(data, project, tbl_name, region):
    dynamodb_resource = boto3.resource("dynamodb", region_name=region)
    table_instance = dynamodb_resource.Table(f"{project}.{tbl_name}")
    for entity_key, features, timestamp, created_ts in data:
        entity_id = compute_entity_id(entity_key, entity_key_serialization_version=2)
        with table_instance.batch_writer() as batch:
            batch.put_item(
                Item={
                    "entity_id": entity_id,
                    "event_ts": str(utils.make_tzaware(timestamp)),
                    "values": {k: v.SerializeToString() for k, v in features.items()},
                }
            )
