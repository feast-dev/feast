import os

from tests.universal.feature_repos.universal.data_sources.redshift import (
    RedshiftDataSourceCreator,
)

DYNAMO_CONFIG = {"type": "dynamodb", "region": "us-west-2"}

AVAILABLE_OFFLINE_STORES = [("aws", RedshiftDataSourceCreator)]
AVAILABLE_ONLINE_STORES = {"dynamodb": (DYNAMO_CONFIG, None)}

if os.getenv("FEAST_LOCAL_ONLINE_CONTAINER", "False").lower() == "true":
    from tests.universal.feature_repos.universal.online_store.dynamodb import (
        DynamoDBOnlineStoreCreator,
    )

    AVAILABLE_ONLINE_STORES["dynamodb"] = (DYNAMO_CONFIG, DynamoDBOnlineStoreCreator)
