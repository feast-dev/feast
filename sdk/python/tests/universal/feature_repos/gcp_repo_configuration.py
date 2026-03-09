import os

from tests.universal.feature_repos.universal.data_sources.bigquery import (
    BigQueryDataSourceCreator,
)

BIGTABLE_CONFIG = {
    "type": "bigtable",
    "project_id": os.getenv("GCLOUD_PROJECT", "kf-feast"),
    "instance": os.getenv("BIGTABLE_INSTANCE_ID", "feast-integration-tests"),
}

AVAILABLE_OFFLINE_STORES = [("gcp", BigQueryDataSourceCreator)]
AVAILABLE_ONLINE_STORES = {
    "datastore": ("datastore", None),
    "bigtable": (BIGTABLE_CONFIG, None),
}

if os.getenv("FEAST_LOCAL_ONLINE_CONTAINER", "False").lower() == "true":
    from tests.universal.feature_repos.universal.online_store.bigtable import (
        BigtableOnlineStoreCreator,
    )
    from tests.universal.feature_repos.universal.online_store.datastore import (
        DatastoreOnlineStoreCreator,
    )

    AVAILABLE_ONLINE_STORES["datastore"] = ("datastore", DatastoreOnlineStoreCreator)
    AVAILABLE_ONLINE_STORES["bigtable"] = ("bigtable", BigtableOnlineStoreCreator)
