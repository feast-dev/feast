from datetime import datetime
from typing import List, Optional

from feast import FeatureTable
from feast.infra.provider import Provider
from feast.repo_config import DatastoreOnlineStoreConfig


def _delete_all_values(client, key) -> None:
    """
    Delete all data under the key path in datastore.
    """
    while True:
        query = client.query(kind="Value", ancestor=key)
        entities = list(query.fetch(limit=1000))
        if not entities:
            return

        for entity in entities:
            print("Deleting: {}".format(entity))
            client.delete(entity.key)


class Gcp(Provider):
    _project_id: Optional[str]

    def __init__(self, config: Optional[DatastoreOnlineStoreConfig]):
        if config:
            self._project_id = config.project_id
        else:
            self._project_id = None

    def _initialize_client(self):
        from google.cloud import datastore

        if self._project_id is not None:
            return datastore.Client(self.project_id)
        else:
            return datastore.Client()

    def update_infra(
        self,
        project: str,
        tables_to_delete: List[FeatureTable],
        tables_to_keep: List[FeatureTable],
    ):
        from google.cloud import datastore

        client = self._initialize_client()

        for table in tables_to_keep:
            key = client.key("FeastProject", project, "FeatureTable", table.name)
            entity = datastore.Entity(key=key)
            entity.update({"created_at": datetime.utcnow()})
            client.put(entity)

        for table in tables_to_delete:
            _delete_all_values(
                client, client.key("FeastProject", project, "FeatureTable", table.name)
            )

            # Delete the table metadata datastore entity
            key = client.key("FeastProject", project, "FeatureTable", table.name)
            client.delete(key)

    def teardown_infra(self, project: str, tables: List[FeatureTable]) -> None:
        client = self._initialize_client()

        for table in tables:
            _delete_all_values(
                client, client.key("FeastProject", project, "FeatureTable", table.name)
            )

            # Delete the table metadata datastore entity
            key = client.key("FeastProject", project, "FeatureTable", table.name)
            client.delete(key)
