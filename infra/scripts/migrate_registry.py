import sys
from feast import Client

"""
Usage: python migrate_registry.py FEAST_CORE_URL REGISTRY_PATH

The object store registry will be written to the REGISTRY_PATH. If there is already
a registry at that path, this script will update that registry, so please make sure
the path does not have a file/blob.
"""

feast_core_url = sys.argv[1]
registry_path = sys.argv[2]

core_client = Client(core_url=feast_core_url)
object_store_client = Client(registry_path=registry_path)

projects = core_client.list_projects()
for project in projects:
    entities = core_client.list_entities(project)
    for entity in entities:
        object_store_client.apply_entity(entity, project)
    for feature_table in core_client.list_feature_tables(project):
        object_store_client.apply_feature_table(feature_table, project)
