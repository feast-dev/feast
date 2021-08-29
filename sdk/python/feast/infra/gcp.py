import uuid
from datetime import datetime
from tempfile import TemporaryFile
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple, Union
from urllib.parse import urlparse

import pandas
from tqdm import tqdm

from feast import FeatureTable
from feast.entity import Entity
from feast.feature_view import FeatureView
from feast.infra.offline_stores.offline_utils import get_offline_store_from_config
from feast.infra.online_stores.helpers import get_online_store_from_config
from feast.infra.provider import (
    Provider,
    RetrievalJob,
    _convert_arrow_to_proto,
    _get_column_names,
    _run_field_mapping,
)
from feast.protos.feast.core.Registry_pb2 import Registry as RegistryProto
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from feast.registry import Registry, RegistryStore
from feast.repo_config import RepoConfig


class GcpProvider(Provider):
    _gcp_project_id: Optional[str]
    _namespace: Optional[str]

    def __init__(self, config: RepoConfig):
        self.repo_config = config
        self.offline_store = get_offline_store_from_config(config.offline_store)
        self.online_store = get_online_store_from_config(config.online_store)

    def update_infra(
        self,
        project: str,
        tables_to_delete: Sequence[Union[FeatureTable, FeatureView]],
        tables_to_keep: Sequence[Union[FeatureTable, FeatureView]],
        entities_to_delete: Sequence[Entity],
        entities_to_keep: Sequence[Entity],
        partial: bool,
    ):
        self.online_store.update(
            config=self.repo_config,
            tables_to_delete=tables_to_delete,
            tables_to_keep=tables_to_keep,
            entities_to_keep=entities_to_keep,
            entities_to_delete=entities_to_delete,
            partial=partial,
        )

    def teardown_infra(
        self,
        project: str,
        tables: Sequence[Union[FeatureTable, FeatureView]],
        entities: Sequence[Entity],
    ) -> None:
        self.online_store.teardown(self.repo_config, tables, entities)

    def online_write_batch(
        self,
        config: RepoConfig,
        table: Union[FeatureTable, FeatureView],
        data: List[
            Tuple[EntityKeyProto, Dict[str, ValueProto], datetime, Optional[datetime]]
        ],
        progress: Optional[Callable[[int], Any]],
    ) -> None:
        self.online_store.online_write_batch(config, table, data, progress)

    def online_read(
        self,
        config: RepoConfig,
        table: Union[FeatureTable, FeatureView],
        entity_keys: List[EntityKeyProto],
        requested_features: List[str] = None,
    ) -> List[Tuple[Optional[datetime], Optional[Dict[str, ValueProto]]]]:
        result = self.online_store.online_read(config, table, entity_keys)

        return result

    def materialize_single_feature_view(
        self,
        config: RepoConfig,
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

        offline_job = self.offline_store.pull_latest_from_table_or_query(
            config=config,
            data_source=feature_view.batch_source,
            join_key_columns=join_key_columns,
            feature_name_columns=feature_name_columns,
            event_timestamp_column=event_timestamp_column,
            created_timestamp_column=created_timestamp_column,
            start_date=start_date,
            end_date=end_date,
        )
        table = offline_job.to_arrow()

        if feature_view.batch_source.field_mapping is not None:
            table = _run_field_mapping(table, feature_view.batch_source.field_mapping)

        join_keys = [entity.join_key for entity in entities]
        rows_to_write = _convert_arrow_to_proto(table, feature_view, join_keys)

        with tqdm_builder(len(rows_to_write)) as pbar:
            self.online_write_batch(
                self.repo_config, feature_view, rows_to_write, lambda x: pbar.update(x)
            )

    def get_historical_features(
        self,
        config: RepoConfig,
        feature_views: List[FeatureView],
        feature_refs: List[str],
        entity_df: Union[pandas.DataFrame, str],
        registry: Registry,
        project: str,
        full_feature_names: bool,
    ) -> RetrievalJob:
        job = self.offline_store.get_historical_features(
            config=config,
            feature_views=feature_views,
            feature_refs=feature_refs,
            entity_df=entity_df,
            registry=registry,
            project=project,
            full_feature_names=full_feature_names,
        )
        return job


class GCSRegistryStore(RegistryStore):
    def __init__(self, uri: str):
        try:
            from google.cloud import storage
        except ImportError as e:
            from feast.errors import FeastExtrasDependencyImportError

            raise FeastExtrasDependencyImportError("gcp", str(e))

        self.gcs_client = storage.Client()
        self._uri = urlparse(uri)
        self._bucket = self._uri.hostname
        self._blob = self._uri.path.lstrip("/")

    def get_registry_proto(self):
        from google.cloud import storage
        from google.cloud.exceptions import NotFound

        file_obj = TemporaryFile()
        registry_proto = RegistryProto()
        try:
            bucket = self.gcs_client.get_bucket(self._bucket)
        except NotFound:
            raise Exception(
                f"No bucket named {self._bucket} exists; please create it first."
            )
        if storage.Blob(bucket=bucket, name=self._blob).exists(self.gcs_client):
            self.gcs_client.download_blob_to_file(
                self._uri.geturl(), file_obj, timeout=30
            )
            file_obj.seek(0)
            registry_proto.ParseFromString(file_obj.read())
            return registry_proto
        raise FileNotFoundError(
            f'Registry not found at path "{self._uri.geturl()}". Have you run "feast apply"?'
        )

    def update_registry_proto(self, registry_proto: RegistryProto):
        self._write_registry(registry_proto)

    def teardown(self):
        from google.cloud.exceptions import NotFound

        gs_bucket = self.gcs_client.get_bucket(self._bucket)
        try:
            gs_bucket.delete_blob(self._blob)
        except NotFound:
            # If the blob deletion fails with NotFound, it has already been deleted.
            pass

    def _write_registry(self, registry_proto: RegistryProto):
        registry_proto.version_id = str(uuid.uuid4())
        registry_proto.last_updated.FromDatetime(datetime.utcnow())
        # we have already checked the bucket exists so no need to do it again
        gs_bucket = self.gcs_client.get_bucket(self._bucket)
        blob = gs_bucket.blob(self._blob)
        file_obj = TemporaryFile()
        file_obj.write(registry_proto.SerializeToString())
        file_obj.seek(0)
        blob.upload_from_file(file_obj)
