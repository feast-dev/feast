import pandas as pd

from feast.infra.offline_stores.contrib.chronon_offline_store.chronon import (
    ChrononRetrievalJob,
)
from feast.infra.offline_stores.file_source import FileSource, SavedDatasetFileStorage
from feast.infra.offline_stores.offline_store import RetrievalJob
from feast.infra.passthrough_provider import PassthroughProvider
from feast.repo_config import RepoConfig
from feast.saved_dataset import SavedDataset


class ChrononProvider(PassthroughProvider):
    """Optional provider wrapper for Chronon-backed Feast configurations."""

    def retrieve_saved_dataset(
        self, config: RepoConfig, dataset: SavedDataset
    ) -> RetrievalJob:
        if not isinstance(dataset.storage, SavedDatasetFileStorage):
            raise ValueError("Chronon saved datasets require file storage.")
        options = dataset.storage.file_options
        uri = FileSource.get_uri_for_file_path(
            repo_path=config.repo_path, uri=options.uri
        )
        filesystem, path = FileSource.create_filesystem_and_path(
            str(uri), options.s3_endpoint_override
        )

        def evaluate() -> pd.DataFrame:
            # Read the saved result as-is: preserve request columns, custom time
            # column names, and already-computed on-demand features.
            return pd.read_parquet(path, filesystem=filesystem)

        return ChrononRetrievalJob(
            evaluate, dataset.full_feature_names, repo_path=str(config.repo_path)
        )
