# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.
import warnings
from datetime import datetime, timedelta
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple, Union

import pandas
import pyarrow as pa
from tqdm import tqdm

from feast import FeatureService
from feast.entity import Entity
from feast.feature_logging import FeatureServiceLoggingSource
from feast.feature_view import FeatureView
from feast.infra.offline_stores.offline_store import RetrievalJob
from feast.infra.offline_stores.offline_utils import get_offline_store_from_config
from feast.infra.online_stores.helpers import get_online_store_from_config
from feast.infra.passthrough_provider import PassthroughProvider
from feast.infra.provider import Provider
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from feast.registry import BaseRegistry
from feast.repo_config import RepoConfig
from feast.saved_dataset import SavedDataset
from feast.utils import (
    _convert_arrow_to_proto,
    _get_column_names,
    _run_pyarrow_field_mapping,
    make_tzaware,
)

DEFAULT_BATCH_SIZE = 10_000


class AzureProvider(PassthroughProvider):
    def materialize_single_feature_view(
        self,
        config: RepoConfig,
        feature_view: FeatureView,
        start_date: datetime,
        end_date: datetime,
        registry: BaseRegistry,
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
            timestamp_field=event_timestamp_column,
            created_timestamp_column=created_timestamp_column,
            start_date=start_date,
            end_date=end_date,
        )

        table = offline_job.to_arrow()

        if feature_view.batch_source.field_mapping is not None:
            table = _run_pyarrow_field_mapping(
                table, feature_view.batch_source.field_mapping
            )

        join_keys = {entity.join_key: entity.value_type for entity in entities}

        with tqdm_builder(table.num_rows) as pbar:
            for batch in table.to_batches(DEFAULT_BATCH_SIZE):
                rows_to_write = _convert_arrow_to_proto(batch, feature_view, join_keys)
                self.online_write_batch(
                    self.repo_config,
                    feature_view,
                    rows_to_write,
                    lambda x: pbar.update(x),
                )
