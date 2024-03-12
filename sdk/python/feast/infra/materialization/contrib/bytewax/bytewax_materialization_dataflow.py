import logging
import os
from typing import List

import pyarrow as pa
import pyarrow.parquet as pq
from bytewax.dataflow import Dataflow  # type: ignore
from bytewax.execution import cluster_main
from bytewax.inputs import ManualInputConfig
from bytewax.outputs import ManualOutputConfig

from feast import FeatureStore, FeatureView, RepoConfig
from feast.utils import _convert_arrow_to_proto, _run_pyarrow_field_mapping

logger = logging.getLogger(__name__)
DEFAULT_BATCH_SIZE = 1000


class BytewaxMaterializationDataflow:
    def __init__(
        self,
        config: RepoConfig,
        feature_view: FeatureView,
        paths: List[str],
        worker_index: int,
    ):
        self.config = config
        self.feature_store = FeatureStore(config=config)

        self.feature_view = feature_view
        self.worker_index = worker_index
        self.paths = paths
        self.mini_batch_size = int(
            os.getenv("BYTEWAX_MINI_BATCH_SIZE", DEFAULT_BATCH_SIZE)
        )

        self._run_dataflow()

    def process_path(self, path):
        logger.info(f"Processing path {path}")
        dataset = pq.ParquetDataset(path, use_legacy_dataset=False)
        batches = []
        for fragment in dataset.fragments:
            for batch in fragment.to_table().to_batches(
                max_chunksize=self.mini_batch_size
            ):
                batches.append(batch)

        return batches

    def input_builder(self, worker_index, worker_count, _state):
        return [(None, self.paths[self.worker_index])]

    def output_builder(self, worker_index, worker_count):
        def output_fn(mini_batch):
            table: pa.Table = pa.Table.from_batches([mini_batch])

            if self.feature_view.batch_source.field_mapping is not None:
                table = _run_pyarrow_field_mapping(
                    table, self.feature_view.batch_source.field_mapping
                )
            join_key_to_value_type = {
                entity.name: entity.dtype.to_value_type()
                for entity in self.feature_view.entity_columns
            }
            rows_to_write = _convert_arrow_to_proto(
                table, self.feature_view, join_key_to_value_type
            )
            self.feature_store._get_provider().online_write_batch(
                config=self.config,
                table=self.feature_view,
                data=rows_to_write,
                progress=None,
            )

        return output_fn

    def _run_dataflow(self):
        flow = Dataflow()
        flow.input("inp", ManualInputConfig(self.input_builder))
        flow.flat_map(self.process_path)
        flow.capture(ManualOutputConfig(self.output_builder))
        cluster_main(flow, [], 0)
