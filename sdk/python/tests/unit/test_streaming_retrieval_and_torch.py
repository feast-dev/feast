# Copyright 2024 The Feast Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import subprocess
import sys
from typing import Iterator, List, Optional
from unittest.mock import MagicMock

import pyarrow as pa
import pytest
import torch
from torch.utils.data import DataLoader

from feast.infra.offline_stores.offline_store import (
    RetrievalJob,
    RetrievalMetadata,
)
from feast.integrations.pytorch import FeastIterableDataset as PytorchFID
from feast.on_demand_feature_view import OnDemandFeatureView
from feast.saved_dataset import SavedDatasetStorage
from feast.torch import FeastIterableDataset


class DummyRetrievalJob(RetrievalJob):
    """A minimal RetrievalJob implementation for unit testing batching and cleanup."""

    def __init__(self, table: pa.Table, close_spy: Optional[MagicMock] = None):
        super().__init__()
        self._table = table
        self._close_spy = close_spy or MagicMock()
        self._odfv_list: List[OnDemandFeatureView] = []

    def _to_arrow_internal(self, timeout: Optional[int] = None) -> pa.Table:
        return self._table

    def _to_arrow_batches_internal(
        self, batch_size: int = 8192, timeout: Optional[int] = None
    ) -> Iterator[pa.RecordBatch]:
        reader = self._table.to_reader(max_chunksize=batch_size)
        try:
            for batch in reader:
                yield batch
        finally:
            self._close_spy()
            if hasattr(reader, "close"):
                reader.close()

    @property
    def full_feature_names(self) -> bool:
        return False

    @property
    def on_demand_feature_views(self) -> List[OnDemandFeatureView]:
        return self._odfv_list

    def persist(
        self,
        storage: SavedDatasetStorage,
        allow_overwrite: bool = False,
        timeout: Optional[int] = None,
    ):
        pass

    @property
    def metadata(self) -> Optional[RetrievalMetadata]:
        return None


def test_core_feast_does_not_import_torch():
    """Verify that `import feast` never imports `torch` into sys.modules."""
    code = (
        "import sys\n"
        "import feast\n"
        "assert 'torch' not in sys.modules, f'torch was eagerly imported: {sys.modules.get(\"torch\")}'\n"
        "print('PASS')\n"
    )
    res = subprocess.run(
        [sys.executable, "-c", code],
        capture_output=True,
        text=True,
        check=True,
    )
    assert "PASS" in res.stdout


def test_optional_pytorch_adapter_exports():
    """Verify both `feast.torch` and `feast.integrations.pytorch` export FeastIterableDataset."""
    assert FeastIterableDataset is not None
    assert PytorchFID is FeastIterableDataset


def test_to_arrow_batches_bounded_chunks():
    """Verify RetrievalJob.to_arrow_batches yields bounded RecordBatch chunks."""
    total_rows = 250
    batch_size = 64
    table = pa.Table.from_arrays(
        [
            pa.array(list(range(total_rows)), type=pa.int64()),
            pa.array([float(x) * 1.5 for x in range(total_rows)], type=pa.float64()),
        ],
        names=["id", "feature_val"],
    )
    job = DummyRetrievalJob(table)

    batches = list(job.to_arrow_batches(batch_size=batch_size))
    assert len(batches) == 4  # 64 + 64 + 64 + 58 = 250

    for i, b in enumerate(batches):
        assert isinstance(b, pa.RecordBatch)
        if i < 3:
            assert b.num_rows == batch_size
        else:
            assert b.num_rows == 58

    recovered_table = pa.Table.from_batches(batches)
    assert recovered_table.equals(table)


def test_to_arrow_batches_early_close_cleanup():
    """Verify early exit from to_arrow_batches generator triggers resource cleanup."""
    table = pa.Table.from_arrays(
        [pa.array(list(range(100)), type=pa.int64())], names=["id"]
    )
    close_spy = MagicMock()
    job = DummyRetrievalJob(table, close_spy=close_spy)

    gen = job.to_arrow_batches(batch_size=10)
    first_batch = next(gen)
    assert first_batch.num_rows == 10
    assert close_spy.call_count == 0

    # Break early / close generator
    gen.close()
    assert close_spy.call_count == 1


def test_to_arrow_batches_error_propagation():
    """Verify errors encountered during batch generation propagate and clean up."""

    class FailingJob(DummyRetrievalJob):
        def _to_arrow_batches_internal(
            self, batch_size: int = 8192, timeout: Optional[int] = None
        ) -> Iterator[pa.RecordBatch]:
            try:
                yield pa.RecordBatch.from_arrays([pa.array([1])], names=["id"])
                raise RuntimeError("Connection broken")
            finally:
                self._close_spy()

    close_spy = MagicMock()
    table = pa.Table.from_arrays([pa.array([1])], names=["id"])
    job = FailingJob(table, close_spy=close_spy)

    gen = job.to_arrow_batches(batch_size=1)
    batch = next(gen)
    assert batch.num_rows == 1

    with pytest.raises(RuntimeError, match="Connection broken"):
        next(gen)

    assert close_spy.call_count == 1


def test_dataset_single_worker_fidelity_and_ordering():
    """Verify FeastIterableDataset yields individual samples preserving ordering and types."""
    n_samples = 50
    table = pa.Table.from_arrays(
        [
            pa.array(list(range(n_samples)), type=pa.int64()),
            pa.array([f"entity_{i}" for i in range(n_samples)], type=pa.string()),
            pa.array([i * 0.1 for i in range(n_samples)], type=pa.float32()),
        ],
        names=["id", "entity_id", "score"],
    )
    job = DummyRetrievalJob(table)
    dataset = FeastIterableDataset(job, batch_size=16)

    samples = list(dataset)
    assert len(samples) == n_samples

    for idx, sample in enumerate(samples):
        assert isinstance(sample["id"], torch.Tensor)
        assert sample["id"].item() == idx
        assert sample["entity_id"] == f"entity_{idx}"
        assert isinstance(sample["score"], torch.Tensor)
        assert pytest.approx(sample["score"].item(), 1e-4) == idx * 0.1


def test_dataset_multi_rank_deterministic_sharding():
    """Verify multi-rank sharding (e.g. DDP ranks) produces zero duplicate rows across ranks."""
    n_samples = 100
    world_size = 4
    table = pa.Table.from_arrays(
        [pa.array(list(range(n_samples)), type=pa.int64())], names=["id"]
    )

    all_rank_ids = []
    for rank in range(world_size):
        job = DummyRetrievalJob(table)
        dataset = FeastIterableDataset(
            job, batch_size=15, rank=rank, world_size=world_size
        )
        rank_ids = [s["id"].item() for s in dataset]

        # Verify ordering is monotonically increasing within this rank
        assert rank_ids == sorted(rank_ids)
        all_rank_ids.extend(rank_ids)

    # Verify zero duplicates and full coverage
    assert len(all_rank_ids) == n_samples
    assert sorted(all_rank_ids) == list(range(n_samples))


def test_dataset_dataloader_multi_worker_zero_duplicates():
    """Verify PyTorch DataLoader with num_workers > 1 yields all samples with zero duplicates."""
    n_samples = 80
    num_workers = 3
    table = pa.Table.from_arrays(
        [
            pa.array(list(range(n_samples)), type=pa.int64()),
            pa.array([i * 2.0 for i in range(n_samples)], type=pa.float32()),
        ],
        names=["id", "feature_a"],
    )

    # Pass a factory callable to ensure clean worker isolation
    def job_factory():
        return DummyRetrievalJob(table)

    dataset = FeastIterableDataset(job_factory, batch_size=16)
    loader = DataLoader(dataset, batch_size=8, num_workers=num_workers)

    collected_ids = []
    for batch in loader:
        collected_ids.extend(batch["id"].tolist())

    assert len(collected_ids) == n_samples
    assert sorted(collected_ids) == list(range(n_samples))


def test_dataset_combined_multi_rank_and_multi_worker():
    """Verify distributed rank + DataLoader multi-worker sharding coverage."""
    n_samples = 60
    world_size = 2
    num_workers = 2
    table = pa.Table.from_arrays(
        [pa.array(list(range(n_samples)), type=pa.int64())], names=["id"]
    )

    all_collected = []

    for rank in range(world_size):

        def job_factory():
            return DummyRetrievalJob(table)

        dataset = FeastIterableDataset(
            job_factory, batch_size=10, rank=rank, world_size=world_size
        )
        loader = DataLoader(dataset, batch_size=5, num_workers=num_workers)
        for batch in loader:
            all_collected.extend(batch["id"].tolist())

    assert len(all_collected) == n_samples
    assert sorted(all_collected) == list(range(n_samples))


def test_dataset_yield_batches_mode():
    """Verify yield_batches=True returns batch dictionaries with 1D tensors."""
    n_samples = 35
    table = pa.Table.from_arrays(
        [
            pa.array(list(range(n_samples)), type=pa.int32()),
            pa.array([float(x) for x in range(n_samples)], type=pa.float32()),
        ],
        names=["id", "val"],
    )
    job = DummyRetrievalJob(table)
    dataset = FeastIterableDataset(job, batch_size=10, yield_batches=True)

    batches = list(dataset)
    assert len(batches) == 4  # 10 + 10 + 10 + 5

    total_rows = 0
    for b in batches:
        assert isinstance(b["id"], torch.Tensor)
        assert isinstance(b["val"], torch.Tensor)
        assert b["id"].dim() == 1
        total_rows += len(b["id"])

    assert total_rows == n_samples


def test_duckdb_ibis_streaming_retrieval_job():
    """Verify DuckDB / IbisRetrievalJob streams batches directly without full table materialization."""
    import ibis

    from feast.infra.offline_stores.ibis import IbisRetrievalJob

    con = ibis.duckdb.connect()
    n_rows = 150
    tbl = pa.table(
        {
            "entity_id": list(range(n_rows)),
            "feature_v": [x * 0.5 for x in range(n_rows)],
        }
    )
    con.con.register("test_table", tbl)
    ibis_table = con.table("test_table")

    job = IbisRetrievalJob(
        table=ibis_table,
        on_demand_feature_views=[],
        full_feature_names=False,
        metadata=None,
        data_source_writer=MagicMock(),
        staging_location="dummy",
        staging_location_endpoint_override=None,
        repo_path="dummy",
    )

    batches = list(job.to_arrow_batches(batch_size=40))
    assert len(batches) == 4  # 40 + 40 + 40 + 30
    assert sum(b.num_rows for b in batches) == n_rows
    assert isinstance(batches[0], pa.RecordBatch)
