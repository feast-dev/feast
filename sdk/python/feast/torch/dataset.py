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
import zlib
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Iterator,
    List,
    Optional,
    Union,
)

import pyarrow as pa

try:
    import torch
    from torch.utils.data import IterableDataset, get_worker_info

    TORCH_AVAILABLE = True
except ImportError as e:
    TORCH_AVAILABLE = False
    _torch_import_error = e

    # Fallback dummy class if torch is not installed
    class IterableDataset:  # type: ignore
        pass

    def get_worker_info():  # type: ignore
        return None


if TYPE_CHECKING:
    from feast.infra.offline_stores.offline_store import RetrievalJob
    from feast.saved_dataset import ValidationReference


def _check_torch_available() -> None:
    if not TORCH_AVAILABLE:
        raise ImportError(
            "PyTorch is required to use FeastIterableDataset.\n"
            "Install PyTorch with: pip install 'feast[torch]' or pip install torch"
        ) from _torch_import_error


def _arrow_array_to_tensor_or_list(col: pa.Array) -> Any:
    """Converts a PyArrow Array to a PyTorch Tensor if numeric/boolean, else to a Python list."""
    pa_type = col.type

    # Boolean
    if pa.types.is_boolean(pa_type):
        return torch.tensor(col.to_pylist(), dtype=torch.bool)

    # Integers
    if pa.types.is_int8(pa_type):
        return torch.tensor(col.to_pylist(), dtype=torch.int8)
    if pa.types.is_int16(pa_type):
        return torch.tensor(col.to_pylist(), dtype=torch.int16)
    if pa.types.is_int32(pa_type):
        return torch.tensor(col.to_pylist(), dtype=torch.int32)
    if pa.types.is_int64(pa_type):
        return torch.tensor(col.to_pylist(), dtype=torch.int64)
    if pa.types.is_uint8(pa_type):
        return torch.tensor(col.to_pylist(), dtype=torch.uint8)

    # Floats
    if pa.types.is_float16(pa_type):
        return torch.tensor(col.to_pylist(), dtype=torch.float16)
    if pa.types.is_float32(pa_type):
        return torch.tensor(col.to_pylist(), dtype=torch.float32)
    if pa.types.is_float64(pa_type):
        return torch.tensor(col.to_pylist(), dtype=torch.float64)

    # Default fallback: python list for strings, timestamps, nested lists, etc.
    return col.to_pylist()


def _scalar_to_tensor_or_value(val: Any) -> Any:
    """Converts a single scalar to a torch scalar tensor if numeric/boolean, preserving nulls and strings."""
    if val is None:
        return None
    if isinstance(val, bool):
        return torch.tensor(val, dtype=torch.bool)
    if isinstance(val, int):
        return torch.tensor(val, dtype=torch.int64)
    if isinstance(val, float):
        return torch.tensor(val, dtype=torch.float64)
    return val


class FeastIterableDataset(IterableDataset):
    """
    PyTorch IterableDataset backed by Feast RetrievalJob.to_arrow_batches.

    Streams bounded pyarrow.RecordBatch chunks directly from Feast offline store
    retrieval without materializing the full dataset in memory.

    Features:
    - Worker- and rank-aware deterministic sharding: Automatically detects
      PyTorch DataLoader multi-worker processes (via `torch.utils.data.get_worker_info()`)
      and distributed ranks (via `rank` and `world_size` or `torch.distributed`),
      guaranteeing zero duplicate rows across all workers/ranks.
    - Deterministic ordering: Preserves schema fidelity and point-in-time ordering.
    - Key-based vs. Positional sharding:
      * When `partition_key` is specified (recommended for distributed / multi-worker
        setups with unordered backend queries), rows are deterministically assigned
        to workers via key hashing/modulo (`hash(row[partition_key]) % total_shards == worker_id`),
        ensuring zero duplicates and complete coverage even if query results arrive in
        different orders on each worker.
      * When `partition_key` is None (default), positional round-robin sharding is used,
        which assumes the underlying retrieval job produces an identical, deterministically
        ordered stream across worker processes (e.g. sorted queries or deterministic entity DataFrames).
    - Resource cleanup: Ensures Arrow readers and iterators are cleanly closed on
      early generator break or cancellation.
    - Flexible output: Can yield individual row sample dicts (default, ready for standard
      DataLoader collation) or batch dicts / RecordBatches.

    Example:
        >>> from feast import FeatureStore
        >>> from feast.torch import FeastIterableDataset
        >>> from torch.utils.data import DataLoader
        >>>
        >>> store = FeatureStore(".")
        >>> job = store.get_historical_features(entity_df=entity_df, features=features)
        >>> dataset = FeastIterableDataset(job, batch_size=8192, partition_key="driver_id")
        >>> dataloader = DataLoader(dataset, batch_size=32, num_workers=2)
        >>> for batch in dataloader:
        ...     train(batch)
    """

    def __init__(
        self,
        retrieval_job_or_factory: Union["RetrievalJob", Callable[[], "RetrievalJob"]],
        batch_size: int = 8192,
        rank: Optional[int] = None,
        world_size: Optional[int] = None,
        yield_batches: bool = False,
        return_tensors: bool = True,
        field_names: Optional[List[str]] = None,
        partition_key: Optional[str] = None,
        validation_reference: Optional["ValidationReference"] = None,
        timeout: Optional[int] = None,
    ) -> None:
        """
        Initialize FeastIterableDataset.

        Args:
            retrieval_job_or_factory: A Feast RetrievalJob instance or a callable factory
                returning a RetrievalJob. Using a factory callable is recommended when
                running with multi-worker DataLoader (`num_workers > 0`) so each worker
                constructs an isolated retrieval stream.
            batch_size: Maximum chunk size when retrieving Arrow record batches (default: 8192).
            rank: Distributed worker rank (0 to world_size - 1). If None, auto-detected from
                `torch.distributed` when initialized.
            world_size: Total distributed ranks. If None, auto-detected from `torch.distributed`.
            yield_batches: If True, yields batches (`Dict[str, Tensor]` or RecordBatch).
                If False (default), yields individual row sample dictionaries.
            return_tensors: If True (default), numeric/boolean columns are returned as PyTorch
                Tensors. Strings and non-numeric types remain Python objects.
            field_names: Optional subset of feature/column names to project and yield.
            partition_key: Optional column name (e.g. entity id or primary key) to use for
                deterministic key-based row routing. When specified, guarantees zero duplicate
                rows even if underlying queries yield rows in non-deterministic order across workers.
            validation_reference: Optional validation profile for Feast dataset validation.
            timeout: Optional query timeout.
        """
        _check_torch_available()
        self.retrieval_job_or_factory = retrieval_job_or_factory
        self.batch_size = batch_size
        self.rank = rank
        self.world_size = world_size
        self.yield_batches = yield_batches
        self.return_tensors = return_tensors
        self.field_names = field_names
        self.partition_key = partition_key
        self.validation_reference = validation_reference
        self.timeout = timeout

    def _resolve_sharding(self) -> tuple[int, int]:
        """Resolves (global_worker_id, total_shards) from distributed rank and DataLoader worker info."""
        rank = self.rank
        world_size = self.world_size
        if rank is None or world_size is None:
            if torch.distributed.is_available() and torch.distributed.is_initialized():
                rank = rank if rank is not None else torch.distributed.get_rank()
                world_size = (
                    world_size
                    if world_size is not None
                    else torch.distributed.get_world_size()
                )
            else:
                rank = rank if rank is not None else 0
                world_size = world_size if world_size is not None else 1

        worker_info = get_worker_info()
        if worker_info is not None:
            worker_id = worker_info.id
            num_workers = worker_info.num_workers
        else:
            worker_id = 0
            num_workers = 1

        total_shards = world_size * num_workers
        global_worker_id = rank * num_workers + worker_id
        return global_worker_id, total_shards

    def _get_retrieval_job(self) -> "RetrievalJob":
        if callable(self.retrieval_job_or_factory):
            return self.retrieval_job_or_factory()
        return self.retrieval_job_or_factory

    def __iter__(self) -> Iterator[Any]:
        global_worker_id, total_shards = self._resolve_sharding()
        job = self._get_retrieval_job()
        batch_iter = job.to_arrow_batches(
            batch_size=self.batch_size,
            validation_reference=self.validation_reference,
            timeout=self.timeout,
        )

        start_idx = 0
        try:
            for batch in batch_iter:
                batch_len = batch.num_rows
                if batch_len == 0:
                    continue

                if self.field_names is not None:
                    batch = batch.select(self.field_names)

                if total_shards == 1:
                    sharded_batch = batch
                elif self.partition_key is not None:
                    if self.partition_key not in batch.schema.names:
                        raise KeyError(
                            f"partition_key '{self.partition_key}' was not found in batch schema: {batch.schema.names}"
                        )
                    key_col = batch[self.partition_key]
                    keys = key_col.to_pylist()
                    if pa.types.is_integer(key_col.type):
                        indices = [
                            i
                            for i, k in enumerate(keys)
                            if k is not None
                            and (abs(k) % total_shards) == global_worker_id
                        ]
                    else:
                        indices = [
                            i
                            for i, k in enumerate(keys)
                            if k is not None
                            and (zlib.crc32(str(k).encode("utf-8")) % total_shards)
                            == global_worker_id
                        ]
                    if not indices:
                        start_idx += batch_len
                        continue
                    sharded_batch = batch.take(pa.array(indices, type=pa.int64()))
                else:
                    first_i = (
                        global_worker_id - (start_idx % total_shards)
                    ) % total_shards
                    if first_i >= batch_len:
                        start_idx += batch_len
                        continue
                    indices = list(range(first_i, batch_len, total_shards))
                    sharded_batch = batch.take(pa.array(indices, type=pa.int64()))

                start_idx += batch_len

                if sharded_batch.num_rows == 0:
                    continue

                if self.yield_batches:
                    if self.return_tensors:
                        yield {
                            name: _arrow_array_to_tensor_or_list(col)
                            for name, col in zip(
                                sharded_batch.schema.names,
                                sharded_batch.columns,
                            )
                        }
                    else:
                        yield sharded_batch
                else:
                    pydict = sharded_batch.to_pydict()
                    keys = list(pydict.keys())
                    num_rows = sharded_batch.num_rows
                    if self.return_tensors:
                        for r in range(num_rows):
                            yield {
                                k: _scalar_to_tensor_or_value(pydict[k][r])
                                for k in keys
                            }
                    else:
                        for r in range(num_rows):
                            yield {k: pydict[k][r] for k in keys}
        finally:
            if hasattr(batch_iter, "close"):
                try:
                    batch_iter.close()
                except Exception:
                    pass
