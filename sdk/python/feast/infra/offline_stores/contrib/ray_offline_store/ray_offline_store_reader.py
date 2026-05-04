import logging
from typing import Any

from feast.infra.ray_initializer import get_ray_wrapper

logger = logging.getLogger(__name__)


def load_ray_dataset_from_source(source: Any) -> Any:
    """Loads a ray.data.Dataset from a RaySource descriptor.

    All ray.data loading logic for RaySource lives here, in the offline store
    layer. The RaySource descriptor carries only metadata (reader_type, path,
    reader_options); it contains no loading logic itself.

    This follows the same pattern as SparkOfflineStore, which reads
    SparkSource.file_format and calls spark.read.format(fmt).load(path) — the
    source describes what to read, the store decides how.

    Every reader type is dispatched through get_ray_wrapper() so that:
    - Local / GCS-connected mode   → StandardRayWrapper  → direct ray.data call
    - KubeRay / Ray Client mode    → CodeFlareRayWrapper → @ray.remote dispatch

    This ensures ray.data operations always run on the cluster, never on a thin
    Ray Client driver where they are not supported.

    Path semantics for file-backed reader types (``parquet``, ``csv``, …):
    - Remote URIs (``s3://``, ``gs://``, ``hdfs://``) are passed through unchanged;
      Ray Data resolves them natively.
    - Absolute local paths are passed through unchanged.
    - Relative paths are passed through unchanged and resolved by each Ray worker
      relative to its own working directory.  This is intentional for shared
      storage setups (e.g. a PVC mounted at the same path on every node) where
      the path is meaningful to the workers, not to the driver.  Unlike
      ``FileSource``, ``RaySource`` data is always read on workers, never on the
      driver, so driver-side ``repo_path`` resolution would produce incorrect
      absolute paths on remote workers.

    Args:
        source: A RaySource instance.

    Returns:
        A ray.data.Dataset (or RemoteDatasetProxy for KubeRay mode) produced by
        the reader indicated by source.reader_type.

    Raises:
        ValueError: If source.reader_type is not a recognised reader type.
        ImportError: If a reader-specific dependency (e.g. datasets) is missing.
    """
    reader_type = source.reader_type
    path = source.path or ""
    opts = source.reader_options or {}

    logger.debug(
        "RayOfflineStore: reading RaySource '%s' (reader_type=%s, path=%r)",
        source.name,
        reader_type,
        path,
    )

    ray_wrapper = get_ray_wrapper()

    if reader_type == "parquet":
        return ray_wrapper.read_parquet(path, **opts)

    if reader_type == "csv":
        return ray_wrapper.read_csv(path, **opts)

    if reader_type == "json":
        return ray_wrapper.read_json(path, **opts)

    if reader_type == "text":
        return ray_wrapper.read_text(path, **opts)

    if reader_type == "images":
        return ray_wrapper.read_images(path, **opts)

    if reader_type == "binary_files":
        return ray_wrapper.read_binary_files(path, **opts)

    if reader_type == "tfrecords":
        return ray_wrapper.read_tfrecords(path, **opts)

    if reader_type == "webdataset":
        return ray_wrapper.read_webdataset(path, **opts)

    if reader_type == "huggingface":
        from datasets import load_dataset

        dataset_name = opts.get("dataset_name") or path
        split = opts.get("split", "train")
        # trust_remote_code was removed in datasets>=3.0; skip silently if present.
        extra = {
            k: v
            for k, v in opts.items()
            if k not in ("dataset_name", "split", "trust_remote_code")
        }
        hf_dataset = load_dataset(dataset_name, split=split, **extra)
        return ray_wrapper.from_huggingface(hf_dataset)

    if reader_type == "mongo":
        return ray_wrapper.read_mongo(
            uri=opts["uri"],
            database=opts["database"],
            collection=opts["collection"],
            **{
                k: v
                for k, v in opts.items()
                if k not in ("uri", "database", "collection")
            },
        )

    if reader_type == "sql":
        # Pass connection_url as a plain string so it is serialisable across
        # the Ray object store boundary (CodeFlareRayWrapper rebuilds the
        # connection_factory on the remote worker).
        return ray_wrapper.read_sql(
            sql=opts["sql"],
            connection_url=opts["connection_url"],
            **{k: v for k, v in opts.items() if k not in ("sql", "connection_url")},
        )

    from feast.infra.offline_stores.contrib.ray_offline_store.ray_source import (
        SUPPORTED_READER_TYPES,
    )

    raise ValueError(
        f"Unknown reader_type '{reader_type}' on RaySource '{source.name}'. "
        f"Supported types: {sorted(SUPPORTED_READER_TYPES)}"
    )
