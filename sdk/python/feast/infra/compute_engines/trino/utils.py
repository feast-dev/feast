from __future__ import annotations

import concurrent.futures
import logging
from datetime import date, datetime, timezone
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Iterable,
    Iterator,
    Optional,
    TypeVar,
)

import numpy as np
import pandas as pd
import pyarrow as pa
from trino.exceptions import TrinoConnectionError, TrinoQueryError

from feast.infra.offline_stores.contrib.trino_offline_store.trino_queries import (
    Trino,
)
from feast.infra.offline_stores.contrib.trino_offline_store.trino_type_map import (
    trino_to_pa_value_type,
)
from feast.types import (
    Array,
    PrimitiveFeastType,
    Set,
    Struct,
)
from feast.utils import _convert_arrow_to_proto, _run_pyarrow_field_mapping

if TYPE_CHECKING:
    from feast.feature_view import FeatureView
    from feast.infra.online_stores.online_store import OnlineStore
    from feast.repo_config import RepoConfig

logger = logging.getLogger(__name__)

T = TypeVar("T")


def unique_ordered(items: Iterable[T]) -> tuple[T, ...]:
    """Return unique elements from an iterable, preserving first-seen insertion order."""
    return tuple(dict.fromkeys(items))


def quote_identifier(name: str) -> str:
    """Safely quote a Trino SQL identifier (table, column, or CTE alias)."""
    escaped = name.replace('"', '""')
    return f'"{escaped}"'


def _has_unquoted_semicolon(sql: str) -> bool:
    """Return True if sql contains a semicolon outside of quotes."""
    in_quote = False
    quote_char: Optional[str] = None
    is_escaped = False

    for char in sql:
        if is_escaped:
            is_escaped = False
            continue

        if char == "\\":
            is_escaped = True
            continue

        if char in ("'", '"'):
            if not in_quote:
                in_quote = True
                quote_char = char
            elif char == quote_char:
                in_quote = False
                quote_char = None
        elif char == ";" and not in_quote:
            return True

    return False


def from_feast_to_trino_type(feast_type: Any) -> Optional[str]:
    """Convert a Feast data type to an explicit Trino SQL type string.

    Never infers NULL; returns strongly-typed Trino SQL types.
    """
    if isinstance(feast_type, Struct):
        inner_fields = []
        for name, ftype in feast_type.fields.items():
            trino_type = from_feast_to_trino_type(ftype)
            if trino_type is None:
                return None
            inner_fields.append(f"{quote_identifier(name)} {trino_type}")
        return f"ROW({', '.join(inner_fields)})"

    if isinstance(feast_type, PrimitiveFeastType):
        mapping = {
            PrimitiveFeastType.BYTES: "VARBINARY",
            PrimitiveFeastType.STRING: "VARCHAR",
            PrimitiveFeastType.INT32: "INTEGER",
            PrimitiveFeastType.INT64: "BIGINT",
            PrimitiveFeastType.FLOAT64: "DOUBLE",
            PrimitiveFeastType.FLOAT32: "REAL",
            PrimitiveFeastType.BOOL: "BOOLEAN",
            PrimitiveFeastType.UNIX_TIMESTAMP: "TIMESTAMP",
            PrimitiveFeastType.MAP: "MAP(VARCHAR, VARCHAR)",
            PrimitiveFeastType.JSON: "VARCHAR",
        }
        return mapping.get(feast_type)

    if isinstance(feast_type, Array):
        base_type = feast_type.base_type
        if isinstance(base_type, Struct):
            inner = from_feast_to_trino_type(base_type)
            return f"ARRAY({inner})" if inner else None
        if isinstance(base_type, PrimitiveFeastType):
            if base_type == PrimitiveFeastType.MAP:
                return "ARRAY(MAP(VARCHAR, VARCHAR))"
            inner = from_feast_to_trino_type(base_type)
            return f"ARRAY({inner})" if inner else None

    if isinstance(feast_type, Set):
        inner = from_feast_to_trino_type(feast_type.base_type)
        return f"ARRAY({inner})" if inner else None

    return None


def _is_nan(value: Any) -> bool:
    """Check if value is NaN or null, guarded against container types."""
    if value is None:
        return True
    if isinstance(value, (list, tuple, np.ndarray)):
        return False
    try:
        return bool(pd.isna(value))
    except (TypeError, ValueError):
        return False


def _trino_sql_literal(val: Any) -> str:
    """Format Python objects into valid, strongly-typed Trino SQL literals.

    Guarantees:
    - Normalizes timezone-aware datetimes and timestamps to UTC.
    - Handles containers before null checks to avoid array truth-value errors.
    - Escapes single quotes in strings.
    - Emits TRUE/FALSE for booleans.
    - Emits NULL for None, pd.NaT, and NaN.
    """
    if val is None or val is pd.NaT:
        return "NULL"

    # Container types must be inspected before checking pd.isna()
    if isinstance(val, list):
        items = [_trino_sql_literal(item) for item in val]
        return f"ARRAY[{', '.join(items)}]"
    if isinstance(val, tuple):
        items = [_trino_sql_literal(item) for item in val]
        return f"ARRAY[{', '.join(items)}]"
    if isinstance(val, np.ndarray):
        items = [_trino_sql_literal(item) for item in val.tolist()]
        return f"ARRAY[{', '.join(items)}]"

    if _is_nan(val):
        return "NULL"

    if isinstance(val, (bool, np.bool_)):
        return "TRUE" if val else "FALSE"

    if isinstance(val, (int, float, np.integer, np.floating)):
        return str(val)

    if isinstance(val, (datetime, pd.Timestamp)):
        if val.tzinfo is not None and val.tzinfo.utcoffset(val) is not None:
            val = val.astimezone(timezone.utc)
        return f"TIMESTAMP '{val.strftime('%Y-%m-%d %H:%M:%S.%f')}'"

    if isinstance(val, np.datetime64):
        val = pd.Timestamp(val)
        if val.tzinfo is not None and val.tzinfo.utcoffset(val) is not None:
            val = val.astimezone(timezone.utc)
        return f"TIMESTAMP '{val.strftime('%Y-%m-%d %H:%M:%S.%f')}'"

    if isinstance(val, date):
        return f"DATE '{val.strftime('%Y-%m-%d')}'"

    if isinstance(val, str):
        escaped = val.replace("'", "''")
        return f"'{escaped}'"

    return str(val)


def get_or_create_trino_client(config: Any) -> Trino:
    """Instantiate a Trino client with authentication credentials unwrapped."""
    auth = None
    if getattr(config, "auth", None) is not None:
        auth = config.auth.to_trino_auth()

    return Trino(
        host=config.host,
        port=config.port,
        user=config.user,
        catalog=config.catalog,
        source=getattr(config, "source", "trino-python-client"),
        http_scheme=getattr(config, "http_scheme", "http"),
        verify=getattr(config, "verify", True),
        extra_credential=getattr(config, "extra_credential", None),
        auth=auth,
    )


def stream_trino_arrow_batches(
    client: Trino,
    query_text: str,
    batch_size: int = 10000,
) -> Iterator[pa.RecordBatch]:
    """Execute a query and yield PyArrow RecordBatches in streaming chunks.

    Maintains a strictly bounded memory footprint O(batch_size) without
    accumulating the entire dataset in Python memory.
    """
    cursor = client.get_cursor()
    try:
        cursor.execute(query_text)

        if not cursor.description:
            return

        arrow_schema = pa.schema(
            [
                pa.field(str(col[0]), trino_to_pa_value_type(str(col[1])))
                for col in cursor.description
            ]
        )

        while rows := cursor.fetchmany(batch_size):
            arrays = [
                pa.array(
                    [row[col_idx] for row in rows],
                    type=arrow_schema.field(col_idx).type,
                )
                for col_idx in range(len(cursor.description))
            ]
            yield pa.RecordBatch.from_arrays(arrays, schema=arrow_schema)
    except TrinoConnectionError:
        # Never swallow connection errors; bubble up immediately to caller
        logger.error("Trino connection dropped while streaming batches.")
        raise
    except TrinoQueryError as e:
        logger.debug("Trino query failed during streaming execution: %s", e)
        raise
    finally:
        try:
            cursor.close()
        except Exception:
            pass


def write_arrow_batches_to_online_store(
    batches: Iterable[pa.RecordBatch],
    feature_view: "FeatureView",
    online_store: "OnlineStore",
    repo_config: "RepoConfig",
    concurrency: int = 4,
    progress_callback: Optional[Callable[[int], None]] = None,
) -> None:
    """Stream PyArrow record batches to the online store concurrently via threadpool."""
    join_key_to_value_type = {
        entity.name: entity.dtype.to_value_type()
        for entity in feature_view.entity_columns
    }
    batch_chunk_size = repo_config.materialization_config.online_write_batch_size

    def _process_single_batch(batch: pa.RecordBatch) -> int:
        table = pa.Table.from_batches([batch])
        if (
            hasattr(feature_view, "batch_source")
            and feature_view.batch_source is not None
            and getattr(feature_view.batch_source, "field_mapping", None) is not None
        ):
            table = _run_pyarrow_field_mapping(
                table, feature_view.batch_source.field_mapping
            )

        if (
            hasattr(feature_view, "batch_source")
            and feature_view.batch_source is not None
            and getattr(feature_view.batch_source, "created_timestamp_column", None)
            and feature_view.batch_source.created_timestamp_column
            not in table.column_names
        ):
            created_col = feature_view.batch_source.created_timestamp_column
            table = table.append_column(
                created_col,
                pa.nulls(table.num_rows, type=pa.timestamp("us", tz="UTC")),
            )

        sub_batches = (
            [table]
            if batch_chunk_size is None
            else table.to_batches(max_chunksize=batch_chunk_size)
        )
        rows_written = 0
        for sub_batch in sub_batches:
            rows_to_write = _convert_arrow_to_proto(
                sub_batch, feature_view, join_key_to_value_type
            )
            online_store.online_write_batch(
                config=repo_config,
                table=feature_view,
                data=rows_to_write,
                progress=lambda x: None,
            )
            rows_written += len(rows_to_write)
        return rows_written

    with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as executor:
        futures = []
        for batch in batches:
            if batch.num_rows > 0:
                futures.append(executor.submit(_process_single_batch, batch))

        for future in concurrent.futures.as_completed(futures):
            written = future.result()
            if progress_callback:
                progress_callback(written)
