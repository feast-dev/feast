# Copyright 2026 The Feast Authors
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

"""An online store backed by Lance datasets.

Each feature view is one Lance dataset, ``<path>/<project>_<feature_view>.lance``,
holding one row per entity key. A row carries:

* ``__entity_key`` -- the hex-encoded serialized entity key. It is the merge key
  for upserts and carries a BTREE index for point lookups.
* ``__event_ts`` / ``__created_ts`` -- the row's timestamps.
* ``__proto_<feature>`` -- the serialized ``ValueProto``. Reads use it, so every
  Feast type round-trips exactly.
* ``<feature>`` -- a native column, only for the features that are searched or
  filtered on: scalar primitives (for metadata filters), string features (for
  full-text search) and the vector field (for nearest-neighbour search). A
  ``Float32`` vector is stored natively only, since its ``ValueProto`` is
  rebuilt from the native column without loss.

The native vector column is what makes Lance useful here: vector search, metadata
prefilters and full-text search all run inside Lance's own scanner, over the same
dataset the point lookups read.
"""

import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import (
    Any,
    Callable,
    Dict,
    List,
    Literal,
    Optional,
    Sequence,
    Tuple,
    Union,
)

import pyarrow as pa
from pydantic import StrictStr

from feast.entity import Entity
from feast.feature_view import FeatureView
from feast.field import Field
from feast.filter_models import (
    ComparisonFilter,
    CompoundFilter,
    FilterTranslator,
    FilterType,
)
from feast.infra.key_encoding_utils import deserialize_entity_key, serialize_entity_key
from feast.infra.online_stores.helpers import compute_table_id
from feast.infra.online_stores.online_store import OnlineStore
from feast.infra.online_stores.vector_store import VectorStoreConfig
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import FloatList
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from feast.repo_config import FeastConfigBaseModel, RepoConfig
from feast.type_map import feast_value_type_to_python_type
from feast.types import (
    Array,
    Bool,
    Float32,
    Float64,
    Int32,
    Int64,
    PrimitiveFeastType,
    String,
)
from feast.utils import _get_feature_view_vector_field_metadata, to_naive_utc

logger = logging.getLogger(__name__)

ENTITY_KEY_COLUMN = "__entity_key"
EVENT_TS_COLUMN = "__event_ts"
CREATED_TS_COLUMN = "__created_ts"
PROTO_COLUMN_PREFIX = "__proto_"

_SCALAR_ARROW_TYPES = {
    Int32: pa.int32(),
    Int64: pa.int64(),
    Float32: pa.float32(),
    Float64: pa.float64(),
    String: pa.string(),
    Bool: pa.bool_(),
}

# Feast metric names to the names Lance's ``nearest`` accepts.
_DISTANCE_METRICS = {
    "cosine": "cosine",
    "l2": "l2",
    "euclidean": "l2",
    "dot": "dot",
    "inner_product": "dot",
    "hamming": "hamming",
}

_TIMESTAMP_TYPE = pa.timestamp("us", tz="UTC")


class LanceOnlineStoreConfig(FeastConfigBaseModel, VectorStoreConfig):
    """Configuration for the Lance online store.

    Requires the ``lance`` extra (``feast[lance]``).
    """

    type: Literal[
        "lance",
        "feast.infra.online_stores.lance_online_store.lance_online_store.LanceOnlineStore",
    ] = "lance"
    """Online store type selector."""

    path: StrictStr = "data/online_store"
    """Directory holding one Lance dataset per feature view. A relative path is
    resolved against the repo; object-storage URIs (``s3://``, ``gs://``, ...)
    are used as given."""

    storage_options: Optional[Dict[str, str]] = None
    """Object-storage options passed through to Lance."""

    text_search_enabled: bool = False
    """Build a full-text (inverted) index over the string features of every
    feature view, enabling ``query_string`` retrieval."""


class LanceFilterTranslator(FilterTranslator):
    """Translate Feast metadata filters into a Lance SQL filter string.

    Only features stored in a native column can be filtered on, since the
    serialized ``ValueProto`` column is opaque to Lance. An unknown key is an
    error rather than an empty result, so a typo cannot pass for "no match".
    """

    _OPERATORS = {
        "eq": "=",
        "ne": "!=",
        "gt": ">",
        "gte": ">=",
        "lt": "<",
        "lte": "<=",
    }

    def __init__(self, filterable_columns: Sequence[str]):
        self._filterable_columns = set(filterable_columns)

    def translate(self, filters: FilterType) -> Optional[str]:
        if filters is None:
            return None
        return self._dispatch(filters)

    def translate_comparison(self, f: ComparisonFilter) -> str:
        if f.key not in self._filterable_columns:
            raise ValueError(
                f"Cannot filter on '{f.key}': the Lance online store can filter "
                f"only on scalar Int32, Int64, Float32, Float64, String and Bool "
                f"features. Filterable features: {sorted(self._filterable_columns)}."
            )
        column = _quote_identifier(f.key)
        if f.type in ("in", "nin"):
            values = f.value if isinstance(f.value, (list, tuple, set)) else [f.value]
            if not values:
                return "FALSE" if f.type == "in" else "TRUE"
            operator = "IN" if f.type == "in" else "NOT IN"
            literals = ", ".join(_sql_literal(v) for v in values)
            return f"{column} {operator} ({literals})"
        if f.value is None:
            if f.type == "eq":
                return f"{column} IS NULL"
            if f.type == "ne":
                return f"{column} IS NOT NULL"
            raise ValueError(f"Filter '{f.type}' cannot compare against null.")
        return f"{column} {self._OPERATORS[f.type]} {_sql_literal(f.value)}"

    def translate_compound(self, f: CompoundFilter) -> str:
        if not f.filters:
            return "TRUE" if f.type == "and" else "FALSE"
        joiner = " AND " if f.type == "and" else " OR "
        return "(" + joiner.join(self._dispatch(sub) for sub in f.filters) + ")"


def _quote_identifier(name: str) -> str:
    return "`" + name.replace("`", "``") + "`"


def _sql_literal(value: Any) -> str:
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float)):
        return repr(value)
    if isinstance(value, str):
        return "'" + value.replace("'", "''") + "'"
    raise ValueError(
        f"Unsupported filter value {value!r} of type {type(value).__name__}."
    )


def _is_float32_vector(field: Field) -> bool:
    return isinstance(field.dtype, Array) and field.dtype.base_type == Float32


class _TableLayout:
    """How one feature view maps onto the columns of its Lance dataset."""

    def __init__(self, config: RepoConfig, table: FeatureView):
        self.vector_field = _get_feature_view_vector_field_metadata(table)
        online_store = config.online_store
        if self.vector_field is not None and online_store.vector_enabled:
            if not self.vector_field.vector_length:
                raise ValueError(
                    f"Vector field '{self.vector_field.name}' of feature view "
                    f"'{table.name}' needs a vector_length to be stored in the "
                    f"Lance online store."
                )
        else:
            self.vector_field = None

        self.features: List[Field] = list(table.features)
        self.native_types: Dict[str, pa.DataType] = {}
        for field in self.features:
            if self.vector_field is not None and field.name == self.vector_field.name:
                self.native_types[field.name] = pa.list_(
                    pa.float32(), self.vector_field.vector_length
                )
            elif isinstance(field.dtype, PrimitiveFeastType) and (
                field.dtype in _SCALAR_ARROW_TYPES
            ):
                self.native_types[field.name] = _SCALAR_ARROW_TYPES[field.dtype]

        self.string_features = [f.name for f in self.features if f.dtype == String]

    def has_proto_column(self, feature: str) -> bool:
        vector = self.vector_field
        return not (
            vector is not None and feature == vector.name and _is_float32_vector(vector)
        )

    @property
    def filterable_columns(self) -> List[str]:
        vector_name = self.vector_field.name if self.vector_field else None
        return [name for name in self.native_types if name != vector_name]

    def schema(self) -> pa.Schema:
        fields = [
            pa.field(ENTITY_KEY_COLUMN, pa.string(), nullable=False),
            pa.field(EVENT_TS_COLUMN, _TIMESTAMP_TYPE),
            pa.field(CREATED_TS_COLUMN, _TIMESTAMP_TYPE),
        ]
        for feature in self.features:
            if feature.name in self.native_types:
                fields.append(pa.field(feature.name, self.native_types[feature.name]))
            if self.has_proto_column(feature.name):
                fields.append(pa.field(PROTO_COLUMN_PREFIX + feature.name, pa.binary()))
        return pa.schema(fields)

    def read_columns(self, requested_features: Optional[List[str]]) -> List[str]:
        names = (
            requested_features
            if requested_features is not None
            else [f.name for f in self.features]
        )
        columns = [ENTITY_KEY_COLUMN, EVENT_TS_COLUMN]
        for name in names:
            if self.has_proto_column(name):
                columns.append(PROTO_COLUMN_PREFIX + name)
            elif name in self.native_types:
                columns.append(name)
        return columns

    def to_value_protos(
        self, row: Dict[str, Any], requested_features: Optional[List[str]]
    ) -> Dict[str, ValueProto]:
        names = (
            requested_features
            if requested_features is not None
            else [f.name for f in self.features]
        )
        values: Dict[str, ValueProto] = {}
        for name in names:
            proto_column = PROTO_COLUMN_PREFIX + name
            if proto_column in row:
                raw = row[proto_column]
                value = ValueProto()
                if raw is not None:
                    value.ParseFromString(raw)
                values[name] = value
            elif name in row:
                vector = row[name]
                values[name] = (
                    ValueProto(float_list_val=FloatList(val=vector))
                    if vector is not None
                    else ValueProto()
                )
        return values


class LanceOnlineStore(OnlineStore):
    """Online store backed by one Lance dataset per feature view.

    Supports point reads by entity key, vector search with metadata prefilters,
    and full-text search, all against the same dataset.

    Every write is an upsert keyed on the entity key, and only replaces a stored
    row when its event timestamp is not older, so out-of-order materialization
    cannot roll a value back. Each write commits a new Lance version; run Lance's
    ``optimize.compact_files`` and ``cleanup_old_versions`` periodically to keep
    a frequently written dataset compact.
    """

    # ----------------------------------------------------------- locations

    @staticmethod
    def _base_path(config: RepoConfig) -> str:
        path = config.online_store.path
        if "://" in path or Path(path).is_absolute() or not config.repo_path:
            return path
        return str(Path(config.repo_path) / path)

    def _dataset_uri(self, config: RepoConfig, table: FeatureView) -> str:
        table_id = compute_table_id(
            config.project,
            table,
            config.registry.enable_online_feature_view_versioning,
        )
        return f"{self._base_path(config).rstrip('/')}/{table_id}.lance"

    @staticmethod
    def _storage_options(config: RepoConfig) -> Optional[Dict[str, str]]:
        options = config.online_store.storage_options
        return dict(options) if options else None

    def _open(self, config: RepoConfig, table: FeatureView) -> Optional[Any]:
        """Open the latest version of a feature view's dataset, if it exists."""
        import lance

        uri = self._dataset_uri(config, table)
        try:
            return lance.dataset(uri, storage_options=self._storage_options(config))
        except ValueError:
            # Lance reports a missing dataset as a ValueError.
            return None

    # -------------------------------------------------------------- writes

    def online_write_batch(
        self,
        config: RepoConfig,
        table: FeatureView,
        data: List[
            Tuple[EntityKeyProto, Dict[str, ValueProto], datetime, Optional[datetime]]
        ],
        progress: Optional[Callable[[int], Any]],
    ) -> None:
        if not data:
            return

        layout = _TableLayout(config, table)

        # merge_insert inserts every duplicate source row, so collapse the batch
        # to the newest row per entity key first.
        latest: Dict[str, Tuple[Dict[str, ValueProto], datetime, Optional[datetime]]]
        latest = {}
        for entity_key, values, event_ts, created_ts in data:
            key = serialize_entity_key(
                entity_key,
                entity_key_serialization_version=config.entity_key_serialization_version,
            ).hex()
            event_ts = _to_utc(event_ts)
            created_ts = _to_utc(created_ts) if created_ts is not None else None
            current = latest.get(key)
            if current is None or (event_ts, created_ts or event_ts) >= (
                current[1],
                current[2] or current[1],
            ):
                latest[key] = (values, event_ts, created_ts)

        batch = self._to_arrow(layout, latest)

        dataset = self._open(config, table)
        if dataset is None:
            dataset = self._create(config, table, layout)
        (
            dataset.merge_insert(ENTITY_KEY_COLUMN)
            .when_matched_update_all(
                condition=f"source.{EVENT_TS_COLUMN} >= target.{EVENT_TS_COLUMN}"
            )
            .when_not_matched_insert_all()
            .execute(batch)
        )
        if progress:
            progress(len(data))

    @staticmethod
    def _to_arrow(
        layout: _TableLayout,
        rows: Dict[str, Tuple[Dict[str, ValueProto], datetime, Optional[datetime]]],
    ) -> pa.Table:
        schema = layout.schema()
        columns: Dict[str, List[Any]] = {name: [] for name in schema.names}
        for key, (values, event_ts, created_ts) in rows.items():
            columns[ENTITY_KEY_COLUMN].append(key)
            columns[EVENT_TS_COLUMN].append(event_ts)
            columns[CREATED_TS_COLUMN].append(created_ts)
            for feature in layout.features:
                value = values.get(feature.name)
                if value is not None and value.WhichOneof("val") is None:
                    value = None
                if feature.name in layout.native_types:
                    columns[feature.name].append(
                        feast_value_type_to_python_type(value)
                        if value is not None
                        else None
                    )
                if layout.has_proto_column(feature.name):
                    columns[PROTO_COLUMN_PREFIX + feature.name].append(
                        value.SerializeToString() if value is not None else None
                    )
        return pa.Table.from_pydict(columns, schema=schema)

    # --------------------------------------------------------------- reads

    def online_read(
        self,
        config: RepoConfig,
        table: FeatureView,
        entity_keys: List[EntityKeyProto],
        requested_features: Optional[List[str]] = None,
    ) -> List[Tuple[Optional[datetime], Optional[Dict[str, ValueProto]]]]:
        missing: List[Tuple[Optional[datetime], Optional[Dict[str, ValueProto]]]]
        missing = [(None, None)] * len(entity_keys)
        if not entity_keys:
            return missing

        dataset = self._open(config, table)
        if dataset is None:
            return missing

        layout = _TableLayout(config, table)
        keys = [
            serialize_entity_key(
                entity_key,
                entity_key_serialization_version=config.entity_key_serialization_version,
            ).hex()
            for entity_key in entity_keys
        ]
        # Hex keys need no escaping inside a SQL string literal.
        key_list = ", ".join(f"'{key}'" for key in set(keys))
        rows = dataset.to_table(
            columns=layout.read_columns(requested_features),
            filter=f"{ENTITY_KEY_COLUMN} IN ({key_list})",
        ).to_pylist()

        by_key = {row[ENTITY_KEY_COLUMN]: row for row in rows}
        result: List[Tuple[Optional[datetime], Optional[Dict[str, ValueProto]]]] = []
        for key in keys:
            row = by_key.get(key)
            if row is None:
                result.append((None, None))
                continue
            result.append(
                (
                    _from_utc(row[EVENT_TS_COLUMN]),
                    layout.to_value_protos(row, requested_features),
                )
            )
        return result

    # -------------------------------------------------------------- search

    def retrieve_online_documents_v2(
        self,
        config: RepoConfig,
        table: FeatureView,
        requested_features: List[str],
        embedding: Optional[List[float]],
        top_k: int,
        distance_metric: Optional[str] = None,
        query_string: Optional[str] = None,
        filters: Optional[Union[ComparisonFilter, CompoundFilter]] = None,
        include_feature_view_version_metadata: bool = False,
    ) -> List[
        Tuple[
            Optional[datetime],
            Optional[EntityKeyProto],
            Optional[Dict[str, ValueProto]],
        ]
    ]:
        online_store = config.online_store
        if embedding is None and query_string is None:
            raise ValueError("Either embedding or query_string must be specified.")
        if embedding is not None and query_string is not None:
            raise ValueError(
                "The Lance online store does not support hybrid search yet; pass "
                "either an embedding or a query_string."
            )

        layout = _TableLayout(config, table)
        dataset = self._open(config, table)
        if dataset is None:
            return []

        where = LanceFilterTranslator(layout.filterable_columns).translate(filters)
        columns = layout.read_columns(requested_features)
        scan: Dict[str, Any] = {"columns": columns}
        if where:
            scan["filter"] = where
            scan["prefilter"] = True

        if embedding is not None:
            if not online_store.vector_enabled or layout.vector_field is None:
                raise ValueError(
                    f"Vector search needs vector_enabled: true in the online store "
                    f"config and a vector_index field on feature view '{table.name}'."
                )
            metric_name = (distance_metric or online_store.similarity or "l2").lower()
            if metric_name not in _DISTANCE_METRICS:
                raise ValueError(
                    f"Unsupported distance metric '{metric_name}'. Supported: "
                    f"{sorted(_DISTANCE_METRICS)}."
                )
            scan["nearest"] = {
                "column": layout.vector_field.name,
                "q": [float(v) for v in embedding],
                "k": top_k,
                "metric": _DISTANCE_METRICS[metric_name],
            }
            score_column, score_feature = "_distance", "distance"
        else:
            if not online_store.text_search_enabled or not layout.string_features:
                raise ValueError(
                    f"Text search needs text_search_enabled: true in the online "
                    f"store config and a String feature on feature view "
                    f"'{table.name}'."
                )
            scan["full_text_query"] = {
                "query": query_string,
                "columns": layout.string_features,
            }
            scan["limit"] = top_k
            score_column, score_feature = "_score", "text_rank"

        rows = dataset.to_table(**scan).to_pylist()

        results: List[
            Tuple[
                Optional[datetime],
                Optional[EntityKeyProto],
                Optional[Dict[str, ValueProto]],
            ]
        ] = []
        for row in rows:
            entity_key = deserialize_entity_key(
                bytes.fromhex(row[ENTITY_KEY_COLUMN]),
                entity_key_serialization_version=config.entity_key_serialization_version,
            )
            features = layout.to_value_protos(row, requested_features)
            score = ValueProto(float_val=float(row[score_column]))
            features[score_feature] = score
            if score_feature == "text_rank":
                features["distance"] = score
            results.append((_from_utc(row[EVENT_TS_COLUMN]), entity_key, features))
        return results

    # --------------------------------------------------------- provisioning

    def _create(
        self, config: RepoConfig, table: FeatureView, layout: _TableLayout
    ) -> Any:
        import lance

        dataset = lance.write_dataset(
            layout.schema().empty_table(),
            self._dataset_uri(config, table),
            mode="create",
            storage_options=self._storage_options(config),
        )
        self._ensure_indices(config, dataset, layout)
        return dataset

    @staticmethod
    def _ensure_indices(config: RepoConfig, dataset: Any, layout: _TableLayout) -> None:
        """Create the scalar indices a dataset should carry but does not yet."""
        indexed = {
            name for index in dataset.describe_indices() for name in index.field_names
        }
        wanted = {ENTITY_KEY_COLUMN: "BTREE"}
        if config.online_store.text_search_enabled:
            wanted.update({name: "INVERTED" for name in layout.string_features})
        for column, index_type in wanted.items():
            if column not in indexed:
                dataset.create_scalar_index(column, index_type)

    def _drop(self, config: RepoConfig, table: FeatureView) -> None:
        import lance

        lance.LanceDataset.drop(
            self._dataset_uri(config, table),
            storage_options=self._storage_options(config),
            ignore_not_found=True,
        )

    def update(
        self,
        config: RepoConfig,
        tables_to_delete: Sequence[FeatureView],
        tables_to_keep: Sequence[FeatureView],
        entities_to_delete: Sequence[Entity],
        entities_to_keep: Sequence[Entity],
        partial: bool,
    ):
        for table in tables_to_delete:
            self._drop(config, table)

        for table in tables_to_keep:
            layout = _TableLayout(config, table)
            dataset = self._open(config, table)
            if dataset is None:
                self._create(config, table, layout)
                continue
            if dataset.schema != layout.schema():
                dataset = self._evolve(config, table, dataset, layout)
            self._ensure_indices(config, dataset, layout)

    def _evolve(
        self,
        config: RepoConfig,
        table: FeatureView,
        dataset: Any,
        layout: _TableLayout,
    ) -> Any:
        """Bring an existing dataset's schema in line with its feature view.

        Added features become all-null columns and removed features are
        dropped, so the values of unchanged features survive ``feast apply``. A
        feature whose type changed cannot be carried over, so the dataset is
        recreated empty and must be materialized again.
        """
        wanted = layout.schema()
        existing = dataset.schema
        changed = [
            field.name
            for field in wanted
            if field.name in existing.names
            and existing.field(field.name).type != field.type
        ]
        if changed:
            logger.warning(
                "Lance online store: the type of %s in feature view '%s' changed; "
                "recreating its dataset empty. Materialize it again.",
                changed,
                table.name,
            )
            self._drop(config, table)
            return self._create(config, table, layout)

        removed = [name for name in existing.names if name not in wanted.names]
        if removed:
            dataset.drop_columns(removed)
        added = [field for field in wanted if field.name not in existing.names]
        if added:
            dataset.add_columns(added)
        return dataset

    def teardown(
        self,
        config: RepoConfig,
        tables: Sequence[FeatureView],
        entities: Sequence[Entity],
    ):
        for table in tables:
            self._drop(config, table)


def _to_utc(ts: datetime) -> datetime:
    return to_naive_utc(ts).replace(tzinfo=timezone.utc)


def _from_utc(ts: Optional[datetime]) -> Optional[datetime]:
    return to_naive_utc(ts) if ts is not None else None
