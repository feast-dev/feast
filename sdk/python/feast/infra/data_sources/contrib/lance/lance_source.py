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

"""A non-JVM read path for Lance datasets.

``LanceFormat`` (``feast.table_format``) is a format descriptor only. Today it is
consumed by exactly one source, ``SparkSource``, which turns it into
``spark.read.format("lance")``. That makes Lance reachable through the JVM, and
nothing else. ``LanceSource`` is the engine-agnostic half: it addresses a Lance
dataset either by URI or through a Lance namespace catalog, applies the
``version``/``tag`` pin carried by ``LanceFormat``, and hands the result to the
offline store as a PyArrow table.

Pin semantics follow what was argued upstream: **a pin selects data, never
shape.** The declared ``FeatureView`` schema stays the contract, so a pinned
version whose schema disagrees fails explicitly rather than silently returning a
different width. See :func:`validate_lance_source_schema`.
"""

import json
from typing import TYPE_CHECKING, Any, Callable, Dict, Iterable, List, Optional, Tuple

from feast.data_source import DataSource
from feast.protos.feast.core.DataSource_pb2 import DataSource as DataSourceProto
from feast.repo_config import RepoConfig
from feast.table_format import LanceFormat
from feast.type_map import pa_to_feast_value_type
from feast.value_type import ValueType

if TYPE_CHECKING:
    import pyarrow

    from feast.feature_view import FeatureView


_CLASS_TYPE = "feast.infra.data_sources.contrib.lance.lance_source.LanceSource"


class LanceSource(DataSource):
    """Data source backed by a Lance dataset, readable without Spark.

    A Lance dataset is addressed in exactly one of two ways, which correspond to
    the two layers of the read path:

    1. **Path-based** — pass ``uri``. The dataset is opened directly off local
       disk or object storage. This is the smaller layer and needs no catalog.
    2. **Catalog-based** — pass ``table`` plus ``namespace_impl``. The dataset is
       resolved through ``lance_namespace.connect(impl, properties)`` and a
       ``table_id``, which is built from the ``namespace`` on the attached
       ``LanceFormat`` plus ``table``.

    Either way, the ``version``/``tag`` pin on the attached ``LanceFormat`` is
    applied at read time. Lance accepts an ``int`` version or a tag name string
    in the same ``version=`` argument, so a tag is resolved by Lance itself
    rather than by Feast.

    Args:
        uri: Path or object-storage URI of the Lance dataset. Mutually exclusive
            with ``table``.
        table: Table name within the namespace. Requires ``namespace_impl``.
            Mutually exclusive with ``uri``.
        table_format: The ``LanceFormat`` carrying ``catalog``, ``namespace`` and
            the optional ``version``/``tag`` pin. One is created implicitly if
            omitted, so that the pin accessors always have something to read.
        namespace_impl: Lance namespace implementation. ``lance_namespace``
            ships ``"rest"`` and ``"dir"``; the ``lance-namespace-impls`` package
            adds ``"polaris"``, ``"glue"``, ``"hive2"``, ``"hive3"``,
            ``"iceberg"`` and ``"unity"``. ``"dir"`` is a local-filesystem
            namespace and exercises the identical ``namespace_client`` code path
            as a remote catalog with no server to run.
        namespace_properties: Properties passed through to
            ``lance_namespace.connect``, e.g. ``{"root": "/path/to/warehouse"}``
            for the ``dir`` impl.
        storage_options: Object-storage options passed to ``lance.dataset``.

    Raises:
        ValueError: If neither or both of ``uri`` and ``table`` are given, if
            ``table`` is given without ``namespace_impl``, or if ``table_format``
            is not a ``LanceFormat``.

    Examples:
        Path-based, reading the latest version:

        >>> source = LanceSource(                            # doctest: +SKIP
        ...     uri="/data/user_embeddings.lance",
        ...     timestamp_field="event_timestamp",
        ... )

        Catalog-based, pinned to a tag so an eval set is reproducible:

        >>> from feast.table_format import LanceFormat
        >>> source = LanceSource(                            # doctest: +SKIP
        ...     table="user_embeddings",
        ...     namespace_impl="dir",
        ...     namespace_properties={"root": "/warehouse"},
        ...     table_format=LanceFormat(namespace="features", tag="eval_2026_q1"),
        ...     timestamp_field="event_timestamp",
        ... )
    """

    def __init__(
        self,
        *,
        uri: Optional[str] = None,
        table: Optional[str] = None,
        table_format: Optional[LanceFormat] = None,
        namespace_impl: Optional[str] = None,
        namespace_properties: Optional[Dict[str, str]] = None,
        storage_options: Optional[Dict[str, str]] = None,
        name: Optional[str] = None,
        timestamp_field: Optional[str] = None,
        created_timestamp_column: Optional[str] = None,
        field_mapping: Optional[Dict[str, str]] = None,
        description: Optional[str] = "",
        tags: Optional[Dict[str, str]] = None,
        owner: Optional[str] = "",
    ):
        if table_format is not None and not isinstance(table_format, LanceFormat):
            raise ValueError(
                "LanceSource requires a LanceFormat table_format, got "
                f"{type(table_format).__name__}."
            )
        if uri and table:
            raise ValueError(
                "LanceSource accepts either uri or table, not both. A uri addresses "
                "the dataset directly; a table addresses it through a namespace."
            )
        if not uri and not table:
            raise ValueError(
                "LanceSource requires either uri (path-based) or table "
                "(catalog-based) to address the dataset."
            )
        if table and not namespace_impl:
            raise ValueError(
                "LanceSource with a table requires namespace_impl, e.g. 'dir' for a "
                "local filesystem namespace or 'rest' for a Lance namespace server."
            )

        # Keep a format object unconditionally so the pin accessors never have to
        # branch on None.
        self.table_format = table_format or LanceFormat()

        _name = name or self._default_name(uri, table, self.table_format)
        super().__init__(
            name=_name,
            timestamp_field=timestamp_field,
            created_timestamp_column=created_timestamp_column,
            field_mapping=field_mapping,
            description=description,
            tags=tags,
            owner=owner,
        )
        self.uri = uri
        self.lance_table = table
        self.namespace_impl = namespace_impl
        self.namespace_properties = namespace_properties or {}
        self.storage_options = storage_options or {}

    @staticmethod
    def _default_name(
        uri: Optional[str], table: Optional[str], table_format: LanceFormat
    ) -> str:
        if uri:
            return uri
        parts = [p for p in (table_format.catalog, table_format.namespace, table) if p]
        return ".".join(parts)

    # ------------------------------------------------------------------ pin

    @property
    def is_catalog_based(self) -> bool:
        """Whether this source resolves through a Lance namespace."""
        return self.lance_table is not None

    @property
    def pin(self) -> Optional[Any]:
        """The Lance ``version=`` argument for this source's pin, if pinned.

        Lance accepts either an ``int`` version or a tag name string in the same
        argument, so a tag needs no separate resolution step here.
        """
        if self.table_format.version is not None:
            return self.table_format.version
        if self.table_format.tag:
            return self.table_format.tag
        return None

    @property
    def pin_description(self) -> str:
        """Human-readable description of the pin, for error messages."""
        if self.table_format.version is not None:
            return f"version {self.table_format.version}"
        if self.table_format.tag:
            return f"tag '{self.table_format.tag}'"
        return "latest version"

    @property
    def table_id(self) -> List[str]:
        """The Lance namespace ``table_id`` for this source.

        A Lance ``table_id`` is a list of identifier components. The namespace on
        the attached ``LanceFormat`` is split on ``.`` so that a hierarchical
        namespace such as ``"a.b"`` becomes ``["a", "b", "<table>"]``. The ``dir``
        impl is flat and so takes a single-element id.
        """
        if not self.lance_table:
            raise ValueError(
                f"LanceSource '{self.name}' is path-based and has no table_id. "
                "Use the uri instead."
            )
        namespace = self.table_format.namespace
        components = namespace.split(".") if namespace else []
        return [*components, self.lance_table]

    # --------------------------------------------------------------- reading

    def get_namespace_client(self) -> Any:
        """Connect to the Lance namespace addressed by this source."""
        if not self.namespace_impl:
            raise ValueError(
                f"LanceSource '{self.name}' has no namespace_impl to connect with."
            )
        import lance_namespace

        return lance_namespace.connect(
            self.namespace_impl, dict(self.namespace_properties)
        )

    def get_dataset(self) -> Any:
        """Open the pinned ``lance.LanceDataset`` this source addresses.

        ``lance.write_dataset`` and ``lance.dataset`` both reject being given a
        uri and a ``namespace_client`` at the same time, which is why the two
        addressing modes are mutually exclusive on the source as well.
        """
        import lance

        kwargs: Dict[str, Any] = {}
        pin = self.pin
        if pin is not None:
            kwargs["version"] = pin
        if self.storage_options:
            kwargs["storage_options"] = dict(self.storage_options)

        if self.is_catalog_based:
            return lance.dataset(
                namespace_client=self.get_namespace_client(),
                table_id=self.table_id,
                **kwargs,
            )
        return lance.dataset(uri=self.uri, **kwargs)

    def get_schema(self) -> "pyarrow.Schema":
        """Return the Arrow schema of the pinned dataset without scanning it."""
        return self.get_dataset().schema

    def to_arrow(
        self,
        columns: Optional[List[str]] = None,
        feature_view: Optional["FeatureView"] = None,
    ) -> "pyarrow.Table":
        """Materialise the pinned dataset as a PyArrow table.

        Args:
            columns: Optional column projection, pushed down into the Lance scan.
            feature_view: When supplied, the declared schema is enforced against
                the data actually read, so that a pin can never change the shape
                of the result.
        """
        dataset = self.get_dataset()
        if feature_view is not None:
            _validate_against_feature_view(self, dataset.schema, feature_view)

        table = dataset.to_table(columns=columns)
        if feature_view is not None:
            from feast.utils import _validate_vector_field_lengths

            _validate_vector_field_lengths(table, feature_view)
        return table

    # ----------------------------------------------------------- DataSource

    def source_type(self) -> DataSourceProto.SourceType.ValueType:
        return DataSourceProto.CUSTOM_SOURCE

    def __eq__(self, other):
        if not isinstance(other, LanceSource):
            raise TypeError("Comparisons should only involve LanceSource class objects")
        return (
            super().__eq__(other)
            and self.uri == other.uri
            and self.lance_table == other.lance_table
            and self.namespace_impl == other.namespace_impl
            and self.namespace_properties == other.namespace_properties
            and self.storage_options == other.storage_options
            and self.table_format.to_dict() == other.table_format.to_dict()
        )

    def __hash__(self):
        return super().__hash__()

    @staticmethod
    def source_datatype_to_feast_value_type() -> Callable[[str], ValueType]:
        # Lance surfaces its schema as Arrow, so the Arrow decoder applies
        # unchanged, exactly as it does for FileSource.
        return pa_to_feast_value_type

    def get_table_column_names_and_types(
        self, config: RepoConfig
    ) -> Iterable[Tuple[str, str]]:
        """Return ``(name, str(arrow_type))`` pairs for the **pinned** schema.

        Reading the pinned schema rather than the latest one is deliberate: the
        schema that ``feast apply`` infers must be the schema that reads will
        actually see.
        """
        schema = self.get_schema()
        return list(zip(schema.names, map(str, schema.types)))

    def validate(self, config: RepoConfig):
        schema = self.get_schema()
        if self.timestamp_field and self.timestamp_field not in schema.names:
            raise ValueError(
                f"Timestamp field '{self.timestamp_field}' is not present in Lance "
                f"dataset '{self.name}' at {self.pin_description}. "
                f"Available columns: {sorted(schema.names)}."
            )

    def get_table_query_string(self) -> str:
        raise NotImplementedError(
            "LanceSource is read through PyArrow, not SQL. It is supported by "
            "offline stores that can accept an Arrow table."
        )

    def _to_proto_impl(self) -> DataSourceProto:
        config_json = json.dumps(
            {
                "uri": self.uri,
                "table": self.lance_table,
                "namespace_impl": self.namespace_impl,
                "namespace_properties": self.namespace_properties,
                "storage_options": self.storage_options,
                "table_format": self.table_format.to_dict(),
            }
        )
        return DataSourceProto(
            name=self.name,
            type=DataSourceProto.CUSTOM_SOURCE,
            data_source_class_type=_CLASS_TYPE,
            custom_options=DataSourceProto.CustomSourceOptions(
                configuration=bytes(config_json, encoding="utf8")
            ),
            description=self.description,
            tags=self.tags,
            owner=self.owner,
            timestamp_field=self.timestamp_field,
            created_timestamp_column=self.created_timestamp_column,
            field_mapping=self.field_mapping,
        )

    @staticmethod
    def from_proto(data_source: DataSourceProto) -> Any:
        assert data_source.HasField("custom_options")
        config = json.loads(
            str(data_source.custom_options.configuration, encoding="utf8")
        )
        table_format_dict = config.get("table_format") or {}
        table_format = (
            LanceFormat.from_dict(table_format_dict) if table_format_dict else None
        )
        tags = dict(data_source.tags)
        return LanceSource(
            name=data_source.name,
            uri=config.get("uri") or None,
            table=config.get("table") or None,
            table_format=table_format,
            namespace_impl=config.get("namespace_impl") or None,
            namespace_properties=config.get("namespace_properties") or None,
            storage_options=config.get("storage_options") or None,
            timestamp_field=data_source.timestamp_field,
            created_timestamp_column=data_source.created_timestamp_column,
            field_mapping=dict(data_source.field_mapping),
            description=data_source.description,
            tags=tags,
            owner=data_source.owner,
        )


def _declared_columns(feature_view: "FeatureView") -> List[str]:
    """Columns a feature view's declared schema requires from its source."""
    source = feature_view.batch_source
    reverse_mapping = (
        {v: k for k, v in source.field_mapping.items()} if source is not None else {}
    )

    required: List[str] = []
    for field in feature_view.schema:
        required.append(reverse_mapping.get(field.name, field.name))
    for entity_column in feature_view.entity_columns:
        required.append(reverse_mapping.get(entity_column.name, entity_column.name))
    if source is not None:
        if source.timestamp_field:
            required.append(source.timestamp_field)
        if source.created_timestamp_column:
            required.append(source.created_timestamp_column)

    seen = set()
    ordered = []
    for column in required:
        if column not in seen:
            seen.add(column)
            ordered.append(column)
    return ordered


def _validate_against_feature_view(
    source: LanceSource,
    schema: "pyarrow.Schema",
    feature_view: "FeatureView",
) -> None:
    """Fail if the pinned schema cannot satisfy the declared schema.

    A pin selects data, never shape. If a pinned version is missing a column the
    declared ``FeatureView`` asks for, the honest outcome is an error naming the
    pin, not a narrower result set.
    """
    available = set(schema.names)
    missing = [c for c in _declared_columns(feature_view) if c not in available]
    if missing:
        raise ValueError(
            f"Lance dataset '{source.name}' at {source.pin_description} is missing "
            f"column(s) {missing} required by the declared schema of feature view "
            f"'{feature_view.name}'. A pin selects data, not shape: either pin a "
            f"version whose schema matches the declared schema, or change the "
            f"declared schema. Available columns: {sorted(available)}."
        )


def validate_lance_source_schema(feature_view: "FeatureView") -> None:
    """Pre-flight check that a feature view's Lance pin agrees with its schema.

    Offline stores call this before building a retrieval plan. It reads only the
    pinned dataset's schema, so it costs one metadata round trip and never scans
    data. A no-op for feature views that are not Lance-backed.

    Vector widths are checked through ``_validate_vector_field_lengths``, the
    validator added in #6909, rather than through a second implementation. Lance
    stores vectors as ``fixed_size_list``, for which that validator's check is a
    property of the Arrow type and so is exact on a zero-row table.
    """
    source = getattr(feature_view, "batch_source", None)
    if not isinstance(source, LanceSource):
        return

    schema = source.get_schema()
    _validate_against_feature_view(source, schema, feature_view)

    from feast.utils import _validate_vector_field_lengths

    _validate_vector_field_lengths(schema.empty_table(), feature_view)
