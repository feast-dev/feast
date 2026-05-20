import copy
import warnings
from datetime import timedelta
from typing import Dict, List, Optional, Type

from google.protobuf.duration_pb2 import Duration
from google.protobuf.message import Message
from typeguard import typechecked

from feast.base_feature_view import BaseFeatureView
from feast.data_source import DataSource
from feast.entity import Entity
from feast.feature_view_projection import FeatureViewProjection
from feast.field import Field
from feast.labeling.conflict_policy import ConflictPolicy
from feast.proto_utils import serialize_data_source
from feast.protos.feast.core.LabelView_pb2 import LabelView as LabelViewProto
from feast.protos.feast.core.LabelView_pb2 import LabelViewMeta as LabelViewMetaProto
from feast.protos.feast.core.LabelView_pb2 import LabelViewSpec as LabelViewSpecProto
from feast.types import from_value_type
from feast.value_type import ValueType

warnings.simplefilter("once", DeprecationWarning)


@typechecked
class LabelView(BaseFeatureView):
    """[Alpha] A LabelView manages mutable labels decoupled from immutable feature data.

    A LabelView defines a mutable set of labels or annotations that are kept
    separate from the immutable feature data stored in regular FeatureViews.
    It supports multi-labeler workflows where different sources (human reviewers,
    automated safety scanners, reward models) can independently write labels for
    the same entity keys.

    .. note::

        **Alpha limitations:** ``conflict_policy`` and ``retain_history`` are
        persisted in the registry and available for querying, but they are
        **not yet enforced** at read or write time. Currently, the online store
        returns the last-written row regardless of the policy, and write
        operations always overwrite the previous value for a given entity key.
        Enforcement will require online-store query-path and write-path changes
        in a future release.

    Attributes:
        name: The unique name of the label view.
        entities: The list of entity names associated with this label view.
        ttl: How long labels are valid for online serving. ``timedelta(0)``
            means labels never expire.
        source: The data source (typically a ``PushSource``) feeding label data.
        entity_columns: The entity key columns in the schema.
        features: The label columns (non-entity fields in the schema).
        online: Whether labels are served from the online store.
        description: A human-readable description.
        tags: Arbitrary key-value metadata.
        owner: Owner email or identifier.
        labeler_field: Name of the schema field that identifies who wrote the
            label (default ``"labeler"``).
        conflict_policy: How conflicting labels from different labelers are
            resolved (default ``ConflictPolicy.LAST_WRITE_WINS``).
            **Currently metadata-only** — not enforced at read time.
        retain_history: Whether to keep full write history or only the latest
            value (default ``True``). **Currently metadata-only** — not
            enforced at write time; the online store always overwrites.
        reference_feature_view: Optional name of the ``FeatureView`` whose
            entities this label view annotates.
    """

    name: str
    entities: List[str]
    ttl: Optional[timedelta]
    source: Optional[DataSource]
    entity_columns: List[Field]
    features: List[Field]
    online: bool
    description: str
    tags: Dict[str, str]
    owner: str

    labeler_field: str
    conflict_policy: ConflictPolicy
    retain_history: bool
    reference_feature_view: Optional[str]

    def __init__(
        self,
        *,
        name: str,
        source: Optional[DataSource] = None,
        schema: Optional[List[Field]] = None,
        entities: Optional[List[Entity]] = None,
        ttl: Optional[timedelta] = timedelta(days=0),
        online: bool = True,
        description: str = "",
        tags: Optional[Dict[str, str]] = None,
        owner: str = "",
        labeler_field: str = "labeler",
        conflict_policy: ConflictPolicy = ConflictPolicy.LAST_WRITE_WINS,
        retain_history: bool = True,
        reference_feature_view: Optional[str] = None,
    ):
        """Creates a LabelView object.

        Args:
            name: The unique name of this label view.
            source: The data source for ingesting labels, typically a
                ``PushSource``. If ``None``, labels can only be written
                programmatically via ``FeatureStore.push()``.
            schema: The list of ``Field`` objects describing both entity
                columns and label columns. Entity columns are identified
                by matching against entity join keys.
            entities: The list of ``Entity`` objects whose join keys are
                used to key the labels.
            ttl: The time-to-live for labels in the online store.
                ``timedelta(0)`` means labels never expire. ``None`` means
                the label view inherits the default TTL.
            online: Whether this label view should be materialized to the
                online store for low-latency serving.
            description: A human-readable description of what the labels
                represent.
            tags: A dictionary of key-value pairs for arbitrary metadata.
            owner: The owner of this label view, typically an email address.
            labeler_field: The name of the field in the schema that
                identifies the labeler. Defaults to ``"labeler"``.
            conflict_policy: The policy for resolving conflicting labels
                from different labelers. Defaults to
                ``ConflictPolicy.LAST_WRITE_WINS``. **Note:** this value
                is persisted in the registry but not yet enforced at
                read time; the online store currently returns the
                last-written row regardless of policy.
            retain_history: Whether to retain the full history of label
                writes or only the latest value per entity key. Defaults
                to ``True``. **Note:** this value is persisted in the
                registry but not yet enforced at write time; the online
                store currently always overwrites the previous value.
            reference_feature_view: The name of the ``FeatureView`` whose
                entities this label view annotates. This is informational
                and does not create a hard dependency.
        """
        self.ttl = ttl
        self.entities = []
        self.source = source

        schema = schema or []

        features: List[Field] = []
        self.entity_columns = []

        join_keys: List[str] = []
        if entities:
            for entity in entities:
                join_keys.append(entity.join_key)
                if entity.name != entity.join_key:
                    self.entities.append(entity.name)
                else:
                    self.entities.append(entity.name)

        if len(set(join_keys)) < len(join_keys):
            raise ValueError(
                "A label view should not have entities that share a join key."
            )

        for field in schema:
            if field.name in join_keys:
                self.entity_columns.append(field)
                matching_entities = (
                    [e for e in entities if e.join_key == field.name]
                    if entities
                    else []
                )
                if matching_entities:
                    entity = matching_entities[0]
                    if entity.value_type != ValueType.UNKNOWN:
                        if from_value_type(entity.value_type) != field.dtype:
                            raise ValueError(
                                f"Entity {entity.name} has type {entity.value_type}, "
                                f"which does not match the inferred type {field.dtype}."
                            )
            else:
                features.append(field)

        self.labeler_field = labeler_field
        self.conflict_policy = conflict_policy
        self.retain_history = retain_history
        self.reference_feature_view = reference_feature_view or ""

        super().__init__(
            name=name,
            features=features,
            description=description,
            tags=tags,
            owner=owner,
            source=source,
        )
        self.online = online

    def __hash__(self):
        return super().__hash__()

    def __copy__(self):
        lv = LabelView(
            name=self.name,
            ttl=self.ttl,
            source=self.source,
            schema=self.schema,
            tags=self.tags,
            online=self.online,
            description=self.description,
            owner=self.owner,
            labeler_field=self.labeler_field,
            conflict_policy=self.conflict_policy,
            retain_history=self.retain_history,
            reference_feature_view=self.reference_feature_view or None,
        )
        lv.entities = list(self.entities)
        lv.features = copy.copy(self.features)
        lv.entity_columns = copy.copy(self.entity_columns)
        lv.projection = copy.copy(self.projection)
        lv.version = self.version
        lv.current_version_number = self.current_version_number
        return lv

    def __eq__(self, other):
        if not isinstance(other, LabelView):
            raise TypeError("Comparisons should only involve LabelView class objects.")

        if not super().__eq__(other):
            return False

        if (
            sorted(self.entities) != sorted(other.entities)
            or self.ttl != other.ttl
            or self.online != other.online
            or sorted(self.entity_columns) != sorted(other.entity_columns)
            or self.labeler_field != other.labeler_field
            or self.conflict_policy != other.conflict_policy
            or self.retain_history != other.retain_history
            or self.reference_feature_view != other.reference_feature_view
        ):
            return False

        return True

    @property
    def join_keys(self) -> List[str]:
        """The entity join key column names for this label view."""
        return [ec.name for ec in self.entity_columns]

    @property
    def schema(self) -> List[Field]:
        """The full schema including both entity columns and label columns."""
        return list(set(self.entity_columns + self.features))

    @property
    def batch_source(self) -> Optional[DataSource]:
        """The batch data source for this label view.

        If the source is a ``PushSource``, returns its underlying
        ``batch_source``. Otherwise returns the source directly.
        This property enables compatibility with offline store
        ``get_historical_features`` implementations.
        """
        from feast.data_source import PushSource

        if self.source is None:
            return None
        if isinstance(self.source, PushSource):
            return self.source.batch_source
        return self.source

    @property
    def stream_source(self) -> Optional[DataSource]:
        """The stream data source for this label view.

        Returns the source if it is a ``PushSource``, ``None`` otherwise.
        """
        from feast.data_source import PushSource

        if self.source is not None and isinstance(self.source, PushSource):
            return self.source
        return None

    def ensure_valid(self):
        """Validates the label view configuration.

        Raises:
            ValueError: If the label view has no name (from ``BaseFeatureView``)
                or no entities.
        """
        super().ensure_valid()
        if not self.entities:
            raise ValueError("Label view has no entities.")

    @property
    def proto_class(self) -> Type[Message]:
        """The protobuf message class for LabelView."""
        return LabelViewProto

    def to_proto(self) -> LabelViewProto:
        """Converts this LabelView to its protobuf representation.

        Returns:
            A ``LabelViewProto`` message with the spec and metadata populated.
        """
        meta = LabelViewMetaProto()
        if self.created_timestamp:
            meta.created_timestamp.FromDatetime(self.created_timestamp)
        if self.last_updated_timestamp:
            meta.last_updated_timestamp.FromDatetime(self.last_updated_timestamp)

        ttl_duration = None
        if self.ttl is not None:
            ttl_duration = Duration()
            ttl_duration.FromTimedelta(self.ttl)

        source_proto = serialize_data_source(self.source)

        spec = LabelViewSpecProto(
            name=self.name,
            entities=self.entities,
            entity_columns=[field.to_proto() for field in self.entity_columns],
            features=[feature.to_proto() for feature in self.features],
            description=self.description,
            tags=self.tags,
            owner=self.owner,
            ttl=(ttl_duration if ttl_duration is not None else None),
            online=self.online,
            source=source_proto,
            labeler_field=self.labeler_field,
            conflict_policy=self.conflict_policy.to_proto(),  # type: ignore[arg-type]
            retain_history=self.retain_history,
            reference_feature_view=self.reference_feature_view or "",
        )

        return LabelViewProto(spec=spec, meta=meta)

    @classmethod
    def from_proto(cls, label_view_proto: LabelViewProto) -> "LabelView":
        """Creates a LabelView from a protobuf representation.

        Args:
            label_view_proto: A ``LabelViewProto`` message to deserialize.

        Returns:
            A ``LabelView`` instance populated from the protobuf data.
        """
        source = (
            DataSource.from_proto(label_view_proto.spec.source)
            if label_view_proto.spec.HasField("source")
            else None
        )

        label_view = cls(
            name=label_view_proto.spec.name,
            description=label_view_proto.spec.description,
            tags=dict(label_view_proto.spec.tags),
            owner=label_view_proto.spec.owner,
            online=label_view_proto.spec.online,
            ttl=(
                timedelta(days=0)
                if label_view_proto.spec.ttl.ToNanoseconds() == 0
                else label_view_proto.spec.ttl.ToTimedelta()
            ),
            source=source,
            labeler_field=label_view_proto.spec.labeler_field or "labeler",
            conflict_policy=ConflictPolicy.from_proto(
                label_view_proto.spec.conflict_policy
            ),
            retain_history=label_view_proto.spec.retain_history,
            reference_feature_view=(
                label_view_proto.spec.reference_feature_view or None
            ),
        )

        label_view.entities = list(label_view_proto.spec.entities)

        label_view.features = [
            Field.from_proto(field_proto)
            for field_proto in label_view_proto.spec.features
        ]
        label_view.entity_columns = [
            Field.from_proto(field_proto)
            for field_proto in label_view_proto.spec.entity_columns
        ]

        label_view.projection = FeatureViewProjection.from_definition(label_view)

        if label_view_proto.meta.HasField("created_timestamp"):
            label_view.created_timestamp = (
                label_view_proto.meta.created_timestamp.ToDatetime()
            )
        if label_view_proto.meta.HasField("last_updated_timestamp"):
            label_view.last_updated_timestamp = (
                label_view_proto.meta.last_updated_timestamp.ToDatetime()
            )

        return label_view
