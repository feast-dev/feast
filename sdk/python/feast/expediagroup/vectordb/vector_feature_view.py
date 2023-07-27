import copy
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Type

from typeguard import typechecked

from feast.base_feature_view import BaseFeatureView
from feast.data_source import DataSource
from feast.entity import Entity
from feast.feature_view import FeatureView
from feast.field import Field
from feast.expediagroup.vectordb.index_type import IndexType
from feast.protos.feast.core.FeatureView_pb2 import FeatureView as FeatureViewProto
from feast.protos.feast.core.VectorFeatureView_pb2 import VectorFeatureView as VectorFeatureViewProto

from feast.usage import log_exceptions

@typechecked
class VectorFeatureView(BaseFeatureView):
    """
    A FeatureView for vector databases.

    Attributes:
        name: The unique name of the feature view.
        entities: The list of names of entities that this feature view is associated with.
        ttl: The amount of time this group of features lives. A ttl of 0 indicates that
            this group of features lives forever. Note that large ttl's or a ttl of 0
            can result in extremely computationally intensive queries.
        batch_source: The batch source of data where this group of features
            is stored. This is optional ONLY if a push source is specified as the
            stream_source, since push sources contain their own batch sources.
        stream_source: The stream source of data where this group of features is stored.
        vector_field: The name of the field containing the vector in the source
        dimensions: The number of dimensions of the vector
        index_algorithm: The indexing algorithm that should be used in the vector database
        schema: The schema of the feature view, including feature, timestamp, and entity
            columns. If not specified, can be inferred from the underlying data source.
        entity_columns: The list of entity columns contained in the schema. If not specified,
            can be inferred from the underlying data source.
        features: The list of feature columns contained in the schema. If not specified,
            can be inferred from the underlying data source.
        online: A boolean indicating whether online retrieval is enabled for this feature
            view.
        description: A human-readable description.
        tags: A dictionary of key-value pairs to store arbitrary metadata.
        owner: The owner of the feature view, typically the email of the primary
            maintainer.
    """
    # inheriting from FeatureView wouldn't work due to issue with conflicting proto classes
    # therefore using composition instead
    feature_view: FeatureView
    vector_field: str
    dimensions: int
    index_algorithm: IndexType

    @log_exceptions
    def __init__(
        self,
        *,
        name: str,
        source: DataSource,
        vector_field: str,
        dimensions: int,
        index_algorithm: IndexType,
        schema: Optional[List[Field]] = None,
        entities: List[Entity] = None,
        ttl: Optional[timedelta] = timedelta(days=0),
        online: bool = True,
        description: str = "",
        tags: Optional[Dict[str, str]] = None,
        owner: str = "",
    ):
        """
        Creates a VectorFeatureView object.

        Args:
            name: The unique name of the feature view.
            source: The source of data for this group of features. May be a stream source, or a batch source.
                If a stream source, the source should contain a batch_source for backfills & batch materialization.
            vector_field: The name of the field containing the vector in the source
            dimensions: The number of dimensions of the vector
            index_algorithm: The indexing algorithm that should be used in the vector database
            schema (optional): The schema of the feature view, including feature, timestamp,
                and entity columns.
            entities (optional): The list of entities with which this group of features is associated.
            ttl (optional): The amount of time this group of features lives. A ttl of 0 indicates that
                this group of features lives forever. Note that large ttl's or a ttl of 0
                can result in extremely computationally intensive queries.
            online (optional): A boolean indicating whether online retrieval is enabled for
                this feature view.
            description (optional): A human-readable description.
            tags (optional): A dictionary of key-value pairs to store arbitrary metadata.
            owner (optional): The owner of the feature view, typically the email of the
                primary maintainer.
        """
        feature_view = FeatureView(
            name=name,
            source=source,
            schema=schema,
            entities=entities,
            ttl=ttl,
            online=online,
            description=description,
            tags=tags,
            owner=owner
        )

        self.feature_view = feature_view
        self.vector_field = vector_field
        self.dimensions = dimensions
        self.index_algorithm = index_algorithm

    def __hash__(self):
        return self.feature_view.__hash__()

    def __copy__(self):
        vector_feature_view = VectorFeatureView(
            name=self.feature_view.name,
            source=self.feature_view.stream_source if self.feature_view.stream_source else self.feature_view.batch_source,
            vector_field=self.vector_field,
            dimensions=self.dimensions,
            index_algorithm=self.index_algorithm,
            schema=self.feature_view.schema,
            ttl=self.feature_view.ttl,
            online=self.feature_view.online,
            description=self.feature_view.description,
            tags=self.feature_view.tags,
            owner=self.feature_view.owner
        )

        # This is deliberately set outside of the FV initialization as we do not have the Entity objects.
        vector_feature_view.feature_view.entities = self.feature_view.entities
        vector_feature_view.feature_view.features = copy.copy(self.feature_view.features)
        vector_feature_view.feature_view.entity_columns = copy.copy(self.feature_view.entity_columns)
        vector_feature_view.feature_view.projection = copy.copy(self.feature_view.projection)
        vector_feature_view.feature_view.original_schema = copy.copy(self.feature_view.original_schema)
        vector_feature_view.feature_view.original_entities = copy.copy(self.feature_view.original_entities)

        return vector_feature_view

    def __eq__(self, other):
        if not isinstance(other, VectorFeatureView):
            raise TypeError(
                "Comparisons should only involve VectorFeatureView class objects."
            )

        if not self.feature_view.__eq__(other.feature_view):
            return False

        if (self.vector_field != other.vector_field
            or self.dimensions != other.dimensions
            or self.index_algorithm != other.index_algorithm
        ):
            return False

        return True

    @property
    def join_keys(self) -> List[str]:
        """Returns a list of all the join keys."""
        return self.feature_view.join_keys

    @property
    def schema(self) -> List[Field]:
        return self.feature_view.schema()

    @property
    def proto_class(self) -> Type[VectorFeatureViewProto]:
        return VectorFeatureViewProto

    def to_proto(self) -> VectorFeatureViewProto:
        feature_view_proto = self.feature_view.to_proto()

        proto = VectorFeatureViewProto(
            feature_view=feature_view_proto,
            entities=[entity.to_proto() for entity in self.feature_view.original_entities],
            vector_field=self.vector_field,
            dimensions=self.dimensions,
            index_type=self.index_algorithm.value
        )

        return proto

    @classmethod
    def from_proto(cls, proto: VectorFeatureViewProto):
        """
        Creates a vector feature view from a protobuf representation of a vector feature view.

        Args:
            feature_view_proto: A protobuf representation of a vector feature view.

        Returns:
            A VectorFeatureViewProto object based on the vector feature view protobuf.
        """
        feature_view = FeatureView.from_proto(proto.feature_view)

        # convert Entity protos to Entity - necessary because FeatureView doesn't preserve them properly
        converted_entities = [Entity.from_proto(entity) for entity in proto.entities]

        vector_field = proto.vector_field
        dimensions = proto.dimensions
        index_algorithm = IndexType[proto.index_type]

        vector_feature_view = VectorFeatureView(
            name=feature_view.name,
            source=feature_view.stream_source if feature_view.stream_source else feature_view.batch_source,
            vector_field=vector_field,
            dimensions=dimensions,
            index_algorithm=index_algorithm,
            schema=feature_view.original_schema,
            entities=converted_entities,
            ttl=feature_view.ttl,
            online=feature_view.online,
            description=feature_view.description,
            tags=feature_view.tags,
            owner=feature_view.owner
        )

        return vector_feature_view
