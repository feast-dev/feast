"""Cross-type ``__eq__`` regression tests (see #6636).

Comparing two Feast registry objects of different types must return ``False``
instead of raising ``TypeError``. This exercises the shared
``if not isinstance(other, X): return False`` guard across the importable core
object model in one place; the per-type tests for ``DataSource``, ``Entity``,
``LabelView``, and ``RoleBasedPolicy`` live in their own modules.

Most contrib offline sources (athena, couchbase, mssql, oracle, postgres, ray,
trino) and the optional-dependency transformations are intentionally omitted:
the unit environment does not install their drivers, so they cannot be imported
here. Their ``__eq__`` follows the identical, mechanical pattern. SparkSource
is the exception — its module imports without pyspark, and its ``__eq__``
accesses spark-only attributes after the shared ``DataSource`` base check, so
it gets a dedicated cross-subclass test below.
"""

from datetime import timedelta

import pytest

from feast import Entity, FeatureService, FeatureView, Project
from feast.aggregation import Aggregation
from feast.data_format import ParquetFormat, ProtoFormat
from feast.data_source import KafkaSource, KinesisSource, RequestSource
from feast.feature import Feature
from feast.field import Field
from feast.infra.offline_stores.bigquery_source import BigQuerySource
from feast.infra.offline_stores.file_source import FileSource
from feast.infra.offline_stores.redshift_source import RedshiftSource
from feast.infra.offline_stores.snowflake_source import SnowflakeSource
from feast.permissions.permission import Permission
from feast.permissions.policy import (
    CombinedGroupNamespacePolicy,
    GroupBasedPolicy,
    NamespaceBasedPolicy,
    RoleBasedPolicy,
)
from feast.types import Array, Float32, Int64
from feast.value_type import ValueType


def _instances():
    """One instance of each importable type touched by the __eq__ sweep."""
    return {
        "Entity": Entity(name="e"),
        "Feature": Feature(name="f", dtype=ValueType.INT64),
        "ParquetFormat": ParquetFormat(),
        "ProtoFormat": ProtoFormat("com.example.Msg"),
        "Project": Project(name="proj"),
        "Aggregation": Aggregation(column="c", function="sum"),
        "Permission": Permission(name="perm"),
        "RoleBasedPolicy": RoleBasedPolicy(roles=["reader"]),
        "GroupBasedPolicy": GroupBasedPolicy(groups=["g"]),
        "NamespaceBasedPolicy": NamespaceBasedPolicy(namespaces=["n"]),
        "CombinedGroupNamespacePolicy": CombinedGroupNamespacePolicy(
            groups=["g"], namespaces=["n"]
        ),
        "FileSource": FileSource(
            name="fs", path="/tmp/x.parquet", timestamp_field="ts"
        ),
        "BigQuerySource": BigQuerySource(
            name="bq", table="p.d.t", timestamp_field="ts"
        ),
        "RedshiftSource": RedshiftSource(name="rs", table="t", timestamp_field="ts"),
        "SnowflakeSource": SnowflakeSource(
            name="sf", database="D", schema="S", table="T", timestamp_field="ts"
        ),
        "KafkaSource": KafkaSource(
            name="ks",
            kafka_bootstrap_servers="s",
            message_format=ProtoFormat("cp"),
            topic="t",
            timestamp_field="ts",
        ),
        "KinesisSource": KinesisSource(
            name="kn",
            region="r",
            record_format=ProtoFormat("cp"),
            stream_name="s",
            timestamp_field="ts",
        ),
        "RequestSource": RequestSource(
            name="rq", schema=[Field(name="f", dtype=Int64)]
        ),
        "FeatureView": FeatureView(name="fv", ttl=timedelta(days=1)),
        "FeatureService": FeatureService(name="svc", features=[]),
    }


_CASES = list(_instances().items())


@pytest.mark.parametrize("name,obj", _CASES, ids=[n for n, _ in _CASES])
def test_eq_cross_type_returns_false(name, obj):
    # A different-typed operand must compare False, never raise (#6636).
    assert (obj == object()) is False
    assert (obj == "not a feast object") is False
    # __ne__ derives from __eq__, so it must be the inverse.
    assert (obj != object()) is True
    # The isinstance guard must not break same-object equality.
    assert (obj == obj) is True


def test_spark_source_vs_file_source_eq():
    # Reported on #6636: swapping a FeatureView's source from FileSource to
    # SparkSource. The two sources share the base DataSource fields, so
    # SparkSource.__eq__ used to pass the base check and then raise
    # AttributeError on FileSource's missing `table`. Both directions must
    # simply compare False.
    spark_source = pytest.importorskip(
        "feast.infra.offline_stores.contrib.spark_offline_store.spark_source"
    )
    SparkSource = spark_source.SparkSource

    spark = SparkSource(name="src", table="t", timestamp_field="ts")
    file = FileSource(name="src", path="/tmp/x.parquet", timestamp_field="ts")

    assert (spark == file) is False
    assert (file == spark) is False
    assert (spark == object()) is False
    assert (spark == SparkSource(name="src", table="t", timestamp_field="ts")) is True


def test_field_eq_includes_vector_attributes():
    """vector_index and vector_search_metric participate in Field equality.

    They were previously excluded because an unset metric deserialized as "" while
    the constructor default was None, so a Field never compared equal to its own
    deserialized copy. With that normalized, a change to either attribute is a real
    change and must be visible to feast plan / apply.
    """
    base = Field(
        name="embedding",
        dtype=Array(Float32),
        vector_index=True,
        vector_length=4,
        vector_search_metric="COSINE",
    )

    assert base == Field(
        name="embedding",
        dtype=Array(Float32),
        vector_index=True,
        vector_length=4,
        vector_search_metric="COSINE",
    )

    # Changing the metric alters retrieval semantics and must not compare equal.
    assert (
        base
        == Field(
            name="embedding",
            dtype=Array(Float32),
            vector_index=True,
            vector_length=4,
            vector_search_metric="L2",
        )
    ) is False

    # Turning the index off must not compare equal either.
    assert (
        base
        == Field(
            name="embedding",
            dtype=Array(Float32),
            vector_index=False,
            vector_length=4,
            vector_search_metric="COSINE",
        )
    ) is False


@pytest.mark.parametrize(
    "field",
    [
        Field(name="plain", dtype=Int64),
        Field(
            name="embedding", dtype=Array(Float32), vector_index=True, vector_length=4
        ),
        Field(
            name="embedding",
            dtype=Array(Float32),
            vector_index=True,
            vector_length=4,
            vector_search_metric="COSINE",
        ),
    ],
    ids=["plain", "vector_without_metric", "vector_with_metric"],
)
def test_field_survives_a_proto_round_trip(field):
    """A Field must compare equal to its own deserialized copy.

    This is the backwards-compatibility property that previously forced the vector
    comparisons out of __eq__: without it, every field lacking an explicit metric
    would show as changed on every feast plan.
    """
    assert Field.from_proto(field.to_proto()) == field


def test_field_normalizes_an_empty_metric_to_none():
    """An empty metric means unset, whichever construction path supplied it.

    Normalizing in __init__ rather than from_proto matters because callers such as
    OnDemandFeatureView._parse_features_from_proto build a Field straight from a
    proto message without going through from_proto.
    """
    assert (
        Field(name="x", dtype=Int64, vector_search_metric="").vector_search_metric
        is None
    )
    assert Field(name="x", dtype=Int64).vector_search_metric is None
    assert Field(name="x", dtype=Int64, vector_search_metric="") == Field(
        name="x", dtype=Int64
    )
