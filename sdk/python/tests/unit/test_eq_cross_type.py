"""Cross-type ``__eq__`` regression tests (see #6636).

Comparing two Feast registry objects of different types must return ``False``
instead of raising ``TypeError``. This exercises the shared
``if not isinstance(other, X): return False`` guard across the importable core
object model in one place; the per-type tests for ``DataSource``, ``Entity``,
``LabelView``, and ``RoleBasedPolicy`` live in their own modules.

The contrib offline sources (athena, couchbase, mssql, oracle, postgres, ray,
trino) and the optional-dependency transformations are intentionally omitted:
the unit environment does not install their drivers, so they cannot be imported
here. Their ``__eq__`` follows the identical, mechanical pattern.
"""

from datetime import timedelta

import pytest

from feast import Entity, FeatureService, FeatureView, Project
from feast.aggregation import Aggregation
from feast.data_format import ProtoFormat
from feast.data_source import KafkaSource, KinesisSource, RequestSource
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
from feast.types import Int64


def _instances():
    """One instance of each importable type touched by the __eq__ sweep."""
    return {
        "Entity": Entity(name="e"),
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
