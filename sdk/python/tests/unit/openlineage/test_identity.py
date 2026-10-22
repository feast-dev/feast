"""Tests for feast.openlineage.identity."""

from feast.openlineage.identity import (
    FeastJobKind,
    LineageParentContext,
    materialize_job_name,
    resolve_namespace,
    spark_compute_job_name,
)


class TestResolveNamespace:
    def test_default_feast_uses_project(self):
        assert resolve_namespace("feast", "my_project") == "my_project"
        assert resolve_namespace("", "my_project") == "my_project"
        assert resolve_namespace(None, "my_project") == "my_project"

    def test_matching_namespace_not_doubled(self):
        assert resolve_namespace("customer_churn", "customer_churn") == "customer_churn"

    def test_prefix_when_distinct(self):
        assert resolve_namespace("org", "proj") == "org/proj"

    def test_already_suffixed(self):
        assert resolve_namespace("org/proj", "proj") == "org/proj"


class TestJobNames:
    def test_materialize(self):
        assert materialize_job_name("p") == "materialize_p"

    def test_spark_compute(self):
        assert spark_compute_job_name("p") == "spark_compute_p"


class TestLineageParentContext:
    def test_to_spark_conf(self):
        ctx = LineageParentContext("ns", "materialize_p", "run-1")
        conf = ctx.to_spark_openlineage_conf()
        assert conf["spark.openlineage.parentJobNamespace"] == "ns"
        assert conf["spark.openlineage.parentJobName"] == "materialize_p"
        assert conf["spark.openlineage.parentRunId"] == "run-1"
        assert conf["spark.openlineage.rootParentRunId"] == "run-1"

    def test_from_mapping(self):
        ctx = LineageParentContext.from_mapping(
            {"jobNamespace": "ns", "jobName": "j", "runId": "r"}
        )
        assert ctx == LineageParentContext("ns", "j", "r")

    def test_job_kind_values(self):
        assert FeastJobKind.DEFINITION.value == "definition"
        assert FeastJobKind.TRANSFORM.value == "transform"
