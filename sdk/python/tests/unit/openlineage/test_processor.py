"""Comprehensive tests for OpenLineageProcessor."""

import pytest
from sqlalchemy import create_engine

from feast.openlineage.processor import OpenLineageProcessor
from feast.openlineage.store import OpenLineageStore


@pytest.fixture
def store():
    engine = create_engine("sqlite://", echo=False)
    s = OpenLineageStore(engine=engine)
    s.initialize()
    return s


@pytest.fixture
def processor(store):
    return OpenLineageProcessor(store=store)


@pytest.fixture
def processor_with_mapping(store):
    return OpenLineageProcessor(
        store=store,
        namespace_mapping={"spark-ns": "feast-project"},
    )


def _run_event(
    job_ns="test-ns",
    job_name="etl-job",
    run_id="run-001",
    event_type="COMPLETE",
    producer="https://spark.apache.org",
    inputs=None,
    outputs=None,
    job_facets=None,
    run_facets=None,
):
    return {
        "eventType": event_type,
        "eventTime": "2026-06-01T12:00:00Z",
        "producer": producer,
        "job": {"namespace": job_ns, "name": job_name, "facets": job_facets or {}},
        "run": {"runId": run_id, "facets": run_facets or {}},
        "inputs": inputs or [],
        "outputs": outputs or [],
    }


def _dataset_event(
    ds_ns="data-ns",
    ds_name="my-table",
    producer="https://dbt.com",
    ds_facets=None,
):
    return {
        "eventTime": "2026-06-01T12:00:00Z",
        "producer": producer,
        "dataset": {
            "namespace": ds_ns,
            "name": ds_name,
            "facets": ds_facets or {},
        },
    }


def _job_event(
    job_ns="pipeline-ns",
    job_name="my-pipeline",
    producer="https://airflow.apache.org",
    inputs=None,
    outputs=None,
    job_facets=None,
):
    return {
        "eventTime": "2026-06-01T12:00:00Z",
        "producer": producer,
        "job": {
            "namespace": job_ns,
            "name": job_name,
            "facets": job_facets or {},
        },
        "inputs": inputs or [],
        "outputs": outputs or [],
    }


# ── Event type routing ──


class TestEventRouting:
    def test_run_event_detected(self, processor, store):
        eid = processor.process_event(_run_event())
        assert eid
        assert len(store.get_events()) == 1
        assert len(store.get_jobs()) == 1
        assert len(store.get_runs()) == 1

    def test_dataset_event_detected(self, processor, store):
        eid = processor.process_event(_dataset_event())
        assert eid
        assert len(store.get_events()) == 1
        assert len(store.get_datasets()) == 1
        assert len(store.get_jobs()) == 0

    def test_job_event_detected(self, processor, store):
        eid = processor.process_event(_job_event())
        assert eid
        assert len(store.get_events()) == 1
        assert len(store.get_jobs()) == 1
        assert len(store.get_runs()) == 0

    def test_unknown_event_stored_as_raw(self, processor, store):
        eid = processor.process_event(
            {"eventTime": "2026-01-01T00:00:00Z", "producer": "x"}
        )
        assert eid
        assert len(store.get_events()) == 1

    def test_returns_unique_event_ids(self, processor):
        id1 = processor.process_event(_run_event(run_id="r1"))
        id2 = processor.process_event(_run_event(run_id="r2"))
        assert id1 != id2


# ── RunEvent processing ──


class TestProcessRunEvent:
    def test_job_created(self, processor, store):
        processor.process_event(_run_event(job_ns="ns", job_name="j1"))
        jobs = store.get_jobs()
        assert len(jobs) == 1
        assert jobs[0]["job_namespace"] == "ns"
        assert jobs[0]["job_name"] == "j1"

    def test_run_created(self, processor, store):
        processor.process_event(_run_event(run_id="r1", event_type="START"))
        runs = store.get_runs()
        assert len(runs) == 1
        assert runs[0]["run_id"] == "r1"
        assert runs[0]["state"] == "START"

    def test_producer_propagated_to_job(self, processor, store):
        processor.process_event(_run_event(producer="https://spark.apache.org"))
        jobs = store.get_jobs()
        assert jobs[0]["producer"] == "https://spark.apache.org"

    def test_inputs_create_datasets_and_edges(self, processor, store):
        processor.process_event(
            _run_event(
                inputs=[{"namespace": "s3://bucket", "name": "raw-data", "facets": {}}],
            )
        )
        datasets = store.get_datasets()
        assert any(d["dataset_name"] == "raw-data" for d in datasets)

        edges = store.get_all_lineage_edges()
        input_edges = [e for e in edges if e["edge_type"] == "input"]
        assert len(input_edges) == 1
        assert input_edges[0]["source_name"] == "raw-data"
        assert input_edges[0]["target_name"] == "etl-job"

    def test_outputs_create_datasets_and_edges(self, processor, store):
        processor.process_event(
            _run_event(
                outputs=[
                    {"namespace": "s3://bucket", "name": "processed", "facets": {}}
                ],
            )
        )
        datasets = store.get_datasets()
        assert any(d["dataset_name"] == "processed" for d in datasets)

        edges = store.get_all_lineage_edges()
        output_edges = [e for e in edges if e["edge_type"] == "output"]
        assert len(output_edges) == 1
        assert output_edges[0]["source_name"] == "etl-job"
        assert output_edges[0]["target_name"] == "processed"

    def test_run_io_stored(self, processor, store):
        processor.process_event(
            _run_event(
                run_id="r1",
                inputs=[{"namespace": "ns", "name": "in1", "facets": {}}],
                outputs=[{"namespace": "ns", "name": "out1", "facets": {}}],
            )
        )
        detail = store.get_run_detail("r1")
        assert len(detail["inputs"]) == 1
        assert len(detail["outputs"]) == 1

    def test_run_facets_stored(self, processor, store):
        processor.process_event(
            _run_event(
                run_id="r1",
                run_facets={"spark.logicalPlan": {"plan": "..."}},
            )
        )
        runs = store.get_runs()
        import json

        facets = json.loads(runs[0]["facets_json"])
        assert "spark.logicalPlan" in facets

    def test_input_defaults_namespace_to_job_namespace(self, processor, store):
        processor.process_event(
            _run_event(
                job_ns="my-ns",
                inputs=[{"name": "table1", "facets": {}}],
            )
        )
        datasets = store.get_datasets()
        assert datasets[0]["dataset_namespace"] == "my-ns"


# ── Dataset-to-dataset transitive edges ──


class TestDatasetToDatasetEdges:
    def test_derived_edges_created(self, processor, store):
        processor.process_event(
            _run_event(
                inputs=[{"namespace": "ns", "name": "raw", "facets": {}}],
                outputs=[{"namespace": "ns", "name": "clean", "facets": {}}],
            )
        )
        edges = store.get_all_lineage_edges()
        derived = [e for e in edges if e["edge_type"] == "derived"]
        assert len(derived) == 1
        assert derived[0]["source_name"] == "raw"
        assert derived[0]["target_name"] == "clean"

    def test_multiple_inputs_outputs_create_cross_product(self, processor, store):
        processor.process_event(
            _run_event(
                inputs=[
                    {"namespace": "ns", "name": "in1", "facets": {}},
                    {"namespace": "ns", "name": "in2", "facets": {}},
                ],
                outputs=[
                    {"namespace": "ns", "name": "out1", "facets": {}},
                    {"namespace": "ns", "name": "out2", "facets": {}},
                ],
            )
        )
        edges = store.get_all_lineage_edges()
        derived = [e for e in edges if e["edge_type"] == "derived"]
        assert len(derived) == 4

    def test_no_derived_edge_for_empty_name(self, processor, store):
        processor.process_event(
            _run_event(
                inputs=[{"namespace": "ns", "name": "", "facets": {}}],
                outputs=[{"namespace": "ns", "name": "out", "facets": {}}],
            )
        )
        edges = store.get_all_lineage_edges()
        derived = [e for e in edges if e["edge_type"] == "derived"]
        assert len(derived) == 0


# ── DatasetEvent processing ──


class TestProcessDatasetEvent:
    def test_dataset_created(self, processor, store):
        processor.process_event(_dataset_event(ds_ns="ns", ds_name="tbl"))
        datasets = store.get_datasets()
        assert len(datasets) == 1
        assert datasets[0]["dataset_namespace"] == "ns"
        assert datasets[0]["dataset_name"] == "tbl"

    def test_producer_set_on_dataset(self, processor, store):
        processor.process_event(_dataset_event(producer="https://dbt.com"))
        datasets = store.get_datasets()
        assert datasets[0]["producer"] == "https://dbt.com"

    def test_facets_extracted(self, processor, store):
        processor.process_event(
            _dataset_event(
                ds_facets={
                    "documentation": {"description": "Customer table"},
                    "schema": {"fields": [{"name": "id", "type": "INT"}]},
                },
            )
        )
        datasets = store.get_datasets()
        assert datasets[0]["description"] == "Customer table"


# ── JobEvent processing ──


class TestProcessJobEvent:
    def test_job_created(self, processor, store):
        processor.process_event(_job_event(job_ns="ns", job_name="pipeline"))
        jobs = store.get_jobs()
        assert len(jobs) == 1
        assert jobs[0]["job_name"] == "pipeline"

    def test_job_inputs_create_edges(self, processor, store):
        processor.process_event(
            _job_event(
                inputs=[{"namespace": "ns", "name": "src-table", "facets": {}}],
            )
        )
        edges = store.get_all_lineage_edges()
        assert any(
            e["edge_type"] == "input" and e["source_name"] == "src-table" for e in edges
        )

    def test_job_outputs_create_edges(self, processor, store):
        processor.process_event(
            _job_event(
                outputs=[{"namespace": "ns", "name": "dest-table", "facets": {}}],
            )
        )
        edges = store.get_all_lineage_edges()
        assert any(
            e["edge_type"] == "output" and e["target_name"] == "dest-table"
            for e in edges
        )


# ── Symlink processing ──


class TestSymlinkProcessing:
    def test_symlinks_facet_creates_symlinks(self, processor, store):
        processor.process_event(
            _run_event(
                inputs=[
                    {
                        "namespace": "spark-ns",
                        "name": "spark_table",
                        "facets": {
                            "symlinks": {
                                "identifiers": [
                                    {
                                        "namespace": "hive-ns",
                                        "name": "hive_table",
                                        "type": "TABLE",
                                    },
                                ]
                            }
                        },
                    }
                ],
            )
        )
        aliases = store.get_dataset_aliases("spark-ns", "spark_table")
        assert len(aliases) == 1
        assert aliases[0]["namespace"] == "hive-ns"
        assert aliases[0]["name"] == "hive_table"

    def test_symlinks_create_bidirectional_edges(self, processor, store):
        processor.process_event(
            _run_event(
                inputs=[
                    {
                        "namespace": "ns-a",
                        "name": "ds-a",
                        "facets": {
                            "symlinks": {
                                "identifiers": [
                                    {
                                        "namespace": "ns-b",
                                        "name": "ds-b",
                                        "type": "TABLE",
                                    },
                                ]
                            }
                        },
                    }
                ],
            )
        )
        edges = store.get_all_lineage_edges()
        symlink_edges = [e for e in edges if e["edge_type"] == "symlink"]
        assert len(symlink_edges) == 2
        directions = {(e["source_name"], e["target_name"]) for e in symlink_edges}
        assert ("ds-a", "ds-b") in directions
        assert ("ds-b", "ds-a") in directions

    def test_symlink_to_self_ignored(self, processor, store):
        processor.process_event(
            _run_event(
                inputs=[
                    {
                        "namespace": "ns",
                        "name": "ds",
                        "facets": {
                            "symlinks": {
                                "identifiers": [
                                    {"namespace": "ns", "name": "ds", "type": "TABLE"},
                                ]
                            }
                        },
                    }
                ],
            )
        )
        aliases = store.get_dataset_aliases("ns", "ds")
        assert len(aliases) == 0

    def test_symlink_linked_dataset_created(self, processor, store):
        processor.process_event(
            _run_event(
                inputs=[
                    {
                        "namespace": "ns-a",
                        "name": "ds-a",
                        "facets": {
                            "symlinks": {
                                "identifiers": [
                                    {
                                        "namespace": "ns-b",
                                        "name": "ds-b",
                                        "type": "TABLE",
                                    },
                                ]
                            }
                        },
                    }
                ],
            )
        )
        datasets = store.get_datasets()
        names = {d["dataset_name"] for d in datasets}
        assert "ds-b" in names

    def test_datasource_uri_links_datasets(self, processor, store):
        store.upsert_dataset(
            "existing-ns",
            "existing-ds",
            facets={"dataSource": {"uri": "postgres://host/db/table"}},
        )
        processor.process_event(
            _run_event(
                inputs=[
                    {
                        "namespace": "new-ns",
                        "name": "new-ds",
                        "facets": {
                            "dataSource": {"uri": "postgres://host/db/table"},
                        },
                    }
                ],
            )
        )
        aliases = store.get_dataset_aliases("new-ns", "new-ds")
        assert any(
            a["namespace"] == "existing-ns" and a["name"] == "existing-ds"
            for a in aliases
        )

    def test_datasource_uri_no_match(self, processor, store):
        processor.process_event(
            _run_event(
                inputs=[
                    {
                        "namespace": "ns",
                        "name": "ds",
                        "facets": {"dataSource": {"uri": "s3://unique/path"}},
                    }
                ],
            )
        )
        aliases = store.get_dataset_aliases("ns", "ds")
        assert len(aliases) == 0

    def test_symlinks_in_dataset_event(self, processor, store):
        processor.process_event(
            _dataset_event(
                ds_ns="ns-a",
                ds_name="ds-a",
                ds_facets={
                    "symlinks": {
                        "identifiers": [
                            {"namespace": "ns-b", "name": "ds-b", "type": "TABLE"},
                        ]
                    }
                },
            )
        )
        aliases = store.get_dataset_aliases("ns-a", "ds-a")
        assert len(aliases) == 1


# ── Feast mapping ──


class TestFeastMapping:
    def test_online_store_prefix_mapped(self, processor, store):
        processor.process_event(
            _run_event(
                inputs=[
                    {
                        "namespace": "test-ns",
                        "name": "online_store_driver_fv",
                        "facets": {},
                    }
                ],
            )
        )
        datasets = store.get_datasets()
        ds = [d for d in datasets if d["dataset_name"] == "online_store_driver_fv"][0]
        assert ds["feast_object_type"] == "onlineStore"
        assert ds["feast_object_name"] == "driver_fv"

    def test_request_source_prefix_mapped(self, processor, store):
        processor.process_event(
            _run_event(
                inputs=[
                    {
                        "namespace": "test-ns",
                        "name": "request_source_input",
                        "facets": {},
                    }
                ],
            )
        )
        datasets = store.get_datasets()
        ds = [d for d in datasets if d["dataset_name"] == "request_source_input"][0]
        assert ds["feast_object_type"] == "dataSource"

    def test_default_mapping(self, processor, store):
        processor.process_event(
            _run_event(
                inputs=[
                    {"namespace": "test-ns", "name": "regular_dataset", "facets": {}}
                ],
            )
        )
        datasets = store.get_datasets()
        ds = [d for d in datasets if d["dataset_name"] == "regular_dataset"][0]
        # Unresolvable datasets stay untyped rather than "unknown"
        assert ds["feast_object_type"] in (None, "unknown")

    def test_facet_mapping_feature_view(self, processor, store):
        processor.process_event(
            _run_event(
                inputs=[
                    {
                        "namespace": "test-ns",
                        "name": "driver_hourly_stats",
                        "facets": {
                            "feast_featureView": {"name": "driver_hourly_stats"}
                        },
                    }
                ],
            )
        )
        ds = [
            d
            for d in store.get_datasets()
            if d["dataset_name"] == "driver_hourly_stats"
        ][0]
        assert ds["feast_object_type"] == "featureView"
        assert ds["feast_object_name"] == "driver_hourly_stats"

    def test_facet_mapping_entity(self, processor, store):
        processor.process_event(
            _run_event(
                inputs=[
                    {
                        "namespace": "test-ns",
                        "name": "driver",
                        "facets": {"feast_entity": {"name": "driver"}},
                    }
                ],
            )
        )
        ds = [d for d in store.get_datasets() if d["dataset_name"] == "driver"][0]
        assert ds["feast_object_type"] == "entity"
        assert ds["feast_object_name"] == "driver"

    def test_namespace_mapping_applied(self, processor_with_mapping, store):
        processor_with_mapping.process_event(
            _run_event(
                job_ns="spark-ns",
                inputs=[
                    {"namespace": "spark-ns", "name": "online_store_fv1", "facets": {}}
                ],
            )
        )
        datasets = store.get_datasets()
        ds = [d for d in datasets if d["dataset_name"] == "online_store_fv1"][0]
        assert ds["feast_project"] == "feast-project"


# ── Batch processing ──


class TestBatchProcessing:
    def test_batch_all_succeed(self, processor, store):
        events = [_run_event(run_id=f"r{i}") for i in range(3)]
        result = processor.process_batch(events)
        assert result["received"] == 3
        assert result["successful"] == 3
        assert result["failed"] == 0
        assert len(result["event_ids"]) == 3

    def test_batch_with_failures(self, processor, store):
        events = [
            _run_event(run_id="r1"),
            "not a dict",
            _run_event(run_id="r2"),
        ]
        result = processor.process_batch(events)
        assert result["received"] == 3
        assert result["successful"] == 2
        assert result["failed"] == 1

    def test_batch_empty(self, processor, store):
        result = processor.process_batch([])
        assert result["received"] == 0
        assert result["successful"] == 0
        assert result["failed"] == 0


# ── End-to-end lineage chain ──


class TestEndToEndLineage:
    def test_full_pipeline_lineage(self, processor, store):
        """Simulate: raw_data -> spark_etl -> clean_data -> dbt_model -> analytics_table"""
        processor.process_event(
            _run_event(
                job_ns="spark",
                job_name="spark_etl",
                run_id="r1",
                producer="https://spark.apache.org",
                inputs=[{"namespace": "s3", "name": "raw_data", "facets": {}}],
                outputs=[
                    {"namespace": "warehouse", "name": "clean_data", "facets": {}}
                ],
            )
        )
        processor.process_event(
            _run_event(
                job_ns="dbt",
                job_name="dbt_model",
                run_id="r2",
                producer="https://getdbt.com",
                inputs=[{"namespace": "warehouse", "name": "clean_data", "facets": {}}],
                outputs=[
                    {"namespace": "warehouse", "name": "analytics_table", "facets": {}}
                ],
            )
        )

        graph = store.get_lineage_graph(
            "dataset", "s3", "raw_data", direction="downstream", depth=10
        )
        names = {n["name"] for n in graph["nodes"]}
        assert "raw_data" in names
        assert "spark_etl" in names
        assert "clean_data" in names
        assert "dbt_model" in names
        assert "analytics_table" in names

    def test_upstream_from_output(self, processor, store):
        """Verify upstream traversal from the final output."""
        processor.process_event(
            _run_event(
                job_ns="ns",
                job_name="j1",
                run_id="r1",
                inputs=[{"namespace": "ns", "name": "a", "facets": {}}],
                outputs=[{"namespace": "ns", "name": "b", "facets": {}}],
            )
        )
        processor.process_event(
            _run_event(
                job_ns="ns",
                job_name="j2",
                run_id="r2",
                inputs=[{"namespace": "ns", "name": "b", "facets": {}}],
                outputs=[{"namespace": "ns", "name": "c", "facets": {}}],
            )
        )

        graph = store.get_lineage_graph(
            "dataset", "ns", "c", direction="upstream", depth=10
        )
        names = {n["name"] for n in graph["nodes"]}
        assert "a" in names
        assert "j1" in names
        assert "c" in names


# ── Parent hierarchy extraction ──


class TestParentHierarchy:
    def test_parent_run_id_extracted(self, processor, store):
        event = _run_event(
            run_id="child-run",
            run_facets={
                "parent": {
                    "run": {"runId": "parent-run"},
                    "job": {"namespace": "ns", "name": "parent-job"},
                }
            },
        )
        processor.process_event(event)
        runs = store.get_runs()
        assert len(runs) == 1
        assert runs[0]["parent_run_id"] == "parent-run"
        assert runs[0]["root_run_id"] == "parent-run"

    def test_root_run_id_from_nested_root(self, processor, store):
        event = _run_event(
            run_id="grandchild",
            run_facets={
                "parent": {
                    "run": {"runId": "child-run"},
                    "job": {"namespace": "ns", "name": "child-job"},
                    "root": {
                        "run": {"runId": "root-run"},
                        "job": {"namespace": "ns", "name": "root-job"},
                    },
                }
            },
        )
        processor.process_event(event)
        runs = store.get_runs()
        assert runs[0]["parent_run_id"] == "child-run"
        assert runs[0]["root_run_id"] == "root-run"

    def test_no_parent_facet(self, processor, store):
        event = _run_event(run_id="standalone")
        processor.process_event(event)
        runs = store.get_runs()
        assert runs[0]["parent_run_id"] is None
        assert runs[0]["root_run_id"] is None

    def test_child_runs_query(self, processor, store):
        processor.process_event(
            _run_event(
                job_name="parent-j",
                run_id="parent-run",
            )
        )
        processor.process_event(
            _run_event(
                job_name="child-j",
                run_id="child-1",
                run_facets={
                    "parent": {
                        "run": {"runId": "parent-run"},
                        "job": {"namespace": "test-ns", "name": "parent-j"},
                    }
                },
            )
        )
        processor.process_event(
            _run_event(
                job_name="child-j2",
                run_id="child-2",
                run_facets={
                    "parent": {
                        "run": {"runId": "parent-run"},
                        "job": {"namespace": "test-ns", "name": "parent-j"},
                    }
                },
            )
        )
        children = store.get_child_runs("parent-run")
        assert len(children) == 2
        child_ids = {c["run_id"] for c in children}
        assert "child-1" in child_ids
        assert "child-2" in child_ids

    def test_run_tree_query(self, processor, store):
        processor.process_event(_run_event(run_id="root"))
        for i in range(3):
            processor.process_event(
                _run_event(
                    job_name=f"child-{i}",
                    run_id=f"child-{i}",
                    run_facets={
                        "parent": {
                            "run": {"runId": "root"},
                            "job": {"namespace": "test-ns", "name": "etl-job"},
                            "root": {
                                "run": {"runId": "root"},
                                "job": {"namespace": "test-ns", "name": "etl-job"},
                            },
                        }
                    },
                )
            )
        tree = store.get_run_tree("root")
        assert len(tree) == 4
        run_ids = {r["run_id"] for r in tree}
        assert "root" in run_ids


# ── Column-level lineage ──


class TestColumnLineage:
    def test_column_lineage_extracted(self, processor, store):
        event = _run_event(
            outputs=[
                {
                    "namespace": "ns",
                    "name": "output_table",
                    "facets": {
                        "columnLineage": {
                            "fields": {
                                "full_name": {
                                    "inputFields": [
                                        {
                                            "namespace": "ns",
                                            "name": "input_table",
                                            "field": "first_name",
                                            "transformations": [
                                                {
                                                    "type": "DIRECT",
                                                    "description": "concatenation",
                                                }
                                            ],
                                        },
                                        {
                                            "namespace": "ns",
                                            "name": "input_table",
                                            "field": "last_name",
                                        },
                                    ]
                                }
                            }
                        }
                    },
                }
            ],
        )
        processor.process_event(event)
        cl = store.get_column_lineage("ns", "output_table", direction="upstream")
        assert len(cl) == 2
        fields = {(c["input_field"], c["output_field"]) for c in cl}
        assert ("first_name", "full_name") in fields
        assert ("last_name", "full_name") in fields

        xform = next(c for c in cl if c["input_field"] == "first_name")
        assert xform["transformation_type"] == "DIRECT"

    def test_column_lineage_downstream_query(self, processor, store):
        event = _run_event(
            outputs=[
                {
                    "namespace": "ns",
                    "name": "derived",
                    "facets": {
                        "columnLineage": {
                            "fields": {
                                "score": {
                                    "inputFields": [
                                        {
                                            "namespace": "ns",
                                            "name": "source",
                                            "field": "raw_score",
                                        }
                                    ]
                                }
                            }
                        }
                    },
                }
            ],
        )
        processor.process_event(event)
        downstream = store.get_column_lineage("ns", "source", direction="downstream")
        assert len(downstream) == 1
        assert downstream[0]["output_field"] == "score"
        assert downstream[0]["dataset_name"] == "derived"

    def test_no_column_lineage_when_absent(self, processor, store):
        event = _run_event(
            outputs=[{"namespace": "ns", "name": "tbl", "facets": {}}],
        )
        processor.process_event(event)
        cl = store.get_column_lineage("ns", "tbl")
        assert len(cl) == 0


# ── Dataset versioning ──


class TestDatasetVersioning:
    def test_version_created_on_complete(self, processor, store):
        event = _run_event(
            event_type="COMPLETE",
            run_id="ver-run-1",
            outputs=[
                {
                    "namespace": "ns",
                    "name": "versioned_ds",
                    "facets": {"schema": {"fields": [{"name": "id", "type": "INT"}]}},
                }
            ],
        )
        processor.process_event(event)
        versions = store.get_dataset_versions("ns", "versioned_ds")
        assert len(versions) == 1
        assert versions[0]["version"] == 1
        assert versions[0]["created_by_run_id"] == "ver-run-1"

        datasets = store.get_datasets(namespaces=["ns"])
        ds = next(d for d in datasets if d["dataset_name"] == "versioned_ds")
        assert ds["current_version"] == 1

    def test_multiple_versions(self, processor, store):
        for i in range(3):
            event = _run_event(
                event_type="COMPLETE",
                run_id=f"run-{i}",
                job_name=f"job-{i}",
                outputs=[{"namespace": "ns", "name": "multi_ver", "facets": {}}],
            )
            processor.process_event(event)

        versions = store.get_dataset_versions("ns", "multi_ver")
        assert len(versions) == 3
        assert versions[0]["version"] == 3
        assert versions[2]["version"] == 1

    def test_no_version_on_start(self, processor, store):
        event = _run_event(
            event_type="START",
            outputs=[{"namespace": "ns", "name": "ds", "facets": {}}],
        )
        processor.process_event(event)
        versions = store.get_dataset_versions("ns", "ds")
        assert len(versions) == 0

    def test_get_specific_version(self, processor, store):
        for i in range(2):
            processor.process_event(
                _run_event(
                    event_type="COMPLETE",
                    run_id=f"r{i}",
                    job_name=f"j{i}",
                    outputs=[{"namespace": "ns", "name": "ds", "facets": {}}],
                )
            )
        v1 = store.get_dataset_version("ns", "ds", 1)
        assert v1 is not None
        assert v1["version"] == 1
        assert store.get_dataset_version("ns", "ds", 99) is None


# ── Ownership extraction ──


class TestOwnershipExtraction:
    def test_ownership_facet_indexed(self, processor, store):
        event = _run_event(
            outputs=[
                {
                    "namespace": "ns",
                    "name": "owned_ds",
                    "facets": {
                        "ownership": {
                            "owners": [
                                {"name": "team-ml", "type": "TEAM"},
                                {"name": "alice@example.com", "type": "PERSON"},
                            ]
                        }
                    },
                }
            ],
        )
        processor.process_event(event)

        datasets = store.get_datasets(namespaces=["ns"])
        ds = next(d for d in datasets if d["dataset_name"] == "owned_ds")
        assert ds["owner_name"] == "team-ml"
        assert ds["owner_type"] == "TEAM"

        owners = store.get_dataset_owners("ns", "owned_ds")
        assert len(owners) == 2
        names = {o["name"] for o in owners}
        assert "team-ml" in names
        assert "alice@example.com" in names

    def test_no_ownership_when_absent(self, processor, store):
        event = _run_event(
            outputs=[{"namespace": "ns", "name": "no_owner", "facets": {}}],
        )
        processor.process_event(event)
        owners = store.get_dataset_owners("ns", "no_owner")
        assert len(owners) == 0
        datasets = store.get_datasets(namespaces=["ns"])
        ds = next(d for d in datasets if d["dataset_name"] == "no_owner")
        assert ds["owner_name"] is None


# ── Lifecycle tracking ──


class TestLifecycleTracking:
    def test_lifecycle_state_indexed(self, processor, store):
        event = _dataset_event(
            ds_facets={"lifecycleStateChange": {"lifecycleStateChange": "CREATE"}},
        )
        processor.process_event(event)
        datasets = store.get_datasets()
        assert datasets[0]["lifecycle_state"] == "CREATE"

    def test_lifecycle_update(self, processor, store):
        processor.process_event(
            _dataset_event(
                ds_facets={"lifecycleStateChange": {"lifecycleStateChange": "CREATE"}},
            )
        )
        processor.process_event(
            _dataset_event(
                ds_facets={"lifecycleStateChange": {"lifecycleStateChange": "ALTER"}},
            )
        )
        datasets = store.get_datasets()
        assert len(datasets) == 1
        assert datasets[0]["lifecycle_state"] == "ALTER"


# ── Assurance level computation ──


class TestAssuranceLevel:
    def test_none_for_unknown_dataset(self, store):
        result = store.compute_assurance_level("ns", "nonexistent")
        assert result["level"] == "none"

    def test_none_when_no_edges(self, store):
        store.upsert_dataset("ns", "isolated")
        result = store.compute_assurance_level("ns", "isolated")
        assert result["level"] == "none"

    def test_linked_with_edges(self, store):
        store.upsert_dataset("ns", "ds1")
        store.upsert_lineage_edge("dataset", "ns", "ds1", "job", "ns", "j1")
        result = store.compute_assurance_level("ns", "ds1")
        assert result["level"] == "linked"
        assert result["details"]["edge_count"] >= 1

    def test_observed_with_source_evidence(self, store):
        store.upsert_dataset(
            "ns",
            "ds2",
            facets={"dataSource": {"uri": "s3://bucket/path"}},
        )
        store.upsert_lineage_edge("dataset", "ns", "ds2", "job", "ns", "j1")
        result = store.compute_assurance_level("ns", "ds2")
        assert result["level"] == "observed"
        assert "dataSource.uri" in result["details"]["evidence"]

    def test_reproducible_with_versions(self, store):
        store.upsert_dataset(
            "ns",
            "ds3",
            facets={
                "dataSource": {"uri": "s3://bucket/path"},
                "schema": {"fields": [{"name": "id", "type": "INT"}]},
            },
        )
        store.upsert_lineage_edge("dataset", "ns", "ds3", "job", "ns", "j1")
        store.create_dataset_version(
            "ns", "ds3", run_id="r1", schema_json='{"fields": []}'
        )
        result = store.compute_assurance_level("ns", "ds3")
        assert result["level"] == "reproducible"
        assert result["details"]["version_count"] == 1
