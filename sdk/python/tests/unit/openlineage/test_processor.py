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
        assert ds["feast_object_type"] == "featureView"
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
        assert ds["feast_object_type"] == "unknown"

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
