"""Comprehensive tests for OpenLineageStore."""

import json
import time

import pytest
from sqlalchemy import create_engine, select

from feast.openlineage.models import OL_TABLES
from feast.openlineage.store import OpenLineageStore


@pytest.fixture
def store():
    engine = create_engine("sqlite://", echo=False)
    s = OpenLineageStore(engine=engine)
    s.initialize()
    return s


def _make_event(
    event_id="evt-1",
    event_type="COMPLETE",
    job_ns="ns-a",
    job_name="job-1",
    run_id="run-1",
    producer="test-producer",
):
    return {
        "eventType": event_type,
        "eventTime": "2026-01-01T00:00:00Z",
        "producer": producer,
        "job": {"namespace": job_ns, "name": job_name},
        "run": {"runId": run_id},
    }


# ── Initialization ──


class TestStoreInitialize:
    def test_tables_created(self, store):
        with store.engine.connect() as conn:
            for tbl in OL_TABLES.values():
                conn.execute(select(tbl)).fetchall()

    def test_idempotent_reinit(self, store):
        store.initialize()
        with store.engine.connect() as conn:
            conn.execute(select(OL_TABLES["events"])).fetchall()

    def test_requires_engine_or_connection_string(self):
        with pytest.raises(ValueError):
            OpenLineageStore()

    def test_connection_string_init(self, tmp_path):
        db = tmp_path / "test.db"
        s = OpenLineageStore(connection_string=f"sqlite:///{db}")
        s.initialize()
        assert s.engine is not None


# ── store_event / get_events ──


class TestStoreEvent:
    def test_insert_and_retrieve(self, store):
        store.store_event("e1", _make_event(event_id="e1"))
        events = store.get_events()
        assert len(events) == 1
        assert events[0]["event_id"] == "e1"
        assert events[0]["event_type"] == "COMPLETE"

    def test_event_json_round_trip(self, store):
        data = _make_event(event_id="e2")
        store.store_event("e2", data)
        events = store.get_events()
        parsed = json.loads(events[0]["event_json"])
        assert parsed["producer"] == "test-producer"


class TestGetEvents:
    def test_filter_by_namespace(self, store):
        store.store_event("e1", _make_event(event_id="e1", job_ns="ns-a"))
        store.store_event("e2", _make_event(event_id="e2", job_ns="ns-b"))
        events = store.get_events(namespace="ns-a")
        assert len(events) == 1
        assert events[0]["job_namespace"] == "ns-a"

    def test_filter_by_job_name(self, store):
        store.store_event("e1", _make_event(event_id="e1", job_name="j1"))
        store.store_event("e2", _make_event(event_id="e2", job_name="j2"))
        events = store.get_events(job_name="j1")
        assert len(events) == 1

    def test_filter_by_namespaces_list(self, store):
        store.store_event("e1", _make_event(event_id="e1", job_ns="ns-a"))
        store.store_event("e2", _make_event(event_id="e2", job_ns="ns-b"))
        store.store_event("e3", _make_event(event_id="e3", job_ns="ns-c"))
        events = store.get_events(namespaces=["ns-a", "ns-c"])
        assert len(events) == 2

    def test_limit_and_offset(self, store):
        for i in range(5):
            store.store_event(f"e{i}", _make_event(event_id=f"e{i}"))
        events = store.get_events(limit=2)
        assert len(events) == 2
        events = store.get_events(limit=2, offset=3)
        assert len(events) == 2


# ── upsert_job / get_jobs ──


class TestUpsertJob:
    def test_insert(self, store):
        store.upsert_job("ns", "j1", {"facets": {}}, producer="spark")
        jobs = store.get_jobs()
        assert len(jobs) == 1
        assert jobs[0]["job_name"] == "j1"
        assert jobs[0]["producer"] == "spark"

    def test_update_preserves_fields(self, store):
        store.upsert_job(
            "ns",
            "j1",
            {"facets": {"documentation": {"description": "desc1"}}},
            producer="spark",
        )
        store.upsert_job("ns", "j1", {"facets": {}}, producer="spark-v2")
        jobs = store.get_jobs()
        assert len(jobs) == 1
        assert jobs[0]["description"] == "desc1"
        assert jobs[0]["producer"] == "spark-v2"

    def test_job_type_extraction(self, store):
        store.upsert_job(
            "ns", "j1", {"facets": {"jobType": {"processingType": "BATCH"}}}
        )
        jobs = store.get_jobs()
        assert jobs[0]["job_type"] == "BATCH"

    def test_filter_by_namespaces(self, store):
        store.upsert_job("ns-a", "j1", {"facets": {}})
        store.upsert_job("ns-b", "j2", {"facets": {}})
        jobs = store.get_jobs(namespaces=["ns-a"])
        assert len(jobs) == 1
        assert jobs[0]["job_namespace"] == "ns-a"


# ── upsert_dataset / get_datasets ──


class TestUpsertDataset:
    def test_insert(self, store):
        store.upsert_dataset("ns", "ds1", producer="dbt")
        datasets = store.get_datasets()
        assert len(datasets) == 1
        assert datasets[0]["dataset_name"] == "ds1"
        assert datasets[0]["producer"] == "dbt"

    def test_schema_and_description(self, store):
        facets = {
            "schema": {"fields": [{"name": "id", "type": "INT"}]},
            "documentation": {"description": "my dataset"},
        }
        store.upsert_dataset("ns", "ds1", facets=facets)
        datasets = store.get_datasets()
        assert datasets[0]["description"] == "my dataset"
        assert json.loads(datasets[0]["schema_json"])["fields"][0]["name"] == "id"

    def test_feast_mapping(self, store):
        store.upsert_dataset(
            "ns",
            "ds1",
            feast_mapping={"type": "FeatureView", "name": "fv1", "project": "proj"},
        )
        datasets = store.get_datasets()
        assert datasets[0]["feast_object_type"] == "FeatureView"
        assert datasets[0]["feast_object_name"] == "fv1"
        assert datasets[0]["feast_project"] == "proj"

    def test_update(self, store):
        store.upsert_dataset(
            "ns", "ds1", facets={"documentation": {"description": "v1"}}
        )
        store.upsert_dataset(
            "ns", "ds1", facets={"documentation": {"description": "v2"}}
        )
        datasets = store.get_datasets()
        assert len(datasets) == 1
        assert datasets[0]["description"] == "v2"

    def test_filter_by_namespaces(self, store):
        store.upsert_dataset("ns-a", "ds1")
        store.upsert_dataset("ns-b", "ds2")
        datasets = store.get_datasets(namespaces=["ns-b"])
        assert len(datasets) == 1
        assert datasets[0]["dataset_namespace"] == "ns-b"


# ── upsert_run / get_runs ──


class TestUpsertRun:
    def test_insert_start(self, store):
        store.upsert_job("ns", "j1", {"facets": {}})
        store.upsert_run("r1", "ns", "j1", "START")
        runs = store.get_runs()
        assert len(runs) == 1
        assert runs[0]["state"] == "START"
        assert runs[0]["started_at"] is not None
        assert runs[0]["ended_at"] is None

    def test_update_to_complete(self, store):
        store.upsert_job("ns", "j1", {"facets": {}})
        store.upsert_run("r1", "ns", "j1", "START")
        store.upsert_run("r1", "ns", "j1", "COMPLETE")
        runs = store.get_runs()
        assert runs[0]["state"] == "COMPLETE"
        assert runs[0]["ended_at"] is not None

    def test_update_to_fail(self, store):
        store.upsert_job("ns", "j1", {"facets": {}})
        store.upsert_run("r1", "ns", "j1", "START")
        store.upsert_run("r1", "ns", "j1", "FAIL")
        runs = store.get_runs()
        assert runs[0]["state"] == "FAIL"
        assert runs[0]["ended_at"] is not None

    def test_latest_run_id_updated_on_job(self, store):
        store.upsert_job("ns", "j1", {"facets": {}})
        store.upsert_run("r1", "ns", "j1", "COMPLETE")
        jobs = store.get_jobs()
        assert jobs[0]["latest_run_id"] == "r1"

    def test_facets_stored(self, store):
        store.upsert_job("ns", "j1", {"facets": {}})
        store.upsert_run("r1", "ns", "j1", "COMPLETE", facets={"key": "val"})
        runs = store.get_runs()
        assert json.loads(runs[0]["facets_json"])["key"] == "val"


class TestGetRuns:
    def test_filter_by_job_namespace(self, store):
        store.upsert_job("ns-a", "j1", {"facets": {}})
        store.upsert_job("ns-b", "j2", {"facets": {}})
        store.upsert_run("r1", "ns-a", "j1", "COMPLETE")
        store.upsert_run("r2", "ns-b", "j2", "COMPLETE")
        runs = store.get_runs(job_namespace="ns-a")
        assert len(runs) == 1
        assert runs[0]["job_namespace"] == "ns-a"

    def test_filter_by_job_name(self, store):
        store.upsert_job("ns", "j1", {"facets": {}})
        store.upsert_job("ns", "j2", {"facets": {}})
        store.upsert_run("r1", "ns", "j1", "COMPLETE")
        store.upsert_run("r2", "ns", "j2", "COMPLETE")
        runs = store.get_runs(job_name="j2")
        assert len(runs) == 1
        assert runs[0]["job_name"] == "j2"

    def test_limit_and_offset(self, store):
        store.upsert_job("ns", "j1", {"facets": {}})
        for i in range(5):
            store.upsert_run(f"r{i}", "ns", "j1", "COMPLETE")
        runs = store.get_runs(limit=2)
        assert len(runs) == 2
        runs = store.get_runs(limit=10, offset=3)
        assert len(runs) == 2

    def test_ordering(self, store):
        store.upsert_job("ns", "j1", {"facets": {}})
        store.upsert_run("r1", "ns", "j1", "START")
        time.sleep(0.01)
        store.upsert_run("r2", "ns", "j1", "COMPLETE")
        runs = store.get_runs()
        assert runs[0]["run_id"] == "r2"


# ── get_run_detail ──


class TestGetRunDetail:
    def test_returns_run_with_io(self, store):
        store.upsert_job("ns", "j1", {"facets": {}})
        store.upsert_run("r1", "ns", "j1", "COMPLETE")
        store.store_run_io("r1", "ns-ds", "input-ds", "INPUT", {"key": "v"})
        store.store_run_io("r1", "ns-ds", "output-ds", "OUTPUT")

        detail = store.get_run_detail("r1")
        assert detail is not None
        assert detail["run_id"] == "r1"
        assert len(detail["inputs"]) == 1
        assert detail["inputs"][0]["name"] == "input-ds"
        assert detail["inputs"][0]["facets"]["key"] == "v"
        assert len(detail["outputs"]) == 1
        assert detail["outputs"][0]["name"] == "output-ds"

    def test_missing_run_returns_none(self, store):
        assert store.get_run_detail("nonexistent") is None

    def test_run_with_no_io(self, store):
        store.upsert_job("ns", "j1", {"facets": {}})
        store.upsert_run("r1", "ns", "j1", "COMPLETE")
        detail = store.get_run_detail("r1")
        assert detail is not None
        assert detail["inputs"] == []
        assert detail["outputs"] == []


# ── store_run_io ──


class TestStoreRunIO:
    def test_insert(self, store):
        store.store_run_io("r1", "ns", "ds1", "INPUT")
        tbl = OL_TABLES["run_io"]
        with store.engine.connect() as conn:
            rows = conn.execute(select(tbl)).fetchall()
            assert len(rows) == 1

    def test_dedup(self, store):
        store.store_run_io("r1", "ns", "ds1", "INPUT")
        store.store_run_io("r1", "ns", "ds1", "INPUT")
        tbl = OL_TABLES["run_io"]
        with store.engine.connect() as conn:
            rows = conn.execute(select(tbl)).fetchall()
            assert len(rows) == 1

    def test_input_and_output_separate(self, store):
        store.store_run_io("r1", "ns", "ds1", "INPUT")
        store.store_run_io("r1", "ns", "ds1", "OUTPUT")
        tbl = OL_TABLES["run_io"]
        with store.engine.connect() as conn:
            rows = conn.execute(select(tbl)).fetchall()
            assert len(rows) == 2


# ── upsert_lineage_edge / get_all_lineage_edges ──


class TestUpsertLineageEdge:
    def test_insert(self, store):
        store.upsert_lineage_edge("dataset", "ns", "ds1", "job", "ns", "j1", "consumes")
        edges = store.get_all_lineage_edges()
        assert len(edges) == 1
        assert edges[0]["edge_type"] == "consumes"

    def test_update(self, store):
        store.upsert_lineage_edge("dataset", "ns", "ds1", "job", "ns", "j1", "consumes")
        store.upsert_lineage_edge("dataset", "ns", "ds1", "job", "ns", "j1", "produces")
        edges = store.get_all_lineage_edges()
        assert len(edges) == 1
        assert edges[0]["edge_type"] == "produces"

    def test_dedup(self, store):
        store.upsert_lineage_edge("dataset", "ns", "ds1", "job", "ns", "j1")
        store.upsert_lineage_edge("dataset", "ns", "ds1", "job", "ns", "j1")
        edges = store.get_all_lineage_edges()
        assert len(edges) == 1


class TestGetAllLineageEdges:
    def test_unfiltered(self, store):
        store.upsert_lineage_edge("dataset", "ns-a", "ds1", "job", "ns-a", "j1")
        store.upsert_lineage_edge("dataset", "ns-b", "ds2", "job", "ns-b", "j2")
        edges = store.get_all_lineage_edges()
        assert len(edges) == 2

    def test_namespace_filtered(self, store):
        store.upsert_lineage_edge("dataset", "ns-a", "ds1", "job", "ns-a", "j1")
        store.upsert_lineage_edge("dataset", "ns-b", "ds2", "job", "ns-b", "j2")
        edges = store.get_all_lineage_edges(namespaces=["ns-a"])
        assert len(edges) == 1
        assert edges[0]["source_namespace"] == "ns-a"


# ── Lineage graph traversal ──


class TestLineageGraph:
    def _setup_chain(self, store):
        """ds1 -> j1 -> ds2 -> j2 -> ds3"""
        store.upsert_lineage_edge("dataset", "ns", "ds1", "job", "ns", "j1")
        store.upsert_lineage_edge("job", "ns", "j1", "dataset", "ns", "ds2")
        store.upsert_lineage_edge("dataset", "ns", "ds2", "job", "ns", "j2")
        store.upsert_lineage_edge("job", "ns", "j2", "dataset", "ns", "ds3")

    def test_downstream(self, store):
        self._setup_chain(store)
        graph = store.get_lineage_graph("dataset", "ns", "ds1", direction="downstream")
        names = {n["name"] for n in graph["nodes"]}
        assert "ds1" in names
        assert "j1" in names
        assert "ds2" in names

    def test_upstream(self, store):
        self._setup_chain(store)
        graph = store.get_lineage_graph("dataset", "ns", "ds3", direction="upstream")
        names = {n["name"] for n in graph["nodes"]}
        assert "ds3" in names
        assert "j2" in names
        assert "ds2" in names

    def test_both(self, store):
        self._setup_chain(store)
        graph = store.get_lineage_graph("dataset", "ns", "ds2", direction="both")
        names = {n["name"] for n in graph["nodes"]}
        assert "ds1" in names
        assert "ds3" in names

    def test_depth_limit(self, store):
        self._setup_chain(store)
        graph = store.get_lineage_graph(
            "dataset", "ns", "ds1", depth=1, direction="downstream"
        )
        names = {n["name"] for n in graph["nodes"]}
        assert "ds1" in names
        assert "j1" in names
        assert "ds3" not in names

    def test_root_included_when_no_edges(self, store):
        graph = store.get_lineage_graph("dataset", "ns", "orphan", direction="both")
        assert len(graph["nodes"]) == 1
        assert graph["nodes"][0]["name"] == "orphan"

    def test_namespace_rbac_filtering(self, store):
        store.upsert_lineage_edge("dataset", "ns-a", "ds1", "job", "ns-b", "j1")
        graph = store.get_lineage_graph(
            "dataset",
            "ns-a",
            "ds1",
            direction="downstream",
            allowed_namespaces=["ns-a"],
        )
        assert len(graph["edges"]) == 0


# ── Symlinks ──


class TestSymlinks:
    def test_upsert_and_retrieve(self, store):
        store.upsert_dataset_symlink("ns-a", "ds-a", "ns-b", "ds-b", "symlink")
        aliases = store.get_dataset_aliases("ns-a", "ds-a")
        assert len(aliases) == 1
        assert aliases[0]["namespace"] == "ns-b"
        assert aliases[0]["name"] == "ds-b"

    def test_bidirectional_lookup(self, store):
        store.upsert_dataset_symlink("ns-a", "ds-a", "ns-b", "ds-b")
        reverse = store.get_dataset_aliases("ns-b", "ds-b")
        assert len(reverse) == 1
        assert reverse[0]["namespace"] == "ns-a"

    def test_get_all_symlinks(self, store):
        store.upsert_dataset_symlink("ns-a", "ds-a", "ns-b", "ds-b")
        store.upsert_dataset_symlink("ns-c", "ds-c", "ns-d", "ds-d")
        all_links = store.get_all_symlinks()
        assert len(all_links) == 2

    def test_upsert_updates_existing(self, store):
        store.upsert_dataset_symlink("ns-a", "ds-a", "ns-b", "ds-b", "symlink")
        store.upsert_dataset_symlink("ns-a", "ds-a", "ns-b", "ds-b", "alias")
        aliases = store.get_dataset_aliases("ns-a", "ds-a")
        assert len(aliases) == 1
        assert aliases[0]["link_type"] == "alias"


# ── find_datasets_by_uri ──


class TestFindDatasetsByUri:
    def test_match(self, store):
        store.upsert_dataset(
            "ns", "ds1", facets={"dataSource": {"uri": "s3://bucket/path"}}
        )
        store.upsert_dataset(
            "ns", "ds2", facets={"dataSource": {"uri": "other://path"}}
        )
        results = store.find_datasets_by_uri("s3://bucket/path")
        assert len(results) == 1
        assert results[0]["name"] == "ds1"

    def test_no_match(self, store):
        store.upsert_dataset(
            "ns", "ds1", facets={"dataSource": {"uri": "s3://bucket/path"}}
        )
        results = store.find_datasets_by_uri("s3://bucket/other")
        assert len(results) == 0

    def test_no_facets(self, store):
        store.upsert_dataset("ns", "ds1")
        results = store.find_datasets_by_uri("s3://bucket/path")
        assert len(results) == 0


# ── purge_all ──


class TestPurgeAll:
    def _populate(self, store):
        store.store_event("e1", _make_event())
        store.upsert_job("ns", "j1", {"facets": {}})
        store.upsert_dataset("ns", "ds1")
        store.upsert_run("r1", "ns", "j1", "COMPLETE")
        store.store_run_io("r1", "ns", "ds1", "INPUT")
        store.upsert_lineage_edge("dataset", "ns", "ds1", "job", "ns", "j1")
        store.upsert_dataset_symlink("ns", "ds1", "ns2", "ds2")

    def test_purge_all_empties_tables(self, store):
        self._populate(store)
        store.purge_all()
        assert len(store.get_events()) == 0
        assert len(store.get_jobs()) == 0
        assert len(store.get_datasets()) == 0
        assert len(store.get_runs()) == 0
        assert len(store.get_all_lineage_edges()) == 0
        assert len(store.get_all_symlinks()) == 0

    def test_purge_all_idempotent(self, store):
        store.purge_all()
        store.purge_all()


# ── purge_namespace ──


class TestPurgeNamespace:
    def _populate_two_ns(self, store):
        store.store_event("e1", _make_event(event_id="e1", job_ns="ns-a"))
        store.store_event("e2", _make_event(event_id="e2", job_ns="ns-b"))
        store.upsert_job("ns-a", "j1", {"facets": {}})
        store.upsert_job("ns-b", "j2", {"facets": {}})
        store.upsert_dataset("ns-a", "ds1")
        store.upsert_dataset("ns-b", "ds2")
        store.upsert_run("r1", "ns-a", "j1", "COMPLETE")
        store.upsert_run("r2", "ns-b", "j2", "COMPLETE")
        store.store_run_io("r1", "ns-a", "ds1", "INPUT")
        store.store_run_io("r2", "ns-b", "ds2", "INPUT")
        store.upsert_lineage_edge("dataset", "ns-a", "ds1", "job", "ns-a", "j1")
        store.upsert_lineage_edge("dataset", "ns-b", "ds2", "job", "ns-b", "j2")
        store.upsert_dataset_symlink("ns-a", "ds1", "ns-a", "ds1-alias")
        store.upsert_dataset_symlink("ns-b", "ds2", "ns-b", "ds2-alias")

    def test_purge_removes_only_target_namespace(self, store):
        self._populate_two_ns(store)
        store.purge_namespace("ns-a")

        assert len(store.get_events(namespace="ns-a")) == 0
        assert len(store.get_events(namespace="ns-b")) == 1
        assert len(store.get_jobs(namespaces=["ns-a"])) == 0
        assert len(store.get_jobs(namespaces=["ns-b"])) == 1
        assert len(store.get_datasets(namespaces=["ns-a"])) == 0
        assert len(store.get_datasets(namespaces=["ns-b"])) == 1
        assert len(store.get_runs(job_namespace="ns-a")) == 0
        assert len(store.get_runs(job_namespace="ns-b")) == 1

    def test_purge_nonexistent_namespace(self, store):
        self._populate_two_ns(store)
        store.purge_namespace("ns-nonexistent")
        assert len(store.get_events()) == 2
