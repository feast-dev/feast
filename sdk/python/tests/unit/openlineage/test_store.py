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


class TestRetention:
    """Tests for prune_expired() and get_retention_stats()."""

    def _insert_old_and_new(self, store, old_age_days=60, new_age_days=5):
        """Insert events/runs at two ages: one old (should be pruned), one new."""
        old_ms = int((time.time() - old_age_days * 86400) * 1000)
        new_ms = int((time.time() - new_age_days * 86400) * 1000)

        tbl_ev = OL_TABLES["events"]
        tbl_runs = OL_TABLES["runs"]
        tbl_rio = OL_TABLES["run_io"]

        with store.engine.begin() as conn:
            conn.execute(
                tbl_ev.insert().values(
                    event_id="old-evt",
                    event_type="COMPLETE",
                    event_time=old_ms,
                    producer="test",
                    job_namespace="ns",
                    job_name="job1",
                    run_id="old-run",
                    event_json="{}",
                    created_at=old_ms,
                )
            )
            conn.execute(
                tbl_ev.insert().values(
                    event_id="new-evt",
                    event_type="COMPLETE",
                    event_time=new_ms,
                    producer="test",
                    job_namespace="ns",
                    job_name="job1",
                    run_id="new-run",
                    event_json="{}",
                    created_at=new_ms,
                )
            )
            conn.execute(
                tbl_runs.insert().values(
                    run_id="old-run",
                    job_namespace="ns",
                    job_name="job1",
                    state="COMPLETE",
                    updated_at=old_ms,
                )
            )
            conn.execute(
                tbl_runs.insert().values(
                    run_id="new-run",
                    job_namespace="ns",
                    job_name="job1",
                    state="COMPLETE",
                    updated_at=new_ms,
                )
            )
            conn.execute(
                tbl_rio.insert().values(
                    run_id="old-run",
                    dataset_namespace="ns",
                    dataset_name="ds1",
                    io_type="INPUT",
                )
            )
            conn.execute(
                tbl_rio.insert().values(
                    run_id="new-run",
                    dataset_namespace="ns",
                    dataset_name="ds1",
                    io_type="INPUT",
                )
            )

        store.upsert_job("ns", "job1", {"namespace": "ns", "name": "job1"})
        store.upsert_dataset("ns", "ds1")

    def test_prune_removes_old_keeps_new(self, store):
        self._insert_old_and_new(store)
        deleted = store.prune_expired(retention_days=30)

        assert deleted["events"] == 1
        assert deleted["runs"] == 1
        assert deleted["run_io"] == 1

        assert len(store.get_events()) == 1
        assert len(store.get_runs()) == 1

    def test_prune_preserves_graph_tables(self, store):
        self._insert_old_and_new(store)
        store.upsert_lineage_edge("dataset", "ns", "ds1", "job", "ns", "job1")

        store.prune_expired(retention_days=30)

        assert len(store.get_jobs(namespaces=["ns"])) == 1
        assert len(store.get_datasets(namespaces=["ns"])) == 1
        edges = store.get_all_lineage_edges(namespaces=["ns"])
        assert len(edges) == 1

    def test_prune_disabled_when_zero(self, store):
        self._insert_old_and_new(store)
        deleted = store.prune_expired(retention_days=0)

        assert deleted == {}
        assert len(store.get_events()) == 2

    def test_prune_noop_when_nothing_expired(self, store):
        self._insert_old_and_new(store, old_age_days=5, new_age_days=1)
        deleted = store.prune_expired(retention_days=30)

        assert deleted["events"] == 0
        assert deleted["runs"] == 0
        assert len(store.get_events()) == 2

    def test_retention_stats(self, store):
        self._insert_old_and_new(store)
        stats = store.get_retention_stats()

        assert stats["events"]["count"] == 2
        assert stats["runs"]["count"] == 2
        assert stats["jobs"]["count"] == 1
        assert stats["datasets"]["count"] == 1
        assert "oldest_ms" in stats["events"]

    def test_prune_deletes_old_dataset_versions(self, store):
        self._insert_old_and_new(store)

        old_ms = int((time.time() - 60 * 86400) * 1000)
        new_ms = int((time.time() - 5 * 86400) * 1000)
        tbl_ver = OL_TABLES["dataset_versions"]
        store.upsert_dataset("ns", "ds1")
        with store.engine.begin() as conn:
            conn.execute(
                tbl_ver.insert().values(
                    dataset_namespace="ns",
                    dataset_name="ds1",
                    version=1,
                    created_by_run_id="old-run",
                    created_at=old_ms,
                )
            )
            conn.execute(
                tbl_ver.insert().values(
                    dataset_namespace="ns",
                    dataset_name="ds1",
                    version=2,
                    created_by_run_id="new-run",
                    created_at=new_ms,
                )
            )

        deleted = store.prune_expired(retention_days=30)
        assert deleted["dataset_versions"] == 1

        versions = store.get_dataset_versions("ns", "ds1")
        assert len(versions) == 1
        assert versions[0]["version"] == 2


# ── Dataset versioning ──


class TestDatasetVersioning:
    def test_create_version(self, store):
        store.upsert_dataset("ns", "ds1")
        ver = store.create_dataset_version("ns", "ds1", run_id="r1")
        assert ver == 1

    def test_increment_versions(self, store):
        store.upsert_dataset("ns", "ds1")
        assert store.create_dataset_version("ns", "ds1") == 1
        assert store.create_dataset_version("ns", "ds1") == 2
        assert store.create_dataset_version("ns", "ds1") == 3

    def test_current_version_updated(self, store):
        store.upsert_dataset("ns", "ds1")
        store.create_dataset_version("ns", "ds1")
        store.create_dataset_version("ns", "ds1")
        datasets = store.get_datasets(namespaces=["ns"])
        assert datasets[0]["current_version"] == 2

    def test_list_versions(self, store):
        store.upsert_dataset("ns", "ds1")
        for i in range(5):
            store.create_dataset_version("ns", "ds1", run_id=f"run-{i}")
        versions = store.get_dataset_versions("ns", "ds1", limit=3)
        assert len(versions) == 3
        assert versions[0]["version"] == 5

    def test_get_specific_version(self, store):
        store.upsert_dataset("ns", "ds1")
        store.create_dataset_version(
            "ns",
            "ds1",
            run_id="r1",
            schema_json='{"fields": []}',
            facets_json='{"key": "val"}',
        )
        v = store.get_dataset_version("ns", "ds1", 1)
        assert v is not None
        assert v["created_by_run_id"] == "r1"

    def test_get_missing_version(self, store):
        store.upsert_dataset("ns", "ds1")
        assert store.get_dataset_version("ns", "ds1", 99) is None


# ── Column lineage store ──


class TestColumnLineageStore:
    def test_upsert_and_query(self, store):
        store.upsert_column_lineage(
            "ns",
            "out_ds",
            "col_a",
            "ns",
            "in_ds",
            "col_x",
            transformation_type="DIRECT",
        )
        cl = store.get_column_lineage("ns", "out_ds", direction="upstream")
        assert len(cl) == 1
        assert cl[0]["output_field"] == "col_a"
        assert cl[0]["input_field"] == "col_x"
        assert cl[0]["transformation_type"] == "DIRECT"

    def test_dedup(self, store):
        for _ in range(3):
            store.upsert_column_lineage(
                "ns",
                "out_ds",
                "col_a",
                "ns",
                "in_ds",
                "col_x",
            )
        cl = store.get_column_lineage("ns", "out_ds")
        upstream = [c for c in cl if c["direction"] == "upstream"]
        assert len(upstream) == 1

    def test_downstream_query(self, store):
        store.upsert_column_lineage(
            "ns",
            "out_ds",
            "col_a",
            "ns",
            "in_ds",
            "col_x",
        )
        cl = store.get_column_lineage("ns", "in_ds", direction="downstream")
        assert len(cl) == 1
        assert cl[0]["dataset_name"] == "out_ds"
        assert cl[0]["direction"] == "downstream"


# ── Dataset ownership store ──


class TestDatasetOwnershipStore:
    def test_upsert_and_query(self, store):
        store.upsert_dataset("ns", "owned_ds")
        store._upsert_dataset_owners(
            "ns",
            "owned_ds",
            [
                {"name": "alice", "type": "PERSON"},
                {"name": "team-data", "type": "TEAM"},
            ],
        )
        owners = store.get_dataset_owners("ns", "owned_ds")
        assert len(owners) == 2
        names = {o["name"] for o in owners}
        assert "alice" in names
        assert "team-data" in names

    def test_upsert_updates_type(self, store):
        store._upsert_dataset_owners("ns", "ds1", [{"name": "alice", "type": "PERSON"}])
        store._upsert_dataset_owners("ns", "ds1", [{"name": "alice", "type": "ADMIN"}])
        owners = store.get_dataset_owners("ns", "ds1")
        assert len(owners) == 1
        assert owners[0]["type"] == "ADMIN"

    def test_empty_owner_skipped(self, store):
        store._upsert_dataset_owners("ns", "ds1", [{"name": "", "type": "PERSON"}])
        owners = store.get_dataset_owners("ns", "ds1")
        assert len(owners) == 0


# ── Run hierarchy store ──


class TestRunHierarchyStore:
    def test_parent_and_root_stored(self, store):
        store.upsert_job("ns", "j1", {"facets": {}})
        store.upsert_run(
            "child-1",
            "ns",
            "j1",
            "COMPLETE",
            parent_run_id="parent-1",
            root_run_id="root-1",
        )
        runs = store.get_runs()
        assert runs[0]["parent_run_id"] == "parent-1"
        assert runs[0]["root_run_id"] == "root-1"

    def test_child_runs(self, store):
        store.upsert_job("ns", "j1", {"facets": {}})
        store.upsert_run("parent", "ns", "j1", "COMPLETE")
        store.upsert_run(
            "child-a",
            "ns",
            "j1",
            "COMPLETE",
            parent_run_id="parent",
        )
        store.upsert_run(
            "child-b",
            "ns",
            "j1",
            "COMPLETE",
            parent_run_id="parent",
        )
        children = store.get_child_runs("parent")
        assert len(children) == 2

    def test_run_tree(self, store):
        store.upsert_job("ns", "j1", {"facets": {}})
        store.upsert_run("root", "ns", "j1", "COMPLETE")
        store.upsert_run(
            "child-1",
            "ns",
            "j1",
            "COMPLETE",
            parent_run_id="root",
            root_run_id="root",
        )
        store.upsert_run(
            "grandchild",
            "ns",
            "j1",
            "COMPLETE",
            parent_run_id="child-1",
            root_run_id="root",
        )
        tree = store.get_run_tree("root")
        assert len(tree) == 3
        run_ids = {r["run_id"] for r in tree}
        assert {"root", "child-1", "grandchild"} == run_ids


# ── Purge with extended tables ──


class TestPurgeExtendedTables:
    def test_purge_all_clears_extended(self, store):
        store.upsert_dataset("ns", "ds1")
        store.create_dataset_version("ns", "ds1")
        store.upsert_column_lineage(
            "ns",
            "ds1",
            "col",
            "ns",
            "src",
            "src_col",
        )
        store._upsert_dataset_owners("ns", "ds1", [{"name": "owner", "type": "PERSON"}])

        store.purge_all()
        assert len(store.get_dataset_versions("ns", "ds1")) == 0
        assert len(store.get_column_lineage("ns", "ds1")) == 0
        assert len(store.get_dataset_owners("ns", "ds1")) == 0

    def test_purge_namespace_clears_extended(self, store):
        store.upsert_dataset("ns-a", "ds1")
        store.upsert_dataset("ns-b", "ds2")
        store.create_dataset_version("ns-a", "ds1")
        store.create_dataset_version("ns-b", "ds2")
        store.upsert_column_lineage(
            "ns-a",
            "ds1",
            "col",
            "ns-a",
            "src",
            "src_col",
        )
        store._upsert_dataset_owners(
            "ns-a", "ds1", [{"name": "owner", "type": "PERSON"}]
        )

        store.purge_namespace("ns-a")
        assert len(store.get_dataset_versions("ns-a", "ds1")) == 0
        assert len(store.get_column_lineage("ns-a", "ds1")) == 0
        assert len(store.get_dataset_owners("ns-a", "ds1")) == 0
        assert len(store.get_dataset_versions("ns-b", "ds2")) == 1

    def test_delete_dataset_clears_extended(self, store):
        store.upsert_dataset("ns", "ds1")
        store.create_dataset_version("ns", "ds1")
        store.upsert_column_lineage(
            "ns",
            "ds1",
            "col",
            "ns",
            "src",
            "src_col",
        )
        store._upsert_dataset_owners("ns", "ds1", [{"name": "owner", "type": "PERSON"}])

        store.delete_dataset("ns", "ds1")
        assert len(store.get_dataset_versions("ns", "ds1")) == 0
        assert len(store.get_column_lineage("ns", "ds1")) == 0
        assert len(store.get_dataset_owners("ns", "ds1")) == 0
