"""Comprehensive tests for the OpenLineage consumer API endpoints."""

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.pool import StaticPool

from feast.openlineage.config import OpenLineageConfig, OpenLineageConsumerConfig
from feast.openlineage.consumer import get_consumer_router
from feast.openlineage.processor import OpenLineageProcessor
from feast.openlineage.store import OpenLineageStore


def _make_run_event(
    job_ns="test-ns",
    job_name="test-job",
    run_id="run-001",
    event_type="COMPLETE",
    producer="https://github.com/test/producer",
    inputs=None,
    outputs=None,
):
    event = {
        "eventType": event_type,
        "eventTime": "2026-01-15T10:00:00Z",
        "producer": producer,
        "job": {"namespace": job_ns, "name": job_name, "facets": {}},
        "run": {"runId": run_id, "facets": {}},
        "inputs": inputs or [],
        "outputs": outputs or [],
    }
    return event


@pytest.fixture
def app_no_key():
    """App with consumer that has no API key requirement."""
    engine = create_engine(
        "sqlite://",
        echo=False,
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    store = OpenLineageStore(engine=engine)
    store.initialize()
    processor = OpenLineageProcessor(store=store)
    config = OpenLineageConfig(
        enabled=True,
        consumer=OpenLineageConsumerConfig(enabled=True, api_key=None),
    )
    app = FastAPI()
    router = get_consumer_router(config, store, processor)
    app.include_router(router, prefix="/api/v1")
    return app, store


@pytest.fixture
def app_with_key():
    """App with consumer that requires API key."""
    engine = create_engine(
        "sqlite://",
        echo=False,
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    store = OpenLineageStore(engine=engine)
    store.initialize()
    processor = OpenLineageProcessor(store=store)
    config = OpenLineageConfig(
        enabled=True,
        consumer=OpenLineageConsumerConfig(enabled=True, api_key="secret-key"),
    )
    app = FastAPI()
    router = get_consumer_router(config, store, processor)
    app.include_router(router, prefix="/api/v1")
    return app, store


@pytest.fixture
def app_with_rbac():
    """App with namespace RBAC filtering."""
    engine = create_engine(
        "sqlite://",
        echo=False,
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    store = OpenLineageStore(engine=engine)
    store.initialize()
    processor = OpenLineageProcessor(store=store)
    config = OpenLineageConfig(
        enabled=True,
        consumer=OpenLineageConsumerConfig(enabled=True),
    )
    app = FastAPI()
    router = get_consumer_router(
        config,
        store,
        processor,
        get_allowed_namespaces=lambda: ["allowed-ns"],
    )
    app.include_router(router, prefix="/api/v1")
    return app, store


# ── Event Ingestion ──


class TestEventIngestion:
    def test_single_event_returns_201(self, app_no_key):
        app, _ = app_no_key
        client = TestClient(app)
        resp = client.post("/api/v1/v1/lineage", json=_make_run_event())
        assert resp.status_code == 201
        assert "event_id" in resp.json()

    def test_array_of_events_returns_200(self, app_no_key):
        app, _ = app_no_key
        client = TestClient(app)
        events = [_make_run_event(run_id=f"r{i}") for i in range(3)]
        resp = client.post("/api/v1/v1/lineage", json=events)
        assert resp.status_code == 200
        assert resp.json()["summary"]["received"] == 3
        assert resp.json()["summary"]["successful"] == 3

    def test_batch_endpoint(self, app_no_key):
        app, _ = app_no_key
        client = TestClient(app)
        events = [_make_run_event(run_id=f"r{i}") for i in range(2)]
        resp = client.post("/api/v1/v1/lineage/batch", json=events)
        assert resp.status_code == 204

    def test_batch_rejects_non_array(self, app_no_key):
        app, _ = app_no_key
        client = TestClient(app)
        resp = client.post("/api/v1/v1/lineage/batch", json={"not": "array"})
        assert resp.status_code == 400

    def test_api_key_required(self, app_with_key):
        app, _ = app_with_key
        client = TestClient(app)
        resp = client.post("/api/v1/v1/lineage", json=_make_run_event())
        assert resp.status_code == 401

    def test_api_key_via_header(self, app_with_key):
        app, _ = app_with_key
        client = TestClient(app)
        resp = client.post(
            "/api/v1/v1/lineage",
            json=_make_run_event(),
            headers={"X-API-Key": "secret-key"},
        )
        assert resp.status_code == 201

    def test_api_key_via_bearer(self, app_with_key):
        app, _ = app_with_key
        client = TestClient(app)
        resp = client.post(
            "/api/v1/v1/lineage",
            json=_make_run_event(),
            headers={"Authorization": "Bearer secret-key"},
        )
        assert resp.status_code == 201

    def test_wrong_api_key_rejected(self, app_with_key):
        app, _ = app_with_key
        client = TestClient(app)
        resp = client.post(
            "/api/v1/v1/lineage",
            json=_make_run_event(),
            headers={"X-API-Key": "wrong-key"},
        )
        assert resp.status_code == 401


# ── Query Endpoints ──


class TestQueryEndpoints:
    def _ingest(self, client, events):
        for evt in events:
            client.post("/api/v1/v1/lineage", json=evt)

    def test_get_events(self, app_no_key):
        app, _ = app_no_key
        client = TestClient(app)
        self._ingest(client, [_make_run_event()])
        resp = client.get("/api/v1/lineage/openlineage/events")
        assert resp.status_code == 200
        assert resp.json()["total"] >= 1

    def test_get_events_filtered(self, app_no_key):
        app, _ = app_no_key
        client = TestClient(app)
        self._ingest(
            client,
            [
                _make_run_event(job_ns="ns-a", job_name="j1", run_id="r1"),
                _make_run_event(job_ns="ns-b", job_name="j2", run_id="r2"),
            ],
        )
        resp = client.get("/api/v1/lineage/openlineage/events?namespace=ns-a")
        assert resp.status_code == 200
        for evt in resp.json()["events"]:
            assert evt["job_namespace"] == "ns-a"

    def test_get_jobs(self, app_no_key):
        app, _ = app_no_key
        client = TestClient(app)
        self._ingest(client, [_make_run_event()])
        resp = client.get("/api/v1/lineage/openlineage/jobs")
        assert resp.status_code == 200
        assert len(resp.json()["jobs"]) >= 1

    def test_get_datasets(self, app_no_key):
        app, _ = app_no_key
        client = TestClient(app)
        self._ingest(
            client,
            [
                _make_run_event(
                    inputs=[
                        {"namespace": "s3://bucket", "name": "input-ds", "facets": {}}
                    ],
                    outputs=[
                        {"namespace": "s3://bucket", "name": "output-ds", "facets": {}}
                    ],
                ),
            ],
        )
        resp = client.get("/api/v1/lineage/openlineage/datasets")
        assert resp.status_code == 200
        assert len(resp.json()["datasets"]) >= 1

    def test_get_full_graph(self, app_no_key):
        app, _ = app_no_key
        client = TestClient(app)
        self._ingest(
            client,
            [
                _make_run_event(
                    inputs=[
                        {"namespace": "s3://bucket", "name": "input-ds", "facets": {}}
                    ],
                    outputs=[
                        {"namespace": "s3://bucket", "name": "output-ds", "facets": {}}
                    ],
                ),
            ],
        )
        resp = client.get("/api/v1/lineage/openlineage/graph")
        assert resp.status_code == 200
        data = resp.json()
        assert "nodes" in data
        assert "edges" in data

    def test_get_graph_by_node(self, app_no_key):
        app, _ = app_no_key
        client = TestClient(app)
        self._ingest(
            client,
            [
                _make_run_event(
                    inputs=[
                        {"namespace": "s3://bucket", "name": "input-ds", "facets": {}}
                    ],
                ),
            ],
        )
        resp = client.get("/api/v1/lineage/openlineage/graph/job/test-ns/test-job")
        assert resp.status_code == 200

    def test_get_graph_invalid_node_type(self, app_no_key):
        app, _ = app_no_key
        client = TestClient(app)
        resp = client.get("/api/v1/lineage/openlineage/graph/invalid/ns/name")
        assert resp.status_code == 400


# ── Reset Endpoint ──


class TestResetEndpoint:
    def test_reset_requires_api_key(self, app_with_key):
        app, _ = app_with_key
        client = TestClient(app)
        resp = client.delete("/api/v1/lineage/openlineage/reset")
        assert resp.status_code == 401

    def test_reset_full_purge(self, app_with_key):
        app, store = app_with_key
        client = TestClient(app)
        client.post(
            "/api/v1/v1/lineage",
            json=_make_run_event(),
            headers={"X-API-Key": "secret-key"},
        )
        resp = client.delete(
            "/api/v1/lineage/openlineage/reset",
            headers={"X-API-Key": "secret-key"},
        )
        assert resp.status_code == 200
        assert resp.json()["status"] == "success"
        assert len(store.get_events()) == 0

    def test_reset_by_namespace(self, app_with_key):
        app, store = app_with_key
        client = TestClient(app)
        client.post(
            "/api/v1/v1/lineage",
            json=_make_run_event(job_ns="ns-keep", run_id="r1"),
            headers={"X-API-Key": "secret-key"},
        )
        client.post(
            "/api/v1/v1/lineage",
            json=_make_run_event(job_ns="ns-delete", run_id="r2"),
            headers={"X-API-Key": "secret-key"},
        )
        resp = client.delete(
            "/api/v1/lineage/openlineage/reset?namespace=ns-delete",
            headers={"X-API-Key": "secret-key"},
        )
        assert resp.status_code == 200
        assert len(store.get_events(namespace="ns-delete")) == 0
        assert len(store.get_events(namespace="ns-keep")) >= 1

    def test_reset_no_key_when_not_required(self, app_no_key):
        app, _ = app_no_key
        client = TestClient(app)
        resp = client.delete("/api/v1/lineage/openlineage/reset")
        assert resp.status_code == 200


# ── Runs Endpoints ──


class TestRunsEndpoints:
    def _seed(self, client):
        client.post(
            "/api/v1/v1/lineage",
            json=_make_run_event(
                job_ns="ns-a",
                job_name="j1",
                run_id="r1",
                inputs=[{"namespace": "s3://b", "name": "in1", "facets": {}}],
                outputs=[{"namespace": "s3://b", "name": "out1", "facets": {}}],
            ),
        )
        client.post(
            "/api/v1/v1/lineage",
            json=_make_run_event(
                job_ns="ns-b",
                job_name="j2",
                run_id="r2",
            ),
        )

    def test_list_runs_unfiltered(self, app_no_key):
        app, _ = app_no_key
        client = TestClient(app)
        self._seed(client)
        resp = client.get("/api/v1/lineage/openlineage/runs")
        assert resp.status_code == 200
        assert resp.json()["total"] >= 2

    def test_list_runs_filtered_by_job(self, app_no_key):
        app, _ = app_no_key
        client = TestClient(app)
        self._seed(client)
        resp = client.get(
            "/api/v1/lineage/openlineage/runs?job_namespace=ns-a&job_name=j1"
        )
        assert resp.status_code == 200
        runs = resp.json()["runs"]
        assert all(r["job_namespace"] == "ns-a" for r in runs)

    def test_run_detail_found(self, app_no_key):
        app, _ = app_no_key
        client = TestClient(app)
        self._seed(client)
        resp = client.get("/api/v1/lineage/openlineage/runs/r1")
        assert resp.status_code == 200
        detail = resp.json()
        assert detail["run_id"] == "r1"
        assert len(detail["inputs"]) >= 1
        assert len(detail["outputs"]) >= 1

    def test_run_detail_not_found(self, app_no_key):
        app, _ = app_no_key
        client = TestClient(app)
        resp = client.get("/api/v1/lineage/openlineage/runs/nonexistent")
        assert resp.status_code == 404


# ── Namespace RBAC ──


class TestNamespaceRBAC:
    def test_events_filtered_by_allowed_namespaces(self, app_with_rbac):
        app, store = app_with_rbac
        client = TestClient(app)
        store.store_event(
            "e1",
            {
                "eventType": "COMPLETE",
                "eventTime": "2026-01-01T00:00:00Z",
                "producer": "test",
                "job": {"namespace": "allowed-ns", "name": "j1"},
                "run": {"runId": "r1"},
            },
        )
        store.store_event(
            "e2",
            {
                "eventType": "COMPLETE",
                "eventTime": "2026-01-01T00:00:00Z",
                "producer": "test",
                "job": {"namespace": "blocked-ns", "name": "j2"},
                "run": {"runId": "r2"},
            },
        )
        resp = client.get("/api/v1/lineage/openlineage/events")
        assert resp.status_code == 200
        events = resp.json()["events"]
        namespaces = {e["job_namespace"] for e in events}
        assert "blocked-ns" not in namespaces

    def test_jobs_filtered_by_allowed_namespaces(self, app_with_rbac):
        app, store = app_with_rbac
        client = TestClient(app)
        store.upsert_job("allowed-ns", "j1", {"facets": {}})
        store.upsert_job("blocked-ns", "j2", {"facets": {}})
        resp = client.get("/api/v1/lineage/openlineage/jobs")
        assert resp.status_code == 200
        jobs = resp.json()["jobs"]
        namespaces = {j["job_namespace"] for j in jobs}
        assert "allowed-ns" in namespaces
        assert "blocked-ns" not in namespaces
