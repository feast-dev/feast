# Copyright 2026 The Feast Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
OpenLineage consumer for Feast.

Provides FastAPI router endpoints for receiving OpenLineage events
(POST /api/v1/lineage) and querying lineage data, making Feast
act as an OpenLineage consumer alongside its existing producer role.
"""

import json
import logging
from typing import Any, List, Optional

from fastapi import APIRouter, Header, HTTPException, Query, Request
from fastapi.responses import JSONResponse

from feast.openlineage.config import OpenLineageConfig
from feast.openlineage.processor import OpenLineageProcessor
from feast.openlineage.store import OpenLineageStore

logger = logging.getLogger(__name__)


def _safe_parse_json(val: Optional[str]) -> Optional[Any]:
    if not val:
        return None
    try:
        return json.loads(val)
    except (json.JSONDecodeError, TypeError):
        return None


def _verify_api_key(
    expected_key: Optional[str],
    x_api_key: Optional[str] = Header(None, alias="X-API-Key"),
    authorization: Optional[str] = Header(None),
) -> bool:
    if not expected_key:
        return True

    if x_api_key and x_api_key == expected_key:
        return True

    if authorization:
        parts = authorization.split(" ", 1)
        if (
            len(parts) == 2
            and parts[0].lower() == "bearer"
            and parts[1] == expected_key
        ):
            return True

    return False


def get_consumer_router(
    config: OpenLineageConfig,
    store: OpenLineageStore,
    processor: OpenLineageProcessor,
    get_allowed_namespaces=None,
) -> APIRouter:
    """
    Create FastAPI router for the OpenLineage consumer endpoints.

    Args:
        config: OpenLineage configuration with consumer settings
        store: The lineage store instance
        processor: The event processor instance
        get_allowed_namespaces: Optional callable that returns allowed namespaces
            for the current user (for RBAC filtering). If None, all namespaces visible.
    """
    router = APIRouter()

    # ── Producer-facing: receive events ──

    @router.post("/v1/lineage")
    async def receive_lineage_event(
        request: Request,
        x_api_key: Optional[str] = Header(None, alias="X-API-Key"),
        authorization: Optional[str] = Header(None),
    ):
        """
        Receive an OpenLineage event.

        Compatible with the standard OpenLineage API endpoint.
        Accepts RunEvent, DatasetEvent, or JobEvent.
        """
        api_key = getattr(config, "consumer_api_key", None)
        if not _verify_api_key(api_key, x_api_key, authorization):
            raise HTTPException(status_code=401, detail="Invalid API key")

        try:
            body = await request.json()
        except Exception:
            raise HTTPException(status_code=400, detail="Invalid JSON body")

        if isinstance(body, list):
            results = processor.process_batch(body)
            return JSONResponse(
                status_code=200 if results["failed"] == 0 else 207,
                content={
                    "status": "success"
                    if results["failed"] == 0
                    else "partial_success",
                    "summary": {
                        "received": results["received"],
                        "successful": results["successful"],
                        "failed": results["failed"],
                    },
                },
            )
        else:
            try:
                event_id = processor.process_event(body)
                return JSONResponse(status_code=201, content={"event_id": event_id})
            except Exception as e:
                logger.error(f"Failed to process event: {e}")
                raise HTTPException(status_code=500, detail=str(e))

    @router.post("/v1/lineage/batch")
    async def receive_lineage_batch(
        request: Request,
        x_api_key: Optional[str] = Header(None, alias="X-API-Key"),
        authorization: Optional[str] = Header(None),
    ):
        """Receive a batch of OpenLineage events."""
        api_key = getattr(config, "consumer_api_key", None)
        if not _verify_api_key(api_key, x_api_key, authorization):
            raise HTTPException(status_code=401, detail="Invalid API key")

        try:
            body = await request.json()
        except Exception:
            raise HTTPException(status_code=400, detail="Invalid JSON body")

        if not isinstance(body, list):
            raise HTTPException(status_code=400, detail="Expected array of events")

        results = processor.process_batch(body)
        status = 204 if results["failed"] == 0 else 200
        if status == 204:
            return JSONResponse(status_code=204, content=None)

        return JSONResponse(
            status_code=200,
            content={
                "status": "partial_success",
                "summary": {
                    "received": results["received"],
                    "successful": results["successful"],
                    "failed": results["failed"],
                },
            },
        )

    # ── Admin endpoints ──

    @router.delete("/lineage/openlineage/reset")
    async def reset_lineage(
        namespace: Optional[str] = Query(None),
        x_api_key: Optional[str] = Header(None, alias="X-API-Key"),
        authorization: Optional[str] = Header(None),
    ):
        """
        Purge OpenLineage data. Requires API key.

        If ?namespace=X is provided, only that namespace's data is deleted.
        Otherwise, all OpenLineage data is purged.
        """
        api_key = getattr(config, "consumer_api_key", None)
        if not _verify_api_key(api_key, x_api_key, authorization):
            raise HTTPException(status_code=401, detail="Invalid API key")

        if namespace:
            store.purge_namespace(namespace)
            return {"status": "success", "message": f"Purged namespace: {namespace}"}
        else:
            store.purge_all()
            return {"status": "success", "message": "Purged all OpenLineage data"}

    # ── Query endpoints ──

    @router.get("/lineage/openlineage/events")
    def list_events(
        namespace: Optional[str] = Query(None),
        job_name: Optional[str] = Query(None),
        limit: int = Query(100, ge=1, le=1000),
        offset: int = Query(0, ge=0),
    ):
        """List stored OpenLineage events with optional filtering."""
        ns_filter = _get_namespace_filter(get_allowed_namespaces)
        events = store.get_events(
            namespace=namespace,
            job_name=job_name,
            limit=limit,
            offset=offset,
            namespaces=ns_filter if not namespace else None,
        )
        return {"events": events, "total": len(events)}

    @router.get("/lineage/openlineage/jobs")
    def list_jobs():
        """List all jobs from all OpenLineage producers."""
        ns_filter = _get_namespace_filter(get_allowed_namespaces)
        jobs = store.get_jobs(namespaces=ns_filter)
        return {"jobs": jobs}

    @router.get("/lineage/openlineage/datasets")
    def list_datasets():
        """List all datasets from OpenLineage events."""
        ns_filter = _get_namespace_filter(get_allowed_namespaces)
        datasets = store.get_datasets(namespaces=ns_filter)
        return {"datasets": datasets}

    @router.get("/lineage/openlineage/graph/{node_type}/{namespace}/{name}")
    def get_lineage_graph(
        node_type: str,
        namespace: str,
        name: str,
        depth: int = Query(10, ge=1, le=50),
        direction: str = Query("both", pattern="^(both|upstream|downstream)$"),
    ):
        """
        Get the lineage graph for a specific node.

        Traverses upstream and/or downstream from the given node
        up to the specified depth.
        """
        valid_types = ["job", "dataset"]
        if node_type not in valid_types:
            raise HTTPException(
                status_code=400,
                detail=f"node_type must be one of: {valid_types}",
            )

        ns_filter = _get_namespace_filter(get_allowed_namespaces)
        graph = store.get_lineage_graph(
            node_type=node_type,
            namespace=namespace,
            name=name,
            depth=depth,
            direction=direction,
            allowed_namespaces=ns_filter,
        )
        return graph

    @router.get("/lineage/openlineage/graph")
    def get_full_lineage_graph():
        """
        Get all lineage edges (RBAC-filtered by namespace).

        Includes symlink edges that connect datasets across producers
        when they reference the same physical data (via SymlinksDatasetFacet
        or matching dataSource URIs).
        """
        ns_filter = _get_namespace_filter(get_allowed_namespaces)
        edges = store.get_all_lineage_edges(namespaces=ns_filter)
        datasets = store.get_datasets(namespaces=ns_filter)
        jobs = store.get_jobs(namespaces=ns_filter)

        nodes = []
        for ds in datasets:
            facets = _safe_parse_json(ds.get("facets_json"))
            schema = _safe_parse_json(ds.get("schema_json"))
            nodes.append(
                {
                    "type": "dataset",
                    "namespace": ds["dataset_namespace"],
                    "name": ds["dataset_name"],
                    "producer": ds.get("producer"),
                    "feast_object_type": ds.get("feast_object_type"),
                    "feast_object_name": ds.get("feast_object_name"),
                    "feast_project": ds.get("feast_project"),
                    "schema": schema,
                    "description": ds.get("description"),
                    "source_type": ds.get("source_type"),
                    "facets": facets,
                }
            )
        for job in jobs:
            facets = _safe_parse_json(job.get("facets_json"))
            nodes.append(
                {
                    "type": "job",
                    "namespace": job["job_namespace"],
                    "name": job["job_name"],
                    "producer": job.get("producer"),
                    "job_type": job.get("job_type"),
                    "description": job.get("description"),
                    "facets": facets,
                }
            )

        symlinks = store.get_all_symlinks()

        return {"nodes": nodes, "edges": edges, "symlinks": symlinks}

    # ── Run history endpoints ──

    @router.get("/lineage/openlineage/runs")
    def list_runs(
        job_namespace: Optional[str] = Query(None),
        job_name: Optional[str] = Query(None),
        limit: int = Query(50, ge=1, le=500),
        offset: int = Query(0, ge=0),
    ):
        """List runs, optionally filtered by job namespace and name."""
        runs = store.get_runs(
            job_namespace=job_namespace,
            job_name=job_name,
            limit=limit,
            offset=offset,
        )
        return {"runs": runs, "total": len(runs)}

    @router.get("/lineage/openlineage/runs/{run_id}")
    def get_run_detail(run_id: str):
        """Get a single run with its input and output datasets."""
        run = store.get_run_detail(run_id)
        if not run:
            raise HTTPException(status_code=404, detail=f"Run {run_id} not found")
        return run

    def _get_namespace_filter(ns_callable) -> Optional[List[str]]:
        if ns_callable:
            try:
                return ns_callable()
            except Exception:
                return None
        return None

    return router
