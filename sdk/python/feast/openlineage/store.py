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
OpenLineage lineage store for Feast.

Provides CRUD operations for OpenLineage events, jobs, datasets, runs,
and lineage edges using SQLAlchemy Core, consistent with Feast's SQL registry.
"""

import json
import logging
import time
from typing import Any, Dict, List, Optional

from sqlalchemy import create_engine, select
from sqlalchemy.engine import Engine

from feast.openlineage.models import OL_TABLES, ol_metadata

logger = logging.getLogger(__name__)


class OpenLineageStore:
    """
    Storage layer for OpenLineage lineage data.

    Stores events, jobs, datasets, runs, and lineage graph edges
    in PostgreSQL (or SQLite for dev/testing).
    """

    def __init__(
        self, engine: Optional[Engine] = None, connection_string: Optional[str] = None
    ):
        if engine:
            self._engine = engine
        elif connection_string:
            self._engine = create_engine(connection_string)
        else:
            raise ValueError("Either engine or connection_string must be provided")

    def initialize(self):
        ol_metadata.create_all(self._engine)
        logger.info("OpenLineage store tables created/verified")

    @property
    def engine(self) -> Engine:
        return self._engine

    def store_event(self, event_id: str, event_data: Dict[str, Any]):
        now = int(time.time() * 1000)
        job = event_data.get("job", {})
        run = event_data.get("run", {})

        row = {
            "event_id": event_id,
            "event_type": event_data.get("eventType", "UNKNOWN"),
            "event_time": _parse_timestamp(event_data.get("eventTime", "")),
            "producer": event_data.get("producer"),
            "job_namespace": job.get("namespace", ""),
            "job_name": job.get("name", ""),
            "run_id": run.get("runId"),
            "event_json": json.dumps(event_data),
            "created_at": now,
        }

        tbl = OL_TABLES["events"]
        with self._engine.begin() as conn:
            conn.execute(tbl.insert().values(**row))

    def upsert_job(
        self,
        namespace: str,
        name: str,
        job_data: Dict[str, Any],
        producer: Optional[str] = None,
    ):
        now = int(time.time() * 1000)
        facets = job_data.get("facets", {})
        job_type = None
        if "jobType" in facets:
            jt = facets["jobType"]
            job_type = jt.get("processingType", jt.get("integration"))

        description = None
        if "documentation" in facets:
            description = facets["documentation"].get("description")

        tbl = OL_TABLES["jobs"]
        with self._engine.begin() as conn:
            existing = conn.execute(
                select(tbl).where(
                    tbl.c.job_namespace == namespace,
                    tbl.c.job_name == name,
                )
            ).first()

            if existing:
                update_vals = {
                    "job_type": job_type or existing.job_type,
                    "description": description or existing.description,
                    "facets_json": json.dumps(facets)
                    if facets
                    else existing.facets_json,
                    "updated_at": now,
                }
                if producer:
                    update_vals["producer"] = producer
                conn.execute(
                    tbl.update()
                    .where(tbl.c.job_namespace == namespace, tbl.c.job_name == name)
                    .values(**update_vals)
                )
            else:
                conn.execute(
                    tbl.insert().values(
                        job_namespace=namespace,
                        job_name=name,
                        job_type=job_type,
                        producer=producer,
                        description=description,
                        facets_json=json.dumps(facets) if facets else None,
                        updated_at=now,
                    )
                )

    def upsert_dataset(
        self,
        namespace: str,
        name: str,
        facets: Optional[Dict[str, Any]] = None,
        feast_mapping: Optional[Dict[str, str]] = None,
        producer: Optional[str] = None,
    ):
        now = int(time.time() * 1000)
        facets = facets or {}

        schema_json = None
        if "schema" in facets:
            schema_json = json.dumps(facets["schema"])

        description = None
        if "documentation" in facets:
            description = facets["documentation"].get("description")

        source_type = None
        if "dataSource" in facets:
            source_type = facets["dataSource"].get("name")

        feast_obj_type = feast_mapping.get("type") if feast_mapping else None
        feast_obj_name = feast_mapping.get("name") if feast_mapping else None
        feast_project = feast_mapping.get("project") if feast_mapping else None

        tbl = OL_TABLES["datasets"]
        with self._engine.begin() as conn:
            existing = conn.execute(
                select(tbl).where(
                    tbl.c.dataset_namespace == namespace,
                    tbl.c.dataset_name == name,
                )
            ).first()

            values = {
                "source_type": source_type,
                "description": description,
                "schema_json": schema_json,
                "facets_json": json.dumps(facets) if facets else None,
                "updated_at": now,
            }
            if producer:
                values["producer"] = producer
            if feast_obj_type:
                values["feast_object_type"] = feast_obj_type
            if feast_obj_name:
                values["feast_object_name"] = feast_obj_name
            if feast_project:
                values["feast_project"] = feast_project

            if existing:
                conn.execute(
                    tbl.update()
                    .where(
                        tbl.c.dataset_namespace == namespace,
                        tbl.c.dataset_name == name,
                    )
                    .values(**values)
                )
            else:
                values["dataset_namespace"] = namespace
                values["dataset_name"] = name
                conn.execute(tbl.insert().values(**values))

    def upsert_run(
        self,
        run_id: str,
        job_namespace: str,
        job_name: str,
        state: str,
        facets: Optional[Dict] = None,
    ):
        now = int(time.time() * 1000)
        tbl = OL_TABLES["runs"]

        with self._engine.begin() as conn:
            existing = conn.execute(select(tbl).where(tbl.c.run_id == run_id)).first()

            if existing:
                update_vals: Dict[str, Any] = {"state": state, "updated_at": now}
                if state in ("COMPLETE", "FAIL", "ABORT"):
                    update_vals["ended_at"] = now
                if facets:
                    update_vals["facets_json"] = json.dumps(facets)
                conn.execute(
                    tbl.update().where(tbl.c.run_id == run_id).values(**update_vals)
                )
            else:
                conn.execute(
                    tbl.insert().values(
                        run_id=run_id,
                        job_namespace=job_namespace,
                        job_name=job_name,
                        state=state,
                        started_at=now if state == "START" else None,
                        ended_at=now
                        if state in ("COMPLETE", "FAIL", "ABORT")
                        else None,
                        facets_json=json.dumps(facets) if facets else None,
                        updated_at=now,
                    )
                )

            tbl_jobs = OL_TABLES["jobs"]
            conn.execute(
                tbl_jobs.update()
                .where(
                    tbl_jobs.c.job_namespace == job_namespace,
                    tbl_jobs.c.job_name == job_name,
                )
                .values(latest_run_id=run_id, updated_at=now)
            )

    def store_run_io(
        self,
        run_id: str,
        dataset_namespace: str,
        dataset_name: str,
        io_type: str,
        facets: Optional[Dict] = None,
    ):
        tbl = OL_TABLES["run_io"]
        with self._engine.begin() as conn:
            existing = conn.execute(
                select(tbl).where(
                    tbl.c.run_id == run_id,
                    tbl.c.dataset_namespace == dataset_namespace,
                    tbl.c.dataset_name == dataset_name,
                    tbl.c.io_type == io_type,
                )
            ).first()

            if not existing:
                conn.execute(
                    tbl.insert().values(
                        run_id=run_id,
                        dataset_namespace=dataset_namespace,
                        dataset_name=dataset_name,
                        io_type=io_type,
                        facets_json=json.dumps(facets) if facets else None,
                    )
                )

    def upsert_lineage_edge(
        self,
        source_type: str,
        source_namespace: str,
        source_name: str,
        target_type: str,
        target_namespace: str,
        target_name: str,
        edge_type: Optional[str] = None,
    ):
        now = int(time.time() * 1000)
        tbl = OL_TABLES["lineage_edges"]
        with self._engine.begin() as conn:
            existing = conn.execute(
                select(tbl).where(
                    tbl.c.source_type == source_type,
                    tbl.c.source_namespace == source_namespace,
                    tbl.c.source_name == source_name,
                    tbl.c.target_type == target_type,
                    tbl.c.target_namespace == target_namespace,
                    tbl.c.target_name == target_name,
                )
            ).first()

            if existing:
                conn.execute(
                    tbl.update()
                    .where(
                        tbl.c.source_type == source_type,
                        tbl.c.source_namespace == source_namespace,
                        tbl.c.source_name == source_name,
                        tbl.c.target_type == target_type,
                        tbl.c.target_namespace == target_namespace,
                        tbl.c.target_name == target_name,
                    )
                    .values(edge_type=edge_type, updated_at=now)
                )
            else:
                conn.execute(
                    tbl.insert().values(
                        source_type=source_type,
                        source_namespace=source_namespace,
                        source_name=source_name,
                        target_type=target_type,
                        target_namespace=target_namespace,
                        target_name=target_name,
                        edge_type=edge_type,
                        updated_at=now,
                    )
                )

    # ── Query methods ──

    def get_events(
        self,
        namespace: Optional[str] = None,
        job_name: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
        namespaces: Optional[List[str]] = None,
    ) -> List[Dict[str, Any]]:
        tbl = OL_TABLES["events"]
        query = (
            select(tbl).order_by(tbl.c.event_time.desc()).limit(limit).offset(offset)
        )

        if namespace:
            query = query.where(tbl.c.job_namespace == namespace)
        elif namespaces:
            query = query.where(tbl.c.job_namespace.in_(namespaces))
        if job_name:
            query = query.where(tbl.c.job_name == job_name)

        with self._engine.connect() as conn:
            rows = conn.execute(query).fetchall()
            return [dict(row._mapping) for row in rows]

    def get_jobs(self, namespaces: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        tbl = OL_TABLES["jobs"]
        query = select(tbl).order_by(tbl.c.updated_at.desc())
        if namespaces:
            query = query.where(tbl.c.job_namespace.in_(namespaces))

        with self._engine.connect() as conn:
            rows = conn.execute(query).fetchall()
            return [dict(row._mapping) for row in rows]

    def get_datasets(
        self, namespaces: Optional[List[str]] = None
    ) -> List[Dict[str, Any]]:
        tbl = OL_TABLES["datasets"]
        query = select(tbl).order_by(tbl.c.updated_at.desc())
        if namespaces:
            query = query.where(tbl.c.dataset_namespace.in_(namespaces))

        with self._engine.connect() as conn:
            rows = conn.execute(query).fetchall()
            return [dict(row._mapping) for row in rows]

    def get_lineage_graph(
        self,
        node_type: str,
        namespace: str,
        name: str,
        depth: int = 10,
        direction: str = "both",
        allowed_namespaces: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """
        Get the lineage graph for a node, traversing upstream and/or downstream.

        Returns a dict with 'nodes' and 'edges' suitable for UI rendering.
        """
        nodes: Dict[str, Dict[str, Any]] = {}
        edges: List[Dict[str, Any]] = []

        tbl = OL_TABLES["lineage_edges"]

        with self._engine.connect() as conn:
            if direction in ("both", "downstream"):
                self._traverse(
                    conn,
                    tbl,
                    node_type,
                    namespace,
                    name,
                    depth,
                    "downstream",
                    nodes,
                    edges,
                    allowed_namespaces,
                )
            if direction in ("both", "upstream"):
                self._traverse(
                    conn,
                    tbl,
                    node_type,
                    namespace,
                    name,
                    depth,
                    "upstream",
                    nodes,
                    edges,
                    allowed_namespaces,
                )

        root_key = f"{node_type}:{namespace}:{name}"
        if root_key not in nodes:
            nodes[root_key] = {
                "type": node_type,
                "namespace": namespace,
                "name": name,
            }

        return {
            "nodes": list(nodes.values()),
            "edges": edges,
        }

    def _traverse(
        self,
        conn,
        tbl,
        node_type: str,
        namespace: str,
        name: str,
        depth: int,
        direction: str,
        nodes: Dict[str, Dict],
        edges: List[Dict],
        allowed_namespaces: Optional[List[str]],
    ):
        visited = set()
        queue = [(node_type, namespace, name, 0)]

        while queue:
            n_type, n_ns, n_name, d = queue.pop(0)
            key = f"{n_type}:{n_ns}:{n_name}"

            if key in visited or d > depth:
                continue
            visited.add(key)

            if direction == "downstream":
                query = select(tbl).where(
                    tbl.c.source_type == n_type,
                    tbl.c.source_namespace == n_ns,
                    tbl.c.source_name == n_name,
                )
            else:
                query = select(tbl).where(
                    tbl.c.target_type == n_type,
                    tbl.c.target_namespace == n_ns,
                    tbl.c.target_name == n_name,
                )

            rows = conn.execute(query).fetchall()
            for row in rows:
                r = row._mapping
                src_key = (
                    f"{r['source_type']}:{r['source_namespace']}:{r['source_name']}"
                )
                tgt_key = (
                    f"{r['target_type']}:{r['target_namespace']}:{r['target_name']}"
                )

                if allowed_namespaces:
                    if (
                        r["source_namespace"] not in allowed_namespaces
                        or r["target_namespace"] not in allowed_namespaces
                    ):
                        continue

                edge = {
                    "source_type": r["source_type"],
                    "source_namespace": r["source_namespace"],
                    "source_name": r["source_name"],
                    "target_type": r["target_type"],
                    "target_namespace": r["target_namespace"],
                    "target_name": r["target_name"],
                    "edge_type": r.get("edge_type"),
                }
                if edge not in edges:
                    edges.append(edge)

                for node_key, nt, ns, nn in [
                    (
                        src_key,
                        r["source_type"],
                        r["source_namespace"],
                        r["source_name"],
                    ),
                    (
                        tgt_key,
                        r["target_type"],
                        r["target_namespace"],
                        r["target_name"],
                    ),
                ]:
                    if node_key not in nodes:
                        nodes[node_key] = {
                            "type": nt,
                            "namespace": ns,
                            "name": nn,
                        }

                if direction == "downstream":
                    next_key = tgt_key
                    next_type = r["target_type"]
                    next_ns = r["target_namespace"]
                    next_name = r["target_name"]
                else:
                    next_key = src_key
                    next_type = r["source_type"]
                    next_ns = r["source_namespace"]
                    next_name = r["source_name"]

                if next_key not in visited:
                    queue.append((next_type, next_ns, next_name, d + 1))

    def upsert_dataset_symlink(
        self,
        dataset_namespace: str,
        dataset_name: str,
        linked_namespace: str,
        linked_name: str,
        link_type: str = "symlink",
    ):
        """Store a symlink between two dataset identifiers (bidirectional lookup)."""
        now = int(time.time() * 1000)
        tbl = OL_TABLES["dataset_symlinks"]
        with self._engine.begin() as conn:
            existing = conn.execute(
                select(tbl).where(
                    tbl.c.dataset_namespace == dataset_namespace,
                    tbl.c.dataset_name == dataset_name,
                    tbl.c.linked_namespace == linked_namespace,
                    tbl.c.linked_name == linked_name,
                )
            ).first()
            if existing:
                conn.execute(
                    tbl.update()
                    .where(
                        tbl.c.dataset_namespace == dataset_namespace,
                        tbl.c.dataset_name == dataset_name,
                        tbl.c.linked_namespace == linked_namespace,
                        tbl.c.linked_name == linked_name,
                    )
                    .values(link_type=link_type, updated_at=now)
                )
            else:
                conn.execute(
                    tbl.insert().values(
                        dataset_namespace=dataset_namespace,
                        dataset_name=dataset_name,
                        linked_namespace=linked_namespace,
                        linked_name=linked_name,
                        link_type=link_type,
                        updated_at=now,
                    )
                )

    def get_dataset_aliases(self, namespace: str, name: str) -> List[Dict[str, str]]:
        """Get all known aliases for a dataset (both directions)."""
        tbl = OL_TABLES["dataset_symlinks"]
        results = []
        with self._engine.connect() as conn:
            rows = conn.execute(
                select(tbl).where(
                    tbl.c.dataset_namespace == namespace,
                    tbl.c.dataset_name == name,
                )
            ).fetchall()
            for r in rows:
                results.append(
                    {
                        "namespace": r._mapping["linked_namespace"],
                        "name": r._mapping["linked_name"],
                        "link_type": r._mapping["link_type"],
                    }
                )

            rows = conn.execute(
                select(tbl).where(
                    tbl.c.linked_namespace == namespace,
                    tbl.c.linked_name == name,
                )
            ).fetchall()
            for r in rows:
                results.append(
                    {
                        "namespace": r._mapping["dataset_namespace"],
                        "name": r._mapping["dataset_name"],
                        "link_type": r._mapping["link_type"],
                    }
                )
        return results

    def find_datasets_by_uri(self, uri: str) -> List[Dict[str, str]]:
        """Find all datasets whose dataSource facet contains the given URI."""
        tbl = OL_TABLES["datasets"]
        with self._engine.connect() as conn:
            rows = conn.execute(
                select(
                    tbl.c.dataset_namespace, tbl.c.dataset_name, tbl.c.facets_json
                ).where(tbl.c.facets_json.isnot(None))
            ).fetchall()

            matches = []
            for r in rows:
                try:
                    facets = json.loads(r._mapping["facets_json"])
                    ds_uri = facets.get("dataSource", {}).get("uri", "")
                    if ds_uri and ds_uri == uri:
                        matches.append(
                            {
                                "namespace": r._mapping["dataset_namespace"],
                                "name": r._mapping["dataset_name"],
                            }
                        )
                except (json.JSONDecodeError, AttributeError):
                    pass
            return matches

    def get_all_symlinks(self) -> List[Dict[str, Any]]:
        """Get all dataset symlinks."""
        tbl = OL_TABLES["dataset_symlinks"]
        with self._engine.connect() as conn:
            rows = conn.execute(select(tbl)).fetchall()
            return [dict(r._mapping) for r in rows]

    # ── Cleanup methods ──

    def purge_all(self):
        """Delete all data from all OpenLineage tables."""
        table_order = [
            "run_io",
            "runs",
            "lineage_edges",
            "dataset_symlinks",
            "events",
            "datasets",
            "jobs",
        ]
        with self._engine.begin() as conn:
            for tbl_name in table_order:
                conn.execute(OL_TABLES[tbl_name].delete())
        logger.info("Purged all OpenLineage data")

    def purge_namespace(self, namespace: str):
        """Delete all data associated with a specific namespace."""
        with self._engine.begin() as conn:
            tbl_runs = OL_TABLES["runs"]
            run_ids_q = select(tbl_runs.c.run_id).where(
                tbl_runs.c.job_namespace == namespace
            )
            run_ids = [r[0] for r in conn.execute(run_ids_q).fetchall()]
            if run_ids:
                tbl_rio = OL_TABLES["run_io"]
                conn.execute(tbl_rio.delete().where(tbl_rio.c.run_id.in_(run_ids)))
                conn.execute(tbl_runs.delete().where(tbl_runs.c.run_id.in_(run_ids)))

            tbl_ev = OL_TABLES["events"]
            conn.execute(tbl_ev.delete().where(tbl_ev.c.job_namespace == namespace))

            tbl_edges = OL_TABLES["lineage_edges"]
            conn.execute(
                tbl_edges.delete().where(
                    (tbl_edges.c.source_namespace == namespace)
                    | (tbl_edges.c.target_namespace == namespace)
                )
            )

            tbl_sym = OL_TABLES["dataset_symlinks"]
            conn.execute(
                tbl_sym.delete().where(
                    (tbl_sym.c.dataset_namespace == namespace)
                    | (tbl_sym.c.linked_namespace == namespace)
                )
            )

            tbl_ds = OL_TABLES["datasets"]
            conn.execute(tbl_ds.delete().where(tbl_ds.c.dataset_namespace == namespace))

            tbl_jobs = OL_TABLES["jobs"]
            conn.execute(tbl_jobs.delete().where(tbl_jobs.c.job_namespace == namespace))

        logger.info(f"Purged OpenLineage data for namespace: {namespace}")

    # ── Run query methods ──

    def get_runs(
        self,
        job_namespace: Optional[str] = None,
        job_name: Optional[str] = None,
        limit: int = 50,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
        """Get runs, optionally filtered by job."""
        tbl = OL_TABLES["runs"]
        query = (
            select(tbl).order_by(tbl.c.updated_at.desc()).limit(limit).offset(offset)
        )
        if job_namespace:
            query = query.where(tbl.c.job_namespace == job_namespace)
        if job_name:
            query = query.where(tbl.c.job_name == job_name)
        with self._engine.connect() as conn:
            rows = conn.execute(query).fetchall()
            return [dict(row._mapping) for row in rows]

    def get_run_detail(self, run_id: str) -> Optional[Dict[str, Any]]:
        """Get a single run with its I/O datasets."""
        tbl_runs = OL_TABLES["runs"]
        tbl_rio = OL_TABLES["run_io"]
        with self._engine.connect() as conn:
            run_row = conn.execute(
                select(tbl_runs).where(tbl_runs.c.run_id == run_id)
            ).first()
            if not run_row:
                return None
            run = dict(run_row._mapping)

            io_rows = conn.execute(
                select(tbl_rio).where(tbl_rio.c.run_id == run_id)
            ).fetchall()
            run["inputs"] = []
            run["outputs"] = []
            for io_row in io_rows:
                io = dict(io_row._mapping)
                entry = {
                    "namespace": io["dataset_namespace"],
                    "name": io["dataset_name"],
                    "facets": _safe_parse_json(io.get("facets_json")),
                }
                if io["io_type"] == "INPUT":
                    run["inputs"].append(entry)
                else:
                    run["outputs"].append(entry)
            run["facets"] = _safe_parse_json(run.pop("facets_json", None))
            return run

    def get_all_lineage_edges(
        self, namespaces: Optional[List[str]] = None
    ) -> List[Dict[str, Any]]:
        tbl = OL_TABLES["lineage_edges"]
        query = select(tbl)
        if namespaces:
            query = query.where(
                tbl.c.source_namespace.in_(namespaces)
                | tbl.c.target_namespace.in_(namespaces)
            )
        with self._engine.connect() as conn:
            rows = conn.execute(query).fetchall()
            return [dict(row._mapping) for row in rows]


def _safe_parse_json(val: Optional[str]) -> Optional[Any]:
    if not val:
        return None
    try:
        return json.loads(val)
    except (json.JSONDecodeError, TypeError):
        return None


def _parse_timestamp(ts_str: str) -> int:
    if not ts_str:
        return int(time.time() * 1000)
    try:
        from datetime import datetime

        if ts_str.endswith("Z"):
            ts_str = ts_str[:-1] + "+00:00"
        dt = datetime.fromisoformat(ts_str)
        return int(dt.timestamp() * 1000)
    except (ValueError, TypeError):
        return int(time.time() * 1000)
