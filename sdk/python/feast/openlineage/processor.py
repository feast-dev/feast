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
OpenLineage event processor for Feast.

Parses incoming OpenLineage events (RunEvent, DatasetEvent, JobEvent),
extracts metadata, and builds the lineage graph in the store.
"""

import logging
import uuid
from typing import Any, Dict, List, Optional

from feast.openlineage.store import OpenLineageStore

logger = logging.getLogger(__name__)


class OpenLineageProcessor:
    """
    Processes OpenLineage events and stores them in the lineage store.

    Handles RunEvent, DatasetEvent, and JobEvent types, extracting
    jobs, datasets, runs, I/O relationships, and building lineage edges.
    """

    def __init__(
        self,
        store: OpenLineageStore,
        namespace_mapping: Optional[Dict[str, str]] = None,
    ):
        self._store = store
        self._namespace_mapping = namespace_mapping or {}

    def process_event(self, event: Dict[str, Any]) -> str:
        """
        Process a single OpenLineage event.

        Determines the event type and delegates to the appropriate handler.
        Returns the event ID.
        """
        event_id = str(uuid.uuid4())

        if "run" in event and "job" in event:
            self._process_run_event(event_id, event)
        elif "dataset" in event and "job" not in event and "run" not in event:
            self._process_dataset_event(event_id, event)
        elif "job" in event and "run" not in event:
            self._process_job_event(event_id, event)
        else:
            logger.warning(f"Unknown event structure, storing as raw event: {event_id}")
            self._store.store_event(event_id, event)

        return event_id

    def process_batch(self, events: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Process a batch of OpenLineage events."""
        successful = 0
        failed = 0
        event_ids: List[str] = []

        for event in events:
            try:
                event_id = self.process_event(event)
                successful += 1
                event_ids.append(event_id)
            except Exception as e:
                logger.error(f"Failed to process event: {e}")
                failed += 1

        return {
            "received": len(events),
            "successful": successful,
            "failed": failed,
            "event_ids": event_ids,
        }

    def _process_run_event(self, event_id: str, event: Dict[str, Any]):
        job = event.get("job", {})
        run = event.get("run", {})
        event_type = event.get("eventType", "OTHER")
        producer = event.get("producer")

        job_namespace = job.get("namespace", "")
        job_name = job.get("name", "")
        run_id = run.get("runId", "")

        self._store.store_event(event_id, event)

        self._store.upsert_job(job_namespace, job_name, job, producer=producer)

        run_facets = run.get("facets", {})
        self._store.upsert_run(run_id, job_namespace, job_name, event_type, run_facets)

        inputs = event.get("inputs", [])
        outputs = event.get("outputs", [])

        for inp in inputs:
            ds_namespace = inp.get("namespace", job_namespace)
            ds_name = inp.get("name", "")
            ds_facets = inp.get("facets", {})

            feast_mapping = self._resolve_feast_mapping(ds_namespace, ds_name)
            self._store.upsert_dataset(
                ds_namespace, ds_name, ds_facets, feast_mapping, producer=producer
            )
            self._store.store_run_io(run_id, ds_namespace, ds_name, "INPUT", ds_facets)
            self._process_dataset_symlinks(ds_namespace, ds_name, ds_facets)

            self._store.upsert_lineage_edge(
                source_type="dataset",
                source_namespace=ds_namespace,
                source_name=ds_name,
                target_type="job",
                target_namespace=job_namespace,
                target_name=job_name,
                edge_type="input",
            )

        for out in outputs:
            ds_namespace = out.get("namespace", job_namespace)
            ds_name = out.get("name", "")
            ds_facets = out.get("facets", {})

            feast_mapping = self._resolve_feast_mapping(ds_namespace, ds_name)
            self._store.upsert_dataset(
                ds_namespace, ds_name, ds_facets, feast_mapping, producer=producer
            )
            self._store.store_run_io(run_id, ds_namespace, ds_name, "OUTPUT", ds_facets)
            self._process_dataset_symlinks(ds_namespace, ds_name, ds_facets)

            self._store.upsert_lineage_edge(
                source_type="job",
                source_namespace=job_namespace,
                source_name=job_name,
                target_type="dataset",
                target_namespace=ds_namespace,
                target_name=ds_name,
                edge_type="output",
            )

        self._build_dataset_to_dataset_edges(inputs, outputs, job_namespace)

    def _process_dataset_event(self, event_id: str, event: Dict[str, Any]):
        dataset = event.get("dataset", {})
        ds_namespace = dataset.get("namespace", "")
        ds_name = dataset.get("name", "")
        ds_facets = dataset.get("facets", {})
        producer = event.get("producer")

        self._store.store_event(event_id, event)

        feast_mapping = self._resolve_feast_mapping(ds_namespace, ds_name)
        self._store.upsert_dataset(
            ds_namespace, ds_name, ds_facets, feast_mapping, producer=producer
        )
        self._process_dataset_symlinks(ds_namespace, ds_name, ds_facets)

    def _process_job_event(self, event_id: str, event: Dict[str, Any]):
        job = event.get("job", {})
        job_namespace = job.get("namespace", "")
        job_name = job.get("name", "")
        producer = event.get("producer")

        self._store.store_event(event_id, event)

        self._store.upsert_job(job_namespace, job_name, job, producer=producer)

        inputs = event.get("inputs", [])
        outputs = event.get("outputs", [])

        for inp in inputs:
            ds_namespace = inp.get("namespace", job_namespace)
            ds_name = inp.get("name", "")
            ds_facets = inp.get("facets", {})

            feast_mapping = self._resolve_feast_mapping(ds_namespace, ds_name)
            self._store.upsert_dataset(
                ds_namespace, ds_name, ds_facets, feast_mapping, producer=producer
            )
            self._process_dataset_symlinks(ds_namespace, ds_name, ds_facets)

            self._store.upsert_lineage_edge(
                source_type="dataset",
                source_namespace=ds_namespace,
                source_name=ds_name,
                target_type="job",
                target_namespace=job_namespace,
                target_name=job_name,
                edge_type="input",
            )

        for out in outputs:
            ds_namespace = out.get("namespace", job_namespace)
            ds_name = out.get("name", "")
            ds_facets = out.get("facets", {})

            feast_mapping = self._resolve_feast_mapping(ds_namespace, ds_name)
            self._store.upsert_dataset(
                ds_namespace, ds_name, ds_facets, feast_mapping, producer=producer
            )
            self._process_dataset_symlinks(ds_namespace, ds_name, ds_facets)

            self._store.upsert_lineage_edge(
                source_type="job",
                source_namespace=job_namespace,
                source_name=job_name,
                target_type="dataset",
                target_namespace=ds_namespace,
                target_name=ds_name,
                edge_type="output",
            )

    def _build_dataset_to_dataset_edges(
        self,
        inputs: List[Dict],
        outputs: List[Dict],
        job_namespace: str,
    ):
        """
        Build transitive dataset-to-dataset edges through a job.

        For each input dataset and output dataset of the same job,
        create an edge: input_dataset -> output_dataset.
        This enables dataset-level lineage traversal without
        requiring the UI to understand jobs.
        """
        for inp in inputs:
            in_ns = inp.get("namespace", job_namespace)
            in_name = inp.get("name", "")
            for out in outputs:
                out_ns = out.get("namespace", job_namespace)
                out_name = out.get("name", "")
                if in_name and out_name:
                    self._store.upsert_lineage_edge(
                        source_type="dataset",
                        source_namespace=in_ns,
                        source_name=in_name,
                        target_type="dataset",
                        target_namespace=out_ns,
                        target_name=out_name,
                        edge_type="derived",
                    )

    def _process_dataset_symlinks(
        self,
        ds_namespace: str,
        ds_name: str,
        ds_facets: Dict[str, Any],
    ):
        """
        Process SymlinksDatasetFacet and dataSource URI to create
        cross-producer dataset links.

        OpenLineage SymlinksDatasetFacet spec:
        {
            "symlinks": {
                "identifiers": [
                    {"namespace": "...", "name": "...", "type": "TABLE"}
                ]
            }
        }

        Also auto-links datasets sharing the same dataSource URI
        across different namespaces.
        """
        symlinks_facet = ds_facets.get("symlinks", {})
        identifiers = symlinks_facet.get("identifiers", [])
        for ident in identifiers:
            linked_ns = ident.get("namespace", "")
            linked_name = ident.get("name", "")
            link_type = ident.get("type", "symlink")
            if (
                linked_ns
                and linked_name
                and (linked_ns != ds_namespace or linked_name != ds_name)
            ):
                self._store.upsert_dataset_symlink(
                    ds_namespace,
                    ds_name,
                    linked_ns,
                    linked_name,
                    link_type,
                )
                self._store.upsert_dataset(linked_ns, linked_name)
                self._store.upsert_lineage_edge(
                    source_type="dataset",
                    source_namespace=ds_namespace,
                    source_name=ds_name,
                    target_type="dataset",
                    target_namespace=linked_ns,
                    target_name=linked_name,
                    edge_type="symlink",
                )
                self._store.upsert_lineage_edge(
                    source_type="dataset",
                    source_namespace=linked_ns,
                    source_name=linked_name,
                    target_type="dataset",
                    target_namespace=ds_namespace,
                    target_name=ds_name,
                    edge_type="symlink",
                )

        ds_uri = ds_facets.get("dataSource", {}).get("uri", "")
        if ds_uri:
            existing = self._store.find_datasets_by_uri(ds_uri)
            for match in existing:
                m_ns = match["namespace"]
                m_name = match["name"]
                if m_ns != ds_namespace or m_name != ds_name:
                    self._store.upsert_dataset_symlink(
                        ds_namespace,
                        ds_name,
                        m_ns,
                        m_name,
                        "dataSource_uri",
                    )
                    self._store.upsert_lineage_edge(
                        source_type="dataset",
                        source_namespace=ds_namespace,
                        source_name=ds_name,
                        target_type="dataset",
                        target_namespace=m_ns,
                        target_name=m_name,
                        edge_type="symlink",
                    )
                    self._store.upsert_lineage_edge(
                        source_type="dataset",
                        source_namespace=m_ns,
                        source_name=m_name,
                        target_type="dataset",
                        target_namespace=ds_namespace,
                        target_name=ds_name,
                        edge_type="symlink",
                    )

    def _resolve_feast_mapping(
        self, namespace: str, dataset_name: str
    ) -> Optional[Dict[str, str]]:
        """
        Attempt to map an OpenLineage dataset to a Feast registry object.

        Mapping rules:
        1. If the namespace matches a Feast project (directly or via namespace_mapping),
           check if the dataset name matches a known Feast object naming pattern.
        2. Feast apply emits datasets with names like the FeatureView/FeatureService name.
        3. Feast materialize emits datasets named 'online_store_{fv_name}'.
        """
        feast_project = self._namespace_mapping.get(namespace, namespace)

        if dataset_name.startswith("online_store_"):
            fv_name = dataset_name[len("online_store_") :]
            return {
                "type": "featureView",
                "name": fv_name,
                "project": feast_project,
            }

        if dataset_name.startswith("request_source_"):
            return {
                "type": "dataSource",
                "name": dataset_name,
                "project": feast_project,
            }

        return {
            "type": "unknown",
            "name": dataset_name,
            "project": feast_project,
        }
