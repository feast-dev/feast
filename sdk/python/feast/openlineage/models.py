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
SQLAlchemy table definitions for the OpenLineage consumer lineage store.

Uses SQLAlchemy Core (Table + Column) pattern consistent with Feast's
existing SQL registry in feast/infra/registry/sql.py.
"""

from sqlalchemy import (
    BigInteger,
    Column,
    Index,
    MetaData,
    String,
    Text,
    UniqueConstraint,
)

ol_metadata = MetaData()

openlineage_events = (
    "openlineage_events",
    ol_metadata,
    Column("event_id", String(255), primary_key=True),
    Column("event_type", String(50), nullable=False),
    Column("event_time", BigInteger, nullable=False),
    Column("producer", String(512), nullable=True),
    Column("job_namespace", String(512), nullable=False),
    Column("job_name", String(512), nullable=False),
    Column("run_id", String(255), nullable=True),
    Column("event_json", Text, nullable=False),
    Column("created_at", BigInteger, nullable=False),
)

openlineage_jobs = (
    "openlineage_jobs",
    ol_metadata,
    Column("job_namespace", String(512), primary_key=True),
    Column("job_name", String(512), primary_key=True),
    Column("job_type", String(100), nullable=True),
    Column("producer", String(512), nullable=True),
    Column("description", Text, nullable=True),
    Column("facets_json", Text, nullable=True),
    Column("latest_run_id", String(255), nullable=True),
    Column("updated_at", BigInteger, nullable=False),
)

openlineage_datasets = (
    "openlineage_datasets",
    ol_metadata,
    Column("dataset_namespace", String(512), primary_key=True),
    Column("dataset_name", String(512), primary_key=True),
    Column("source_type", String(100), nullable=True),
    Column("producer", String(512), nullable=True),
    Column("description", Text, nullable=True),
    Column("schema_json", Text, nullable=True),
    Column("facets_json", Text, nullable=True),
    Column("feast_object_type", String(100), nullable=True),
    Column("feast_object_name", String(255), nullable=True),
    Column("feast_project", String(255), nullable=True),
    Column("updated_at", BigInteger, nullable=False),
)

openlineage_runs = (
    "openlineage_runs",
    ol_metadata,
    Column("run_id", String(255), primary_key=True),
    Column("job_namespace", String(512), nullable=False),
    Column("job_name", String(512), nullable=False),
    Column("state", String(50), nullable=False),
    Column("started_at", BigInteger, nullable=True),
    Column("ended_at", BigInteger, nullable=True),
    Column("facets_json", Text, nullable=True),
    Column("updated_at", BigInteger, nullable=False),
)

openlineage_run_io = (
    "openlineage_run_io",
    ol_metadata,
    Column("run_id", String(255), nullable=False),
    Column("dataset_namespace", String(512), nullable=False),
    Column("dataset_name", String(512), nullable=False),
    Column("io_type", String(50), nullable=False),
    Column("facets_json", Text, nullable=True),
    UniqueConstraint(
        "run_id",
        "dataset_namespace",
        "dataset_name",
        "io_type",
        name="uq_run_io",
    ),
)

openlineage_lineage_edges = (
    "openlineage_lineage_edges",
    ol_metadata,
    Column("source_type", String(50), nullable=False),
    Column("source_namespace", String(512), nullable=False),
    Column("source_name", String(512), nullable=False),
    Column("target_type", String(50), nullable=False),
    Column("target_namespace", String(512), nullable=False),
    Column("target_name", String(512), nullable=False),
    Column("edge_type", String(50), nullable=True),
    Column("updated_at", BigInteger, nullable=False),
    UniqueConstraint(
        "source_type",
        "source_namespace",
        "source_name",
        "target_type",
        "target_namespace",
        "target_name",
        name="uq_lineage_edge",
    ),
)

openlineage_dataset_symlinks = (
    "openlineage_dataset_symlinks",
    ol_metadata,
    Column("dataset_namespace", String(512), nullable=False),
    Column("dataset_name", String(512), nullable=False),
    Column("linked_namespace", String(512), nullable=False),
    Column("linked_name", String(512), nullable=False),
    Column("link_type", String(50), nullable=False),
    Column("updated_at", BigInteger, nullable=False),
    UniqueConstraint(
        "dataset_namespace",
        "dataset_name",
        "linked_namespace",
        "linked_name",
        name="uq_dataset_symlink",
    ),
)


def _build_tables():
    """Build Table objects from the tuple definitions above."""
    from sqlalchemy import Table

    tables = {}
    for name, tbl_def in [
        ("events", openlineage_events),
        ("jobs", openlineage_jobs),
        ("datasets", openlineage_datasets),
        ("runs", openlineage_runs),
        ("run_io", openlineage_run_io),
        ("lineage_edges", openlineage_lineage_edges),
        ("dataset_symlinks", openlineage_dataset_symlinks),
    ]:
        tbl_name = tbl_def[0]
        meta = tbl_def[1]
        columns = tbl_def[2:]
        tables[name] = Table(tbl_name, meta, *columns)
    return tables


OL_TABLES = _build_tables()

idx_events_time = Index(
    "idx_ol_events_time",
    OL_TABLES["events"].c.event_time,
)
idx_events_job = Index(
    "idx_ol_events_job",
    OL_TABLES["events"].c.job_namespace,
    OL_TABLES["events"].c.job_name,
)
idx_runs_job = Index(
    "idx_ol_runs_job",
    OL_TABLES["runs"].c.job_namespace,
    OL_TABLES["runs"].c.job_name,
)
idx_run_io_run = Index(
    "idx_ol_run_io_run",
    OL_TABLES["run_io"].c.run_id,
)
idx_run_io_dataset = Index(
    "idx_ol_run_io_dataset",
    OL_TABLES["run_io"].c.dataset_namespace,
    OL_TABLES["run_io"].c.dataset_name,
)
idx_lineage_source = Index(
    "idx_ol_lineage_source",
    OL_TABLES["lineage_edges"].c.source_namespace,
    OL_TABLES["lineage_edges"].c.source_name,
)
idx_lineage_target = Index(
    "idx_ol_lineage_target",
    OL_TABLES["lineage_edges"].c.target_namespace,
    OL_TABLES["lineage_edges"].c.target_name,
)
idx_datasets_feast = Index(
    "idx_ol_datasets_feast",
    OL_TABLES["datasets"].c.feast_project,
    OL_TABLES["datasets"].c.feast_object_type,
)
idx_symlinks_dataset = Index(
    "idx_ol_symlinks_dataset",
    OL_TABLES["dataset_symlinks"].c.dataset_namespace,
    OL_TABLES["dataset_symlinks"].c.dataset_name,
)
idx_symlinks_linked = Index(
    "idx_ol_symlinks_linked",
    OL_TABLES["dataset_symlinks"].c.linked_namespace,
    OL_TABLES["dataset_symlinks"].c.linked_name,
)
