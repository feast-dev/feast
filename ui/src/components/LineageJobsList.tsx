import React, { useMemo, useState } from "react";
import {
  EuiPanel,
  EuiTitle,
  EuiSpacer,
  EuiBasicTable,
  EuiLoadingSpinner,
  EuiEmptyPrompt,
  EuiBadge,
  EuiFieldSearch,
  EuiFlexGroup,
  EuiFlexItem,
  EuiFlyout,
  EuiFlyoutHeader,
  EuiFlyoutBody,
  EuiDescriptionList,
  EuiHorizontalRule,
} from "@elastic/eui";
import type { OpenLineageJob } from "../queries/useLoadOpenLineageGraph";
import { useLoadOpenLineageJobs } from "../queries/useLoadOpenLineageGraph";
import { useRunDetail, useRunHistory } from "../queries/useLoadRunHistory";
import type { RunSummary } from "../queries/useLoadRunHistory";

const formatTimestamp = (ts: number | null | undefined) => {
  if (!ts) return "—";
  return new Date(ts).toLocaleString();
};

const formatDuration = (start: number | null, end: number | null): string => {
  if (!start || !end) return "—";
  const ms = end - start;
  if (ms < 1000) return `${ms}ms`;
  const s = Math.floor(ms / 1000);
  if (s < 60) return `${s}s`;
  const m = Math.floor(s / 60);
  return `${m}m ${s % 60}s`;
};

const stateColor = (state: string) => {
  switch ((state || "").toUpperCase()) {
    case "COMPLETE":
      return "success";
    case "FAIL":
      return "danger";
    case "RUNNING":
    case "START":
      return "primary";
    case "ABORT":
      return "warning";
    default:
      return "default";
  }
};

const JobRunHistory: React.FC<{ job: OpenLineageJob }> = ({ job }) => {
  const { data, isLoading } = useRunHistory(job.job_namespace, job.job_name);
  const [selectedRunId, setSelectedRunId] = useState<string | undefined>();
  const { data: runDetail, isLoading: detailLoading } =
    useRunDetail(selectedRunId);

  const runs = data?.runs || [];

  if (isLoading) {
    return <EuiLoadingSpinner size="m" />;
  }

  if (runs.length === 0) {
    return <p>No runs recorded for this job yet.</p>;
  }

  return (
    <>
      <EuiTitle size="xs">
        <h3>Run History ({runs.length})</h3>
      </EuiTitle>
      <EuiSpacer size="s" />
      <EuiBasicTable
        items={runs}
        rowProps={(run: RunSummary) => ({
          onClick: () => setSelectedRunId(run.run_id),
          style: {
            cursor: "pointer",
            background:
              selectedRunId === run.run_id
                ? "rgba(0, 119, 204, 0.08)"
                : undefined,
          },
        })}
        columns={[
          {
            field: "run_id",
            name: "Run",
            truncateText: true,
            render: (id: string) => (
              <span style={{ fontFamily: "monospace", fontSize: 12 }}>
                {id.slice(0, 8)}…
              </span>
            ),
          },
          {
            field: "state",
            name: "Status",
            width: "120px",
            render: (state: string) => (
              <EuiBadge color={stateColor(state)}>{state}</EuiBadge>
            ),
          },
          {
            field: "started_at",
            name: "Started",
            render: (ts: number | null) => formatTimestamp(ts),
          },
          {
            name: "Duration",
            render: (run: RunSummary) =>
              formatDuration(run.started_at, run.ended_at),
          },
        ]}
      />
      {selectedRunId && (
        <>
          <EuiSpacer />
          <EuiTitle size="xs">
            <h3>Run Detail</h3>
          </EuiTitle>
          <EuiSpacer size="s" />
          {detailLoading && <EuiLoadingSpinner size="s" />}
          {!detailLoading && runDetail && (
            <>
              <EuiDescriptionList
                type="column"
                listItems={[
                  { title: "Run ID", description: runDetail.run_id },
                  {
                    title: "State",
                    description: (
                      <EuiBadge color={stateColor(runDetail.state)}>
                        {runDetail.state}
                      </EuiBadge>
                    ),
                  },
                  {
                    title: "Started",
                    description: formatTimestamp(runDetail.started_at),
                  },
                  {
                    title: "Ended",
                    description: formatTimestamp(runDetail.ended_at),
                  },
                ]}
              />
              <EuiSpacer size="m" />
              <div style={{ fontSize: 13 }}>
                <strong>Inputs</strong>
                {(runDetail.inputs || []).length === 0 ? (
                  <p style={{ color: "#888" }}>None</p>
                ) : (
                  <ul>
                    {runDetail.inputs.map((io) => (
                      <li key={`in-${io.namespace}:${io.name}`}>
                        <code>{io.name}</code>
                      </li>
                    ))}
                  </ul>
                )}
                <strong>Outputs</strong>
                {(runDetail.outputs || []).length === 0 ? (
                  <p style={{ color: "#888" }}>None</p>
                ) : (
                  <ul>
                    {runDetail.outputs.map((io) => (
                      <li key={`out-${io.namespace}:${io.name}`}>
                        <code>{io.name}</code>
                      </li>
                    ))}
                  </ul>
                )}
              </div>
            </>
          )}
        </>
      )}
    </>
  );
};

const LineageJobsList: React.FC = () => {
  const [search, setSearch] = useState("");
  const [selectedJob, setSelectedJob] = useState<OpenLineageJob | null>(null);
  const { data, isLoading, isError } = useLoadOpenLineageJobs();

  const jobs = useMemo(() => {
    const all = (data?.jobs || []).filter(
      // Spark OL often emits a bootstrap START with job name "unknown"
      // before spark.app.name is resolved — hide those stub jobs.
      (j) => (j.job_name || "").toLowerCase() !== "unknown",
    );
    const q = search.trim().toLowerCase();
    if (!q) return all;
    return all.filter(
      (j) =>
        j.job_name.toLowerCase().includes(q) ||
        j.job_namespace.toLowerCase().includes(q) ||
        (j.producer || "").toLowerCase().includes(q) ||
        (j.job_type || "").toLowerCase().includes(q),
    );
  }, [data, search]);

  if (isLoading) {
    return (
      <EuiPanel>
        <div style={{ display: "flex", justifyContent: "center", padding: 50 }}>
          <EuiLoadingSpinner size="xl" />
        </div>
      </EuiPanel>
    );
  }

  if (isError) {
    return (
      <EuiPanel>
        <EuiEmptyPrompt
          iconType="alert"
          title={<h2>Failed to load jobs</h2>}
          body={<p>Could not fetch OpenLineage jobs from the consumer.</p>}
        />
      </EuiPanel>
    );
  }

  if (!data?.jobs?.length) {
    return (
      <EuiPanel>
        <EuiEmptyPrompt
          iconType="compute"
          title={<h2>No Jobs Yet</h2>}
          body={
            <p>
              Jobs appear here when OpenLineage producers (Feast materialize,
              Spark, Airflow, etc.) emit events to this consumer.
            </p>
          }
        />
      </EuiPanel>
    );
  }

  return (
    <>
      <EuiPanel>
        <EuiFlexGroup justifyContent="spaceBetween" alignItems="center">
          <EuiFlexItem grow={false}>
            <EuiTitle size="s">
              <h2>Jobs ({jobs.length})</h2>
            </EuiTitle>
            <p style={{ margin: "4px 0 0", fontSize: 13, color: "#69707D" }}>
              Catalog of all OpenLineage jobs. Runtime transforms (materialize,
              Spark) also appear on the Lineage graph; apply/definition jobs
              (e.g. feature_service_*) are listed here only.
            </p>
          </EuiFlexItem>
          <EuiFlexItem grow={false} style={{ minWidth: 280 }}>
            <EuiFieldSearch
              placeholder="Search jobs…"
              value={search}
              onChange={(e) => setSearch(e.target.value)}
              isClearable
              aria-label="Search jobs"
            />
          </EuiFlexItem>
        </EuiFlexGroup>
        <EuiSpacer size="m" />
        <EuiBasicTable
          items={jobs}
          rowProps={(job: OpenLineageJob) => ({
            onClick: () => setSelectedJob(job),
            style: { cursor: "pointer" },
          })}
          columns={[
            {
              field: "job_name",
              name: "Job",
              truncateText: true,
            },
            {
              field: "job_namespace",
              name: "Namespace",
              truncateText: true,
              width: "220px",
            },
            {
              field: "job_type",
              name: "Type",
              width: "120px",
              render: (val?: string | null) => val || "—",
            },
            {
              field: "producer",
              name: "Producer",
              width: "140px",
              render: (val?: string | null) =>
                val ? <EuiBadge>{val}</EuiBadge> : "—",
            },
            {
              field: "updated_at",
              name: "Last Seen",
              width: "200px",
              render: (ts: number) => formatTimestamp(ts),
            },
          ]}
        />
      </EuiPanel>

      {selectedJob && (
        <EuiFlyout
          size="m"
          onClose={() => setSelectedJob(null)}
          aria-labelledby="job-flyout-title"
        >
          <EuiFlyoutHeader hasBorder>
            <EuiTitle size="m">
              <h2 id="job-flyout-title">{selectedJob.job_name}</h2>
            </EuiTitle>
          </EuiFlyoutHeader>
          <EuiFlyoutBody>
            <EuiDescriptionList
              type="column"
              listItems={[
                { title: "Namespace", description: selectedJob.job_namespace },
                {
                  title: "Type",
                  description: selectedJob.job_type || "—",
                },
                {
                  title: "Producer",
                  description: selectedJob.producer || "—",
                },
                {
                  title: "Last Seen",
                  description: formatTimestamp(selectedJob.updated_at),
                },
                {
                  title: "Latest Run",
                  description: selectedJob.latest_run_id || "—",
                },
              ]}
            />
            {selectedJob.description && (
              <>
                <EuiSpacer />
                <p>{selectedJob.description}</p>
              </>
            )}
            <EuiHorizontalRule />
            <JobRunHistory job={selectedJob} />
          </EuiFlyoutBody>
        </EuiFlyout>
      )}
    </>
  );
};

export default LineageJobsList;
