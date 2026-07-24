import React, { useState } from "react";
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
  EuiCodeBlock,
  EuiButtonEmpty,
} from "@elastic/eui";
import type { OpenLineageEvent } from "../queries/useLoadOpenLineageGraph";
import { useLoadOpenLineageEvents } from "../queries/useLoadOpenLineageGraph";

const eventTypeColor = (eventType: string) => {
  switch (eventType) {
    case "START":
      return "primary";
    case "COMPLETE":
      return "success";
    case "FAIL":
      return "danger";
    case "ABORT":
      return "warning";
    default:
      return "default";
  }
};

const formatTimestamp = (ts: number) => {
  if (!ts) return "-";
  const date = new Date(ts);
  return date.toLocaleString();
};

const LineageEventsList: React.FC = () => {
  const [namespace, setNamespace] = useState("");
  const [jobFilter, setJobFilter] = useState("");
  const [selectedEvent, setSelectedEvent] = useState<OpenLineageEvent | null>(
    null,
  );

  const { data, isLoading, isError } = useLoadOpenLineageEvents(
    namespace || undefined,
    jobFilter || undefined,
    100,
  );

  const columns = [
    {
      field: "event_type",
      name: "Type",
      width: "100px",
      render: (val: string) => (
        <EuiBadge color={eventTypeColor(val)}>{val}</EuiBadge>
      ),
    },
    {
      field: "event_time",
      name: "Event Time",
      width: "200px",
      render: (val: number) => formatTimestamp(val),
    },
    {
      field: "job_namespace",
      name: "Namespace",
      width: "200px",
    },
    {
      field: "job_name",
      name: "Job",
      width: "250px",
    },
    {
      field: "run_id",
      name: "Run ID",
      width: "150px",
      render: (val: string) => (val ? val.substring(0, 8) + "..." : "-"),
    },
    {
      field: "producer",
      name: "Producer",
      width: "150px",
    },
    {
      name: "Actions",
      width: "80px",
      render: (event: OpenLineageEvent) => (
        <EuiButtonEmpty
          size="xs"
          iconType="inspect"
          onClick={() => setSelectedEvent(event)}
        >
          View
        </EuiButtonEmpty>
      ),
    },
  ];

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
          title={<h2>Events Unavailable</h2>}
          body={
            <p>
              Could not load OpenLineage events. The consumer may not be
              enabled.
            </p>
          }
        />
      </EuiPanel>
    );
  }

  const events = data?.events || [];

  return (
    <EuiPanel>
      <EuiTitle size="s">
        <h2>OpenLineage Events ({events.length})</h2>
      </EuiTitle>
      <EuiSpacer size="m" />

      <EuiFlexGroup gutterSize="m">
        <EuiFlexItem grow={false} style={{ width: 250 }}>
          <EuiFieldSearch
            placeholder="Filter by namespace..."
            value={namespace}
            onChange={(e) => setNamespace(e.target.value)}
            aria-label="Filter by namespace"
          />
        </EuiFlexItem>
        <EuiFlexItem grow={false} style={{ width: 250 }}>
          <EuiFieldSearch
            placeholder="Filter by job name..."
            value={jobFilter}
            onChange={(e) => setJobFilter(e.target.value)}
            aria-label="Filter by job name"
          />
        </EuiFlexItem>
      </EuiFlexGroup>

      <EuiSpacer size="m" />

      {events.length === 0 ? (
        <EuiEmptyPrompt
          iconType="branch"
          title={<h2>No Events</h2>}
          body={<p>No OpenLineage events match the current filters.</p>}
        />
      ) : (
        <EuiBasicTable items={events} columns={columns} tableLayout="fixed" />
      )}

      {selectedEvent && (
        <EuiFlyout
          ownFocus
          onClose={() => setSelectedEvent(null)}
          size="m"
          aria-labelledby="eventDetailTitle"
        >
          <EuiFlyoutHeader hasBorder>
            <EuiTitle size="s">
              <h2 id="eventDetailTitle">
                Event: {selectedEvent.event_type} - {selectedEvent.job_name}
              </h2>
            </EuiTitle>
          </EuiFlyoutHeader>
          <EuiFlyoutBody>
            <EuiSpacer size="m" />
            <EuiTitle size="xs">
              <h3>Event Details</h3>
            </EuiTitle>
            <EuiSpacer size="s" />
            <div>
              <strong>Event ID:</strong> {selectedEvent.event_id}
            </div>
            <div>
              <strong>Time:</strong> {formatTimestamp(selectedEvent.event_time)}
            </div>
            <div>
              <strong>Run ID:</strong> {selectedEvent.run_id || "-"}
            </div>
            <div>
              <strong>Producer:</strong> {selectedEvent.producer || "-"}
            </div>
            <EuiSpacer size="l" />
            <EuiTitle size="xs">
              <h3>Raw Event JSON</h3>
            </EuiTitle>
            <EuiSpacer size="s" />
            <EuiCodeBlock language="json" isCopyable paddingSize="m">
              {JSON.stringify(JSON.parse(selectedEvent.event_json), null, 2)}
            </EuiCodeBlock>
          </EuiFlyoutBody>
        </EuiFlyout>
      )}
    </EuiPanel>
  );
};

export default LineageEventsList;
