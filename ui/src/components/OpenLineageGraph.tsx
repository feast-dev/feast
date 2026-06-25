import React, { useEffect, useMemo, useState } from "react";
import {
  ReactFlow,
  Node,
  Edge,
  Controls,
  Background,
  useNodesState,
  useEdgesState,
  ConnectionLineType,
  MarkerType,
  Handle,
  Position,
} from "reactflow";

import "reactflow/dist/style.css";
import dagre from "dagre";
import {
  EuiPanel,
  EuiTitle,
  EuiSpacer,
  EuiLoadingSpinner,
  EuiToolTip,
  EuiEmptyPrompt,
  EuiFlexGroup,
  EuiFlexItem,
  EuiFormRow,
  EuiSelect,
  EuiBadge,
} from "@elastic/eui";
import { useTheme } from "../contexts/ThemeContext";
import type {
  OpenLineageGraphData,
  OpenLineageNode,
} from "../queries/useLoadOpenLineageGraph";
import { useRunHistory, useRunDetail } from "../queries/useLoadRunHistory";
import type { RunSummary } from "../queries/useLoadRunHistory";

const nodeWidth = 280;
const nodeHeight = 65;

// ── Producer-based colors (generated dynamically) ──

const normalizeProducer = (producer?: string | null): string => {
  if (!producer) return "unknown";
  const p = producer.toLowerCase().trim();

  // Extract the last meaningful path segment from URLs like
  // "https://github.com/OpenLineage/OpenLineage/tree/1.0.0/integration/airflow"
  try {
    const url = new URL(p);
    const segments = url.pathname.split("/").filter(Boolean);
    if (segments.length > 0) {
      return segments[segments.length - 1].replace(/-/g, "_");
    }
  } catch {
    // not a URL
  }

  // For plain names like "feast" or "my-custom-producer", just clean up
  return p.replace(/-/g, "_").replace(/\s+/g, "_");
};

const hashString = (str: string): number => {
  let hash = 0;
  for (let i = 0; i < str.length; i++) {
    hash = str.charCodeAt(i) + ((hash << 5) - hash);
  }
  return Math.abs(hash);
};

const generateProducerColor = (
  producer: string,
): { main: string; light: string } => {
  if (producer === "unknown") return { main: "#888888", light: "#f0f0f0" };
  const hue = hashString(producer) % 360;
  const main = `hsl(${hue}, 70%, 45%)`;
  const light = `hsl(${hue}, 60%, 94%)`;
  return { main, light };
};

const producerColorCache: Record<string, { main: string; light: string }> = {};

const getProducerColors = (producer?: string | null) => {
  const key = normalizeProducer(producer);
  if (!producerColorCache[key]) {
    producerColorCache[key] = generateProducerColor(key);
  }
  return producerColorCache[key];
};

const getNodeIcon = (type: string) => {
  return type === "job" ? "\u2699" : "\u2B21";
};

// ── Custom Node ──

interface LineageNodeData {
  label: string;
  type: string;
  producer?: string;
  namespace?: string;
  nodeRef?: OpenLineageNode;
  onNodeClick?: (node: OpenLineageNode) => void;
}

const LineageCustomNode = ({ data }: { data: LineageNodeData }) => {
  const [isHovered, setIsHovered] = useState(false);
  const colors = getProducerColors(data.producer);
  const icon = getNodeIcon(data.type);
  const producerLabel = normalizeProducer(data.producer);

  const handleClick = () => {
    if (data.onNodeClick && data.nodeRef) {
      data.onNodeClick(data.nodeRef);
    }
  };

  return (
    <div
      style={{
        background: colors.light,
        borderRadius: 8,
        width: nodeWidth,
        height: nodeHeight,
        border: `2px solid ${colors.main}`,
        display: "flex",
        alignItems: "stretch",
        position: "relative",
        overflow: "hidden",
        cursor: "pointer",
        boxShadow: isHovered ? `0 0 8px ${colors.main}` : "none",
        transition: "box-shadow 0.2s ease-in-out",
      }}
      onMouseEnter={() => setIsHovered(true)}
      onMouseLeave={() => setIsHovered(false)}
      onClick={handleClick}
    >
      <div
        style={{
          position: "absolute",
          top: 0,
          right: 0,
          backgroundColor: colors.main,
          color: "white",
          padding: "1px 6px",
          fontSize: "10px",
          borderBottomLeftRadius: "4px",
          zIndex: 5,
        }}
      >
        {producerLabel}
      </div>

      {data.namespace && isHovered && (
        <EuiToolTip position="bottom" content={`Namespace: ${data.namespace}`}>
          <div
            style={{
              position: "absolute",
              bottom: 2,
              left: 44,
              fontSize: 10,
              color: "#888",
            }}
          >
            {data.namespace}
          </div>
        </EuiToolTip>
      )}

      <Handle
        type="target"
        position={Position.Left}
        id="target"
        style={{ background: "#999", width: 10, height: 10 }}
      />
      <div
        style={{
          backgroundColor: colors.main,
          width: "40px",
          display: "flex",
          alignItems: "center",
          justifyContent: "center",
          borderRight: `1px solid ${colors.main}`,
        }}
      >
        <div style={{ color: "#ffffff", fontSize: "20px" }}>{icon}</div>
      </div>
      <div
        style={{
          flex: 1,
          display: "flex",
          alignItems: "center",
          justifyContent: "center",
          padding: "0 10px",
          fontSize: "14px",
          fontWeight: "500",
          color: "#333333",
        }}
      >
        {data.label}
      </div>
      <Handle
        type="source"
        position={Position.Right}
        id="source"
        style={{ background: "#999", width: 10, height: 10 }}
      />
    </div>
  );
};

const lineageNodeTypes = { lineageCustom: LineageCustomNode };

// ── Dagre layout ──

const layoutGraph = (nodes: Node[], edges: Edge[], direction = "LR") => {
  const dagreGraph = new dagre.graphlib.Graph();
  dagreGraph.setDefaultEdgeLabel(() => ({}));
  dagreGraph.setGraph({
    rankdir: direction,
    nodesep: 80,
    ranksep: 120,
    marginx: 50,
    marginy: 50,
  });

  nodes.forEach((node) => {
    dagreGraph.setNode(node.id, { width: nodeWidth, height: nodeHeight });
  });

  edges.forEach((edge) => {
    if (dagreGraph.hasNode(edge.source) && dagreGraph.hasNode(edge.target)) {
      dagreGraph.setEdge(edge.source, edge.target);
    }
  });

  dagre.layout(dagreGraph);

  return {
    nodes: nodes.map((node) => {
      const pos = dagreGraph.node(node.id);
      return {
        ...node,
        position: {
          x: (pos?.x ?? 0) - nodeWidth / 2,
          y: (pos?.y ?? 0) - nodeHeight / 2,
        },
        sourcePosition: direction === "TB" ? Position.Bottom : Position.Right,
        targetPosition: direction === "TB" ? Position.Top : Position.Left,
      };
    }),
    edges,
  };
};

// ── Legend ──

const ProducerLegend: React.FC<{ producers: string[] }> = ({ producers }) => {
  const { colorMode } = useTheme();
  const isDarkMode = colorMode === "dark";
  const bg = isDarkMode ? "#1D1E24" : "white";
  const border = isDarkMode ? "#343741" : "#ddd";
  const text = isDarkMode ? "#DFE5EF" : "#333";

  const items = producers.map((p) => ({
    key: p,
    label: p.charAt(0).toUpperCase() + p.slice(1),
    color: getProducerColors(p).main,
  }));

  return (
    <div
      style={{
        position: "absolute",
        left: 10,
        top: 10,
        background: bg,
        border: `1px solid ${border}`,
        borderRadius: 5,
        padding: 10,
        zIndex: 10,
        boxShadow: isDarkMode
          ? "0 2px 5px rgba(0,0,0,0.3)"
          : "0 2px 5px rgba(0,0,0,0.1)",
      }}
    >
      <div
        style={{ fontSize: 14, fontWeight: 600, marginBottom: 5, color: text }}
      >
        Producers
      </div>
      {items.map((item) => (
        <div
          key={item.key}
          style={{
            display: "flex",
            alignItems: "center",
            marginBottom: 5,
          }}
        >
          <div
            style={{
              width: 16,
              height: 16,
              backgroundColor: item.color,
              borderRadius: 3,
              marginRight: 8,
            }}
          />
          <div style={{ fontSize: 12, color: text }}>{item.label}</div>
        </div>
      ))}
    </div>
  );
};

// ── Run History helpers ──

const formatTimestamp = (ts: number | null): string => {
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

const stateColor = (state: string): string => {
  switch (state.toUpperCase()) {
    case "COMPLETE":
      return "#00BFB3";
    case "FAIL":
      return "#FF6666";
    case "RUNNING":
    case "START":
      return "#6092C0";
    case "ABORT":
      return "#D36086";
    default:
      return "#98A2B3";
  }
};

const RunHistorySection: React.FC<{
  jobNamespace: string;
  jobName: string;
  isDarkMode: boolean;
}> = ({ jobNamespace, jobName, isDarkMode }) => {
  const { data, isLoading } = useRunHistory(jobNamespace, jobName);
  const [selectedRunId, setSelectedRunId] = useState<string | undefined>();
  const { data: runDetail, isLoading: detailLoading } =
    useRunDetail(selectedRunId);

  const runs = data?.runs || [];

  if (isLoading) {
    return (
      <div style={{ marginTop: 14 }}>
        <div style={{ fontWeight: 600, marginBottom: 4 }}>Run History</div>
        <EuiLoadingSpinner size="s" />
      </div>
    );
  }

  if (runs.length === 0) return null;

  return (
    <div style={{ marginTop: 14 }}>
      <div style={{ fontWeight: 600, marginBottom: 4 }}>
        Run History ({runs.length})
      </div>
      <table
        style={{
          width: "100%",
          fontSize: 11,
          borderCollapse: "collapse",
        }}
      >
        <thead>
          <tr
            style={{
              borderBottom: `1px solid ${isDarkMode ? "#343741" : "#ddd"}`,
            }}
          >
            <th style={{ textAlign: "left", padding: "4px 0" }}>Run</th>
            <th style={{ textAlign: "left", padding: "4px 0" }}>Status</th>
            <th style={{ textAlign: "left", padding: "4px 0" }}>Started</th>
            <th style={{ textAlign: "left", padding: "4px 0" }}>Duration</th>
          </tr>
        </thead>
        <tbody>
          {runs.map((run: RunSummary) => (
            <tr
              key={run.run_id}
              style={{
                borderBottom: `1px solid ${isDarkMode ? "#2a2b30" : "#eee"}`,
                cursor: "pointer",
                background:
                  selectedRunId === run.run_id
                    ? isDarkMode
                      ? "#2a2b30"
                      : "#f0f4ff"
                    : "transparent",
              }}
              onClick={() =>
                setSelectedRunId(
                  selectedRunId === run.run_id ? undefined : run.run_id,
                )
              }
            >
              <td
                style={{
                  padding: "3px 0",
                  fontFamily: "monospace",
                  fontSize: 10,
                }}
                title={run.run_id}
              >
                {run.run_id.substring(0, 8)}…
              </td>
              <td style={{ padding: "3px 0" }}>
                <EuiBadge color={stateColor(run.state)}>{run.state}</EuiBadge>
              </td>
              <td style={{ padding: "3px 0", fontSize: 10 }}>
                {formatTimestamp(run.started_at)}
              </td>
              <td style={{ padding: "3px 0", fontSize: 10 }}>
                {formatDuration(run.started_at, run.ended_at)}
              </td>
            </tr>
          ))}
        </tbody>
      </table>

      {selectedRunId && (
        <div
          style={{
            marginTop: 8,
            padding: 8,
            background: isDarkMode ? "#25262b" : "#f7f8fc",
            borderRadius: 4,
            fontSize: 11,
          }}
        >
          {detailLoading ? (
            <EuiLoadingSpinner size="s" />
          ) : runDetail ? (
            <>
              <div style={{ fontWeight: 600, marginBottom: 4 }}>
                Run {runDetail.run_id.substring(0, 8)}… I/O
              </div>
              {runDetail.inputs.length > 0 && (
                <div style={{ marginBottom: 4 }}>
                  <div style={{ color: "#888", marginBottom: 2 }}>Inputs:</div>
                  {runDetail.inputs.map((io) => (
                    <div
                      key={`${io.namespace}:${io.name}`}
                      style={{ fontFamily: "monospace", paddingLeft: 8 }}
                    >
                      {io.name}
                    </div>
                  ))}
                </div>
              )}
              {runDetail.outputs.length > 0 && (
                <div>
                  <div style={{ color: "#888", marginBottom: 2 }}>Outputs:</div>
                  {runDetail.outputs.map((io) => (
                    <div
                      key={`${io.namespace}:${io.name}`}
                      style={{ fontFamily: "monospace", paddingLeft: 8 }}
                    >
                      {io.name}
                    </div>
                  ))}
                </div>
              )}
              {runDetail.inputs.length === 0 &&
                runDetail.outputs.length === 0 && (
                  <div style={{ color: "#888" }}>
                    No I/O recorded for this run
                  </div>
                )}
            </>
          ) : (
            <div style={{ color: "#888" }}>Run details not available</div>
          )}
        </div>
      )}
    </div>
  );
};

// ── Node Detail Panel ──

const NodeDetailPanel: React.FC<{
  node: OpenLineageNode;
  onClose: () => void;
}> = ({ node, onClose }) => {
  const { colorMode } = useTheme();
  const isDarkMode = colorMode === "dark";
  const colors = getProducerColors(node.producer);

  const schema = node.schema;
  const fields = schema?.fields || [];
  const facets = node.facets || {};

  const tags: string[] = [];
  if (facets.feast_featureView?.tags) {
    Object.entries(facets.feast_featureView.tags).forEach(([k, v]) =>
      tags.push(`${k}:${v}`),
    );
  }
  if (facets.feast_entity?.tags) {
    Object.entries(facets.feast_entity.tags).forEach(([k, v]) =>
      tags.push(`${k}:${v}`),
    );
  }

  const features: string[] = facets.feast_featureView?.features || [];
  const entities: string[] = facets.feast_featureView?.entities || [];
  const fvDescription =
    node.description ||
    facets.documentation?.description ||
    facets.feast_featureView?.description ||
    facets.feast_featureService?.description ||
    facets.feast_entity?.description;

  const featureViews: string[] =
    facets.feast_featureService?.feature_views || [];
  const dqMetrics = facets.dataQualityMetrics;
  const sqlFacet = facets.sql;
  const dataSource = facets.dataSource;
  const ownership = facets.ownership;

  const knownFacetKeys = new Set([
    "schema",
    "documentation",
    "feast_featureView",
    "feast_featureService",
    "feast_entity",
    "dataQualityMetrics",
    "sql",
    "dataSource",
    "ownership",
    "symlinks",
    "jobType",
  ]);
  const otherFacets = Object.keys(facets).filter((k) => !knownFacetKeys.has(k));

  return (
    <div
      style={{
        width: 340,
        borderLeft: `3px solid ${colors.main}`,
        background: isDarkMode ? "#1D1E24" : "#fff",
        padding: 16,
        overflowY: "auto",
        fontSize: 13,
        color: isDarkMode ? "#DFE5EF" : "#333",
      }}
    >
      <div
        style={{
          display: "flex",
          justifyContent: "space-between",
          alignItems: "flex-start",
          marginBottom: 12,
        }}
      >
        <div style={{ flex: 1 }}>
          <div
            style={{ fontSize: 11, color: "#999", textTransform: "uppercase" }}
          >
            {node.type}
          </div>
          <div
            style={{ fontSize: 16, fontWeight: 600, wordBreak: "break-word" }}
          >
            {node.name}
          </div>
        </div>
        <button
          onClick={onClose}
          style={{
            background: "none",
            border: "none",
            fontSize: 18,
            cursor: "pointer",
            color: isDarkMode ? "#DFE5EF" : "#333",
            padding: "0 4px",
          }}
        >
          ×
        </button>
      </div>

      <EuiBadge color={colors.main}>
        {normalizeProducer(node.producer)}
      </EuiBadge>
      {node.job_type && (
        <EuiBadge color="hollow" style={{ marginLeft: 4 }}>
          {node.job_type}
        </EuiBadge>
      )}
      {node.source_type && (
        <EuiBadge color="hollow" style={{ marginLeft: 4 }}>
          {node.source_type}
        </EuiBadge>
      )}

      <div style={{ marginTop: 12, fontSize: 12, color: "#888" }}>
        {node.namespace}
      </div>

      {fvDescription && (
        <div style={{ marginTop: 14 }}>
          <div style={{ fontWeight: 600, marginBottom: 4 }}>Description</div>
          <div style={{ color: isDarkMode ? "#bbb" : "#555" }}>
            {fvDescription}
          </div>
        </div>
      )}

      {tags.length > 0 && (
        <div style={{ marginTop: 14 }}>
          <div style={{ fontWeight: 600, marginBottom: 4 }}>Tags</div>
          <div style={{ display: "flex", flexWrap: "wrap", gap: 4 }}>
            {tags.map((t) => (
              <EuiBadge key={t} color="default">
                {t}
              </EuiBadge>
            ))}
          </div>
        </div>
      )}

      {entities.length > 0 && (
        <div style={{ marginTop: 14 }}>
          <div style={{ fontWeight: 600, marginBottom: 4 }}>Entities</div>
          {entities.map((e) => (
            <div key={e} style={{ padding: "2px 0", fontFamily: "monospace" }}>
              {e}
            </div>
          ))}
        </div>
      )}

      {features.length > 0 && (
        <div style={{ marginTop: 14 }}>
          <div style={{ fontWeight: 600, marginBottom: 4 }}>Features</div>
          {features.map((f) => (
            <div key={f} style={{ padding: "2px 0", fontFamily: "monospace" }}>
              {f}
            </div>
          ))}
        </div>
      )}

      {featureViews.length > 0 && (
        <div style={{ marginTop: 14 }}>
          <div style={{ fontWeight: 600, marginBottom: 4 }}>Feature Views</div>
          {featureViews.map((fv) => (
            <div key={fv} style={{ padding: "2px 0", fontFamily: "monospace" }}>
              {fv}
            </div>
          ))}
        </div>
      )}

      {fields.length > 0 && (
        <div style={{ marginTop: 14 }}>
          <div style={{ fontWeight: 600, marginBottom: 4 }}>
            Schema ({fields.length} fields)
          </div>
          <table
            style={{
              width: "100%",
              fontSize: 12,
              borderCollapse: "collapse",
            }}
          >
            <thead>
              <tr
                style={{
                  borderBottom: `1px solid ${isDarkMode ? "#343741" : "#ddd"}`,
                }}
              >
                <th style={{ textAlign: "left", padding: "4px 0" }}>Field</th>
                <th style={{ textAlign: "left", padding: "4px 0" }}>Type</th>
              </tr>
            </thead>
            <tbody>
              {fields.map((f: any) => (
                <tr
                  key={f.name}
                  style={{
                    borderBottom: `1px solid ${isDarkMode ? "#2a2b30" : "#eee"}`,
                  }}
                >
                  <td
                    style={{
                      padding: "3px 0",
                      fontFamily: "monospace",
                    }}
                  >
                    {f.name}
                  </td>
                  <td
                    style={{
                      padding: "3px 0",
                      color: isDarkMode ? "#98A2B3" : "#777",
                    }}
                  >
                    {f.type || "—"}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      {node.feast_object_type && node.feast_object_type !== "unknown" && (
        <div style={{ marginTop: 14 }}>
          <div style={{ fontWeight: 600, marginBottom: 4 }}>Feast Mapping</div>
          <div>
            <span style={{ color: "#888" }}>Type:</span>{" "}
            {node.feast_object_type}
          </div>
          {node.feast_object_name && (
            <div>
              <span style={{ color: "#888" }}>Name:</span>{" "}
              {node.feast_object_name}
            </div>
          )}
          {node.feast_project && (
            <div>
              <span style={{ color: "#888" }}>Project:</span>{" "}
              {node.feast_project}
            </div>
          )}
        </div>
      )}

      {dataSource && (
        <div style={{ marginTop: 14 }}>
          <div style={{ fontWeight: 600, marginBottom: 4 }}>Data Source</div>
          {dataSource.name && (
            <div>
              <span style={{ color: "#888" }}>Name:</span> {dataSource.name}
            </div>
          )}
          {dataSource.uri && (
            <div
              style={{
                fontFamily: "monospace",
                fontSize: 11,
                wordBreak: "break-all",
              }}
            >
              {dataSource.uri}
            </div>
          )}
        </div>
      )}

      {ownership && ownership.owners && (
        <div style={{ marginTop: 14 }}>
          <div style={{ fontWeight: 600, marginBottom: 4 }}>Owners</div>
          {ownership.owners.map((o: any, i: number) => (
            <div key={i}>
              {o.name || o.owner}
              {o.type ? ` (${o.type})` : ""}
            </div>
          ))}
        </div>
      )}

      {sqlFacet && sqlFacet.query && (
        <div style={{ marginTop: 14 }}>
          <div style={{ fontWeight: 600, marginBottom: 4 }}>SQL</div>
          <pre
            style={{
              background: isDarkMode ? "#25262b" : "#f5f5f5",
              padding: 8,
              borderRadius: 4,
              fontSize: 11,
              overflowX: "auto",
              maxHeight: 120,
              whiteSpace: "pre-wrap",
              wordBreak: "break-all",
            }}
          >
            {sqlFacet.query}
          </pre>
        </div>
      )}

      {dqMetrics && (
        <div style={{ marginTop: 14 }}>
          <div style={{ fontWeight: 600, marginBottom: 4 }}>Data Quality</div>
          {dqMetrics.rowCount != null && (
            <div>
              <span style={{ color: "#888" }}>Row count:</span>{" "}
              {dqMetrics.rowCount.toLocaleString()}
            </div>
          )}
          {dqMetrics.columnMetrics &&
            Object.entries(dqMetrics.columnMetrics).map(
              ([col, metrics]: [string, any]) => (
                <div key={col} style={{ marginTop: 4 }}>
                  <span style={{ fontFamily: "monospace" }}>{col}</span>
                  <span style={{ color: "#888", fontSize: 11, marginLeft: 6 }}>
                    {metrics.nullCount != null && `nulls: ${metrics.nullCount}`}
                    {metrics.distinctCount != null &&
                      ` distinct: ${metrics.distinctCount}`}
                    {metrics.min != null && ` min: ${metrics.min}`}
                    {metrics.max != null && ` max: ${metrics.max}`}
                  </span>
                </div>
              ),
            )}
        </div>
      )}

      {otherFacets.length > 0 && (
        <div style={{ marginTop: 14 }}>
          <div style={{ fontWeight: 600, marginBottom: 4 }}>Other Facets</div>
          {otherFacets.map((key) => (
            <div key={key} style={{ marginTop: 4 }}>
              <EuiBadge color="hollow">{key}</EuiBadge>
            </div>
          ))}
        </div>
      )}

      {node.type === "job" && (
        <RunHistorySection
          jobNamespace={node.namespace}
          jobName={node.name}
          isDarkMode={isDarkMode}
        />
      )}
    </div>
  );
};

// ── Main LineageGraph component ──

interface LineageGraphProps {
  olData?: OpenLineageGraphData;
  olLoading: boolean;
  olError: boolean;
  feastOnlyCheckbox?: React.ReactNode;
}

const LineageGraph: React.FC<LineageGraphProps> = ({
  olData,
  olLoading,
  olError,
  feastOnlyCheckbox,
}) => {
  const [nodes, setNodes, onNodesChange] = useNodesState([]);
  const [edges, setEdges, onEdgesChange] = useEdgesState([]);

  const [filterType, setFilterType] = useState("");
  const [filterProducer, setFilterProducer] = useState("");
  const [filterObject, setFilterObject] = useState("");
  const [selectedNode, setSelectedNode] = useState<OpenLineageNode | null>(
    null,
  );

  const producers = useMemo(() => {
    if (!olData) return [];
    const set = new Set<string>();
    for (const n of olData.nodes) {
      set.add(normalizeProducer(n.producer));
    }
    return Array.from(set).sort();
  }, [olData]);

  const objectOptions = useMemo(() => {
    if (!olData) return [];
    return olData.nodes
      .filter((n) => {
        if (filterType && n.type !== filterType) return false;
        if (filterProducer && normalizeProducer(n.producer) !== filterProducer)
          return false;
        return true;
      })
      .map((n) => n.name)
      .filter((v, i, a) => a.indexOf(v) === i)
      .sort();
  }, [olData, filterType, filterProducer]);

  useEffect(() => {
    setFilterObject("");
  }, [filterType, filterProducer]);

  useEffect(() => {
    if (!olData) return;

    let filteredNodes = olData.nodes;
    if (filterType) {
      filteredNodes = filteredNodes.filter((n) => n.type === filterType);
    }
    if (filterProducer) {
      filteredNodes = filteredNodes.filter(
        (n) => normalizeProducer(n.producer) === filterProducer,
      );
    }

    const makeId = (type: string, ns: string, name: string) =>
      `${type}:${ns}:${name}`;

    if (filterObject) {
      const focusIds = new Set(
        filteredNodes
          .filter((n) => n.name === filterObject)
          .map((n) => makeId(n.type, n.namespace, n.name)),
      );

      const connectedIds = new Set<string>();
      for (const e of olData.edges) {
        const srcId = makeId(e.source_type, e.source_namespace, e.source_name);
        const tgtId = makeId(e.target_type, e.target_namespace, e.target_name);
        if (focusIds.has(srcId)) connectedIds.add(tgtId);
        if (focusIds.has(tgtId)) connectedIds.add(srcId);
      }

      const visibleIds = new Set(
        Array.from(focusIds).concat(Array.from(connectedIds)),
      );
      filteredNodes = olData.nodes.filter((n) =>
        visibleIds.has(makeId(n.type, n.namespace, n.name)),
      );
    }

    const filteredNodeIds = new Set(
      filteredNodes.map((n) => makeId(n.type, n.namespace, n.name)),
    );

    const flowNodes: Node[] = filteredNodes.map((n) => ({
      id: makeId(n.type, n.namespace, n.name),
      type: "lineageCustom",
      data: {
        label: n.name,
        type: n.type,
        producer: n.producer,
        namespace: n.namespace,
        nodeRef: n,
        onNodeClick: setSelectedNode,
      } as LineageNodeData,
      position: { x: 0, y: 0 },
    }));

    const flowEdges: Edge[] = olData.edges
      .filter((e) => {
        const srcId = makeId(e.source_type, e.source_namespace, e.source_name);
        const tgtId = makeId(e.target_type, e.target_namespace, e.target_name);
        return filteredNodeIds.has(srcId) && filteredNodeIds.has(tgtId);
      })
      .map((e, i) => {
        const isSymlink = e.edge_type === "symlink";
        const color = isSymlink
          ? "#999999"
          : e.edge_type === "derived"
            ? "#3366cc"
            : "#e67300";
        return {
          id: `ol-edge-${i}`,
          source: makeId(e.source_type, e.source_namespace, e.source_name),
          sourceHandle: "source",
          target: makeId(e.target_type, e.target_namespace, e.target_name),
          targetHandle: "target",
          animated: !isSymlink,
          style: {
            strokeWidth: isSymlink ? 1 : 2,
            stroke: color,
            strokeDasharray: isSymlink
              ? "3 3"
              : e.edge_type === "derived"
                ? "5 3"
                : "none",
          },
          type: "smoothstep",
          markerEnd: {
            type: MarkerType.ArrowClosed,
            width: 16,
            height: 16,
            color,
          },
        };
      });

    const { nodes: ln, edges: le } = layoutGraph(flowNodes, flowEdges);
    setNodes(ln);
    setEdges(le);
  }, [olData, filterType, filterProducer, filterObject, setNodes, setEdges]);

  if (olLoading) {
    return (
      <EuiPanel>
        <div style={{ display: "flex", justifyContent: "center", padding: 50 }}>
          <EuiLoadingSpinner size="xl" />
        </div>
      </EuiPanel>
    );
  }

  if (olError || !olData) {
    return (
      <EuiPanel>
        <EuiEmptyPrompt
          iconType="alert"
          title={<h2>OpenLineage Data Unavailable</h2>}
          body={
            <p>
              The OpenLineage consumer is not enabled or no events have been
              received yet. Enable the consumer in your{" "}
              <code>feature_store.yaml</code> configuration.
            </p>
          }
        />
      </EuiPanel>
    );
  }

  if (olData.nodes.length === 0) {
    return (
      <EuiPanel>
        <EuiEmptyPrompt
          iconType="branch"
          title={<h2>No Lineage Events</h2>}
          body={
            <p>
              No OpenLineage events have been received yet. Configure your data
              pipeline producers (Airflow, Spark, dbt, Feast) to send events to
              this instance.
            </p>
          }
        />
      </EuiPanel>
    );
  }

  return (
    <EuiPanel>
      <div
        style={{
          display: "flex",
          justifyContent: "space-between",
          alignItems: "center",
        }}
      >
        <EuiTitle size="s">
          <h2>OpenLineage Graph</h2>
        </EuiTitle>
        {feastOnlyCheckbox && (
          <div style={{ display: "flex", gap: "20px" }}>
            {feastOnlyCheckbox}
          </div>
        )}
      </div>
      <EuiSpacer size="m" />
      <EuiFlexGroup style={{ marginBottom: 16 }}>
        <EuiFlexItem grow={false} style={{ width: 200 }}>
          <EuiFormRow label="Filter by type">
            <EuiSelect
              options={[
                { value: "", text: "All" },
                { value: "job", text: "Job" },
                { value: "dataset", text: "Dataset" },
              ]}
              value={filterType}
              onChange={(e) => setFilterType(e.target.value)}
              aria-label="Filter by type"
            />
          </EuiFormRow>
        </EuiFlexItem>
        <EuiFlexItem grow={false} style={{ width: 200 }}>
          <EuiFormRow label="Filter by producer">
            <EuiSelect
              options={[
                { value: "", text: "All" },
                ...producers.map((p) => ({
                  value: p,
                  text: p.charAt(0).toUpperCase() + p.slice(1),
                })),
              ]}
              value={filterProducer}
              onChange={(e) => setFilterProducer(e.target.value)}
              aria-label="Filter by producer"
            />
          </EuiFormRow>
        </EuiFlexItem>
        <EuiFlexItem grow={false} style={{ width: 300 }}>
          <EuiFormRow label="Select object">
            <EuiSelect
              options={[
                { value: "", text: "All" },
                ...objectOptions.map((name) => ({ value: name, text: name })),
              ]}
              value={filterObject}
              onChange={(e) => setFilterObject(e.target.value)}
              aria-label="Select object"
            />
          </EuiFormRow>
        </EuiFlexItem>
      </EuiFlexGroup>
      <div style={{ display: "flex", height: 600, border: "1px solid #ddd" }}>
        <div style={{ flex: 1, position: "relative" }}>
          <ReactFlow
            nodes={nodes}
            edges={edges}
            onNodesChange={onNodesChange}
            onEdgesChange={onEdgesChange}
            nodeTypes={lineageNodeTypes}
            connectionLineType={ConnectionLineType.SmoothStep}
            fitView
            minZoom={0.1}
            maxZoom={8}
            onPaneClick={() => setSelectedNode(null)}
          >
            <Background color="#f0f0f0" gap={16} />
            <Controls />
            <ProducerLegend producers={producers} />
          </ReactFlow>
        </div>
        {selectedNode && (
          <NodeDetailPanel
            node={selectedNode}
            onClose={() => setSelectedNode(null)}
          />
        )}
      </div>
    </EuiPanel>
  );
};

export { LineageGraph };
export default LineageGraph;
