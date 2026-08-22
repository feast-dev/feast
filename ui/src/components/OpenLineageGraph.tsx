import React, {
  useCallback,
  useEffect,
  useMemo,
  useRef,
  useState,
} from "react";
import {
  ReactFlow,
  Node,
  Edge,
  Controls,
  ControlButton,
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

/**
 * Label for producer badges / filters.
 * User-configured names are shown as-is. Long OpenLineage producer URLs
 * (e.g. …/integration/spark) are shortened to the last path segment so the
 * UI stays readable.
 */
const displayProducer = (producer?: string | null): string => {
  if (!producer) return "unknown";
  const trimmed = producer.trim();
  try {
    const url = new URL(trimmed);
    const segments = url.pathname.split("/").filter(Boolean);
    if (segments.length > 0) {
      return segments[segments.length - 1];
    }
  } catch {
    // not a URL — keep configured value unchanged
  }
  return trimmed;
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
  const key = displayProducer(producer);
  if (!producerColorCache[key]) {
    producerColorCache[key] = generateProducerColor(key);
  }
  return producerColorCache[key];
};

const getNodeIcon = (type: string) => {
  return type === "job" ? "\u2699" : "\u2B21";
};

const nodeKey = (type: string, ns: string, name: string) =>
  `${type}:${ns}:${name}`;

/**
 * Runtime transform jobs appear as pills on the Lineage graph.
 * Prefer server-provided job_type (from feast_jobKind facet). Fall back to
 * known Feast emit conventions for events emitted before jobKind existed.
 */
const isRuntimeTransformJob = (
  name: string,
  producer?: string | null,
  jobType?: string | null,
): boolean => {
  const kind = (jobType || "").toLowerCase();
  if (kind === "transform") return true;
  if (kind === "definition") return false;

  const n = (name || "").toLowerCase();
  if (
    n.startsWith("feature_service_") ||
    n.startsWith("feast_feature_views_") ||
    n.startsWith("feast_feature_services_")
  ) {
    return false;
  }
  if (n.startsWith("materialize_")) return true;
  if (n.startsWith("spark_compute_")) return true;
  if (n.startsWith("stream_")) return true;
  if (n.startsWith("on_demand_feature_view_")) return true;
  const p = (producer || "").toLowerCase();
  if (p.includes("/integration/spark")) return true;
  return false;
};

/**
 * Lineage graph: datasets + runtime transform jobs (materialize, Spark, …).
 * Definition relationships (source→FV, FV→FeatureService) stay as derived
 * dataset edges. Derived shortcuts are omitted only when a *transform* job
 * already connects the same pair.
 */
const buildLineageGraphSlice = (
  olData: OpenLineageGraphData,
): { nodes: OpenLineageNode[]; edges: OpenLineageGraphData["edges"] } => {
  const makeId = (type: string, ns: string, name: string) =>
    nodeKey(type, ns, name);

  const jobById = new Map(
    olData.nodes
      .filter((n) => n.type === "job")
      .map((n) => [makeId(n.type, n.namespace, n.name), n]),
  );

  const ioEdges = olData.edges.filter(
    (e) => e.edge_type === "input" || e.edge_type === "output",
  );
  const parentEdges = olData.edges.filter((e) => e.edge_type === "parent");
  const symlinkEdges = olData.edges.filter((e) => e.edge_type === "symlink");
  const derivedEdges = olData.edges.filter((e) => e.edge_type === "derived");

  const isTransformJobId = (id: string) => {
    const job = jobById.get(id);
    if (!job) return false;
    return isRuntimeTransformJob(job.name, job.producer, job.job_type);
  };

  const jobsOnPath = new Set<string>();
  for (const e of ioEdges) {
    if (e.source_type === "job") {
      const id = makeId(e.source_type, e.source_namespace, e.source_name);
      if (isTransformJobId(id)) jobsOnPath.add(id);
    }
    if (e.target_type === "job") {
      const id = makeId(e.target_type, e.target_namespace, e.target_name);
      if (isTransformJobId(id)) jobsOnPath.add(id);
    }
  }

  // Pull in parent/child compute jobs (e.g. materialize → Spark)
  let grew = true;
  while (grew) {
    grew = false;
    for (const e of parentEdges) {
      const src = makeId(e.source_type, e.source_namespace, e.source_name);
      const tgt = makeId(e.target_type, e.target_namespace, e.target_name);
      if (
        jobsOnPath.has(src) &&
        !jobsOnPath.has(tgt) &&
        isTransformJobId(tgt)
      ) {
        jobsOnPath.add(tgt);
        grew = true;
      }
      if (
        jobsOnPath.has(tgt) &&
        !jobsOnPath.has(src) &&
        isTransformJobId(src)
      ) {
        jobsOnPath.add(src);
        grew = true;
      }
    }
  }

  const nodes = olData.nodes.filter(
    (n) =>
      n.type === "dataset" ||
      (n.type === "job" && jobsOnPath.has(makeId(n.type, n.namespace, n.name))),
  );
  const nodeIds = new Set(
    nodes.map((n) => makeId(n.type, n.namespace, n.name)),
  );

  // Only transform-job I/O suppresses derived dataset shortcuts
  const mediatedPairs = new Set<string>();
  const jobInputs = new Map<string, Set<string>>();
  const jobOutputs = new Map<string, Set<string>>();
  for (const e of ioEdges) {
    if (e.edge_type === "input" && e.target_type === "job") {
      const jid = makeId(e.target_type, e.target_namespace, e.target_name);
      if (!jobsOnPath.has(jid)) continue;
      const did = makeId(e.source_type, e.source_namespace, e.source_name);
      if (!jobInputs.has(jid)) jobInputs.set(jid, new Set());
      jobInputs.get(jid)!.add(did);
    }
    if (e.edge_type === "output" && e.source_type === "job") {
      const jid = makeId(e.source_type, e.source_namespace, e.source_name);
      if (!jobsOnPath.has(jid)) continue;
      const did = makeId(e.target_type, e.target_namespace, e.target_name);
      if (!jobOutputs.has(jid)) jobOutputs.set(jid, new Set());
      jobOutputs.get(jid)!.add(did);
    }
  }
  for (const jid of Array.from(jobInputs.keys())) {
    const inputs = jobInputs.get(jid)!;
    const outputs = jobOutputs.get(jid);
    if (!outputs) continue;
    Array.from(inputs).forEach((inn) => {
      Array.from(outputs).forEach((out) => {
        mediatedPairs.add(`${inn}=>${out}`);
      });
    });
  }

  const keepEdge = (e: OpenLineageGraphData["edges"][number]) => {
    const src = makeId(e.source_type, e.source_namespace, e.source_name);
    const tgt = makeId(e.target_type, e.target_namespace, e.target_name);
    if (!nodeIds.has(src) || !nodeIds.has(tgt)) return false;
    if (e.edge_type === "derived") {
      return !mediatedPairs.has(`${src}=>${tgt}`);
    }
    // Drop I/O edges for definition jobs (those jobs are not on the graph)
    if (e.edge_type === "input" || e.edge_type === "output") {
      if (e.source_type === "job" && !jobsOnPath.has(src)) return false;
      if (e.target_type === "job" && !jobsOnPath.has(tgt)) return false;
    }
    return (
      e.edge_type === "input" ||
      e.edge_type === "output" ||
      e.edge_type === "parent" ||
      e.edge_type === "symlink"
    );
  };

  const edges = [
    ...ioEdges,
    ...parentEdges,
    ...symlinkEdges,
    ...derivedEdges,
  ].filter(keepEdge);

  return { nodes, edges };
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
  const producerLabel = displayProducer(data.producer);
  const isJob = data.type === "job";

  const nodeRef = data.nodeRef;
  const feastObjType = nodeRef?.feast_object_type;
  const feastDsFacet = nodeRef?.facets?.feast_dataSource;
  const subtitle = isJob
    ? "click for runs"
    : feastObjType === "dataSource" && feastDsFacet?.source_type
      ? feastDsFacet.source_type
      : feastObjType && feastObjType !== "unknown"
        ? feastObjType
        : undefined;

  const handleClick = () => {
    if (data.onNodeClick && data.nodeRef) {
      data.onNodeClick(data.nodeRef);
    }
  };

  return (
    <div
      style={{
        background: colors.light,
        borderRadius: isJob ? 20 : 8,
        width: nodeWidth,
        height: nodeHeight,
        border: isJob
          ? `2px dashed ${colors.main}`
          : `2px solid ${colors.main}`,
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
        {isJob ? "transform" : producerLabel}
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
          flexDirection: "column",
          alignItems: "center",
          justifyContent: "center",
          padding: "0 10px",
          fontSize: "14px",
          fontWeight: "500",
          color: "#333333",
        }}
      >
        <div
          style={{
            overflow: "hidden",
            textOverflow: "ellipsis",
            whiteSpace: "nowrap",
            maxWidth: "100%",
          }}
        >
          {data.label}
        </div>
        {subtitle && (
          <div style={{ fontSize: 10, color: "#666", fontWeight: 400 }}>
            {subtitle}
          </div>
        )}
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

/**
 * Derive a rank key from a ReactFlow node's data so that nodes of the
 * same semantic type are placed on the same horizontal column (LR) or
 * vertical row (TB).
 *
 * Priority: feast_object_type > OL type (job / dataset).
 * The returned key is used to group nodes into dagre rank-groups.
 */
const rankKeyForNode = (node: Node): string => {
  const ref = (node.data as LineageNodeData | undefined)?.nodeRef;
  const feastType = ref?.feast_object_type;
  if (feastType && feastType !== "unknown") return `feast:${feastType}`;
  const olType = (node.data as LineageNodeData | undefined)?.type;
  return olType === "job" ? "ol:job" : "ol:dataset";
};

/**
 * Ordered rank tiers – earlier entries are placed further left (LR) or
 * further up (TB).  Keys not listed here are appended automatically so
 * external / unknown types still get a stable position.
 */
const RANK_ORDER: string[] = [
  "feast:dataSource",
  "feast:entity",
  "ol:dataset",
  "feast:featureView",
  "feast:onDemandFeatureView",
  "feast:streamFeatureView",
  "ol:job",
  "feast:featureService",
  "feast:savedDataset",
  "feast:onlineStore",
];

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

  // Group nodes by rank key so we can assign same-rank constraints.
  const rankGroups = new Map<string, string[]>();
  nodes.forEach((node) => {
    const key = rankKeyForNode(node);
    if (!rankGroups.has(key)) rankGroups.set(key, []);
    rankGroups.get(key)!.push(node.id);
    dagreGraph.setNode(node.id, { width: nodeWidth, height: nodeHeight });
  });

  edges.forEach((edge) => {
    if (dagreGraph.hasNode(edge.source) && dagreGraph.hasNode(edge.target)) {
      dagreGraph.setEdge(edge.source, edge.target);
    }
  });

  // Assign explicit rank to each group via invisible chain edges so dagre
  // aligns every node of the same type on the same column.
  // We pick one representative node per group and chain them in the
  // canonical order, then mark all other nodes in a group as same-rank
  // by adding zero-weight edges to the representative.
  const orderedKeys = [...RANK_ORDER];
  Array.from(rankGroups.keys()).forEach((key) => {
    if (!orderedKeys.includes(key)) orderedKeys.push(key);
  });
  const activeKeys = orderedKeys.filter((k) => rankGroups.has(k));

  // Chain representatives to enforce ordering between groups
  for (let i = 0; i < activeKeys.length - 1; i++) {
    const repA = rankGroups.get(activeKeys[i])![0];
    const repB = rankGroups.get(activeKeys[i + 1])![0];
    dagreGraph.setEdge(repA, repB, {
      minlen: 1,
      weight: 0,
      style: "invis",
      _rank_edge: true,
    });
  }

  // Pull same-group nodes to the same rank as their representative
  Array.from(rankGroups.values()).forEach((ids) => {
    if (ids.length <= 1) return;
    const rep = ids[0];
    for (let i = 1; i < ids.length; i++) {
      dagreGraph.setEdge(rep, ids[i], {
        minlen: 0,
        weight: 2,
        style: "invis",
        _rank_edge: true,
      });
      dagreGraph.setEdge(ids[i], rep, {
        minlen: 0,
        weight: 2,
        style: "invis",
        _rank_edge: true,
      });
    }
  });

  dagre.layout(dagreGraph);

  // Collect the invisible rank-edge ids so we can strip them from output
  const rankEdgeSet = new Set<string>();
  dagreGraph.edges().forEach((e) => {
    const label = dagreGraph.edge(e);
    if (label?._rank_edge) rankEdgeSet.add(`${e.v}->${e.w}`);
  });

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

  const featureViews: string[] =
    facets.feast_featureService?.feature_views || [];
  const dqMetrics = facets.dataQualityMetrics;
  const sqlFacet = facets.sql;
  const dataSource = facets.dataSource;
  const feastDataSource = facets.feast_dataSource;
  const ownership = facets.ownership;
  const onlineStore = facets.feast_onlineStore;
  const savedDataset = facets.feast_savedDataset;

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
  if (feastDataSource?.tags) {
    Object.entries(feastDataSource.tags).forEach(([k, v]) =>
      tags.push(`${k}:${v}`),
    );
  }
  if (savedDataset?.tags) {
    Object.entries(savedDataset.tags).forEach(([k, v]) =>
      tags.push(`${k}:${v}`),
    );
  }

  const features: string[] =
    facets.feast_featureView?.features || savedDataset?.features || [];
  const entities: string[] = facets.feast_featureView?.entities || [];
  const fvDescription =
    node.description ||
    facets.documentation?.description ||
    facets.feast_featureView?.description ||
    facets.feast_featureService?.description ||
    facets.feast_entity?.description ||
    feastDataSource?.description ||
    savedDataset?.description;

  const knownFacetKeys = new Set([
    "schema",
    "documentation",
    "feast_featureView",
    "feast_featureService",
    "feast_entity",
    "feast_dataSource",
    "feast_savedDataset",
    "feast_onlineStore",
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

      <EuiBadge color={colors.main}>{displayProducer(node.producer)}</EuiBadge>
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
              <span style={{ color: "#888" }}>
                {node.feast_object_type === "onlineStore"
                  ? "Feature View:"
                  : "Name:"}
              </span>{" "}
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

      {onlineStore && (
        <div style={{ marginTop: 14 }}>
          <div style={{ fontWeight: 600, marginBottom: 4 }}>Online Store</div>
          {onlineStore.store_type && (
            <div>
              <span style={{ color: "#888" }}>Backend:</span>{" "}
              {onlineStore.store_type}
            </div>
          )}
          {onlineStore.feature_view && (
            <div>
              <span style={{ color: "#888" }}>Feature View:</span>{" "}
              {onlineStore.feature_view}
            </div>
          )}
        </div>
      )}

      {(dataSource || feastDataSource) && (
        <div style={{ marginTop: 14 }}>
          <div style={{ fontWeight: 600, marginBottom: 4 }}>Data Source</div>
          {(feastDataSource?.name || dataSource?.name) && (
            <div>
              <span style={{ color: "#888" }}>Name:</span>{" "}
              {feastDataSource?.name || dataSource?.name}
            </div>
          )}
          {feastDataSource?.source_type && (
            <div>
              <span style={{ color: "#888" }}>Type:</span>{" "}
              {feastDataSource.source_type}
            </div>
          )}
          {feastDataSource?.timestamp_field && (
            <div>
              <span style={{ color: "#888" }}>Timestamp Field:</span>{" "}
              <code>{feastDataSource.timestamp_field}</code>
            </div>
          )}
          {feastDataSource?.created_timestamp_field && (
            <div>
              <span style={{ color: "#888" }}>Created Timestamp:</span>{" "}
              <code>{feastDataSource.created_timestamp_field}</code>
            </div>
          )}
          {feastDataSource?.path && (
            <div>
              <span style={{ color: "#888" }}>Path:</span>{" "}
              <span
                style={{
                  fontFamily: "monospace",
                  fontSize: 11,
                  wordBreak: "break-all",
                }}
              >
                {feastDataSource.path}
              </span>
            </div>
          )}
          {feastDataSource?.table && (
            <div>
              <span style={{ color: "#888" }}>Table:</span>{" "}
              <code>{feastDataSource.table}</code>
            </div>
          )}
          {feastDataSource?.query && (
            <div>
              <span style={{ color: "#888" }}>Query:</span>{" "}
              <pre
                style={{
                  background: isDarkMode ? "#25262b" : "#f5f5f5",
                  padding: 6,
                  borderRadius: 4,
                  fontSize: 11,
                  overflowX: "auto",
                  maxHeight: 80,
                  margin: "4px 0 0",
                }}
              >
                {feastDataSource.query}
              </pre>
            </div>
          )}
          {dataSource?.uri && (
            <div>
              <span style={{ color: "#888" }}>URI:</span>{" "}
              <span
                style={{
                  fontFamily: "monospace",
                  fontSize: 11,
                  wordBreak: "break-all",
                }}
              >
                {dataSource.uri}
              </span>
            </div>
          )}
          {feastDataSource?.field_mapping &&
            Object.keys(feastDataSource.field_mapping).length > 0 && (
              <div style={{ marginTop: 6 }}>
                <span style={{ color: "#888" }}>Field Mapping:</span>
                <table
                  style={{
                    width: "100%",
                    fontSize: 11,
                    borderCollapse: "collapse",
                    marginTop: 4,
                  }}
                >
                  <thead>
                    <tr
                      style={{
                        borderBottom: `1px solid ${isDarkMode ? "#343741" : "#ddd"}`,
                      }}
                    >
                      <th style={{ textAlign: "left", padding: "2px 0" }}>
                        Source
                      </th>
                      <th style={{ textAlign: "left", padding: "2px 0" }}>
                        Feature
                      </th>
                    </tr>
                  </thead>
                  <tbody>
                    {Object.entries(feastDataSource.field_mapping).map(
                      ([src, dst]) => (
                        <tr
                          key={src}
                          style={{
                            borderBottom: `1px solid ${isDarkMode ? "#2a2b30" : "#eee"}`,
                          }}
                        >
                          <td
                            style={{
                              padding: "2px 0",
                              fontFamily: "monospace",
                            }}
                          >
                            {src}
                          </td>
                          <td
                            style={{
                              padding: "2px 0",
                              fontFamily: "monospace",
                            }}
                          >
                            {String(dst)}
                          </td>
                        </tr>
                      ),
                    )}
                  </tbody>
                </table>
              </div>
            )}
        </div>
      )}

      {savedDataset && (
        <div style={{ marginTop: 14 }}>
          <div style={{ fontWeight: 600, marginBottom: 4 }}>Saved Dataset</div>
          {savedDataset.feature_service_name && (
            <div>
              <span style={{ color: "#888" }}>Feature Service:</span>{" "}
              {savedDataset.feature_service_name}
            </div>
          )}
          {savedDataset.join_keys?.length > 0 && (
            <div>
              <span style={{ color: "#888" }}>Join Keys:</span>{" "}
              <code>{savedDataset.join_keys.join(", ")}</code>
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
        <>
          <div
            style={{
              marginTop: 14,
              padding: "8px 10px",
              background: isDarkMode ? "#2a2b30" : "#f4f6f8",
              borderRadius: 6,
              fontSize: 12,
              color: isDarkMode ? "#bbb" : "#555",
            }}
          >
            Materialization / compute run. Select a run below for inputs,
            outputs, and status. (FeatureService membership is not a transform —
            that stays as dataset links.)
          </div>
          <RunHistorySection
            jobNamespace={node.namespace}
            jobName={node.name}
            isDarkMode={isDarkMode}
          />
        </>
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
  /**
   * lineage — datasets + runtime transforms (materialize, Spark); default
   * objects — datasets only
   * all — every consumer node/edge
   */
  viewMode?: "lineage" | "objects" | "all";
}

const LineageGraph: React.FC<LineageGraphProps> = ({
  olData,
  olLoading,
  olError,
  feastOnlyCheckbox,
  viewMode = "lineage",
}) => {
  const [nodes, setNodes, onNodesChange] = useNodesState([]);
  const [edges, setEdges, onEdgesChange] = useEdgesState([]);

  const [filterType, setFilterType] = useState("");
  const [filterFeastType, setFilterFeastType] = useState("");
  const [filterProducer, setFilterProducer] = useState("");
  const [filterObject, setFilterObject] = useState("");
  const [selectedNode, setSelectedNode] = useState<OpenLineageNode | null>(
    null,
  );
  const [hoveredNodeId, setHoveredNodeId] = useState<string | null>(null);
  const edgesRef = useRef<Edge[]>([]);
  const graphContainerRef = useRef<HTMLDivElement>(null);
  const [isFullscreen, setIsFullscreen] = useState(false);

  useEffect(() => {
    const onFsChange = () => setIsFullscreen(!!document.fullscreenElement);
    document.addEventListener("fullscreenchange", onFsChange);
    return () => document.removeEventListener("fullscreenchange", onFsChange);
  }, []);

  const toggleFullscreen = useCallback(() => {
    if (!graphContainerRef.current) return;
    if (document.fullscreenElement) {
      document.exitFullscreen();
    } else {
      graphContainerRef.current.requestFullscreen();
    }
  }, []);

  const connectedIds = useMemo(() => {
    if (!hoveredNodeId) return null;
    const ids = new Set<string>([hoveredNodeId]);
    const allEdges = edgesRef.current;

    // Walk upstream (target → source)
    const upQueue = [hoveredNodeId];
    while (upQueue.length > 0) {
      const cur = upQueue.shift()!;
      for (const e of allEdges) {
        if (e.target === cur && !ids.has(e.source)) {
          ids.add(e.source);
          upQueue.push(e.source);
        }
      }
    }

    // Walk downstream (source → target)
    const downQueue = [hoveredNodeId];
    while (downQueue.length > 0) {
      const cur = downQueue.shift()!;
      for (const e of allEdges) {
        if (e.source === cur && !ids.has(e.target)) {
          ids.add(e.target);
          downQueue.push(e.target);
        }
      }
    }

    return ids;
  }, [hoveredNodeId]);

  const onNodeMouseEnter = useCallback(
    (_: React.MouseEvent, node: Node) => setHoveredNodeId(node.id),
    [],
  );
  const onNodeMouseLeave = useCallback(() => setHoveredNodeId(null), []);

  const objectsOnly = viewMode === "objects";
  const showTypeFilter = viewMode !== "objects";

  const { baseNodes, baseEdges } = useMemo(() => {
    if (!olData) return { baseNodes: [] as OpenLineageNode[], baseEdges: [] };
    if (viewMode === "all") {
      return { baseNodes: olData.nodes, baseEdges: olData.edges };
    }
    if (viewMode === "objects") {
      return {
        baseNodes: olData.nodes.filter((n) => n.type === "dataset"),
        baseEdges: olData.edges.filter(
          (e) =>
            e.source_type === "dataset" &&
            e.target_type === "dataset" &&
            e.edge_type !== "parent",
        ),
      };
    }
    const slice = buildLineageGraphSlice(olData);
    return { baseNodes: slice.nodes, baseEdges: slice.edges };
  }, [olData, viewMode]);

  const producers = useMemo(() => {
    const set = new Set<string>();
    for (const n of baseNodes) {
      set.add(displayProducer(n.producer));
    }
    return Array.from(set).sort();
  }, [baseNodes]);

  const feastObjectTypes = useMemo(() => {
    const set = new Set<string>();
    for (const n of baseNodes) {
      if (n.feast_object_type && n.feast_object_type !== "unknown") {
        set.add(n.feast_object_type);
      }
    }
    return Array.from(set).sort();
  }, [baseNodes]);

  const feastTypeLabels: Record<string, string> = {
    dataSource: "Data Source",
    featureView: "Feature View",
    featureService: "Feature Service",
    entity: "Entity",
    onDemandFeatureView: "On-Demand Feature View",
    streamFeatureView: "Stream Feature View",
    savedDataset: "Saved Dataset",
    onlineStore: "Online Store",
  };

  const objectOptions = useMemo(() => {
    return baseNodes
      .filter((n) => {
        if (filterType && n.type !== filterType) return false;
        if (filterFeastType && n.feast_object_type !== filterFeastType)
          return false;
        if (filterProducer && displayProducer(n.producer) !== filterProducer)
          return false;
        return true;
      })
      .map((n) => n.name)
      .filter((v, i, a) => a.indexOf(v) === i)
      .sort();
  }, [baseNodes, filterType, filterFeastType, filterProducer]);

  useEffect(() => {
    setFilterObject("");
  }, [filterType, filterFeastType, filterProducer]);

  useEffect(() => {
    if (objectsOnly && filterType === "job") {
      setFilterType("");
    }
  }, [objectsOnly, filterType]);

  useEffect(() => {
    if (!olData) return;

    let filteredNodes = baseNodes;
    if (filterType) {
      filteredNodes = filteredNodes.filter((n) => n.type === filterType);
    }
    if (filterFeastType) {
      filteredNodes = filteredNodes.filter(
        (n) => n.feast_object_type === filterFeastType,
      );
    }
    if (filterProducer) {
      filteredNodes = filteredNodes.filter(
        (n) => displayProducer(n.producer) === filterProducer,
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

      const visibleIds = new Set(focusIds);

      // Walk upstream: follow edges where target matches current → add source
      const upQueue = Array.from(focusIds);
      while (upQueue.length > 0) {
        const current = upQueue.shift()!;
        for (const e of baseEdges) {
          const srcId = makeId(
            e.source_type,
            e.source_namespace,
            e.source_name,
          );
          const tgtId = makeId(
            e.target_type,
            e.target_namespace,
            e.target_name,
          );
          if (tgtId === current && !visibleIds.has(srcId)) {
            visibleIds.add(srcId);
            upQueue.push(srcId);
          }
        }
      }

      // Walk downstream: follow edges where source matches current → add target
      const downQueue = Array.from(focusIds);
      while (downQueue.length > 0) {
        const current = downQueue.shift()!;
        for (const e of baseEdges) {
          const srcId = makeId(
            e.source_type,
            e.source_namespace,
            e.source_name,
          );
          const tgtId = makeId(
            e.target_type,
            e.target_namespace,
            e.target_name,
          );
          if (srcId === current && !visibleIds.has(tgtId)) {
            visibleIds.add(tgtId);
            downQueue.push(tgtId);
          }
        }
      }

      filteredNodes = baseNodes.filter((n) =>
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

    const flowEdges: Edge[] = baseEdges
      .filter((e) => {
        const srcId = makeId(e.source_type, e.source_namespace, e.source_name);
        const tgtId = makeId(e.target_type, e.target_namespace, e.target_name);
        return filteredNodeIds.has(srcId) && filteredNodeIds.has(tgtId);
      })
      .map((e, i) => {
        const edgeType = e.edge_type || "";
        const isSymlink = edgeType === "symlink";
        const isParent = edgeType === "parent";
        const isDerived = edgeType === "derived";
        const color = isSymlink
          ? "#999999"
          : isParent
            ? "#7a7a7a"
            : isDerived
              ? "#3366cc"
              : "#e67300";
        return {
          id: `ol-edge-${i}`,
          source: makeId(e.source_type, e.source_namespace, e.source_name),
          sourceHandle: "source",
          target: makeId(e.target_type, e.target_namespace, e.target_name),
          targetHandle: "target",
          animated: !isSymlink && !isParent,
          label: isParent ? "runs on" : undefined,
          labelStyle: isParent ? { fontSize: 10, fill: "#666" } : undefined,
          style: {
            strokeWidth: isSymlink || isParent ? 1.5 : 2,
            stroke: color,
            strokeDasharray: isSymlink
              ? "3 3"
              : isParent
                ? "2 4"
                : isDerived
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
    edgesRef.current = le;
    setNodes(ln);
    setEdges(le);
  }, [
    olData,
    baseNodes,
    baseEdges,
    filterType,
    filterFeastType,
    filterProducer,
    filterObject,
    setNodes,
    setEdges,
  ]);

  // Apply hover-dim: fade unrelated nodes and edges
  const styledNodes = useMemo(() => {
    if (!connectedIds) return nodes;
    return nodes.map((n) => ({
      ...n,
      style: {
        ...n.style,
        opacity: connectedIds.has(n.id) ? 1 : 0.15,
        transition: "opacity 0.2s",
      },
    }));
  }, [nodes, connectedIds]);

  const styledEdges = useMemo(() => {
    if (!connectedIds) return edges;
    return edges.map((e) => ({
      ...e,
      style: {
        ...e.style,
        opacity:
          connectedIds.has(e.source) && connectedIds.has(e.target) ? 1 : 0.08,
        transition: "opacity 0.2s",
      },
    }));
  }, [edges, connectedIds]);

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

  if (baseNodes.length === 0) {
    return (
      <EuiPanel>
        {feastOnlyCheckbox && (
          <div
            style={{
              display: "flex",
              justifyContent: "flex-end",
              marginBottom: 12,
            }}
          >
            {feastOnlyCheckbox}
          </div>
        )}
        <EuiEmptyPrompt
          iconType="branch"
          title={
            <h2>{objectsOnly ? "No Dataset Lineage" : "No Lineage Yet"}</h2>
          }
          body={
            <p>
              {objectsOnly
                ? "No dataset lineage edges have been recorded yet. Materialize features or emit OpenLineage events that include datasets."
                : "No OpenLineage events yet. Apply features and run materialization so datasets, jobs, and runs appear here. Toggle 'Feast Only Lineage' above to view registry-based lineage."}
            </p>
          }
        />
      </EuiPanel>
    );
  }

  const title =
    viewMode === "objects"
      ? "Dataset Lineage"
      : viewMode === "lineage"
        ? "Lineage"
        : "OpenLineage Graph";

  return (
    <EuiPanel>
      <div
        style={{
          display: "flex",
          justifyContent: "space-between",
          alignItems: "flex-start",
          gap: 16,
        }}
      >
        <div>
          <EuiTitle size="s">
            <h2>{title}</h2>
          </EuiTitle>
          {viewMode === "lineage" && (
            <p
              style={{
                margin: "6px 0 0",
                fontSize: 13,
                color: "#69707D",
                maxWidth: 640,
              }}
            >
              Datasets link by definition (source → feature view → feature
              service). Runtime transforms — materialize and compute engines
              like Spark — appear as dashed pills; click them for run history.
            </p>
          )}
        </div>
        {feastOnlyCheckbox && (
          <div style={{ display: "flex", gap: "20px", flexShrink: 0 }}>
            {feastOnlyCheckbox}
          </div>
        )}
      </div>
      <EuiSpacer size="m" />
      <EuiFlexGroup style={{ marginBottom: 16 }} wrap>
        {showTypeFilter && (
          <EuiFlexItem grow={false} style={{ width: 160 }}>
            <EuiFormRow label="OL type">
              <EuiSelect
                options={[
                  { value: "", text: "All" },
                  { value: "job", text: "Job" },
                  { value: "dataset", text: "Dataset" },
                ]}
                value={filterType}
                onChange={(e) => setFilterType(e.target.value)}
                aria-label="Filter by OL type"
              />
            </EuiFormRow>
          </EuiFlexItem>
        )}
        {feastObjectTypes.length > 0 && (
          <EuiFlexItem grow={false} style={{ width: 200 }}>
            <EuiFormRow label="Feast type">
              <EuiSelect
                options={[
                  { value: "", text: "All" },
                  ...feastObjectTypes.map((t) => ({
                    value: t,
                    text: feastTypeLabels[t] || t,
                  })),
                ]}
                value={filterFeastType}
                onChange={(e) => setFilterFeastType(e.target.value)}
                aria-label="Filter by Feast type"
              />
            </EuiFormRow>
          </EuiFlexItem>
        )}
        <EuiFlexItem grow={false} style={{ width: 200 }}>
          <EuiFormRow label="Producer">
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
        <EuiFlexItem grow={false} style={{ width: 280 }}>
          <EuiFormRow label="Focus on">
            <EuiSelect
              options={[
                { value: "", text: "All" },
                ...objectOptions.map((name) => ({ value: name, text: name })),
              ]}
              value={filterObject}
              onChange={(e) => setFilterObject(e.target.value)}
              aria-label="Focus on object"
            />
          </EuiFormRow>
        </EuiFlexItem>
      </EuiFlexGroup>
      <div
        ref={graphContainerRef}
        style={{
          display: "flex",
          height: isFullscreen ? "100vh" : 600,
          border: "1px solid #ddd",
          background: "#fff",
        }}
      >
        <div style={{ flex: 1, position: "relative" }}>
          <ReactFlow
            nodes={styledNodes}
            edges={styledEdges}
            onNodesChange={onNodesChange}
            onEdgesChange={onEdgesChange}
            nodeTypes={lineageNodeTypes}
            connectionLineType={ConnectionLineType.SmoothStep}
            fitView
            minZoom={0.1}
            maxZoom={8}
            onNodeMouseEnter={onNodeMouseEnter}
            onNodeMouseLeave={onNodeMouseLeave}
            onPaneClick={() => setSelectedNode(null)}
          >
            <Background color="#f0f0f0" gap={16} />
            <Controls>
              <ControlButton
                onClick={toggleFullscreen}
                title={isFullscreen ? "Exit fullscreen" : "Fullscreen"}
              >
                {isFullscreen ? "⊡" : "⛶"}
              </ControlButton>
            </Controls>
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
