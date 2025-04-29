import React, { useEffect, useState } from "react";
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
import { EuiPanel, EuiTitle, EuiSpacer, EuiLoadingSpinner } from "@elastic/eui";
import { FEAST_FCO_TYPES } from "../parsers/types";
import { EntityRelation } from "../parsers/parseEntityRelationships";
import { feast } from "../protos";

const edgeAnimationStyle = `
  @keyframes dashdraw {
    0% {
      stroke-dashoffset: 10;
    }
    100% {
      stroke-dashoffset: 0;
    }
  }
  
  @keyframes dataflow {
    0% {
      stroke-dashoffset: 20;
      stroke: #999;
    }
    50% {
      stroke: #00cc00;
    }
    100% {
      stroke-dashoffset: 0;
      stroke: #999;
    }
  }
`;

const nodeWidth = 250;
const nodeHeight = 60;

interface NodeData {
  label: string;
  type: FEAST_FCO_TYPES;
  metadata: any;
}

const getNodeColor = (type: FEAST_FCO_TYPES) => {
  switch (type) {
    case FEAST_FCO_TYPES.featureService:
      return "#0066cc"; // Blue
    case FEAST_FCO_TYPES.featureView:
      return "#009900"; // Green
    case FEAST_FCO_TYPES.entity:
      return "#ff8000"; // Orange
    case FEAST_FCO_TYPES.dataSource:
      return "#cc0000"; // Red
    default:
      return "#666666"; // Gray
  }
};

const getLightNodeColor = (type: FEAST_FCO_TYPES) => {
  switch (type) {
    case FEAST_FCO_TYPES.featureService:
      return "#e6f3ff"; // Light blue
    case FEAST_FCO_TYPES.featureView:
      return "#e6ffe6"; // Light green
    case FEAST_FCO_TYPES.entity:
      return "#fff2e6"; // Light orange
    case FEAST_FCO_TYPES.dataSource:
      return "#ffe6e6"; // Light red
    default:
      return "#f0f0f0"; // Light gray
  }
};

const getNodeIcon = (type: FEAST_FCO_TYPES) => {
  switch (type) {
    case FEAST_FCO_TYPES.featureService:
      return "●"; // Circle for feature service
    case FEAST_FCO_TYPES.featureView:
      return "■"; // Square for feature view
    case FEAST_FCO_TYPES.entity:
      return "▲"; // Triangle for entity
    case FEAST_FCO_TYPES.dataSource:
      return "◆"; // Diamond for data source
    default:
      return "●"; // Default circle
  }
};

const CustomNode = ({ data }: { data: NodeData }) => {
  const color = getNodeColor(data.type);
  const lightColor = getLightNodeColor(data.type);
  const icon = getNodeIcon(data.type);

  return (
    <div
      style={{
        background: lightColor,
        borderRadius: 8,
        width: nodeWidth,
        height: nodeHeight,
        border: `1px solid ${color}`,
        display: "flex",
        alignItems: "stretch",
        position: "relative",
        overflow: "hidden",
      }}
    >
      <Handle
        type="target"
        position={Position.Left}
        id="target"
        style={{ background: "#999", width: 10, height: 10 }}
      />
      <div
        style={{
          backgroundColor: color,
          width: "40px",
          display: "flex",
          alignItems: "center",
          justifyContent: "center",
          borderRight: `1px solid ${color}`,
        }}
      >
        <div
          style={{
            color: "#ffffff",
            fontSize: "20px",
          }}
        >
          {icon}
        </div>
      </div>
      <div
        style={{
          flex: 1,
          display: "flex",
          alignItems: "center",
          justifyContent: "center",
          padding: "0 10px",
          fontSize: "16px",
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

const nodeTypes = {
  custom: CustomNode,
};

const getLayoutedElements = (
  nodes: Node[],
  edges: Edge[],
  direction = "TB",
  showIsolatedNodes = false,
) => {
  // Identify connected and isolated nodes
  const connectedNodeIds = new Set<string>();
  edges.forEach((edge) => {
    connectedNodeIds.add(edge.source);
    connectedNodeIds.add(edge.target);
  });

  const connectedNodes = nodes.filter((node) => connectedNodeIds.has(node.id));
  const isolatedNodes = nodes.filter((node) => !connectedNodeIds.has(node.id));

  // Layout connected nodes with dagre
  const dagreGraph = new dagre.graphlib.Graph();
  dagreGraph.setDefaultEdgeLabel(() => ({}));
  dagreGraph.setGraph({
    rankdir: direction,
    nodesep: 100,
    ranksep: 150,
    marginx: 50,
    marginy: 50,
  });

  connectedNodes.forEach((node) => {
    dagreGraph.setNode(node.id, { width: nodeWidth, height: nodeHeight });
  });

  edges.forEach((edge) => {
    dagreGraph.setEdge(edge.source, edge.target);
  });

  dagre.layout(dagreGraph);

  // Position connected nodes according to dagre layout with type-specific adjustments
  const layoutedConnectedNodes = connectedNodes.map((node) => {
    const nodeWithPosition = dagreGraph.node(node.id);

    // Apply type-specific position adjustments
    let xOffset = 0;
    let yOffset = 0;

    if (node.data.type === FEAST_FCO_TYPES.dataSource) {
      // Move data sources to the left/top
      xOffset = direction === "LR" ? -200 : 0;
      yOffset = direction === "TB" ? -200 : 0;
    } else if (node.data.type === FEAST_FCO_TYPES.entity) {
      // Move entities to the right/bottom
      xOffset = direction === "LR" ? 100 : 0;
      yOffset = direction === "TB" ? 100 : 0;
    }

    return {
      ...node,
      position: {
        x: nodeWithPosition.x - nodeWidth / 2 + xOffset,
        y: nodeWithPosition.y - nodeHeight / 2 + yOffset,
      },
      sourcePosition: direction === "TB" ? Position.Bottom : Position.Right,
      targetPosition: direction === "TB" ? Position.Top : Position.Left,
    };
  });

  // If we don't want to show isolated nodes, just return connected nodes
  if (!showIsolatedNodes) {
    return {
      nodes: layoutedConnectedNodes,
      edges,
    };
  }

  // Rest of the function for handling isolated nodes
  let minX = Infinity,
    maxX = -Infinity,
    maxY = -Infinity,
    minY = Infinity;
  layoutedConnectedNodes.forEach((node) => {
    minX = Math.min(minX, node.position.x);
    maxX = Math.max(maxX, node.position.x + nodeWidth);
    minY = Math.min(minY, node.position.y);
    maxY = Math.max(maxY, node.position.y + nodeHeight);
  });

  // Default if graph is empty
  if (minX === Infinity) {
    minX = 0;
    minY = 0;
    maxX = 0;
    maxY = 0;
  }

  // Group isolated nodes by type
  const groupedIsolatedNodes: Record<FEAST_FCO_TYPES, Node[]> = {
    [FEAST_FCO_TYPES.dataSource]: [],
    [FEAST_FCO_TYPES.entity]: [],
    [FEAST_FCO_TYPES.featureView]: [],
    [FEAST_FCO_TYPES.featureService]: [],
  };

  isolatedNodes.forEach((node) => {
    const nodeType = node.data.type as FEAST_FCO_TYPES;
    if (Object.values(FEAST_FCO_TYPES).includes(nodeType)) {
      groupedIsolatedNodes[nodeType].push(node);
    } else {
      groupedIsolatedNodes[FEAST_FCO_TYPES.featureView].push(node);
    }
  });

  // Place isolated nodes, separated by type
  const layoutedIsolatedNodes: Node[] = [];
  const isolatedNodesPadding = 50;
  const isolatedNodesStartX = minX;
  let currentY = maxY + 200;
  const nodesPerRow = 3;

  Object.entries(groupedIsolatedNodes).forEach(([type, typeNodes]) => {
    if (typeNodes.length === 0) return;

    const layoutedTypeNodes = typeNodes.map((node, index) => {
      const row = Math.floor(index / nodesPerRow);
      const col = index % nodesPerRow;

      return {
        ...node,
        position: {
          x: isolatedNodesStartX + col * (nodeWidth + isolatedNodesPadding),
          y: currentY + row * (nodeHeight + isolatedNodesPadding),
        },
        sourcePosition: direction === "TB" ? Position.Bottom : Position.Right,
        targetPosition: direction === "TB" ? Position.Top : Position.Left,
      };
    });

    layoutedIsolatedNodes.push(...layoutedTypeNodes);
    // Add spacing between different types of nodes
    currentY +=
      Math.ceil(typeNodes.length / nodesPerRow) *
        (nodeHeight + isolatedNodesPadding) +
      100;
  });

  return {
    nodes: [...layoutedConnectedNodes, ...layoutedIsolatedNodes],
    edges,
  };
};
const Legend = () => {
  const types = [
    { type: FEAST_FCO_TYPES.featureService, label: "Feature Service" },
    { type: FEAST_FCO_TYPES.featureView, label: "Feature View" },
    { type: FEAST_FCO_TYPES.entity, label: "Entity" },
    { type: FEAST_FCO_TYPES.dataSource, label: "Data Source" },
  ];

  return (
    <div
      style={{
        position: "absolute",
        left: 10,
        top: 10,
        background: "white",
        border: "1px solid #ddd",
        borderRadius: 5,
        padding: 10,
        zIndex: 10,
        boxShadow: "0 2px 5px rgba(0,0,0,0.1)",
      }}
    >
      <div style={{ fontSize: 14, fontWeight: 600, marginBottom: 5 }}>
        Legend
      </div>
      {types.map((item) => (
        <div
          key={item.type}
          style={{ display: "flex", alignItems: "center", marginBottom: 5 }}
        >
          <div
            style={{
              width: 20,
              height: 20,
              backgroundColor: getNodeColor(item.type),
              borderRadius: 4,
              display: "flex",
              alignItems: "center",
              justifyContent: "center",
              marginRight: 8,
              color: "white",
              fontSize: 14,
            }}
          >
            {getNodeIcon(item.type)}
          </div>
          <div style={{ fontSize: 12 }}>{item.label}</div>
        </div>
      ))}
    </div>
  );
};

const registryToFlow = (
  objects: feast.core.Registry,
  relationships: EntityRelation[],
) => {
  const nodes: Node[] = [];
  const edges: Edge[] = [];

  objects.featureServices?.forEach((fs) => {
    nodes.push({
      id: `fs-${fs.spec?.name}`,
      type: "custom",
      data: {
        label: fs.spec?.name,
        type: FEAST_FCO_TYPES.featureService,
        metadata: fs,
      },
      position: { x: 0, y: 0 },
    });
  });

  objects.featureViews?.forEach((fv) => {
    nodes.push({
      id: `fv-${fv.spec?.name}`,
      type: "custom",
      data: {
        label: fv.spec?.name,
        type: FEAST_FCO_TYPES.featureView,
        metadata: fv,
      },
      position: { x: 0, y: 0 },
    });
  });

  objects.onDemandFeatureViews?.forEach((odfv) => {
    nodes.push({
      id: `odfv-${odfv.spec?.name}`,
      type: "custom",
      data: {
        label: odfv.spec?.name,
        type: FEAST_FCO_TYPES.featureView,
        metadata: odfv,
      },
      position: { x: 0, y: 0 },
    });
  });

  objects.streamFeatureViews?.forEach((sfv) => {
    nodes.push({
      id: `sfv-${sfv.spec?.name}`,
      type: "custom",
      data: {
        label: sfv.spec?.name,
        type: FEAST_FCO_TYPES.featureView,
        metadata: sfv,
      },
      position: { x: 0, y: 0 },
    });
  });

  objects.entities?.forEach((entity) => {
    nodes.push({
      id: `entity-${entity.spec?.name}`,
      type: "custom",
      data: {
        label: entity.spec?.name,
        type: FEAST_FCO_TYPES.entity,
        metadata: entity,
      },
      position: { x: 0, y: 0 },
    });
  });

  const dataSources = new Set<string>();

  objects.featureViews?.forEach((fv) => {
    if (fv.spec?.batchSource?.name) {
      dataSources.add(fv.spec.batchSource.name);
    }
  });

  objects.streamFeatureViews?.forEach((sfv) => {
    if (sfv.spec?.batchSource?.name) {
      dataSources.add(sfv.spec.batchSource.name);
    }
    if (sfv.spec?.streamSource?.name) {
      dataSources.add(sfv.spec.streamSource.name);
    }
  });

  Array.from(dataSources).forEach((dsName) => {
    nodes.push({
      id: `ds-${dsName}`,
      type: "custom",
      data: {
        label: dsName,
        type: FEAST_FCO_TYPES.dataSource,
        metadata: { name: dsName },
      },
      position: { x: 0, y: 0 },
    });
  });

  relationships.forEach((rel, index) => {
    const sourcePrefix = getNodePrefix(rel.source.type);
    const targetPrefix = getNodePrefix(rel.target.type);

    edges.push({
      id: `edge-${index}`,
      source: `${sourcePrefix}-${rel.source.name}`,
      sourceHandle: "source",
      target: `${targetPrefix}-${rel.target.name}`,
      targetHandle: "target",
      animated: true,
      style: {
        strokeWidth: 3,
        stroke: "#999",
        strokeDasharray: "10 5",
        animation: "dataflow 2s linear infinite",
      },
      type: "smoothstep",
      markerEnd: {
        type: MarkerType.ArrowClosed,
        width: 20,
        height: 20,
        color: "#999",
      },
    });
  });

  return { nodes, edges };
};

const getNodePrefix = (type: FEAST_FCO_TYPES) => {
  switch (type) {
    case FEAST_FCO_TYPES.featureService:
      return "fs";
    case FEAST_FCO_TYPES.featureView:
      return "fv";
    case FEAST_FCO_TYPES.entity:
      return "entity";
    case FEAST_FCO_TYPES.dataSource:
      return "ds";
    default:
      return "unknown";
  }
};

interface RegistryVisualizationProps {
  registryData: feast.core.Registry;
  relationships: EntityRelation[];
  indirectRelationships: EntityRelation[];
}

const RegistryVisualization: React.FC<RegistryVisualizationProps> = ({
  registryData,
  relationships,
  indirectRelationships,
}) => {
  const [nodes, setNodes, onNodesChange] = useNodesState([]);
  const [edges, setEdges, onEdgesChange] = useEdgesState([]);
  const [loading, setLoading] = useState(true);
  const [showIndirectRelationships, setShowIndirectRelationships] =
    useState(false);
  const [showIsolatedNodes, setShowIsolatedNodes] = useState(false);
  const direction = "LR";

  useEffect(() => {
    if (registryData && relationships) {
      setLoading(true);

      // Only include indirect relationships if the toggle is on
      const relationshipsToShow = showIndirectRelationships
        ? [...relationships, ...indirectRelationships]
        : relationships;

      // Filter out invalid relationships
      const validRelationships = relationshipsToShow.filter((rel) => {
        // Add additional validation as needed for your use case
        return rel.source && rel.target && rel.source.name && rel.target.name;
      });

      const { nodes: initialNodes, edges: initialEdges } = registryToFlow(
        registryData,
        validRelationships,
      );

      const { nodes: layoutedNodes, edges: layoutedEdges } =
        getLayoutedElements(
          initialNodes,
          initialEdges,
          direction,
          showIsolatedNodes,
        );

      setNodes(layoutedNodes);
      setEdges(layoutedEdges);
      setLoading(false);
    }
  }, [
    registryData,
    relationships,
    indirectRelationships,
    showIndirectRelationships,
    showIsolatedNodes,
    setNodes,
    setEdges,
  ]);

  return (
    <EuiPanel>
      <style>{edgeAnimationStyle}</style>
      <div
        style={{
          display: "flex",
          justifyContent: "space-between",
          alignItems: "center",
        }}
      >
        <EuiTitle size="s">
          <h2>Lineage</h2>
        </EuiTitle>
        <div style={{ display: "flex", gap: "20px" }}>
          <label>
            <input
              type="checkbox"
              checked={showIndirectRelationships}
              onChange={(e) => setShowIndirectRelationships(e.target.checked)}
            />
            {" Show Indirect Relationships"}
          </label>
          <label>
            <input
              type="checkbox"
              checked={showIsolatedNodes}
              onChange={(e) => setShowIsolatedNodes(e.target.checked)}
            />
            {" Show Objects Without Relationships"}
          </label>
        </div>
      </div>
      <EuiSpacer size="m" />

      {loading ? (
        <div style={{ display: "flex", justifyContent: "center", padding: 50 }}>
          <EuiLoadingSpinner size="xl" />
        </div>
      ) : (
        <div style={{ height: 600, border: "1px solid #ddd" }}>
          <ReactFlow
            nodes={nodes}
            edges={edges}
            onNodesChange={onNodesChange}
            onEdgesChange={onEdgesChange}
            nodeTypes={nodeTypes}
            connectionLineType={ConnectionLineType.SmoothStep}
            fitView
            minZoom={0.1}
            maxZoom={8}
          >
            <Background color="#f0f0f0" gap={16} />
            <Controls />
            <Legend />
          </ReactFlow>
        </div>
      )}
    </EuiPanel>
  );
};

export default RegistryVisualization;
