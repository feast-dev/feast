import React, { useCallback, useEffect, useState } from "react";
import {
  ReactFlow,
  Node,
  Edge,
  Controls,
  Background,
  useNodesState,
  useEdgesState,
  ConnectionLineType,
  Panel,
} from "reactflow";
import "reactflow/dist/style.css";
import dagre from "dagre";

import {
  EuiPanel,
  EuiTitle,
  EuiSpacer,
  EuiText,
  EuiLoadingSpinner,
} from "@elastic/eui";
import { FEAST_FCO_TYPES } from "../parsers/types";
import { EntityRelation } from "../parsers/parseEntityRelationships";
import { feast } from "../protos";

const nodeColors = {
  [FEAST_FCO_TYPES.dataSource]: "#D6EAF8", // Light blue
  [FEAST_FCO_TYPES.entity]: "#D5F5E3", // Light green
  [FEAST_FCO_TYPES.featureView]: "#FCF3CF", // Light yellow
  [FEAST_FCO_TYPES.featureService]: "#FADBD8", // Light red
};

const nodeWidth = 180;
const nodeHeight = 40;

interface NodeData {
  label: string;
  type: FEAST_FCO_TYPES;
  metadata: any;
}

const CustomNode = ({ data }: { data: NodeData }) => {
  return (
    <div
      style={{
        background: nodeColors[data.type] || "#ffffff",
        padding: 10,
        borderRadius: 5,
        width: nodeWidth - 20,
        height: nodeHeight - 20,
        border: "1px solid #ddd",
        textAlign: "center",
        display: "flex",
        flexDirection: "column",
        justifyContent: "center",
      }}
    >
      <div style={{ fontWeight: "bold" }}>{data.label}</div>
      <div style={{ fontSize: "0.8em" }}>{data.type}</div>
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
) => {
  const dagreGraph = new dagre.graphlib.Graph();
  dagreGraph.setDefaultEdgeLabel(() => ({}));
  dagreGraph.setGraph({ rankdir: direction });

  nodes.forEach((node) => {
    dagreGraph.setNode(node.id, { width: nodeWidth, height: nodeHeight });
  });

  edges.forEach((edge) => {
    dagreGraph.setEdge(edge.source, edge.target);
  });

  dagre.layout(dagreGraph);

  const layoutedNodes = nodes.map((node) => {
    const nodeWithPosition = dagreGraph.node(node.id);
    return {
      ...node,
      position: {
        x: nodeWithPosition.x - nodeWidth / 2,
        y: nodeWithPosition.y - nodeHeight / 2,
      },
    };
  });

  return { nodes: layoutedNodes, edges };
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
      target: `${targetPrefix}-${rel.target.name}`,
      animated: true,
      type: "smoothstep",
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
  const [direction, setDirection] = useState<"TB" | "LR">("TB");

  useEffect(() => {
    if (registryData && relationships) {
      setLoading(true);

      const { nodes: initialNodes, edges: initialEdges } = registryToFlow(
        registryData,
        [...relationships, ...indirectRelationships],
      );

      const { nodes: layoutedNodes, edges: layoutedEdges } =
        getLayoutedElements(initialNodes, initialEdges, direction);

      setNodes(layoutedNodes);
      setEdges(layoutedEdges);
      setLoading(false);
    }
  }, [registryData, relationships, indirectRelationships, direction]);

  const toggleDirection = useCallback(() => {
    setDirection((dir) => (dir === "TB" ? "LR" : "TB"));
  }, []);

  return (
    <EuiPanel>
      <EuiTitle size="s">
        <h2>Registry Visualization</h2>
      </EuiTitle>
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
          >
            <Background />
            <Controls />
            <Panel position="top-right">
              <button 
                onClick={toggleDirection}
                className="euiButton euiButton--primary"
                style={{
                  padding: "8px 12px",
                  backgroundColor: "#006BB4",
                  color: "white",
                  border: "none",
                  borderRadius: "4px",
                  cursor: "pointer",
                  fontSize: "14px",
                  fontWeight: "500",
                  boxShadow: "0 2px 2px -1px rgba(152, 162, 179, 0.3)"
                }}
              >
                {direction === "TB" ? "Horizontal Layout" : "Vertical Layout"}
              </button>
            </Panel>
          </ReactFlow>
        </div>
      )}

      <EuiSpacer size="m" />
      <EuiText size="s">
        <p>
          <strong>Legend:</strong>
        </p>
        <div style={{ display: "flex", gap: 10 }}>
          {Object.entries(nodeColors).map(([type, color]) => (
            <div
              key={type}
              style={{ display: "flex", alignItems: "center", marginRight: 15 }}
            >
              <div
                style={{
                  width: 20,
                  height: 20,
                  backgroundColor: color,
                  marginRight: 5,
                  border: "1px solid #ddd",
                }}
              />
              <span>{type}</span>
            </div>
          ))}
        </div>
      </EuiText>
    </EuiPanel>
  );
};

export default RegistryVisualization;
