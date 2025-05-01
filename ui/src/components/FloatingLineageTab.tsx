import React, { useState } from "react";
import { EuiButtonIcon, EuiPanel, EuiTitle, EuiSpacer } from "@elastic/eui";
import RegistryVisualization from "./RegistryVisualization";
import useLoadRegistry from "../queries/useLoadRegistry";
import { useContext } from "react";
import RegistryPathContext from "../contexts/RegistryPathContext";

const FloatingLineageTab: React.FC = () => {
  const [isMinimized, setIsMinimized] = useState(true);
  const registryUrl = useContext(RegistryPathContext);
  const { isLoading, isSuccess, data } = useLoadRegistry(registryUrl);

  const toggleMinimized = () => {
    setIsMinimized(!isMinimized);
  };

  if (!registryUrl) {
    return null;
  }

  return (
    <div
      style={{
        position: "fixed",
        bottom: isMinimized ? "16px" : "16px",
        right: "16px",
        zIndex: 1000,
        width: isMinimized ? "auto" : "80%",
        height: isMinimized ? "auto" : "70%",
        transition: "all 0.3s ease-in-out",
        boxShadow: "0 0 10px rgba(0, 0, 0, 0.2)",
      }}
    >
      {isMinimized ? (
        <EuiButtonIcon
          display="base"
          size="m"
          color="primary"
          iconType="graphApp"
          aria-label="Open Lineage View"
          onClick={toggleMinimized}
          style={{
            width: "48px",
            height: "48px",
            borderRadius: "24px",
          }}
        />
      ) : (
        <EuiPanel
          paddingSize="m"
          style={{ height: "100%", overflow: "hidden" }}
        >
          <div
            style={{
              display: "flex",
              justifyContent: "space-between",
              alignItems: "center",
            }}
          >
            <EuiTitle size="s">
              <h2>Lineage View</h2>
            </EuiTitle>
            <EuiButtonIcon
              color="danger"
              iconType="cross"
              aria-label="Close Lineage View"
              onClick={toggleMinimized}
            />
          </div>
          <EuiSpacer size="s" />
          <div style={{ height: "calc(100% - 40px)", overflow: "auto" }}>
            {isSuccess && data && (
              <RegistryVisualization
                registryData={data}
                relationships={data.entityRelationships || []}
                indirectRelationships={data.indirectEntityRelationships || []}
              />
            )}
            {isLoading && <div>Loading...</div>}
          </div>
        </EuiPanel>
      )}
    </div>
  );
};

export default FloatingLineageTab;
