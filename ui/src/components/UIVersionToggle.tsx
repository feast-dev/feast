import React from "react";
import { EuiButtonGroup } from "@elastic/eui";
import { useUIVersion } from "../contexts/UIVersionContext";

const VERSION_OPTIONS = [
  { id: "v1", label: "V1" },
  { id: "v2", label: "V2" },
];

const UIVersionToggle: React.FC = () => {
  const { uiVersion, setUIVersion } = useUIVersion();

  return (
    <EuiButtonGroup
      legend="UI version toggle"
      options={VERSION_OPTIONS}
      idSelected={uiVersion}
      onChange={(id) => setUIVersion(id as "v1" | "v2")}
      buttonSize="compressed"
      isFullWidth={false}
    />
  );
};

export default UIVersionToggle;
