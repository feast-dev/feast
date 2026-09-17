import React from "react";
import { EuiText } from "@elastic/eui";

import useFeastVersion from "../queries/useFeastVersion";

const FeastVersion = ({ registryPath }: { registryPath: string }) => {
  const { data, isError, isLoading } = useFeastVersion(registryPath);

  if (
    isLoading ||
    isError ||
    !data ||
    typeof data.version !== "string" ||
    data.version.length === 0
  ) {
    return null;
  }

  const label =
    data.version === "unknown"
      ? "Feast version unknown"
      : `Feast v${data.version}`;

  return (
    <EuiText size="xs" color="subdued">
      <span>{label}</span>
    </EuiText>
  );
};

export default FeastVersion;
