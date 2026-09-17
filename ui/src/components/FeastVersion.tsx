import React from "react";
import { EuiText } from "@elastic/eui";

import useFeastVersion from "../queries/useFeastVersion";

const FeastVersion = ({ registryPath }: { registryPath: string }) => {
  const { data, isError, isLoading } = useFeastVersion(registryPath);

  if (
    isLoading ||
    isError ||
    !data ||
    typeof data.feast_version !== "string" ||
    data.feast_version.length === 0
  ) {
    return null;
  }

  const label =
    data.feast_version === "unknown"
      ? "Feast version unknown"
      : `Feast v${data.feast_version}`;

  return (
    <EuiText size="xs" color="subdued">
      <span>{label}</span>
    </EuiText>
  );
};

export default FeastVersion;
