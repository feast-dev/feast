import React from "react";

interface MonitoringConfig {
  apiBaseUrl: string;
  enabled: boolean;
}

const MonitoringContext = React.createContext<MonitoringConfig>({
  apiBaseUrl: "/api/v1",
  enabled: false,
});

export default MonitoringContext;
export type { MonitoringConfig };
