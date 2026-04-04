import React, { useContext } from "react";

type DataMode = "proto" | "rest" | "rest-external";

interface FetchOptions {
  headers?: Record<string, string>;
  credentials?: RequestCredentials;
}

interface DataModeConfig {
  mode: DataMode;
  fetchOptions?: FetchOptions;
}

const defaultConfig: DataModeConfig = {
  mode: "proto",
};

const DataModeContext = React.createContext<DataModeConfig>(defaultConfig);

const useDataMode = () => useContext(DataModeContext);

export default DataModeContext;
export { useDataMode };
export type { DataMode, DataModeConfig, FetchOptions };
