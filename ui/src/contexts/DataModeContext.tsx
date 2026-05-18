import React, { useContext } from "react";

interface FetchOptions {
  headers?: Record<string, string>;
  credentials?: RequestCredentials;
}

interface DataModeConfig {
  fetchOptions?: FetchOptions;
}

const defaultConfig: DataModeConfig = {};

const DataModeContext = React.createContext<DataModeConfig>(defaultConfig);

const useDataMode = () => useContext(DataModeContext);

export default DataModeContext;
export { useDataMode };
export type { DataModeConfig, FetchOptions };
