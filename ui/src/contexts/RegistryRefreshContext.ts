import React, { useContext } from "react";

interface RegistryRefreshContextInterface {
  refreshing: boolean;
  handleRefresh: () => Promise<void>;
}

const RegistryRefreshContext = React.createContext<
  RegistryRefreshContextInterface | undefined
>(undefined);

const useRegistryRefreshContext = () => {
  const ctx = useContext(RegistryRefreshContext);
  if (!ctx) {
    throw new Error(
      "useRegistryRefreshContext must be used within RegistryRefreshContext.Provider",
    );
  }
  return ctx;
};

export { RegistryRefreshContext, useRegistryRefreshContext };
