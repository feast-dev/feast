import React, { createContext, useState, useContext, useEffect } from "react";

type UIVersion = "v1" | "v2";

interface UIVersionContextType {
  uiVersion: UIVersion;
  isV2: boolean;
  setUIVersion: (version: UIVersion) => void;
  toggleVersion: () => void;
}

const UIVersionContext = createContext<UIVersionContextType>({
  uiVersion: "v1",
  isV2: false,
  setUIVersion: () => {},
  toggleVersion: () => {},
});

export const UIVersionProvider: React.FC<{ children: React.ReactNode }> = ({
  children,
}) => {
  const [uiVersion, setUIVersion] = useState<UIVersion>(() => {
    const saved = localStorage.getItem("feast-ui-version");
    return saved === "v2" ? "v2" : "v1";
  });

  useEffect(() => {
    localStorage.setItem("feast-ui-version", uiVersion);
  }, [uiVersion]);

  const toggleVersion = () => {
    setUIVersion((prev) => (prev === "v1" ? "v2" : "v1"));
  };

  return (
    <UIVersionContext.Provider
      value={{
        uiVersion,
        isV2: uiVersion === "v2",
        setUIVersion,
        toggleVersion,
      }}
    >
      {children}
    </UIVersionContext.Provider>
  );
};

export const useUIVersion = () => useContext(UIVersionContext);

export default UIVersionContext;
