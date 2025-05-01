import React, { createContext, useState, useContext, useEffect } from "react";

type ThemeMode = "light" | "dark";

interface ThemeContextType {
  colorMode: ThemeMode;
  setColorMode: (mode: ThemeMode) => void;
  toggleColorMode: () => void;
}

const ThemeContext = createContext<ThemeContextType>({
  colorMode: "light",
  setColorMode: () => {},
  toggleColorMode: () => {},
});

export const ThemeProvider: React.FC<{ children: React.ReactNode }> = ({ 
  children 
}) => {
  const [colorMode, setColorMode] = useState<ThemeMode>(() => {
    const savedTheme = localStorage.getItem("feast-theme");
    return (savedTheme === "dark" ? "dark" : "light") as ThemeMode;
  });

  useEffect(() => {
    localStorage.setItem("feast-theme", colorMode);
    
    if (colorMode === "dark") {
      document.body.classList.add("euiTheme--dark");
    } else {
      document.body.classList.remove("euiTheme--dark");
    }
  }, [colorMode]);

  const toggleColorMode = () => {
    setColorMode(prevMode => (prevMode === "light" ? "dark" : "light"));
  };

  return (
    <ThemeContext.Provider 
      value={{ colorMode, setColorMode, toggleColorMode }}
    >
      {children}
    </ThemeContext.Provider>
  );
};

export const useTheme = () => useContext(ThemeContext);

export default ThemeContext;
