import React from "react";
import { EuiButtonIcon, EuiToolTip } from "@elastic/eui";
import { useTheme } from "../contexts/ThemeContext";

const ThemeToggle: React.FC = () => {
  const { colorMode, toggleColorMode } = useTheme();

  return (
    <EuiToolTip
      content={`Switch to ${colorMode === "light" ? "dark" : "light"} mode`}
    >
      <EuiButtonIcon
        color="text"
        iconType={colorMode === "light" ? "moon" : "sun"}
        onClick={toggleColorMode}
        aria-label="Toggle theme"
      />
    </EuiToolTip>
  );
};

export default ThemeToggle;
