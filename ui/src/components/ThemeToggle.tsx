import React from "react";
import { EuiButtonIcon, EuiToolTip, useGeneratedHtmlId } from "@elastic/eui";
import { useTheme } from "../contexts/ThemeContext";

const ThemeToggle: React.FC = () => {
  const { colorMode, toggleColorMode } = useTheme();
  const buttonId = useGeneratedHtmlId({ prefix: "themeToggle" });

  return (
    <EuiToolTip
      position="right"
      content={`Switch to ${colorMode === "light" ? "dark" : "light"} theme`}
    >
      <EuiButtonIcon
        id={buttonId}
        onClick={toggleColorMode}
        iconType={colorMode === "light" ? "moon" : "sun"}
        aria-label={`Switch to ${colorMode === "light" ? "dark" : "light"} theme`}
        color="text"
      />
    </EuiToolTip>
  );
};

export default ThemeToggle;
