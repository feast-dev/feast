import React, { useState } from "react";
import {
  EuiButton,
  EuiPopover,
  EuiContextMenuPanel,
  EuiContextMenuItem,
} from "@elastic/eui";

interface ExportButtonProps {
  data: any[];
  fileName: string;
  formats?: ("json" | "csv")[];
}

const ExportButton: React.FC<ExportButtonProps> = ({
  data,
  fileName,
  formats = ["json", "csv"],
}) => {
  const [isPopoverOpen, setIsPopoverOpen] = useState(false);

  const exportData = (format: "json" | "csv") => {
    let content = "";
    let mimeType = "";

    if (format === "json") {
      content = JSON.stringify(data, null, 2);
      mimeType = "application/json";
    } else {
      const headers = Object.keys(data[0] || {}).join(",") + "\n";
      const rows = data.map((item) => Object.values(item).join(",")).join("\n");
      content = headers + rows;
      mimeType = "text/csv";
    }

    const blob = new Blob([content], { type: mimeType });
    const link = document.createElement("a");
    link.href = URL.createObjectURL(blob);
    link.download = `${fileName}.${format}`;
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
  };

  const exportMenu = (
    <EuiContextMenuPanel
      items={formats.map((format) => (
        <EuiContextMenuItem key={format} onClick={() => exportData(format)}>
          Export {format.toUpperCase()}
        </EuiContextMenuItem>
      ))}
    />
  );

  return (
    <EuiPopover
      button={
        <EuiButton
          color="primary"
          fill
          onClick={() => setIsPopoverOpen(!isPopoverOpen)}
        >
          Export
        </EuiButton>
      }
      isOpen={isPopoverOpen}
      closePopover={() => setIsPopoverOpen(false)}
      panelPaddingSize="s"
    >
      {exportMenu}
    </EuiPopover>
  );
};
export default ExportButton;
