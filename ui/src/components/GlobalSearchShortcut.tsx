import React, { useEffect } from "react";

interface GlobalSearchShortcutProps {
  onOpen: () => void;
}

const GlobalSearchShortcut: React.FC<GlobalSearchShortcutProps> = ({
  onOpen,
}) => {
  useEffect(() => {
    const handleKeyDown = (event: KeyboardEvent) => {
      if ((event.metaKey || event.ctrlKey) && event.key === "k") {
        event.preventDefault();
        onOpen();
      }
    };

    document.addEventListener("keydown", handleKeyDown);
    return () => {
      document.removeEventListener("keydown", handleKeyDown);
    };
  }, [onOpen]);

  return null; // This component doesn't render anything
};

export default GlobalSearchShortcut;
