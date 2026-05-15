import React, { useEffect } from "react";

interface GlobalSearchShortcutProps {
  onOpen: () => void;
}

const GlobalSearchShortcut: React.FC<GlobalSearchShortcutProps> = ({
  onOpen,
}) => {
  useEffect(() => {
    const handleKeyDown = (event: KeyboardEvent) => {
      if ((event.metaKey || event.ctrlKey) && event.key.toLowerCase() === "k") {
        event.preventDefault();
        event.stopPropagation();
        onOpen();
      }
    };

    window.addEventListener("keydown", handleKeyDown, true);
    return () => {
      window.removeEventListener("keydown", handleKeyDown, true);
    };
  }, [onOpen]);

  return null; // This component doesn't render anything
};

export default GlobalSearchShortcut;
