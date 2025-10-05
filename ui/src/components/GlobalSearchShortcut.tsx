import React, { useEffect } from "react";

interface GlobalSearchShortcutProps {
  onOpen: () => void;
}

const GlobalSearchShortcut: React.FC<GlobalSearchShortcutProps> = ({
  onOpen,
}) => {
  useEffect(() => {
    const handleKeyDown = (event: KeyboardEvent) => {
      console.log(
        "Key pressed:",
        event.key,
        "metaKey:",
        event.metaKey,
        "ctrlKey:",
        event.ctrlKey,
      );
      if ((event.metaKey || event.ctrlKey) && event.key.toLowerCase() === "k") {
        console.log("Cmd+K detected, preventing default and calling onOpen");
        event.preventDefault();
        event.stopPropagation();
        onOpen();
      }
    };

    console.log("Adding keydown event listener to window");
    window.addEventListener("keydown", handleKeyDown, true);
    return () => {
      window.removeEventListener("keydown", handleKeyDown, true);
    };
  }, [onOpen]);

  return null; // This component doesn't render anything
};

export default GlobalSearchShortcut;
