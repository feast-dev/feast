import React, { useRef, useEffect } from "react";
import {
  EuiModal,
  EuiModalHeader,
  EuiModalHeaderTitle,
  EuiModalBody,
  EuiFieldSearch,
  EuiText,
  EuiSpacer,
  EuiHorizontalRule,
  EuiPanel,
  EuiFlexGroup,
  EuiFlexItem,
  EuiBadge,
  EuiTitle,
} from "@elastic/eui";
import EuiCustomLink from "./EuiCustomLink";

const commandPaletteStyles = {
  modal: {
    width: "600px",
    maxWidth: "90vw",
    maxHeight: "80vh", // Limit modal height to prevent it from going off-screen
    position: "fixed",
    top: "50%",
    left: "50%",
    transform: "translate(-50%, -50%)",
    zIndex: 1000,
  },
  modalBody: {
    padding: "0 16px 16px", // Add padding to prevent content from touching the edges
    maxHeight: "calc(80vh - 80px)", // Account for header height
    overflowY: "auto" as const, // Single scrollable element
  },
  searchResults: {
    marginTop: "8px",
    height: "auto", // Let content determine height
    overflowY: "visible" as const, // Remove nested scrolling
  },
  categoryGroup: {
    marginBottom: "8px",
  },
  searchResultItem: {
    padding: "8px 0",
    borderBottom: "1px solid #eee",
  },
  searchResultItemLast: {
    padding: "8px 0",
    borderBottom: "none",
  },
  itemDescription: {
    fontSize: "0.85em",
    color: "#666",
    marginTop: "4px",
  },
};

interface CommandPaletteProps {
  isOpen: boolean;
  onClose: () => void;
  categories: {
    name: string;
    data: any[];
    getLink: (item: any) => string;
  }[];
}

const getItemType = (item: any, category: string): string | undefined => {
  if (category === "Features" && "valueType" in item) {
    return item.valueType;
  }
  if (category === "Feature Views" && "type" in item) {
    return item.type;
  }
  return undefined;
};

const CommandPalette: React.FC<CommandPaletteProps> = ({
  isOpen,
  onClose,
  categories,
}) => {
  const [searchText, setSearchText] = React.useState("");
  const inputRef = useRef<HTMLInputElement | null>(null);

  useEffect(() => {
    if (isOpen && inputRef.current) {
      setTimeout(() => {
        inputRef.current?.focus();
      }, 100);
    }
  }, [isOpen]);

  useEffect(() => {
    if (!isOpen) {
      setSearchText("");
    }
  }, [isOpen]);

  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === "Escape") {
      onClose();
    }
  };

  const searchResults = categories.map(({ name, data, getLink }) => {
    const filteredItems = searchText
      ? data.filter((item) => {
          const itemName =
            "name" in item
              ? String(item.name)
              : "spec" in item && item.spec && "name" in item.spec
                ? String(item.spec.name ?? "Unknown")
                : "Unknown";

          return itemName.toLowerCase().includes(searchText.toLowerCase());
        })
      : [];

    const items = filteredItems.map((item) => {
      const itemName =
        "name" in item
          ? String(item.name)
          : "spec" in item && item.spec && "name" in item.spec
            ? String(item.spec.name ?? "Unknown")
            : "Unknown";

      return {
        name: itemName,
        link: getLink(item),
        description:
          "spec" in item && item.spec && "description" in item.spec
            ? String(item.spec.description || "")
            : "",
        type: getItemType(item, name),
      };
    });

    return {
      title: name,
      items,
    };
  });

  console.log("CommandPalette isOpen:", isOpen); // Debug log

  if (!isOpen) return null;

  return (
    <div
      style={{
        position: "fixed",
        top: 0,
        left: 0,
        right: 0,
        bottom: 0,
        backgroundColor: "rgba(0, 0, 0, 0.7)",
        zIndex: 9999,
        display: "flex",
        alignItems: "center",
        justifyContent: "center",
      }}
      onClick={onClose}
    >
      <div
        style={
          {
            width: "600px",
            maxWidth: "90vw",
            maxHeight: "80vh",
            backgroundColor: "white",
            borderRadius: "8px",
            boxShadow: "0 4px 12px rgba(0, 0, 0, 0.15)",
            overflow: "hidden",
            display: "flex",
            flexDirection: "column",
          } as React.CSSProperties
        }
        onClick={(e) => e.stopPropagation()}
        onKeyDown={handleKeyDown}
      >
        <div style={{ padding: "16px", borderBottom: "1px solid #D3DAE6" }}>
          <h2 style={{ margin: 0 }}>Search Registry</h2>
        </div>
        <div
          style={
            {
              padding: "0 16px 16px",
              maxHeight: "calc(80vh - 60px)",
              overflowY: "auto",
            } as React.CSSProperties
          }
        >
          <EuiFieldSearch
            placeholder="Search across Feature Views, Features, Entities, etc."
            value={searchText}
            onChange={(e) => setSearchText(e.target.value)}
            isClearable
            fullWidth
            inputRef={(node) => {
              inputRef.current = node;
            }}
            aria-label="Search registry"
            autoFocus
          />
          <EuiSpacer size="s" />
          {searchText ? (
            <div style={{ marginTop: "8px" }}>
              {searchResults.filter((result) => result.items.length > 0)
                .length > 0 ? (
                searchResults
                  .filter((result) => result.items.length > 0)
                  .map((result) => (
                    <div key={result.title} style={{ marginBottom: "8px" }}>
                      <EuiPanel hasBorder={true} paddingSize="m">
                        <EuiTitle size="xs">
                          <h3>
                            {result.title} ({result.items.length})
                          </h3>
                        </EuiTitle>
                        <EuiHorizontalRule margin="xs" />
                        {result.items.map((item, idx) => (
                          <div
                            key={item.name}
                            style={{
                              padding: "8px 0",
                              borderBottom:
                                idx === result.items.length - 1
                                  ? "none"
                                  : "1px solid #eee",
                            }}
                          >
                            <EuiFlexGroup>
                              <EuiFlexItem>
                                <EuiCustomLink
                                  to={item.link}
                                  onClick={() => {
                                    setSearchText("");
                                    onClose();
                                  }}
                                >
                                  <strong>{item.name}</strong>
                                </EuiCustomLink>
                                {item.description && (
                                  <div
                                    style={{
                                      fontSize: "0.85em",
                                      color: "#666",
                                      marginTop: "4px",
                                    }}
                                  >
                                    {item.description}
                                  </div>
                                )}
                              </EuiFlexItem>
                              {item.type && (
                                <EuiFlexItem grow={false}>
                                  <EuiBadge>{item.type}</EuiBadge>
                                </EuiFlexItem>
                              )}
                            </EuiFlexGroup>
                          </div>
                        ))}
                      </EuiPanel>
                      <EuiSpacer size="m" />
                    </div>
                  ))
              ) : (
                <EuiPanel hasBorder={true} paddingSize="m" color="subdued">
                  <EuiText textAlign="center">
                    <p>No matches found for "{searchText}"</p>
                  </EuiText>
                </EuiPanel>
              )}
            </div>
          ) : (
            <EuiText color="subdued" textAlign="center">
              <p>Start typing to search...</p>
            </EuiText>
          )}
        </div>
      </div>
    </div>
  );
};

export default CommandPalette;
