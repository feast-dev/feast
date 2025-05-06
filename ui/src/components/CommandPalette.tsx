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
  EuiOverlayMask,
} from "@elastic/eui";
import EuiCustomLink from "./EuiCustomLink";

const commandPaletteStyles = {
  modal: {
    width: "600px",
    maxWidth: "90vw",
  },
  searchResults: {
    marginTop: "8px",
    maxHeight: "60vh",
    overflowY: "auto" as const,
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

  if (!isOpen) return null;

  return (
    <EuiOverlayMask>
      <EuiModal
        onClose={onClose}
        style={commandPaletteStyles.modal}
        onKeyDown={handleKeyDown}
      >
        <EuiModalHeader>
          <EuiModalHeaderTitle>Search Registry</EuiModalHeaderTitle>
        </EuiModalHeader>
        <EuiModalBody>
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
            <div style={commandPaletteStyles.searchResults as React.CSSProperties}>
              {searchResults.filter((result) => result.items.length > 0)
                .length > 0 ? (
                searchResults
                  .filter((result) => result.items.length > 0)
                  .map((result) => (
                    <div
                      key={result.title}
                      style={commandPaletteStyles.categoryGroup}
                    >
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
                            style={
                              idx === result.items.length - 1
                                ? commandPaletteStyles.searchResultItemLast as React.CSSProperties
                                : commandPaletteStyles.searchResultItem as React.CSSProperties
                            }
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
                                    style={commandPaletteStyles.itemDescription as React.CSSProperties}
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
        </EuiModalBody>
      </EuiModal>
    </EuiOverlayMask>
  );
};

export default CommandPalette;
