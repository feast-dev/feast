import React, {
  useState,
  useRef,
  forwardRef,
  useImperativeHandle,
} from "react";
import {
  EuiText,
  EuiFieldSearch,
  EuiSpacer,
  EuiHorizontalRule,
  EuiPanel,
  EuiFlexGroup,
  EuiFlexItem,
  EuiBadge,
  EuiTitle,
} from "@elastic/eui";
import EuiCustomLink from "./EuiCustomLink";

/** @jsxImportSource @emotion/react */
import { css } from "@emotion/react";

const searchResultsStyles = {
  searchResults: css`
    margin-top: 8px;
  `,
  categoryGroup: css`
    margin-bottom: 8px;
  `,
  searchResultItem: css`
    padding: 8px 0;
    border-bottom: 1px solid #eee;
    &:last-child {
      border-bottom: none;
    }
  `,
  itemDescription: css`
    font-size: 0.85em;
    color: #666;
    margin-top: 4px;
  `,
};

interface RegistrySearchProps {
  categories: {
    name: string;
    data: any[];
    getLink: (item: any) => string;
  }[];
}

export interface RegistrySearchRef {
  focusSearchInput: () => void;
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

const RegistrySearch = forwardRef<RegistrySearchRef, RegistrySearchProps>(
  ({ categories }, ref) => {
    const [searchText, setSearchText] = useState("");
    const inputRef = useRef<HTMLInputElement | null>(null);

    const focusSearchInput = () => {
      if (inputRef.current) {
        inputRef.current.focus();
      }
    };

    useImperativeHandle(
      ref,
      () => ({
        focusSearchInput,
      }),
      [focusSearchInput],
    );

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

    return (
      <>
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
          compressed
          append={
            <EuiText size="xs" color="subdued">
              <span style={{ whiteSpace: "nowrap" }}>⌘K</span>
            </EuiText>
          }
        />
        <EuiSpacer size="s" />
        {searchText && (
          <div css={searchResultsStyles.searchResults}>
            <EuiText>
              <h4>Search Results</h4>
            </EuiText>
            <EuiSpacer size="xs" />
            {searchResults.filter((result) => result.items.length > 0).length >
            0 ? (
              searchResults
                .filter((result) => result.items.length > 0)
                .map((result) => (
                  <div
                    key={result.title}
                    css={searchResultsStyles.categoryGroup}
                  >
                    <EuiPanel hasBorder={true} paddingSize="m">
                      <EuiTitle size="xs">
                        <h3>
                          {result.title} ({result.items.length})
                        </h3>
                      </EuiTitle>
                      <EuiHorizontalRule margin="xs" />
                      {result.items.map((item) => (
                        <div
                          key={item.name}
                          css={searchResultsStyles.searchResultItem}
                        >
                          <EuiFlexGroup>
                            <EuiFlexItem>
                              <EuiCustomLink to={item.link}>
                                <strong>{item.name}</strong>
                              </EuiCustomLink>
                              {item.description && (
                                <div css={searchResultsStyles.itemDescription}>
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
        )}
      </>
    );
  },
);

export default RegistrySearch;
