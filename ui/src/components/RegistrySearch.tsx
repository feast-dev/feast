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
} from "@elastic/eui";
import EuiCustomLink from "./EuiCustomLink";

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

      return { name, items: filteredItems, getLink };
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
        />
        <EuiSpacer size="s" />
        {searchText && (
          <>
            <EuiText>
              <h4>Search Results</h4>
            </EuiText>
            <EuiSpacer size="xs" />
            {searchResults.some(({ items }) => items.length > 0) ? (
              <div className="euiPanel euiPanel--borderRadiusMedium euiPanel--plain euiPanel--hasShadow">
                {searchResults.map(({ name, items, getLink }, index) =>
                  items.length > 0 ? (
                    <div key={index} className="euiPanel__body">
                      <EuiText>
                        <h5>{name}</h5>
                      </EuiText>
                      <EuiSpacer size="xs" />
                      <ul
                        style={{ listStyleType: "none", padding: 0, margin: 0 }}
                      >
                        {items.map((item, idx) => {
                          const itemName =
                            "name" in item
                              ? item.name
                              : "spec" in item
                                ? item.spec?.name
                                : "Unknown";

                          const itemLink = getLink(item);

                          return (
                            <li key={idx} style={{ margin: "8px 0" }}>
                              <EuiCustomLink to={itemLink}>
                                {itemName}
                              </EuiCustomLink>
                            </li>
                          );
                        })}
                      </ul>
                      {index <
                        searchResults.filter(
                          (result) => result.items.length > 0,
                        ).length -
                          1 && <EuiHorizontalRule margin="m" />}
                    </div>
                  ) : null,
                )}
              </div>
            ) : (
              <div className="euiPanel euiPanel--borderRadiusMedium euiPanel--plain euiPanel--hasShadow">
                <div className="euiPanel__body">
                  <p>No matches found.</p>
                </div>
              </div>
            )}
          </>
        )}
      </>
    );
  },
);

export default RegistrySearch;
