import React, { useState } from "react";
import { EuiText, EuiFieldSearch, EuiSpacer } from "@elastic/eui";
import EuiCustomLink from "./EuiCustomLink";

interface RegistrySearchProps {
  categories: {
    name: string;
    data: any[];
    getLink: (item: any) => string;
  }[];
}

const RegistrySearch: React.FC<RegistrySearchProps> = ({ categories }) => {
  const [searchText, setSearchText] = useState("");

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
      <EuiSpacer size="l" />
      <EuiText>
        <h3>Search in registry</h3>
      </EuiText>
      <EuiSpacer size="s" />
      <EuiFieldSearch
        placeholder="Search across Feature Views, Features, Entities, etc."
        value={searchText}
        onChange={(e) => setSearchText(e.target.value)}
        isClearable
        fullWidth
      />
      <EuiSpacer size="m" />

      {searchText && (
        <EuiText>
          <h3>Search Results</h3>
          {searchResults.some(({ items }) => items.length > 0) ? (
            searchResults.map(({ name, items, getLink }, index) =>
              items.length > 0 ? (
                <div key={index}>
                  <h4>{name}</h4>
                  <ul>
                    {items.map((item, idx) => {
                      const itemName =
                        "name" in item
                          ? item.name
                          : "spec" in item
                            ? item.spec?.name
                            : "Unknown";

                      const itemLink = getLink(item);

                      return (
                        <li key={idx}>
                          <EuiCustomLink to={itemLink}>
                            {itemName}
                          </EuiCustomLink>
                        </li>
                      );
                    })}
                  </ul>
                  <EuiSpacer size="m" />
                </div>
              ) : null,
            )
          ) : (
            <p>No matches found.</p>
          )}
        </EuiText>
      )}
    </>
  );
};

export default RegistrySearch;
