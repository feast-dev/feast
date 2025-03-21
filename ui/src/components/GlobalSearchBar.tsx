import React, { useState } from "react";
import {
  EuiFieldSearch,
  EuiInputPopover,
  EuiLoadingSpinner,
  EuiIcon
} from "@elastic/eui";
import useGlobalSearch from "../hooks/useGlobalSearch";
import GlobalSearchResults from "./GlobalSearchResults";

const GlobalSearchBar: React.FC = () => {
  const [searchTerm, setSearchTerm] = useState("");
  const [isPopoverOpen, setIsPopoverOpen] = useState(false);
  // We don't actually need to store the ref for now
  const { isLoading, searchResults } = useGlobalSearch(searchTerm);
  
  const handleSearchChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const value = e.target.value;
    setSearchTerm(value);
    setIsPopoverOpen(value.length > 0);
  };
  
  const handleClosePopover = () => {
    setIsPopoverOpen(false);
  };
  
  const handleResultSelected = () => {
    setSearchTerm("");
    setIsPopoverOpen(false);
  };
  
  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === 'Escape') {
      setIsPopoverOpen(false);
    }
  };
  
  const searchInput = (
    <EuiFieldSearch
      placeholder="Search registry..."
      value={searchTerm}
      onChange={handleSearchChange}
      onKeyDown={handleKeyDown}
      isClearable={true}
      fullWidth
      // No need for inputRef as we're not using it currently
      aria-label="Search registry"
      onFocus={() => setIsPopoverOpen(searchTerm.length > 0)}
      append={
        isLoading ? (
          <EuiLoadingSpinner size="m" />
        ) : (
          <EuiIcon type="search" />
        )
      }
    />
  );
  
  return (
    <EuiInputPopover
      input={searchInput}
      isOpen={isPopoverOpen}
      closePopover={handleClosePopover}
      panelPaddingSize="none"
      fullWidth
    >
      <div style={{ width: '100%', maxHeight: '80vh', overflow: 'auto' }}>
        <GlobalSearchResults
          searchResults={searchResults || []}
          searchTerm={searchTerm}
          onResultSelected={handleResultSelected}
        />
      </div>
    </EuiInputPopover>
  );
};

export default GlobalSearchBar;
