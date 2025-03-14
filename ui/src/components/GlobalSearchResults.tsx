import React from "react";
import {
  EuiSelectable,
  EuiSelectableOption,
  EuiHighlight,
  EuiFlexGroup,
  EuiFlexItem,
  EuiText,
  EuiBadge
} from "@elastic/eui";
import { useNavigate } from "react-router-dom";
import { SearchResult } from "../hooks/useGlobalSearch";

interface GlobalSearchResultsProps {
  searchResults: SearchResult[];
  searchTerm: string;
  onResultSelected: () => void;
}

const GlobalSearchResults: React.FC<GlobalSearchResultsProps> = ({
  searchResults,
  searchTerm,
  onResultSelected
}) => {
  const navigate = useNavigate();
  
  const options: Array<EuiSelectableOption & { 
    url: string;
    type: string;
    description?: string;
  }> = searchResults.map((result) => ({
    label: result.name,
    key: result.id,
    url: result.url,
    type: result.type,
    description: result.description,
    prepend: getIconForType(result.type),
    append: <EuiBadge color="hollow">{result.type}</EuiBadge>,
    searchableLabel: `${result.name} ${result.type} ${result.description || ''}`
  }));
  
  const onChange = (newOptions: EuiSelectableOption[]) => {
    const selectedOption = newOptions.find(option => option.checked === 'on');
    if (selectedOption) {
      // Find the original option with the URL
      const originalOption = options.find(o => o.key === selectedOption.key);
      if (originalOption) {
        navigate(originalOption.url);
        onResultSelected();
      }
    }
  };
  
  if (searchResults.length === 0 && searchTerm.length >= 2) {
    return (
      <EuiText color="subdued" size="s" style={{ padding: '16px' }}>
        <p>No results found for "{searchTerm}"</p>
      </EuiText>
    );
  }
  
  if (searchTerm.length < 2) {
    return (
      <EuiText color="subdued" size="s" style={{ padding: '16px' }}>
        <p>Type at least 2 characters to search</p>
      </EuiText>
    );
  }
  
  return (
    <EuiSelectable
      options={options}
      onChange={onChange}
      singleSelection={true}
      searchable
      searchProps={{
        placeholder: 'Search',
        isClearable: false,
        autoFocus: true,
      }}
      renderOption={(option, searchValue) => {
        // Find the original option with additional properties
        const originalOption = options.find(o => o.key === option.key);
        
        return (
          <EuiFlexGroup gutterSize="s" alignItems="center">
            <EuiFlexItem grow={false}>{option.prepend}</EuiFlexItem>
            <EuiFlexItem>
              <EuiFlexGroup direction="column" gutterSize="xs">
                <EuiFlexItem>
                  <EuiHighlight search={searchValue}>{option.label}</EuiHighlight>
                </EuiFlexItem>
                {originalOption?.description && (
                  <EuiFlexItem>
                    <EuiText size="xs" color="subdued">
                      <EuiHighlight search={searchValue}>
                        {originalOption.description}
                      </EuiHighlight>
                    </EuiText>
                  </EuiFlexItem>
                )}
              </EuiFlexGroup>
            </EuiFlexItem>
            <EuiFlexItem grow={false}>{option.append}</EuiFlexItem>
          </EuiFlexGroup>
        );
      }}
    />
  );
};

// Helper function to get icon for each type
const getIconForType = (type: string) => {
  switch (type) {
    case "Feature View":
      return <span className="euiIcon" data-euiicon-type="FeatureViewIcon" />;
    case "On-Demand Feature View":
      return <span className="euiIcon" data-euiicon-type="FeatureViewIcon" />;
    case "Stream Feature View":
      return <span className="euiIcon" data-euiicon-type="FeatureViewIcon" />;
    case "Entity":
      return <span className="euiIcon" data-euiicon-type="EntityIcon" />;
    case "Data Source":
      return <span className="euiIcon" data-euiicon-type="DataSourceIcon" />;
    case "Feature Service":
      return <span className="euiIcon" data-euiicon-type="FeatureServiceIcon" />;
    case "Dataset":
      return <span className="euiIcon" data-euiicon-type="DatasetIcon" />;
    default:
      return <span className="euiIcon" data-euiicon-type="search" />;
  }
};

export default GlobalSearchResults;
