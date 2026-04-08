import React, { useState } from "react";
import { useParams } from "react-router-dom";

import {
  EuiPageTemplate,
  EuiLoadingSpinner,
  EuiFlexGroup,
  EuiFlexItem,
  EuiTitle,
  EuiFieldSearch,
  EuiSpacer,
  EuiButton,
  EuiCallOut,
} from "@elastic/eui";

import DatasourcesListingTable from "./DataSourcesListingTable";
import { useDocumentTitle } from "../../hooks/useDocumentTitle";
import DataSourceIndexEmptyState from "./DataSourceIndexEmptyState";
import { DataSourceIcon } from "../../graphics/DataSourceIcon";
import { useSearchQuery } from "../../hooks/useSearchInputWithTags";
import { feast } from "../../protos";
import ExportButton from "../../components/ExportButton";
import DataSourceFormModal, {
  DataSourceFormData,
} from "../../components/DataSourceFormModal";
import { useUIVersion } from "../../contexts/UIVersionContext";
import useResourceQuery, {
  dataSourceListPath,
} from "../../queries/useResourceQuery";

const useLoadDatasources = () => {
  const { projectName } = useParams();
  return useResourceQuery<any[]>({
    resourceType: "data-sources-list",
    project: projectName,
    restPath: dataSourceListPath(projectName),
    restSelect: (d) => d.dataSources,
  });
};

const filterFn = (data: feast.core.IDataSource[], searchTokens: string[]) => {
  let filteredByTags = data;

  if (searchTokens.length) {
    return filteredByTags.filter((entry) => {
      return searchTokens.find((token) => {
        return (
          token.length >= 3 && entry.name && entry.name.indexOf(token) >= 0
        );
      });
    });
  }

  return filteredByTags;
};

const Index = () => {
  const { isLoading, isSuccess, isError, data } = useLoadDatasources();
  const { isV2 } = useUIVersion();
  const [isModalOpen, setIsModalOpen] = useState(false);
  const [successMessage, setSuccessMessage] = useState<string | null>(null);

  useDocumentTitle(`Data Sources | Feast`);

  const { searchString, searchTokens, setSearchString } = useSearchQuery();

  const filterResult = data ? filterFn(data, searchTokens) : data;

  const handleCreateSubmit = (formData: DataSourceFormData) => {
    console.log("Data source create payload:", formData);
    setIsModalOpen(false);
    setSuccessMessage(
      `Data source "${formData.name}" is ready to be created. Backend integration coming soon.`,
    );
    setTimeout(() => setSuccessMessage(null), 5000);
  };

  return (
    <EuiPageTemplate panelled>
      <EuiPageTemplate.Header
        restrictWidth
        iconType={DataSourceIcon}
        pageTitle="Data Sources"
        rightSideItems={[
          ...(isV2
            ? [
                <EuiButton
                  fill
                  iconType="plus"
                  onClick={() => setIsModalOpen(true)}
                  key="create"
                >
                  Create Data Source
                </EuiButton>,
              ]
            : []),
          <ExportButton
            data={filterResult ?? []}
            fileName="data_sources"
            formats={["json"]}
            key="export"
          />,
        ]}
      />
      <EuiPageTemplate.Section>
        {successMessage && (
          <>
            <EuiCallOut
              title={successMessage}
              color="success"
              iconType="check"
              size="s"
            />
            <EuiSpacer size="m" />
          </>
        )}
        {isLoading && (
          <p>
            <EuiLoadingSpinner size="m" /> Loading
          </p>
        )}
        {isError && <p>We encountered an error while loading.</p>}
        {isSuccess && !data && <DataSourceIndexEmptyState />}
        {isSuccess && data && data.length > 0 && filterResult && (
          <React.Fragment>
            <EuiFlexGroup>
              <EuiFlexItem grow={2}>
                <EuiTitle size="xs">
                  <h2>Search</h2>
                </EuiTitle>
                <EuiFieldSearch
                  value={searchString}
                  fullWidth={true}
                  onChange={(e) => {
                    setSearchString(e.target.value);
                  }}
                />
              </EuiFlexItem>
            </EuiFlexGroup>
            <EuiSpacer size="m" />
            <DatasourcesListingTable dataSources={filterResult} />
          </React.Fragment>
        )}
      </EuiPageTemplate.Section>

      {isModalOpen && (
        <DataSourceFormModal
          onClose={() => setIsModalOpen(false)}
          onSubmit={handleCreateSubmit}
        />
      )}
    </EuiPageTemplate>
  );
};

export default Index;
