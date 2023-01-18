import React, { useContext } from "react";

import {
    EuiPageHeader,
    EuiPageContent,
    EuiPageContentBody,
    EuiLoadingSpinner, EuiFlexGroup, EuiFlexItem, EuiTitle, EuiFieldSearch, EuiSpacer,
} from "@elastic/eui";

import useLoadRegistry from "../../queries/useLoadRegistry";
import DatasourcesListingTable from "./DataSourcesListingTable";
import { useDocumentTitle } from "../../hooks/useDocumentTitle";
import RegistryPathContext from "../../contexts/RegistryPathContext";
import DataSourceIndexEmptyState from "./DataSourceIndexEmptyState";
import { DataSourceIcon32 } from "../../graphics/DataSourceIcon";
import { useSearchQuery} from "../../hooks/useSearchInputWithTags";
import { feast } from "../../protos";

const useLoadDatasources = () => {
  const registryUrl = useContext(RegistryPathContext);
  const registryQuery = useLoadRegistry(registryUrl);

  const data =
    registryQuery.data === undefined
      ? undefined
      : registryQuery.data.objects.dataSources;

  return {
    ...registryQuery,
    data,
  };
};

const filterFn = (data: feast.core.IDataSource[], searchTokens: string[]) => {
  let filteredByTags = data;

  if (searchTokens.length) {
    return filteredByTags.filter((entry) => {
      return searchTokens.find((token) => {
        return token.length >= 3 && entry.name && entry.name.indexOf(token) >= 0;
      });
    });
  }

  return filteredByTags;
};

const Index = () => {
  const { isLoading, isSuccess, isError, data } = useLoadDatasources();

  useDocumentTitle(`Data Sources | Feast`);

  const { searchString, searchTokens, setSearchString } = useSearchQuery();

    const filterResult = data
    ? filterFn(data, searchTokens)
    : data;


  return (
    <React.Fragment>
      <EuiPageHeader
        restrictWidth
        iconType={DataSourceIcon32}
        pageTitle="Data Sources"
      />
      <EuiPageContent
        hasBorder={false}
        hasShadow={false}
        paddingSize="none"
        color="transparent"
        borderRadius="none"
      >
        <EuiPageContentBody>
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
        </EuiPageContentBody>
      </EuiPageContent>
    </React.Fragment>
  );
};

export default Index;
