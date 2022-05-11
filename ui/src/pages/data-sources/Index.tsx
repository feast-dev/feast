import React, { useContext } from "react";

import {
  EuiPageHeader,
  EuiPageContent,
  EuiPageContentBody,
  EuiLoadingSpinner,
} from "@elastic/eui";

import useLoadRegistry from "../../queries/useLoadRegistry";
import DatasourcesListingTable from "./DataSourcesListingTable";
import { useDocumentTitle } from "../../hooks/useDocumentTitle";
import RegistryPathContext from "../../contexts/RegistryPathContext";
import DataSourceIndexEmptyState from "./DataSourceIndexEmptyState";
import { DataSourceIcon32 } from "../../graphics/DataSourceIcon";

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

const Index = () => {
  const { isLoading, isSuccess, isError, data } = useLoadDatasources();

  useDocumentTitle(`Data Sources | Feast`);

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
          {isSuccess && data && <DatasourcesListingTable dataSources={data} />}
        </EuiPageContentBody>
      </EuiPageContent>
    </React.Fragment>
  );
};

export default Index;
