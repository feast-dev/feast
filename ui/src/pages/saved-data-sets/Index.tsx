import React, { useContext } from "react";

import { EuiPageTemplate, EuiLoadingSpinner } from "@elastic/eui";

import { DatasetIcon } from "../../graphics/DatasetIcon";

import useLoadRegistry from "../../queries/useLoadRegistry";
import { useDocumentTitle } from "../../hooks/useDocumentTitle";
import RegistryPathContext from "../../contexts/RegistryPathContext";
import DatasetsListingTable from "./DatasetsListingTable";
import DatasetsIndexEmptyState from "./DatasetsIndexEmptyState";

const useLoadSavedDataSets = () => {
  const registryUrl = useContext(RegistryPathContext);
  const registryQuery = useLoadRegistry(registryUrl);

  const data =
    registryQuery.data === undefined
      ? undefined
      : registryQuery.data.objects.savedDatasets;

  return {
    ...registryQuery,
    data,
  };
};

const Index = () => {
  const { isLoading, isSuccess, isError, data } = useLoadSavedDataSets();

  useDocumentTitle(`Saved Datasets | Feast`);

  return (
    <EuiPageTemplate panelled>
      <EuiPageTemplate.Header
        restrictWidth
        iconType={DatasetIcon}
        pageTitle="Datasets"
      />
      <EuiPageTemplate.Section>
        {isLoading && (
          <p>
            <EuiLoadingSpinner size="m" /> Loading
          </p>
        )}
        {isError && <p>We encountered an error while loading.</p>}
        {isSuccess && data && <DatasetsListingTable datasets={data} />}
        {isSuccess && !data && <DatasetsIndexEmptyState />}
      </EuiPageTemplate.Section>
    </EuiPageTemplate>
  );
};

export default Index;
