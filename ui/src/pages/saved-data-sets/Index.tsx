import React from "react";
import { useParams } from "react-router-dom";

import { EuiPageTemplate, EuiLoadingSpinner } from "@elastic/eui";

import { DatasetIcon } from "../../graphics/DatasetIcon";

import { useDocumentTitle } from "../../hooks/useDocumentTitle";
import DatasetsListingTable from "./DatasetsListingTable";
import DatasetsIndexEmptyState from "./DatasetsIndexEmptyState";
import useResourceQuery, {
  savedDatasetListPath,
} from "../../queries/useResourceQuery";

const useLoadSavedDataSets = () => {
  const { projectName } = useParams();
  return useResourceQuery<any[]>({
    resourceType: "saved-datasets-list",
    project: projectName,
    protoSelect: (d) => d.objects.savedDatasets,
    restPath: savedDatasetListPath(projectName),
    restSelect: (d) => d.savedDatasets,
  });
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
