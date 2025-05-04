import React, { useContext } from "react";
import {
  EuiPageTemplate,
  EuiTitle,
  EuiSpacer,
  EuiSkeletonText,
  EuiEmptyPrompt,
} from "@elastic/eui";

import { useDocumentTitle } from "../../hooks/useDocumentTitle";
import useLoadRegistry from "../../queries/useLoadRegistry";
import RegistryPathContext from "../../contexts/RegistryPathContext";
import RegistryVisualizationTab from "../../components/RegistryVisualizationTab";
import { useParams } from "react-router-dom";

const LineagePage = () => {
  useDocumentTitle("Feast Lineage");
  const registryUrl = useContext(RegistryPathContext);
  const { isLoading, isSuccess, isError, data } = useLoadRegistry(registryUrl);
  const { projectName } = useParams<{ projectName: string }>();

  return (
    <EuiPageTemplate panelled>
      <EuiPageTemplate.Section>
        <EuiTitle size="l">
          <h1>
            {isLoading && <EuiSkeletonText lines={1} />}
            {isSuccess && data?.project && `${data.project} Lineage`}
          </h1>
        </EuiTitle>
        <EuiSpacer />

        {isError && (
          <EuiEmptyPrompt
            iconType="alert"
            color="danger"
            title={<h2>Error Loading Project Configs</h2>}
            body={
              <p>
                There was an error loading the Project Configurations.
                Please check that <code>feature_store.yaml</code> file is
                available and well-formed.
              </p>
            }
          />
        )}

        {isSuccess && <RegistryVisualizationTab />}
      </EuiPageTemplate.Section>
    </EuiPageTemplate>
  );
};

export default LineagePage;
