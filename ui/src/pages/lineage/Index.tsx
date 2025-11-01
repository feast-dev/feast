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
  const { projectName } = useParams<{ projectName: string }>();
  const { isLoading, isSuccess, isError, data } = useLoadRegistry(
    registryUrl,
    projectName,
  );

  // Show message for "All Projects" view
  if (projectName === "all") {
    return (
      <EuiPageTemplate panelled>
        <EuiPageTemplate.Section>
          <EuiTitle size="l">
            <h1>Lineage Visualization</h1>
          </EuiTitle>
          <EuiSpacer />
          <EuiEmptyPrompt
            iconType="branch"
            title={<h2>Project Selection Required</h2>}
            body={
              <>
                <p>
                  Lineage visualization requires a specific project context to
                  show the relationships between Feature Views, Entities, and
                  Data Sources.
                </p>
                <p>
                  <strong>
                    Please select a specific project from the dropdown above
                  </strong>{" "}
                  to view its lineage graph.
                </p>
              </>
            }
          />
        </EuiPageTemplate.Section>
      </EuiPageTemplate>
    );
  }

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
                There was an error loading the Project Configurations. Please
                check that <code>feature_store.yaml</code> file is available and
                well-formed.
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
