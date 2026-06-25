import React, { useContext, useState } from "react";
import {
  EuiPageTemplate,
  EuiTitle,
  EuiSpacer,
  EuiSkeletonText,
  EuiEmptyPrompt,
  EuiButtonGroup,
} from "@elastic/eui";

import { useDocumentTitle } from "../../hooks/useDocumentTitle";
import useLoadRegistry from "../../queries/useLoadRegistry";
import RegistryPathContext from "../../contexts/RegistryPathContext";
import RegistryVisualizationTab from "../../components/RegistryVisualizationTab";
import { LineageGraph } from "../../components/OpenLineageGraph";
import LineageEventsList from "../../components/LineageEventsList";
import { useLoadOpenLineageGraph } from "../../queries/useLoadOpenLineageGraph";
import { useParams } from "react-router-dom";

type ActiveTab = "lineage" | "events";

const tabButtons = [
  { id: "lineage", label: "Lineage" },
  { id: "events", label: "Events" },
];

const LineagePage = () => {
  useDocumentTitle("Feast Lineage");
  const registryUrl = useContext(RegistryPathContext);
  const { projectName } = useParams<{ projectName: string }>();
  const { isLoading, isSuccess, isError, data } = useLoadRegistry(
    registryUrl,
    projectName,
  );

  const [activeTab, setActiveTab] = useState<ActiveTab>("lineage");
  const [registryOnly, setRegistryOnly] = useState(false);

  const olGraphQuery = useLoadOpenLineageGraph();

  const olConsumerAvailable =
    !olGraphQuery.isError && olGraphQuery.data !== undefined;

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

        {isSuccess && (
          <>
            {olConsumerAvailable ? (
              <>
                <EuiButtonGroup
                  legend="Select lineage tab"
                  options={tabButtons}
                  idSelected={activeTab}
                  onChange={(id) => setActiveTab(id as ActiveTab)}
                  buttonSize="m"
                  isFullWidth={false}
                />
                <EuiSpacer size="l" />

                {activeTab === "lineage" && (
                  <>
                    {registryOnly ? (
                      <RegistryVisualizationTab
                        feastOnlyCheckbox={
                          <label>
                            <input
                              type="checkbox"
                              checked={registryOnly}
                              onChange={(e) =>
                                setRegistryOnly(e.target.checked)
                              }
                            />
                            {" Feast Only Lineage"}
                          </label>
                        }
                      />
                    ) : (
                      <LineageGraph
                        olData={olGraphQuery.data}
                        olLoading={olGraphQuery.isLoading}
                        olError={olGraphQuery.isError}
                        feastOnlyCheckbox={
                          <label>
                            <input
                              type="checkbox"
                              checked={registryOnly}
                              onChange={(e) =>
                                setRegistryOnly(e.target.checked)
                              }
                            />
                            {" Feast Only Lineage"}
                          </label>
                        }
                      />
                    )}
                  </>
                )}

                {activeTab === "events" && <LineageEventsList />}
              </>
            ) : (
              <RegistryVisualizationTab />
            )}
          </>
        )}
      </EuiPageTemplate.Section>
    </EuiPageTemplate>
  );
};

export default LineagePage;
