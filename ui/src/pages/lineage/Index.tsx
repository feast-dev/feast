import React, { useContext, useState } from "react";
import {
  EuiPageTemplate,
  EuiTitle,
  EuiSpacer,
  EuiSkeletonText,
  EuiEmptyPrompt,
  EuiButtonGroup,
  EuiFlexGroup,
  EuiFlexItem,
  EuiSelect,
  EuiFormRow,
  EuiSwitch,
} from "@elastic/eui";

import { useDocumentTitle } from "../../hooks/useDocumentTitle";
import useLoadRegistry from "../../queries/useLoadRegistry";
import RegistryPathContext from "../../contexts/RegistryPathContext";
import RegistryVisualizationTab from "../../components/RegistryVisualizationTab";
import { LineageGraph } from "../../components/OpenLineageGraph";
import LineageEventsList from "../../components/LineageEventsList";
import LineageJobsList from "../../components/LineageJobsList";
import {
  useLoadOpenLineageGraph,
  useLoadNamespaces,
} from "../../queries/useLoadOpenLineageGraph";
import { useParams } from "react-router-dom";

type ActiveTab = "lineage" | "jobs" | "events";

const tabButtons = [
  { id: "lineage", label: "Lineage" },
  { id: "jobs", label: "Jobs" },
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
  const [registryOnly, setRegistryOnly] = useState<boolean | null>(null);
  const [selectedNamespace, setSelectedNamespace] = useState("");

  const { data: nsData } = useLoadNamespaces();
  const namespaces = nsData?.namespaces || [];

  const olGraphQuery = useLoadOpenLineageGraph({
    namespace: selectedNamespace || undefined,
  });

  const olConsumerAvailable =
    !olGraphQuery.isError && olGraphQuery.data !== undefined;

  const olHasData =
    olConsumerAvailable &&
    olGraphQuery.data != null &&
    (olGraphQuery.data.nodes?.length ?? 0) > 0;

  const effectiveRegistryOnly =
    registryOnly !== null ? registryOnly : !olHasData;

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
                <EuiFlexGroup
                  alignItems="center"
                  justifyContent="spaceBetween"
                  gutterSize="l"
                  responsive={false}
                >
                  <EuiFlexItem grow={false}>
                    {!effectiveRegistryOnly && (
                      <EuiFlexGroup
                        alignItems="center"
                        gutterSize="l"
                        responsive={false}
                      >
                        <EuiFlexItem grow={false}>
                          <EuiButtonGroup
                            legend="Select lineage tab"
                            options={tabButtons}
                            idSelected={activeTab}
                            onChange={(id) => setActiveTab(id as ActiveTab)}
                            buttonSize="m"
                            isFullWidth={false}
                          />
                        </EuiFlexItem>
                        {namespaces.length > 1 && (
                          <EuiFlexItem grow={false} style={{ width: 240 }}>
                            <EuiFormRow
                              label="Namespace"
                              display="columnCompressed"
                            >
                              <EuiSelect
                                options={[
                                  { value: "", text: "All namespaces" },
                                  ...namespaces.map((ns) => ({
                                    value: ns,
                                    text: ns,
                                  })),
                                ]}
                                value={selectedNamespace}
                                onChange={(e) =>
                                  setSelectedNamespace(e.target.value)
                                }
                                aria-label="Filter by namespace"
                              />
                            </EuiFormRow>
                          </EuiFlexItem>
                        )}
                      </EuiFlexGroup>
                    )}
                  </EuiFlexItem>
                  <EuiFlexItem grow={false}>
                    <EuiSwitch
                      label="Feast registry lineage"
                      checked={effectiveRegistryOnly}
                      onChange={(e) => setRegistryOnly(e.target.checked)}
                      compressed
                    />
                  </EuiFlexItem>
                </EuiFlexGroup>
                <EuiSpacer size="l" />

                {effectiveRegistryOnly ? (
                  <RegistryVisualizationTab />
                ) : (
                  <>
                    {activeTab === "lineage" && (
                      <LineageGraph
                        viewMode="lineage"
                        olData={olGraphQuery.data}
                        olLoading={olGraphQuery.isLoading}
                        olError={olGraphQuery.isError}
                      />
                    )}

                    {activeTab === "jobs" && <LineageJobsList />}

                    {activeTab === "events" && <LineageEventsList />}
                  </>
                )}
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
