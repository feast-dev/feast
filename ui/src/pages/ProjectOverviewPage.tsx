import React, { useContext, useState } from "react";

import {
  EuiPageTemplate,
  EuiText,
  EuiFlexGroup,
  EuiFlexItem,
  EuiTitle,
  EuiSpacer,
  EuiSkeletonText,
  EuiEmptyPrompt,
  EuiTabs,
  EuiTab,
} from "@elastic/eui";

import { useDocumentTitle } from "../hooks/useDocumentTitle";
import ObjectsCountStats from "../components/ObjectsCountStats";
import ExplorePanel from "../components/ExplorePanel";
import useLoadRegistry from "../queries/useLoadRegistry";
import RegistryPathContext from "../contexts/RegistryPathContext";
import RegistryVisualizationTab from "../components/RegistryVisualizationTab";

const ProjectOverviewPage = () => {
  useDocumentTitle("Feast Home");
  const registryUrl = useContext(RegistryPathContext);
  const { isLoading, isSuccess, isError, data } = useLoadRegistry(registryUrl);
  
  const [selectedTabId, setSelectedTabId] = useState('overview');
  
  const tabs = [
    {
      id: 'overview',
      name: 'Overview',
      disabled: false,
    },
    {
      id: 'visualization',
      name: 'Registry Visualization',
      disabled: false,
    },
  ];
  
  const onSelectedTabChanged = (id: string) => {
    setSelectedTabId(id);
  };
  
  const renderTabs = () => {
    return tabs.map((tab, index) => (
      <EuiTab
        key={index}
        onClick={() => onSelectedTabChanged(tab.id)}
        isSelected={tab.id === selectedTabId}
        disabled={tab.disabled}
      >
        {tab.name}
      </EuiTab>
    ));
  };

  return (
    <EuiPageTemplate panelled>
      <EuiPageTemplate.Section>
        <EuiTitle size="l">
          <h1>
            {isLoading && <EuiSkeletonText lines={1} />}
            {isSuccess && data?.project && `Project: ${data.project}`}
          </h1>
        </EuiTitle>
        <EuiSpacer />
        
        <EuiTabs>{renderTabs()}</EuiTabs>
        <EuiSpacer />
        
        {selectedTabId === 'overview' && (
          <EuiFlexGroup>
            <EuiFlexItem grow={2}>
              {isLoading && <EuiSkeletonText lines={4} />}
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
              {isSuccess &&
                (data?.description ? (
                  <EuiText>
                    <pre>{data.description}</pre>
                  </EuiText>
                ) : (
                  <EuiText>
                    <p>
                      Welcome to your new Feast project. In this UI, you can see
                      Data Sources, Entities, Feature Views and Feature Services
                      registered in Feast.
                    </p>
                    <p>
                      It looks like this project already has some objects
                      registered. If you are new to this project, we suggest
                      starting by exploring the Feature Services, as they
                      represent the collection of Feature Views serving a
                      particular model.
                    </p>
                    <p>
                      <strong>Note</strong>: We encourage you to replace this
                      welcome message with more suitable content for your team.
                      You can do so by specifying a{" "}
                      <code>project_description</code> in your{" "}
                      <code>feature_store.yaml</code> file.
                    </p>
                  </EuiText>
                ))}
              <ObjectsCountStats />
            </EuiFlexItem>
            <EuiFlexItem grow={1}>
              <ExplorePanel />
            </EuiFlexItem>
          </EuiFlexGroup>
        )}
        
        {selectedTabId === 'visualization' && (
          <RegistryVisualizationTab />
        )}
      </EuiPageTemplate.Section>
    </EuiPageTemplate>
  );
};

export default ProjectOverviewPage;
