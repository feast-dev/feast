import React from "react";
import {
  EuiPageTemplate,
  EuiTabs,
  EuiTab,
  EuiSpacer,
  EuiTitle,
  EuiText,
  EuiSkeletonText,
} from "@elastic/eui";
import { useParams, useNavigate } from "react-router-dom";
import { useDocumentTitle } from "../../hooks/useDocumentTitle";
import "./styles.css";

const DocumentationIndex = () => {
  useDocumentTitle("Feast Documentation");
  const { projectName, tab = "cli" } = useParams();
  const navigate = useNavigate();

  const tabs = [
    {
      id: "cli",
      name: "CLI Reference",
      content: React.lazy(() => import("./CLIDocumentation")),
    },
    {
      id: "sdk",
      name: "SDK Reference",
      content: React.lazy(() => import("./SDKDocumentation")),
    },
    {
      id: "api",
      name: "API Reference",
      content: React.lazy(() => import("./APIDocumentation")),
    },
  ];

  const selectedTabId = tab;
  const selectedTabConfig = tabs.find((t) => t.id === selectedTabId) || tabs[0];
  const TabContent = selectedTabConfig.content;

  const onSelectedTabChanged = (id: string) => {
    navigate(`/p/${projectName}/documentation/${id}`);
  };

  const renderTabs = () => {
    return tabs.map((tab, index) => (
      <EuiTab
        key={index}
        onClick={() => onSelectedTabChanged(tab.id)}
        isSelected={tab.id === selectedTabId}
      >
        {tab.name}
      </EuiTab>
    ));
  };

  return (
    <EuiPageTemplate panelled>
      <EuiPageTemplate.Section>
        <EuiTitle size="l">
          <h1>Feast Documentation</h1>
        </EuiTitle>
        <EuiText>
          <p>Documentation for the Feast SDK, REST API, and CLI.</p>
        </EuiText>
        <EuiSpacer size="m" />
        <EuiTabs>{renderTabs()}</EuiTabs>
        <EuiSpacer size="m" />
        <React.Suspense fallback={<EuiSkeletonText lines={10} />}>
          <TabContent />
        </React.Suspense>
      </EuiPageTemplate.Section>
    </EuiPageTemplate>
  );
};

export default DocumentationIndex;
