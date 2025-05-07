import React, { useEffect, useState } from "react";
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
  const { projectName, tab } = useParams();
  const navigate = useNavigate();
  const [activeTab, setActiveTab] = useState<string>("cli");

  useEffect(() => {
    if (tab && ["cli", "sdk", "api"].includes(tab)) {
      setActiveTab(tab);
    }
  }, [tab]);

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

  const selectedTabConfig = tabs.find((t) => t.id === activeTab) || tabs[0];
  const TabContent = selectedTabConfig.content;

  const onSelectedTabChanged = (id: string) => {
    setActiveTab(id);
    navigate(`/p/${projectName}/documentation/${id}`);
  };

  const renderTabs = () => {
    return tabs.map((tabItem, index) => (
      <EuiTab
        key={index}
        onClick={() => onSelectedTabChanged(tabItem.id)}
        isSelected={tabItem.id === activeTab}
      >
        {tabItem.name}
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
