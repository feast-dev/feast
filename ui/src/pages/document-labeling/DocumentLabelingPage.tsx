import React, { useState } from "react";
import {
  EuiPage,
  EuiPageBody,
  EuiPageSection,
  EuiPageHeader,
  EuiTitle,
  EuiSpacer,
  EuiTabs,
  EuiTab,
} from "@elastic/eui";
import RagTab from "./RagTab";
import ClassificationTab from "./ClassificationTab";

const DocumentLabelingPage = () => {
  const [selectedTab, setSelectedTab] = useState("rag");

  const tabs = [
    {
      id: "rag",
      name: "RAG",
      content: <RagTab />,
    },
    {
      id: "classification",
      name: "Classification",
      content: <ClassificationTab />,
    },
  ];

  const selectedTabContent = tabs.find(
    (tab) => tab.id === selectedTab,
  )?.content;

  return (
    <EuiPage>
      <EuiPageBody>
        <EuiPageHeader>
          <EuiTitle size="l">
            <h1>Data Labeling</h1>
          </EuiTitle>
        </EuiPageHeader>

        <EuiPageSection>
          <EuiTabs>
            {tabs.map((tab) => (
              <EuiTab
                key={tab.id}
                onClick={() => setSelectedTab(tab.id)}
                isSelected={tab.id === selectedTab}
              >
                {tab.name}
              </EuiTab>
            ))}
          </EuiTabs>

          <EuiSpacer size="l" />

          {selectedTabContent}
        </EuiPageSection>
      </EuiPageBody>
    </EuiPage>
  );
};

export default DocumentLabelingPage;
