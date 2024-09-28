import React from "react";
import { Route, Routes, useNavigate, useParams } from "react-router-dom";
import { EuiPageTemplate } from "@elastic/eui";

import { DatasetIcon } from "../../graphics/DatasetIcon";

import { useMatchExact, useMatchSubpath } from "../../hooks/useMatchSubpath";
import DatasetOverviewTab from "./DatasetOverviewTab";
import { useDocumentTitle } from "../../hooks/useDocumentTitle";
import DatasetExpectationsTab from "./DatasetExpectationsTab";
import {
  useDatasetCustomTabs,
  useDataSourceCustomTabRoutes,
} from "../../custom-tabs/TabsRegistryContext";

const DatasetInstance = () => {
  const navigate = useNavigate();
  let { datasetName } = useParams();

  useDocumentTitle(`${datasetName} | Saved Datasets | Feast`);

  const { customNavigationTabs } = useDatasetCustomTabs(navigate);
  const CustomTabRoutes = useDataSourceCustomTabRoutes();

  return (
    <EuiPageTemplate panelled>
      <EuiPageTemplate.Header
        restrictWidth
        iconType={DatasetIcon}
        pageTitle={`Entity: ${datasetName}`}
        tabs={[
          {
            label: "Overview",
            isSelected: useMatchExact(""),
            onClick: () => {
              navigate("");
            },
          },
          {
            label: "Expectations",
            isSelected: useMatchSubpath("expectations"),
            onClick: () => {
              navigate("expectations");
            },
          },
          ...customNavigationTabs,
        ]}
      />
      <EuiPageTemplate.Section>
        <Routes>
          <Route path="/" element={<DatasetOverviewTab />} />
          <Route path="/expectations" element={<DatasetExpectationsTab />} />
          {CustomTabRoutes}
        </Routes>
      </EuiPageTemplate.Section>
    </EuiPageTemplate>
  );
};

export default DatasetInstance;
