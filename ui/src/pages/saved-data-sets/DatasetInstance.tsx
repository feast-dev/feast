import React from "react";
import { Route, Routes, useNavigate, useParams } from "react-router-dom";
import {
  EuiPageHeader,
  EuiPageContent,
  EuiPageContentBody,
} from "@elastic/eui";

import { DatasetIcon32 } from "../../graphics/DatasetIcon";

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
    <React.Fragment>
      <EuiPageHeader
        restrictWidth
        iconType={DatasetIcon32}
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
      <EuiPageContent
        hasBorder={false}
        hasShadow={false}
        paddingSize="none"
        color="transparent"
        borderRadius="none"
      >
        <EuiPageContentBody>
          <Routes>
            <Route path="/" element={<DatasetOverviewTab />} />
            <Route path="/expectations" element={<DatasetExpectationsTab />} />
            {CustomTabRoutes}
          </Routes>
        </EuiPageContentBody>
      </EuiPageContent>
    </React.Fragment>
  );
};

export default DatasetInstance;
