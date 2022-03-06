import React from "react";
import { Route, Routes, useNavigate, useParams } from "react-router-dom";
import {
  EuiPageHeader,
  EuiPageContent,
  EuiPageContentBody,
} from "@elastic/eui";

import DatasetIcon from "../../dataset-icon.svg";

import { useMatchExact, useMatchSubpath } from "../../hooks/useMatchSubpath";
import DatasetOverviewTab from "./DatasetOverviewTab";
import { useDocumentTitle } from "../../hooks/useDocumentTitle";
import DatasetExpectationsTab from "./DatasetExpectationsTab";
import {
  datasetCustomTabRoutes,
  useDatasetCustomTabs,
} from "../CustomTabUtils";

const DatasetInstance = () => {
  const navigate = useNavigate();
  let { datasetName } = useParams();

  useDocumentTitle(`${datasetName} | Saved Datasets | Feast`);

  const { customNavigationTabs } = useDatasetCustomTabs(navigate);

  return (
    <React.Fragment>
      <EuiPageHeader
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
            {datasetCustomTabRoutes()}
          </Routes>
        </EuiPageContentBody>
      </EuiPageContent>
    </React.Fragment>
  );
};

export default DatasetInstance;
