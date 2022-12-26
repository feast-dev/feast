import React from "react";
import { Route, Routes, useNavigate, useParams } from "react-router-dom";
import {
  EuiPageHeader,
  EuiPageContent,
  EuiPageContentBody,
} from "@elastic/eui";

import { DataSourceIcon32 } from "../../graphics/DataSourceIcon";
import { useMatchExact } from "../../hooks/useMatchSubpath";
import { useDocumentTitle } from "../../hooks/useDocumentTitle";
import DataSourceRawData from "./DataSourceRawData";
import DataSourceOverviewTab from "./DataSourceOverviewTab";

import {
  useDataSourceCustomTabs,
  useDataSourceCustomTabRoutes,
} from "../../custom-tabs/TabsRegistryContext";

const DataSourceInstance = () => {
  const navigate = useNavigate();
  let { dataSourceName } = useParams();

  useDocumentTitle(`${dataSourceName} | Data Source | Feast`);

  let tabs = [
    {
      label: "Overview",
      isSelected: useMatchExact(""),
      onClick: () => {
        navigate("");
      },
    },
  ];

  const { customNavigationTabs } = useDataSourceCustomTabs(navigate);
  tabs = tabs.concat(customNavigationTabs);

  const CustomTabRoutes = useDataSourceCustomTabRoutes();

  return (
    <React.Fragment>
      <EuiPageHeader
        restrictWidth
        iconType={DataSourceIcon32}
        pageTitle={`Data Source: ${dataSourceName}`}
        tabs={tabs}
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
            <Route path="/" element={<DataSourceOverviewTab />} />
            <Route path="/raw-data" element={<DataSourceRawData />} />
            {CustomTabRoutes}
          </Routes>
        </EuiPageContentBody>
      </EuiPageContent>
    </React.Fragment>
  );
};

export default DataSourceInstance;
