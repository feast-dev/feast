import React from "react";
import { Route, Routes, useNavigate, useParams } from "react-router-dom";
import { EuiPageTemplate } from "@elastic/eui";

import { DataSourceIcon } from "../../graphics/DataSourceIcon";
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
    <EuiPageTemplate panelled>
      <EuiPageTemplate.Header
        restrictWidth
        iconType={DataSourceIcon}
        pageTitle={`Data Source: ${dataSourceName}`}
        tabs={tabs}
      />
      <EuiPageTemplate.Section>
        <Routes>
          <Route path="/" element={<DataSourceOverviewTab />} />
          <Route path="/raw-data" element={<DataSourceRawData />} />
          {CustomTabRoutes}
        </Routes>
      </EuiPageTemplate.Section>
    </EuiPageTemplate>
  );
};

export default DataSourceInstance;
