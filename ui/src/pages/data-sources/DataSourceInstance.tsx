import React from "react";
import { Route, Routes, useNavigate, useParams } from "react-router-dom";
import {
  EuiPageHeader,
  EuiPageContent,
  EuiPageContentBody,
} from "@elastic/eui";

import DataSourceIcon from "../../data-source-icon.svg";
import { useMatchExact, useMatchSubpath } from "../../hooks/useMatchSubpath";
import { useDocumentTitle } from "../../hooks/useDocumentTitle";
import DataSourceRawData from "./DataSourceRawData";
import DataSourceOverviewTab from "./DataSourceOverviewTab";
import DataSourceDbt from "./DataSourceDbt";
import useLoadDataSource from "./useLoadDataSource";

import {
  useDataSourceCustomTabs,
  dataSourceCustomTabRoutes,
} from "../CustomTabUtils";

const DataSourceInstance = () => {
  const navigate = useNavigate();
  let { dataSourceName } = useParams();

  useDocumentTitle(`${dataSourceName} | Data Source | Feast`);
  const dsName = dataSourceName === undefined ? "" : dataSourceName;
  const { isSuccess, data } = useLoadDataSource(dsName);

  let tabs = [
    {
      label: "Overview",
      isSelected: useMatchExact(""),
      onClick: () => {
        navigate("");
      },
    },
  ];

  const dbtTab = {
    label: "Dbt Definition",
    isSelected: useMatchSubpath("dbt"),
    onClick: () => {
      navigate("dbt");
    },
  };
  if (isSuccess && data?.bigqueryOptions?.dbtModelSerialized) {
    tabs.push(dbtTab);
  }

  const { customNavigationTabs } = useDataSourceCustomTabs(navigate);
  tabs = tabs.concat(customNavigationTabs);

  return (
    <React.Fragment>
      <EuiPageHeader
        restrictWidth
        iconType={DataSourceIcon}
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
            <Route path="/dbt" element={<DataSourceDbt />} />
            {dataSourceCustomTabRoutes()}
          </Routes>
        </EuiPageContentBody>
      </EuiPageContent>
    </React.Fragment>
  );
};

export default DataSourceInstance;
