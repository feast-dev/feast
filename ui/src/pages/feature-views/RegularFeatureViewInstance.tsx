import React from "react";
import { Route, Routes, useNavigate } from "react-router";
import {
  EuiPageHeader,
  EuiPageContent,
  EuiPageContentBody,
} from "@elastic/eui";

import FeatureViewIcon from "../../feature-view.svg";
import { useMatchExact, useMatchSubpath } from "../../hooks/useMatchSubpath";
import { FeastFeatureViewType } from "../../parsers/feastFeatureViews";
import RegularFeatureViewOverviewTab from "./RegularFeatureViewOverviewTab";
import FeatureViewSummaryStatisticsTab from "./FeatureViewSummaryStatisticsTab";

import {
  useRegularFeatureViewCustomTabs,
  regularFeatureViewCustomTabRoutes,
} from "../CustomTabUtils";

interface RegularFeatureInstanceProps {
  data: FeastFeatureViewType;
}

const RegularFeatureInstance = ({ data }: RegularFeatureInstanceProps) => {
  const navigate = useNavigate();

  const { customNavigationTabs } = useRegularFeatureViewCustomTabs(navigate);

  return (
    <React.Fragment>
      <EuiPageHeader
        restrictWidth
        iconType={FeatureViewIcon}
        pageTitle={`${data.spec.name}`}
        tabs={[
          {
            label: "Overview",
            isSelected: useMatchExact(""),
            onClick: () => {
              navigate("");
            },
          },
          {
            label: "Statistics",
            isSelected: useMatchSubpath("statistics"),
            onClick: () => {
              navigate("statistics");
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
            <Route
              path="/"
              element={<RegularFeatureViewOverviewTab data={data} />}
            />
            <Route
              path="/statistics"
              element={<FeatureViewSummaryStatisticsTab />}
            />
            {regularFeatureViewCustomTabRoutes()}
          </Routes>
        </EuiPageContentBody>
      </EuiPageContent>
    </React.Fragment>
  );
};

export default RegularFeatureInstance;
