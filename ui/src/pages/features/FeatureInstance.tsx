import React from "react";
import { Route, Routes, useNavigate, useParams } from "react-router-dom";
import { EuiPageTemplate } from "@elastic/eui";

import { FeatureIcon } from "../../graphics/FeatureIcon";
import { useMatchExact, useMatchSubpath } from "../../hooks/useMatchSubpath";
import FeatureOverviewTab from "./FeatureOverviewTab";
import FeatureMonitoringTab from "./FeatureMonitoringTab";
import { useDocumentTitle } from "../../hooks/useDocumentTitle";
import {
  useFeatureCustomTabs,
  useFeatureCustomTabRoutes,
} from "../../custom-tabs/TabsRegistryContext";

const FeatureInstance = () => {
  const navigate = useNavigate();
  let { FeatureViewName, FeatureName } = useParams();

  const { customNavigationTabs } = useFeatureCustomTabs(navigate);
  const CustomTabRoutes = useFeatureCustomTabRoutes();

  useDocumentTitle(`${FeatureName} | ${FeatureViewName} | Feast`);

  return (
    <EuiPageTemplate panelled>
      <EuiPageTemplate.Header
        restrictWidth
        iconType={FeatureIcon}
        pageTitle={`Feature: ${FeatureName}`}
        tabs={[
          {
            label: "Overview",
            isSelected: useMatchExact(""),
            onClick: () => {
              navigate("");
            },
          },
          {
            label: "Monitoring",
            isSelected: useMatchSubpath("monitoring"),
            onClick: () => {
              navigate("monitoring");
            },
          },
          ...customNavigationTabs,
        ]}
      />
      <EuiPageTemplate.Section>
        <Routes>
          <Route path="/" element={<FeatureOverviewTab />} />
          <Route path="/monitoring" element={<FeatureMonitoringTab />} />
          {CustomTabRoutes}
        </Routes>
      </EuiPageTemplate.Section>
    </EuiPageTemplate>
  );
};

export default FeatureInstance;
