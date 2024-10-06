import React from "react";
import { Route, Routes, useNavigate, useParams } from "react-router-dom";
import { EuiPageTemplate } from "@elastic/eui";

import { FeatureServiceIcon } from "../../graphics/FeatureServiceIcon";
import { useMatchExact } from "../../hooks/useMatchSubpath";
import FeatureServiceOverviewTab from "./FeatureServiceOverviewTab";
import { useDocumentTitle } from "../../hooks/useDocumentTitle";

import {
  useFeatureServiceCustomTabs,
  useFeatureServiceCustomTabRoutes,
} from "../../custom-tabs/TabsRegistryContext";

const FeatureServiceInstance = () => {
  const navigate = useNavigate();
  let { featureServiceName } = useParams();

  useDocumentTitle(`${featureServiceName} | Feature Service | Feast`);

  const { customNavigationTabs } = useFeatureServiceCustomTabs(navigate);
  const CustomTabRoutes = useFeatureServiceCustomTabRoutes();

  return (
    <EuiPageTemplate panelled>
      <EuiPageTemplate.Header
        restrictWidth
        iconType={FeatureServiceIcon}
        pageTitle={`Feature Service: ${featureServiceName}`}
        tabs={[
          {
            label: "Overview",
            isSelected: useMatchExact(""),
            onClick: () => {
              navigate("");
            },
          },
          ...customNavigationTabs,
        ]}
      />
      <EuiPageTemplate.Section>
        <Routes>
          <Route path="/" element={<FeatureServiceOverviewTab />} />
          {CustomTabRoutes}
        </Routes>
      </EuiPageTemplate.Section>
    </EuiPageTemplate>
  );
};

export default FeatureServiceInstance;
