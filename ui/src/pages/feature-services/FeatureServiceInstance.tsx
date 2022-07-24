import React from "react";
import { Route, Routes, useNavigate, useParams } from "react-router-dom";
import {
  EuiPageHeader,
  EuiPageContent,
  EuiPageContentBody,
} from "@elastic/eui";

import { FeatureServiceIcon32 } from "../../graphics/FeatureServiceIcon";
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
    <React.Fragment>
      <EuiPageHeader
        restrictWidth
        iconType={FeatureServiceIcon32}
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
      <EuiPageContent
        hasBorder={false}
        hasShadow={false}
        paddingSize="none"
        color="transparent"
        borderRadius="none"
      >
        <EuiPageContentBody>
          <Routes>
            <Route path="/" element={<FeatureServiceOverviewTab />} />
            {CustomTabRoutes}
          </Routes>
        </EuiPageContentBody>
      </EuiPageContent>
    </React.Fragment>
  );
};

export default FeatureServiceInstance;
