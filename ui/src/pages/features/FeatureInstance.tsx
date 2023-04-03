import React from "react";
import { Route, Routes, useNavigate, useParams } from "react-router-dom";
import {
  EuiPageHeader,
  EuiPageContent,
  EuiPageContentBody,
} from "@elastic/eui";

import { FeatureIcon32 } from "../../graphics/FeatureIcon";
import { useMatchExact } from "../../hooks/useMatchSubpath";
import FeatureOverviewTab from "./FeatureOverviewTab";
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
    <React.Fragment>
      <EuiPageHeader
        restrictWidth
        iconType={FeatureIcon32}
        pageTitle={`Feature: ${FeatureName}`}
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
            <Route path="/" element={<FeatureOverviewTab />} />
            {CustomTabRoutes}
          </Routes>
        </EuiPageContentBody>
      </EuiPageContent>
    </React.Fragment>
  );
};

export default FeatureInstance;
