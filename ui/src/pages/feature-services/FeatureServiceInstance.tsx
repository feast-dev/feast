import React from "react";
import { Route, Routes, useNavigate, useParams } from "react-router-dom";
import {
  EuiPageHeader,
  EuiPageContent,
  EuiPageContentBody,
} from "@elastic/eui";

import FeatureServiceIcon from "../../feature-service.svg";
import { useMatchExact } from "../../hooks/useMatchSubpath";
import FeatureServiceOverviewTab from "./FeatureServiceOverviewTab";
import { useDocumentTitle } from "../../hooks/useDocumentTitle";

import {
  useFeatureServiceCustomTabs,
  featureServiceCustomTabRoutes,
} from "../CustomTabUtils";

const FeatureServiceInstance = () => {
  const navigate = useNavigate();
  let { featureServiceName } = useParams();

  useDocumentTitle(`${featureServiceName} | Feature Service | Feast`);

  const { customNavigationTabs } = useFeatureServiceCustomTabs(navigate);

  return (
    <React.Fragment>
      <EuiPageHeader
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
            {featureServiceCustomTabRoutes()}
          </Routes>
        </EuiPageContentBody>
      </EuiPageContent>
    </React.Fragment>
  );
};

export default FeatureServiceInstance;
