import React from "react";
import { Route, Routes, useNavigate } from "react-router-dom";
import { useParams } from "react-router-dom";
import { EuiPageTemplate } from "@elastic/eui";

import { FeatureViewIcon } from "../../graphics/FeatureViewIcon";
import { useMatchExact } from "../../hooks/useMatchSubpath";
import OnDemandFeatureViewOverviewTab from "./OnDemandFeatureViewOverviewTab";

import {
  useOnDemandFeatureViewCustomTabs,
  useOnDemandFeatureViewCustomTabRoutes,
} from "../../custom-tabs/TabsRegistryContext";
import { feast } from "../../protos";

interface OnDemandFeatureInstanceProps {
  data: feast.core.IOnDemandFeatureView;
}

const OnDemandFeatureInstance = ({ data }: OnDemandFeatureInstanceProps) => {
  const navigate = useNavigate();
  let { featureViewName } = useParams();

  const { customNavigationTabs } = useOnDemandFeatureViewCustomTabs(navigate);
  const CustomTabRoutes = useOnDemandFeatureViewCustomTabRoutes();

  return (
    <EuiPageTemplate panelled>
      <EuiPageTemplate.Header
        restrictWidth
        iconType={FeatureViewIcon}
        pageTitle={`${featureViewName}`}
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
          <Route
            path="/"
            element={<OnDemandFeatureViewOverviewTab data={data} />}
          />
          {CustomTabRoutes}
        </Routes>
      </EuiPageTemplate.Section>
    </EuiPageTemplate>
  );
};

export default OnDemandFeatureInstance;
