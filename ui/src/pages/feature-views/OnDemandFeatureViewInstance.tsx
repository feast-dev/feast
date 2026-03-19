import React from "react";
import { Route, Routes, useNavigate } from "react-router-dom";
import { useParams } from "react-router-dom";
import { EuiBadge, EuiPageTemplate } from "@elastic/eui";

import { FeatureViewIcon } from "../../graphics/FeatureViewIcon";
import { useMatchExact, useMatchSubpath } from "../../hooks/useMatchSubpath";
import OnDemandFeatureViewOverviewTab from "./OnDemandFeatureViewOverviewTab";
import FeatureViewVersionsTab from "./FeatureViewVersionsTab";

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
        pageTitle={
          <>
            {featureViewName}
            {data?.meta?.currentVersionNumber != null &&
              data.meta.currentVersionNumber > 0 && (
                <EuiBadge color="hollow" style={{ marginLeft: 8 }}>
                  v{data.meta.currentVersionNumber}
                </EuiBadge>
              )}
          </>
        }
        tabs={[
          {
            label: "Overview",
            isSelected: useMatchExact(""),
            onClick: () => {
              navigate("");
            },
          },
          {
            label: "Versions",
            isSelected: useMatchSubpath("versions"),
            onClick: () => {
              navigate("versions");
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
          <Route
            path="/versions"
            element={
              <FeatureViewVersionsTab featureViewName={featureViewName!} />
            }
          />
          {CustomTabRoutes}
        </Routes>
      </EuiPageTemplate.Section>
    </EuiPageTemplate>
  );
};

export default OnDemandFeatureInstance;
