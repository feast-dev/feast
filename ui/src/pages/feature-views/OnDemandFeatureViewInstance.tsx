import React from "react";
import { Route, Routes, useNavigate } from "react-router-dom";
import { useParams } from "react-router-dom";
import {
  EuiPageHeader,
  EuiPageContent,
  EuiPageContentBody,
} from "@elastic/eui";

import { FeatureViewIcon32 } from "../../graphics/FeatureViewIcon";
import { useMatchExact } from "../../hooks/useMatchSubpath";
import { FeastODFVType } from "../../parsers/feastODFVS";
import OnDemandFeatureViewOverviewTab from "./OnDemandFeatureViewOverviewTab";

import {
  useOnDemandFeatureViewCustomTabs,
  useOnDemandFeatureViewCustomTabRoutes,
} from "../../custom-tabs/TabsRegistryContext";

interface OnDemandFeatureInstanceProps {
  data: FeastODFVType;
}

const OnDemandFeatureInstance = ({ data }: OnDemandFeatureInstanceProps) => {
  const navigate = useNavigate();
  let { featureViewName } = useParams();

  const { customNavigationTabs } = useOnDemandFeatureViewCustomTabs(navigate);
  const CustomTabRoutes = useOnDemandFeatureViewCustomTabRoutes();

  return (
    <React.Fragment>
      <EuiPageHeader
        restrictWidth
        iconType={FeatureViewIcon32}
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
              element={<OnDemandFeatureViewOverviewTab data={data} />}
            />
            {CustomTabRoutes}
          </Routes>
        </EuiPageContentBody>
      </EuiPageContent>
    </React.Fragment>
  );
};

export default OnDemandFeatureInstance;
