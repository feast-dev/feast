import React from "react";
import { Route, Routes, useNavigate } from "react-router";
import { useParams } from "react-router-dom";
import {
  EuiPageHeader,
  EuiPageContent,
  EuiPageContentBody,
} from "@elastic/eui";

import FeatureViewIcon from "../../feature-view.svg";
import { useMatchExact } from "../../hooks/useMatchSubpath";
import { FeastODFVType } from "../../parsers/feastODFVS";
import OnDemandFeatureViewOverviewTab from "./OnDemandFeatureViewOverviewTab";

import {
  useOnDemandFeatureViewCustomTabs,
  onDemandFeatureViewCustomTabRoutes,
} from "../CustomTabUtils";

interface OnDemandFeatureInstanceProps {
  data: FeastODFVType;
}

const OnDemandFeatureInstance = ({ data }: OnDemandFeatureInstanceProps) => {
  const navigate = useNavigate();
  let { featureViewName } = useParams();

  const { customNavigationTabs } = useOnDemandFeatureViewCustomTabs(navigate);

  return (
    <React.Fragment>
      <EuiPageHeader
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
            {onDemandFeatureViewCustomTabRoutes()}
          </Routes>
        </EuiPageContentBody>
      </EuiPageContent>
    </React.Fragment>
  );
};

export default OnDemandFeatureInstance;
