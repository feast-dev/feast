import React from "react";
import { Route, Routes, useNavigate } from "react-router-dom";
import { useParams } from "react-router-dom";
import { EuiPageTemplate } from "@elastic/eui";

import { FeatureViewIcon } from "../../graphics/FeatureViewIcon";
import { useMatchExact } from "../../hooks/useMatchSubpath";
import StreamFeatureViewOverviewTab from "./StreamFeatureViewOverviewTab";

import {
  useStreamFeatureViewCustomTabs,
  useStreamFeatureViewCustomTabRoutes,
} from "../../custom-tabs/TabsRegistryContext";
import { feast } from "../../protos";

interface StreamFeatureInstanceProps {
  data: feast.core.IStreamFeatureView;
}

const StreamFeatureInstance = ({ data }: StreamFeatureInstanceProps) => {
  const navigate = useNavigate();
  let { featureViewName } = useParams();

  const { customNavigationTabs } = useStreamFeatureViewCustomTabs(navigate);
  const CustomTabRoutes = useStreamFeatureViewCustomTabRoutes();

  return (
    <EuiPageTemplate panelled>
      <EuiPageTemplate.Header
        restrictWidth
        paddingSize="l"
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
            element={<StreamFeatureViewOverviewTab data={data} />}
          />
          {CustomTabRoutes}
        </Routes>
      </EuiPageTemplate.Section>
    </EuiPageTemplate>
  );
};

export default StreamFeatureInstance;
