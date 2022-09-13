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
              element={<StreamFeatureViewOverviewTab data={data} />}
            />
            {CustomTabRoutes}
          </Routes>
        </EuiPageContentBody>
      </EuiPageContent>
    </React.Fragment>
  );
};

export default StreamFeatureInstance;
