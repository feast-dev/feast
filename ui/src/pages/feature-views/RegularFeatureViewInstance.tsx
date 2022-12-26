import React, { useContext } from "react";
import { Route, Routes, useNavigate } from "react-router-dom";
import {
  EuiPageHeader,
  EuiPageContent,
  EuiPageContentBody,
} from "@elastic/eui";

import { FeatureViewIcon32 } from "../../graphics/FeatureViewIcon";

import { useMatchExact, useMatchSubpath } from "../../hooks/useMatchSubpath";
import RegularFeatureViewOverviewTab from "./RegularFeatureViewOverviewTab";

import {
  useRegularFeatureViewCustomTabs,
  useRegularFeatureViewCustomTabRoutes,
} from "../../custom-tabs/TabsRegistryContext";
import FeatureFlagsContext from "../../contexts/FeatureFlagsContext";
import { feast } from "../../protos";

interface RegularFeatureInstanceProps {
  data: feast.core.IFeatureView;
}

const RegularFeatureInstance = ({ data }: RegularFeatureInstanceProps) => {
  const { enabledFeatureStatistics } = useContext(FeatureFlagsContext);
  const navigate = useNavigate();

  const { customNavigationTabs } = useRegularFeatureViewCustomTabs(navigate);
  let tabs = [
    {
      label: "Overview",
      isSelected: useMatchExact(""),
      onClick: () => {
        navigate("");
      },
    },
  ];

  let statisticsIsSelected = useMatchSubpath("statistics");
  if (enabledFeatureStatistics) {
    tabs.push({
      label: "Statistics",
      isSelected: statisticsIsSelected,
      onClick: () => {
        navigate("statistics");
      },
    });
  }

  tabs.push(...customNavigationTabs);

  const TabRoutes = useRegularFeatureViewCustomTabRoutes();

  return (
    <React.Fragment>
      <EuiPageHeader
        restrictWidth
        iconType={FeatureViewIcon32}
        pageTitle={`${data?.spec?.name}`}
        tabs={tabs}
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
              element={<RegularFeatureViewOverviewTab data={data} />}
            />
            {TabRoutes}
          </Routes>
        </EuiPageContentBody>
      </EuiPageContent>
    </React.Fragment>
  );
};

export default RegularFeatureInstance;
