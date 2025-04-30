import React, { useContext } from "react";
import { Route, Routes, useNavigate } from "react-router-dom";
import { EuiPageTemplate } from "@elastic/eui";

import { FeatureViewIcon } from "../../graphics/FeatureViewIcon";

import { useMatchExact, useMatchSubpath } from "../../hooks/useMatchSubpath";
import RegularFeatureViewOverviewTab from "./RegularFeatureViewOverviewTab";
import FeatureViewLineageTab from "./FeatureViewLineageTab";

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

  tabs.push({
    label: "Lineage",
    isSelected: useMatchSubpath("lineage"),
    onClick: () => {
      navigate("lineage");
    },
  });

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
    <EuiPageTemplate panelled>
      <EuiPageTemplate.Header
        restrictWidth
        iconType={FeatureViewIcon}
        pageTitle={`${data?.spec?.name}`}
        tabs={tabs}
      />
      <EuiPageTemplate.Section>
        <Routes>
          <Route
            path="/"
            element={<RegularFeatureViewOverviewTab data={data} />}
          />
          <Route
            path="/lineage"
            element={<FeatureViewLineageTab data={data} />}
          />
          {TabRoutes}
        </Routes>
      </EuiPageTemplate.Section>
    </EuiPageTemplate>
  );
};

export default RegularFeatureInstance;
