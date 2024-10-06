import React from "react";
import { Route, Routes, useNavigate, useParams } from "react-router-dom";
import { EuiPageTemplate } from "@elastic/eui";

import { EntityIcon } from "../../graphics/EntityIcon";
import { useMatchExact } from "../../hooks/useMatchSubpath";
import EntityOverviewTab from "./EntityOverviewTab";
import { useDocumentTitle } from "../../hooks/useDocumentTitle";
import {
  useEntityCustomTabs,
  useEntityCustomTabRoutes,
} from "../../custom-tabs/TabsRegistryContext";

const EntityInstance = () => {
  const navigate = useNavigate();
  let { entityName } = useParams();

  const { customNavigationTabs } = useEntityCustomTabs(navigate);
  const CustomTabRoutes = useEntityCustomTabRoutes();

  useDocumentTitle(`${entityName} | Entity | Feast`);

  return (
    <EuiPageTemplate panelled>
      <EuiPageTemplate.Header
        restrictWidth
        iconType={EntityIcon}
        pageTitle={`Entity: ${entityName}`}
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
          <Route path="/" element={<EntityOverviewTab />} />
          {CustomTabRoutes}
        </Routes>
      </EuiPageTemplate.Section>
    </EuiPageTemplate>
  );
};

export default EntityInstance;
