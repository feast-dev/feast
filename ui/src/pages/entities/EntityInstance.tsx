import React from "react";
import { Route, Routes, useNavigate, useParams } from "react-router-dom";
import {
  EuiPageHeader,
  EuiPageContent,
  EuiPageContentBody,
} from "@elastic/eui";

import { EntityIcon32 } from "../../graphics/EntityIcon";
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
    <React.Fragment>
      <EuiPageHeader
        restrictWidth
        iconType={EntityIcon32}
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
      <EuiPageContent
        hasBorder={false}
        hasShadow={false}
        paddingSize="none"
        color="transparent"
        borderRadius="none"
      >
        <EuiPageContentBody>
          <Routes>
            <Route path="/" element={<EntityOverviewTab />} />
            {CustomTabRoutes}
          </Routes>
        </EuiPageContentBody>
      </EuiPageContent>
    </React.Fragment>
  );
};

export default EntityInstance;
