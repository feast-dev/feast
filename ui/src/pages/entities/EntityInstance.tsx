import React from "react";
import { Route, Routes, useNavigate, useParams } from "react-router-dom";
import {
  EuiPageHeader,
  EuiPageContent,
  EuiPageContentBody,
} from "@elastic/eui";

import EntityIcon from "../../entity-icon.svg";
import { useMatchExact, useMatchSubpath } from "../../hooks/useMatchSubpath";
import EntityRawData from "./EntityRawData";
import EntityOverviewTab from "./EntityOverviewTab";
import { useDocumentTitle } from "../../hooks/useDocumentTitle";
import { useEntityCustomTabs, entityCustomTabRoutes } from "../CustomTabUtils";

const EntityInstance = () => {
  const navigate = useNavigate();
  let { entityName } = useParams();

  const { customNavigationTabs } = useEntityCustomTabs(navigate);

  useDocumentTitle(`${entityName} | Entity | Feast`);

  return (
    <React.Fragment>
      <EuiPageHeader
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
            {entityCustomTabRoutes()}
          </Routes>
        </EuiPageContentBody>
      </EuiPageContent>
    </React.Fragment>
  );
};

export default EntityInstance;
