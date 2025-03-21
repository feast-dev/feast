import React from "react";

import {
  EuiPage,
  EuiPageSidebar,
  EuiPageBody,
  EuiErrorBoundary,
  EuiSpacer,
  EuiHeader,
  EuiHeaderSection,
  EuiHeaderSectionItem,
  EuiFlexGroup,
  EuiFlexItem,
} from "@elastic/eui";
import { Outlet } from "react-router-dom";

import RegistryPathContext from "../contexts/RegistryPathContext";
import { useParams } from "react-router-dom";
import { useLoadProjectsList } from "../contexts/ProjectListContext";

import ProjectSelector from "../components/ProjectSelector";
import Sidebar from "./Sidebar";
import FeastWordMark from "../graphics/FeastWordMark";
import GlobalSearchBar from "../components/GlobalSearchBar";

const Layout = () => {
  // Registry Path Context has to be inside Layout
  // because it has to be under routes
  // in order to use useParams
  let { projectName } = useParams();

  const { data } = useLoadProjectsList();

  const currentProject = data?.projects.find((project) => {
    return project.id === projectName;
  });

  const registryPath = currentProject?.registryPath || "";

  return (
    <RegistryPathContext.Provider value={registryPath}>
      <EuiHeader position="fixed" data-testid="feast-header">
        <EuiHeaderSection>
          <EuiHeaderSectionItem>
            <FeastWordMark />
          </EuiHeaderSectionItem>
        </EuiHeaderSection>
        {registryPath && (
          <EuiHeaderSection grow={true}>
            <EuiHeaderSectionItem>
              <EuiFlexGroup gutterSize="s" alignItems="center">
                <EuiFlexItem>
                  <GlobalSearchBar />
                </EuiFlexItem>
              </EuiFlexGroup>
            </EuiHeaderSectionItem>
          </EuiHeaderSection>
        )}
        <EuiHeaderSection>
          <EuiHeaderSectionItem>
            <ProjectSelector />
          </EuiHeaderSectionItem>
        </EuiHeaderSection>
      </EuiHeader>
      <EuiSpacer size="xxl" />
      <EuiPage paddingSize="none" style={{ background: "transparent" }}>
        <EuiPageSidebar
          paddingSize="l"
          sticky={{ offset: 48 }}
          role={"navigation"}
          aria-label={"Top Level"}
        >
          {registryPath && (
            <React.Fragment>
              <Sidebar />
            </React.Fragment>
          )}
        </EuiPageSidebar>

        <EuiPageBody>
          <EuiErrorBoundary>
            <Outlet />
          </EuiErrorBoundary>
        </EuiPageBody>
      </EuiPage>
    </RegistryPathContext.Provider>
  );
};

export default Layout;
