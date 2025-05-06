import React, { useState, useRef } from "react";

import {
  EuiPage,
  EuiPageSidebar,
  EuiPageBody,
  EuiErrorBoundary,
  EuiHorizontalRule,
  EuiSpacer,
  EuiPageHeader,
  EuiFlexGroup,
  EuiFlexItem,
} from "@elastic/eui";
import { Outlet } from "react-router-dom";

import RegistryPathContext from "../contexts/RegistryPathContext";
import { useParams } from "react-router-dom";
import { useLoadProjectsList } from "../contexts/ProjectListContext";
import useLoadRegistry from "../queries/useLoadRegistry";

import ProjectSelector from "../components/ProjectSelector";
import Sidebar from "./Sidebar";
import FeastWordMark from "../graphics/FeastWordMark";
import ThemeToggle from "../components/ThemeToggle";
import RegistrySearch, {
  RegistrySearchRef,
} from "../components/RegistrySearch";
import GlobalSearchShortcut from "../components/GlobalSearchShortcut";
import CommandPalette from "../components/CommandPalette";

const Layout = () => {
  // Registry Path Context has to be inside Layout
  // because it has to be under routes
  // in order to use useParams
  let { projectName } = useParams();
  const [isCommandPaletteOpen, setIsCommandPaletteOpen] = useState(false);
  const searchRef = useRef<RegistrySearchRef>(null);

  const { data: projectsData } = useLoadProjectsList();

  const currentProject = projectsData?.projects.find((project) => {
    return project.id === projectName;
  });

  const registryPath = currentProject?.registryPath || "";
  const { data } = useLoadRegistry(registryPath);

  const categories = data
    ? [
        {
          name: "Data Sources",
          data: data.objects.dataSources || [],
          getLink: (item: any) => `/p/${projectName}/data-source/${item.name}`,
        },
        {
          name: "Entities",
          data: data.objects.entities || [],
          getLink: (item: any) => `/p/${projectName}/entity/${item.name}`,
        },
        {
          name: "Features",
          data: data.allFeatures || [],
          getLink: (item: any) => {
            const featureView = item?.featureView;
            return featureView
              ? `/p/${projectName}/feature-view/${featureView}/feature/${item.name}`
              : "#";
          },
        },
        {
          name: "Feature Views",
          data: data.mergedFVList || [],
          getLink: (item: any) => `/p/${projectName}/feature-view/${item.name}`,
        },
        {
          name: "Feature Services",
          data: data.objects.featureServices || [],
          getLink: (item: any) => {
            const serviceName = item?.name || item?.spec?.name;
            return serviceName
              ? `/p/${projectName}/feature-service/${serviceName}`
              : "#";
          },
        },
      ]
    : [];

  const handleSearchOpen = () => {
    console.log("Opening command palette"); // Debug log
    setIsCommandPaletteOpen(true);
  };

  return (
    <RegistryPathContext.Provider value={registryPath}>
      <GlobalSearchShortcut onOpen={handleSearchOpen} />
      <CommandPalette
        isOpen={isCommandPaletteOpen}
        onClose={() => setIsCommandPaletteOpen(false)}
        categories={categories}
      />
      <EuiPage paddingSize="none" style={{ background: "transparent" }}>
        <EuiPageSidebar
          paddingSize="l"
          sticky={{ offset: 0 }}
          role={"navigation"}
          aria-label={"Top Level"}
        >
          <FeastWordMark />
          <EuiSpacer size="s" />
          <ProjectSelector />
          {registryPath && (
            <React.Fragment>
              <EuiHorizontalRule margin="s" />
              <Sidebar />
              <EuiSpacer size="l" />
              <EuiHorizontalRule margin="s" />
              <div
                style={{
                  display: "flex",
                  justifyContent: "flex-start",
                  alignItems: "center",
                }}
              >
                <ThemeToggle />
              </div>
            </React.Fragment>
          )}
        </EuiPageSidebar>

        <EuiPageBody>
          <EuiErrorBoundary>
            {data && (
              <EuiPageHeader
                paddingSize="l"
                style={{
                  position: "sticky",
                  top: 0,
                  zIndex: 100,
                  borderBottom: "1px solid #D3DAE6",
                }}
              >
                <EuiFlexGroup justifyContent="center">
                  <EuiFlexItem
                    grow={false}
                    style={{ width: "600px", maxWidth: "90%" }}
                  >
                    <RegistrySearch ref={searchRef} categories={categories} />
                  </EuiFlexItem>
                </EuiFlexGroup>
              </EuiPageHeader>
            )}
            <Outlet />
          </EuiErrorBoundary>
        </EuiPageBody>
      </EuiPage>
    </RegistryPathContext.Provider>
  );
};

export default Layout;
