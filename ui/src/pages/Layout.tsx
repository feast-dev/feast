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

const Layout = () => {
  // Registry Path Context has to be inside Layout
  // because it has to be under routes
  // in order to use useParams
  let { projectName } = useParams();
  const [isSearchOpen, setIsSearchOpen] = useState(false);
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
    setIsSearchOpen(true);
    setTimeout(() => {
      if (searchRef.current) {
        searchRef.current.focusSearchInput();
      }
    }, 100);
  };

  return (
    <RegistryPathContext.Provider value={registryPath}>
      <GlobalSearchShortcut onOpen={handleSearchOpen} />
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
              <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                <ThemeToggle />
                <button 
                  onClick={handleSearchOpen}
                  style={{ 
                    background: 'none', 
                    border: 'none', 
                    cursor: 'pointer',
                    padding: '8px',
                    borderRadius: '4px',
                    display: 'flex',
                    alignItems: 'center'
                  }}
                >
                  <span role="img" aria-label="search">🔍</span>
                  <span style={{ marginLeft: '4px' }}>Search (⌘K)</span>
                </button>
              </div>
            </React.Fragment>
          )}
        </EuiPageSidebar>

        <EuiPageBody>
          <EuiErrorBoundary>
            {isSearchOpen && data && (
              <EuiPageHeader
                paddingSize="l"
                style={{
                  position: "sticky",
                  top: 0,
                  zIndex: 100,
                  borderBottom: "1px solid #D3DAE6",
                }}
              >
                <EuiFlexGroup>
                  <EuiFlexItem>
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
