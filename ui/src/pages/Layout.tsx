import React, { useState, useRef, useEffect } from "react";

import {
  EuiPage,
  EuiPageSidebar,
  EuiPageBody,
  EuiErrorBoundary,
  EuiHorizontalRule,
  EuiSpacer,
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

  // For global search, use the first available registry path (typically all projects share the same registry)
  // If projects have different registries, we use the first one as the "global" registry
  const globalRegistryPath =
    projectsData?.projects?.[0]?.registryPath || registryPath;

  // Load filtered data for current project (for sidebar and page-level search)
  const { data } = useLoadRegistry(registryPath, projectName);

  // Load unfiltered data for global search (across all projects)
  const { data: globalData } = useLoadRegistry(globalRegistryPath);

  // Categories for page-level search (filtered to current project)
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

  // Helper function to extract project ID from an item
  const getProjectId = (item: any): string => {
    // Try different possible locations for the project field
    return item?.spec?.project || item?.project || projectName || "unknown";
  };

  // Categories for global search (includes all projects)
  const globalCategories = globalData
    ? [
        {
          name: "Data Sources",
          data: (globalData.objects.dataSources || []).map((item: any) => ({
            ...item,
            projectId: getProjectId(item),
          })),
          getLink: (item: any) => {
            const project = item?.projectId || getProjectId(item);
            return `/p/${project}/data-source/${item.name}`;
          },
        },
        {
          name: "Entities",
          data: (globalData.objects.entities || []).map((item: any) => ({
            ...item,
            projectId: getProjectId(item),
          })),
          getLink: (item: any) => {
            const project = item?.projectId || getProjectId(item);
            return `/p/${project}/entity/${item.name}`;
          },
        },
        {
          name: "Features",
          data: (globalData.allFeatures || []).map((item: any) => ({
            ...item,
            projectId: getProjectId(item),
          })),
          getLink: (item: any) => {
            const featureView = item?.featureView;
            const project = item?.projectId || getProjectId(item);
            return featureView
              ? `/p/${project}/feature-view/${featureView}/feature/${item.name}`
              : "#";
          },
        },
        {
          name: "Feature Views",
          data: (globalData.mergedFVList || []).map((item: any) => ({
            ...item,
            projectId: getProjectId(item),
          })),
          getLink: (item: any) => {
            const project = item?.projectId || getProjectId(item);
            return `/p/${project}/feature-view/${item.name}`;
          },
        },
        {
          name: "Feature Services",
          data: (globalData.objects.featureServices || []).map((item: any) => ({
            ...item,
            projectId: getProjectId(item),
          })),
          getLink: (item: any) => {
            const serviceName = item?.name || item?.spec?.name;
            const project = item?.projectId || getProjectId(item);
            return serviceName
              ? `/p/${project}/feature-service/${serviceName}`
              : "#";
          },
        },
      ]
    : [];

  const handleSearchOpen = () => {
    setIsCommandPaletteOpen(true);
  };

  useEffect(() => {
    const handleKeyDown = (event: KeyboardEvent) => {
      if ((event.metaKey || event.ctrlKey) && event.key.toLowerCase() === "k") {
        event.preventDefault();
        event.stopPropagation();
        handleSearchOpen();
      }
    };

    window.addEventListener("keydown", handleKeyDown, true);
    return () => {
      window.removeEventListener("keydown", handleKeyDown, true);
    };
  }, []);

  return (
    <RegistryPathContext.Provider value={registryPath}>
      <GlobalSearchShortcut onOpen={handleSearchOpen} />
      <CommandPalette
        isOpen={isCommandPaletteOpen}
        onClose={() => setIsCommandPaletteOpen(false)}
        categories={globalCategories}
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
            <div
              style={{
                display: "flex",
                flexDirection: "column",
                height: "100vh",
              }}
            >
              {data && (
                <div
                  style={{
                    position: "sticky",
                    top: 0,
                    zIndex: 100,
                    backgroundColor: "var(--euiPageBackgroundColor)",
                    borderBottom: "1px solid #D3DAE6",
                    boxShadow: "0px 1px 5px rgba(0, 0, 0, 0.05)",
                    padding: "16px",
                    width: "100%",
                  }}
                >
                  <EuiFlexGroup justifyContent="center">
                    <EuiFlexItem
                      grow={false}
                      style={{ width: "600px", maxWidth: "90%" }}
                    >
                      <RegistrySearch
                        ref={searchRef}
                        categories={globalCategories}
                      />
                    </EuiFlexItem>
                  </EuiFlexGroup>
                </div>
              )}
              <div
                style={{
                  flexGrow: 1,
                  overflow: "auto",
                  padding: "16px",
                  height: "calc(100vh - 70px)",
                }}
              >
                <Outlet />
              </div>
            </div>
          </EuiErrorBoundary>
        </EuiPageBody>
      </EuiPage>
    </RegistryPathContext.Provider>
  );
};

export default Layout;
