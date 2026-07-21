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
  EuiAvatar,
  EuiText,
  EuiBadge,
  EuiToolTip,
  EuiPopover,
  EuiButtonEmpty,
  EuiIcon,
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
import { useAuth } from "../contexts/AuthContext";

const Layout = () => {
  let { projectName } = useParams();
  const [isCommandPaletteOpen, setIsCommandPaletteOpen] = useState(false);
  const [isUserMenuOpen, setIsUserMenuOpen] = useState(false);
  const searchRef = useRef<RegistrySearchRef>(null);
  const { user, logout, isAuthEnabled } = useAuth();

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
          name: "Label Views",
          data: (globalData.objects.labelViews || []).map((item: any) => ({
            ...item,
            projectId: getProjectId(item),
          })),
          getLink: (item: any) => {
            const lvName = item?.name || item?.spec?.name;
            const project = item?.projectId || getProjectId(item);
            return `/p/${project}/label-view/${lvName}`;
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
              <div
                style={{
                  position: "sticky",
                  top: 0,
                  zIndex: 100,
                  backgroundColor: "var(--euiPageBackgroundColor)",
                  borderBottom: "1px solid #D3DAE6",
                  boxShadow: "0px 1px 5px rgba(0, 0, 0, 0.05)",
                  padding: "12px 16px",
                  width: "100%",
                }}
              >
                <EuiFlexGroup alignItems="center" gutterSize="m">
                  {data && (
                    <EuiFlexItem>
                      <div
                        style={{
                          maxWidth: 600,
                          margin: "0 auto",
                          width: "100%",
                        }}
                      >
                        <RegistrySearch
                          ref={searchRef}
                          categories={globalCategories}
                        />
                      </div>
                    </EuiFlexItem>
                  )}
                  {!data && <EuiFlexItem />}

                  {isAuthEnabled && user && (
                    <EuiFlexItem grow={false}>
                      <EuiPopover
                        button={
                          <button
                            onClick={() => setIsUserMenuOpen((v) => !v)}
                            style={{
                              display: "flex",
                              alignItems: "center",
                              gap: 8,
                              background: "none",
                              border: "none",
                              cursor: "pointer",
                              padding: "4px 8px",
                              borderRadius: 6,
                            }}
                            aria-label="User menu"
                          >
                            <EuiAvatar
                              name={user.username}
                              size="s"
                              color="#0061a6"
                            />
                            <EuiText size="xs">
                              <strong>{user.username}</strong>
                            </EuiText>
                            <EuiIcon type="arrowDown" size="s" />
                          </button>
                        }
                        isOpen={isUserMenuOpen}
                        closePopover={() => setIsUserMenuOpen(false)}
                        anchorPosition="downRight"
                        panelPaddingSize="m"
                      >
                        <div style={{ minWidth: 220 }}>
                          <EuiFlexGroup
                            gutterSize="s"
                            alignItems="center"
                            responsive={false}
                          >
                            <EuiFlexItem grow={false}>
                              <EuiAvatar
                                name={user.username}
                                size="m"
                                color="#0061a6"
                              />
                            </EuiFlexItem>
                            <EuiFlexItem>
                              <EuiText size="s">
                                <strong>{user.username}</strong>
                              </EuiText>
                              {user.email && (
                                <EuiText size="xs" color="subdued">
                                  {user.email}
                                </EuiText>
                              )}
                            </EuiFlexItem>
                          </EuiFlexGroup>

                          {user.roles.length > 0 && (
                            <>
                              <EuiHorizontalRule margin="s" />
                              <EuiText size="xs" color="subdued">
                                <strong>Roles</strong>
                              </EuiText>
                              <EuiSpacer size="xs" />
                              <div
                                style={{
                                  display: "flex",
                                  flexWrap: "wrap",
                                  gap: 4,
                                }}
                              >
                                {user.roles
                                  .filter(
                                    (r) =>
                                      ![
                                        "default-roles-feast",
                                        "offline_access",
                                        "uma_authorization",
                                      ].includes(r),
                                  )
                                  .map((role) => (
                                    <EuiToolTip content={role} key={role}>
                                      <EuiBadge color="hollow">{role}</EuiBadge>
                                    </EuiToolTip>
                                  ))}
                              </div>
                            </>
                          )}

                          {user.groups.length > 0 && (
                            <>
                              <EuiSpacer size="s" />
                              <EuiText size="xs" color="subdued">
                                <strong>Groups</strong>
                              </EuiText>
                              <EuiSpacer size="xs" />
                              <div
                                style={{
                                  display: "flex",
                                  flexWrap: "wrap",
                                  gap: 4,
                                }}
                              >
                                {user.groups.map((group) => (
                                  <EuiBadge color="default" key={group}>
                                    {group}
                                  </EuiBadge>
                                ))}
                              </div>
                            </>
                          )}

                          <EuiHorizontalRule margin="s" />
                          <EuiButtonEmpty
                            size="s"
                            iconType="exit"
                            onClick={logout}
                            color="danger"
                            flush="left"
                          >
                            Sign out
                          </EuiButtonEmpty>
                        </div>
                      </EuiPopover>
                    </EuiFlexItem>
                  )}
                </EuiFlexGroup>
              </div>
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
