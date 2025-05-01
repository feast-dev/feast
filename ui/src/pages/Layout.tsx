import React from "react";

import {
  EuiPage,
  EuiPageSidebar,
  EuiPageBody,
  EuiErrorBoundary,
  EuiHorizontalRule,
  EuiSpacer,
} from "@elastic/eui";
import { Outlet } from "react-router-dom";

import RegistryPathContext from "../contexts/RegistryPathContext";
import { useParams } from "react-router-dom";
import { useLoadProjectsList } from "../contexts/ProjectListContext";

import ProjectSelector from "../components/ProjectSelector";
import Sidebar from "./Sidebar";
import FeastWordMark from "../graphics/FeastWordMark";
import ThemeToggle from "../components/ThemeToggle";
import FloatingLineageTab from "../components/FloatingLineageTab";

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
      <EuiPage paddingSize="none" style={{ background: "transparent" }}>
        <EuiPageSidebar
          paddingSize="l"
          sticky={{ offset: 0 }}
          role={"navigation"}
          aria-label={"Top Level"}
        >
          <div
            style={{
              display: "flex",
              justifyContent: "space-between",
              alignItems: "center",
            }}
          >
            <FeastWordMark />
            <ThemeToggle />
          </div>
          <EuiSpacer size="s" />
          <ProjectSelector />
          {registryPath && (
            <React.Fragment>
              <EuiHorizontalRule margin="s" />
              <Sidebar />
            </React.Fragment>
          )}
        </EuiPageSidebar>

        <EuiPageBody>
          <EuiErrorBoundary>
            <Outlet />
          </EuiErrorBoundary>
        </EuiPageBody>
        <FloatingLineageTab />
      </EuiPage>
    </RegistryPathContext.Provider>
  );
};

export default Layout;
