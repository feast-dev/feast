import React from "react";

import {
  EuiPage,
  EuiPageSideBar,
  EuiPageBody,
  EuiErrorBoundary,
  EuiHorizontalRule,
  EuiSpacer,
} from "@elastic/eui";
import Sidebar from "./Sidebar";
import { Outlet } from "react-router";
import ProjectSelector from "../components/ProjectSelector";
import { useParams } from "react-router-dom";
import RegistryPathContext from "../contexts/RegistryPathContext";
import useLoadProjectsList from "../queries/useLoadProjectsList";
import feastWorkMark from "../feast-workmark.svg";

const Layout = () => {
  let { projectName } = useParams();

  const { data } = useLoadProjectsList();

  const currentProject = data?.projects.find((project) => {
    return project.id === projectName;
  });

  const registryPath = currentProject?.registryPath || "";

  return (
    <RegistryPathContext.Provider value={registryPath}>
      <EuiPage paddingSize="none" style={{ background: "transparent" }}>
        <EuiPageSideBar
          paddingSize="l"
          sticky
          role={"navigation"}
          aria-label={"Top Level"}
        >
          <img src={feastWorkMark} width={"100%"} alt="Feast" />
          <EuiSpacer size="s" />
          <ProjectSelector />
          {registryPath && (
            <React.Fragment>
              <EuiHorizontalRule margin="s" />
              <Sidebar />
            </React.Fragment>
          )}
        </EuiPageSideBar>

        <EuiPageBody panelled>
          <EuiErrorBoundary>
            <Outlet />
          </EuiErrorBoundary>
        </EuiPageBody>
      </EuiPage>
    </RegistryPathContext.Provider>
  );
};

export default Layout;
