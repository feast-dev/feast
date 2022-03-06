import { EuiEmptyPrompt, EuiLoadingContent } from "@elastic/eui";
import React from "react";
import { Outlet, useParams } from "react-router-dom";
import useLoadProjectsList from "../queries/useLoadProjectsList";
import ProjectSelector from "./ProjectSelector";

const NoProjectGuard = () => {
  const { projectName } = useParams();

  const { isLoading, isError, data } = useLoadProjectsList();

  if (isLoading && !data) {
    return <EuiLoadingContent lines={3} />;
  }

  if (isError) {
    return (
      <EuiEmptyPrompt
        iconType="alert"
        color="danger"
        title={<h2>Error Loading Project List</h2>}
        body={
          <p>
            Unable to find
            <code>projects-list.json</code>. Check that you have a project list
            file defined.
          </p>
        }
      />
    );
  }

  const currentProject = data?.projects.find((project) => {
    return project.id === projectName;
  });

  if (currentProject === undefined) {
    return (
      <EuiEmptyPrompt
        iconType="alert"
        color="danger"
        title={<h2>Error Loading Project</h2>}
        body={
          <React.Fragment>
            <p>
              There is no project with id <strong>{projectName}</strong> in{" "}
              <code>projects-list.json</code>. Check that you have the correct
              project id.
            </p>
            <p>You can also select one of the project in the following list:</p>
            <ProjectSelector />
          </React.Fragment>
        }
      />
    );
  }

  return <Outlet />;
};

export default NoProjectGuard;
