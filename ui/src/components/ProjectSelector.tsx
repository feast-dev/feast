import { EuiSelect, useGeneratedHtmlId } from "@elastic/eui";
import React from "react";
import { useNavigate, useParams, useLocation } from "react-router-dom";
import { useLoadProjectsList } from "../contexts/ProjectListContext";

const ProjectSelector = () => {
  const { projectName } = useParams();
  const navigate = useNavigate();
  const location = useLocation();

  const { isLoading, data } = useLoadProjectsList();

  const currentProject = data?.projects.find((project) => {
    return project.id === projectName;
  });

  const options = data?.projects.map((p) => {
    return {
      value: p.id,
      text: p.name,
    };
  });

  const basicSelectId = useGeneratedHtmlId({ prefix: "basicSelect" });
  const onChange = (e: React.ChangeEvent<HTMLSelectElement>) => {
    const newProjectId = e.target.value;

    // If we're on a project page, maintain the current path context
    if (projectName && location.pathname.startsWith(`/p/${projectName}`)) {
      // Replace the old project name with the new one in the current path
      const newPath = location.pathname.replace(
        `/p/${projectName}`,
        `/p/${newProjectId}`,
      );
      navigate(newPath);
    } else {
      // Otherwise, just navigate to the project home
      navigate(`/p/${newProjectId}`);
    }
  };

  return (
    <EuiSelect
      isLoading={isLoading}
      hasNoInitialSelection={currentProject === undefined}
      fullWidth={true}
      id={basicSelectId}
      options={options}
      value={currentProject?.id || ""}
      onChange={(e) => onChange(e)}
      aria-label="Select a Feast Project"
    />
  );
};

export default ProjectSelector;
