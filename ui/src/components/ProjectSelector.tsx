import React from "react";
import { useNavigate, useParams, useLocation } from "react-router-dom";
import { useGeneratedHtmlId } from "@elastic/eui";
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

  const basicSelectId = useGeneratedHtmlId({ prefix: "projectSelector" });
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
    <select
      id={basicSelectId}
      value={currentProject?.id || ""}
      onChange={(e) => onChange(e)}
      aria-label="Select a Feast Project"
      disabled={isLoading || !options?.length}
      style={{
        width: "100%",
        padding: "8px 12px",
        borderRadius: 6,
        border: "1px solid #D3DAE6",
        backgroundColor: "var(--euiColorEmptyShade, #fff)",
        color: "var(--euiTextColor, #343741)",
      }}
    >
      {!currentProject && (
        <option value="" disabled>
          Select a Feast Project
        </option>
      )}
      {options?.map((option) => (
        <option key={option.value} value={option.value}>
          {option.text}
        </option>
      ))}
    </select>
  );
};

export default ProjectSelector;
