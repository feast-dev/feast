import { EuiSelect, useGeneratedHtmlId } from "@elastic/eui";
import React from "react";
import { useNavigate, useParams } from "react-router-dom";
import { useLoadProjectsList } from "../contexts/ProjectListContext";

const ProjectSelector = () => {
  const { projectName } = useParams();
  const navigate = useNavigate();

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
    navigate(`${process.env.PUBLIC_URL || ""}/p/${e.target.value}`);
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
