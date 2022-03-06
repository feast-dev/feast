import React, { useContext } from "react";
import { useQuery } from "react-query";

import { z } from "zod";

const ProjectEntrySchema = z.object({
  id: z.string(),
  name: z.string(),
  description: z.string().optional(),
  registryPath: z.string(),
});

const ProjectsListSchema = z.object({
  default: z.string().optional(),
  projects: z.array(ProjectEntrySchema),
});

type ProjectsListType = z.infer<typeof ProjectsListSchema>;
interface ProjectsListContextInterface {
  projectsListPromise: Promise<any>;
  isCustom: boolean;
}

const ProjectListContext = React.createContext<
  ProjectsListContextInterface | undefined
>(undefined);

const useLoadProjectsList = () => {
  const projectListPromise = useContext(ProjectListContext);

  return useQuery(
    "feast-projects-list",
    () => {
      return projectListPromise?.projectsListPromise.then<ProjectsListType>(
        (json) => {
          const configs = ProjectsListSchema.parse(json);

          return configs;
        }
      );
    },
    {
      enabled: !!projectListPromise?.projectsListPromise,
    }
  );
};

export { ProjectListContext, ProjectsListSchema, useLoadProjectsList };
export type { ProjectsListType, ProjectsListContextInterface };
