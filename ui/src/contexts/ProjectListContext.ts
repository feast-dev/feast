import React, { useContext, useState } from "react";
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

class ProjectListError extends Error {
  constructor(message?: string | undefined) {
    super(message);
    this.name = "FeastProjectListError";
  }
}

const projectListExampleString = `

\`\`\`json
{
  "projects": [
    {
      "name": "Credit Score Project",
      "description": "Project for credit scoring team and associated models.",
      "id": "credit_score_project",
      "registryPath": "/registry.json"
    }
  ]
}
\`\`\`
`;

const anticipatedProjectListErrors = (
  err: Error,
  isCustomProjectList: boolean
) => {
  const isSyntaxError = err.stack?.indexOf("SyntaxError") === 0;

  // Straight up not a JSON
  if (isSyntaxError) {
    const message = `Unable to properly parse Project List JSON. Check that your project list is formatted properly.`;

    return new ProjectListError(message);
  }

  // Some sort of 404
  const isFailedToFetch = err.message.indexOf("Failed to fetch") > -1;
  if (isFailedToFetch) {
    const followUpMessage = isCustomProjectList
      ? "Check that the promise in your Feast UI configuration is set up properly."
      : "Did you create a `project-list.json` file in the `/public/` directory? e.g." +
        projectListExampleString;

    const message = "Failed to fetch Project List JSON. " + followUpMessage;

    return new ProjectListError(message);
  }

  return null;
};

const useLoadProjectsList = () => {
  const projectListPromise = useContext(ProjectListContext);
  // Use setState to surface errors in Error Boundaries
  // https://github.com/facebook/react/issues/14981#issuecomment-468460187
  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  const [_, setQueryError] = useState(undefined);

  return useQuery(
    "feast-projects-list",
    () => {
      return projectListPromise?.projectsListPromise
        .catch((e) => {
          const anticipatedError = anticipatedProjectListErrors(
            e,
            projectListPromise.isCustom
          );
          setQueryError(() => {
            if (anticipatedError) {
              throw anticipatedError;
            } else {
              throw new Error(e);
            }
          });
        })
        .then<ProjectsListType>((json) => {
          try {
            const configs = ProjectsListSchema.parse(json);
            return configs;
          } catch (e) {
            // If a json object is returned, but
            // does not adhere to our anticipated
            // format.
            setQueryError(() => {
              throw new ProjectListError(
                `Error parsing project list JSON. JSON Object does not match expected type for a Feast project list. A project list JSON file should look like
                ${projectListExampleString}
                Zod (our parser) returned the following: \n\n${e}`
              );
            });

            throw new Error("Zod Error");
          }
        });
    },
    {
      enabled: !!projectListPromise?.projectsListPromise,
    }
  );
};

export { ProjectListContext, ProjectsListSchema, useLoadProjectsList };
export type { ProjectsListType, ProjectsListContextInterface };
