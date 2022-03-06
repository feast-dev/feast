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

const useLoadProjectsList = () => {
  return useQuery("feast-projects-list", () => {
    return fetch("/projects-list.json", {
      headers: {
        "Content-Type": "application/json",
      },
    })
      .then((res) => {
        return res.json();
      })
      .then<ProjectsListType>((json) => {
        const configs = ProjectsListSchema.parse(json);

        return configs;
      });
  });
};

export default useLoadProjectsList;
export type { ProjectsListType };
