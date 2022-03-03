import { rest } from "msw";
import registry from "../../public/registry.json";

const projectsListWithDefaultProject = rest.get(
  "/projects-list.json",
  (req, res, ctx) => {
    return res(
      ctx.status(200),
      ctx.json({
        default: "credit_score_project",
        projects: [
          {
            name: "Credit Score Project",
            description:
              "Project for credit scoring team and associated models.",
            id: "credit_score_project",
            registryPath: "/registry.json",
          },
        ],
      })
    );
  }
);

const creditHistoryRegistry = rest.get("/registry.json", (req, res, ctx) => {
  return res(ctx.status(200), ctx.json(registry));
});

export {
  projectsListWithDefaultProject,
  creditHistoryRegistry
};
