import { rest } from "msw";
import registry from "../../public/registry.json";
import registry_bq from "../../public/registry_bq.json";

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
          {
            name: "Big Query Project",
            xxxdescription: "Doing stuff in Google Big Query",
            id: "big_query_project",
            registryPath: "/registry_bq.json",
          },
        ],
      })
    );
  }
);

const creditHistoryRegistry = rest.get("/registry.json", (req, res, ctx) => {
  return res(ctx.status(200), ctx.json(registry));
});

const bigQueryProjectRegistry = rest.get(
  "/registry_bq.json",
  (req, res, ctx) => {
    return res(ctx.status(200), ctx.json(registry_bq));
  }
);

export {
  projectsListWithDefaultProject,
  creditHistoryRegistry,
  bigQueryProjectRegistry,
};
