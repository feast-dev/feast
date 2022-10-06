import { rest } from "msw";

import fs from 'fs';

const registry = fs.readFileSync("./public/registry.db");

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
            registryPath: "/registry.db",
          },
        ],
      })
    );
  }
);

const creditHistoryRegistry = rest.get("/registry.db", (req, res, ctx) => {
  return res(ctx.status(200), ctx.body(registry));
});

export { projectsListWithDefaultProject, creditHistoryRegistry };
