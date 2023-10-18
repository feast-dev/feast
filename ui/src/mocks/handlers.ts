import { rest } from "msw";
import {readFileSync} from 'fs';
import path from "path";

const registry = readFileSync(path.resolve(__dirname, "../../public/registry.db"));

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
            registryPath: "/registry.pb",
          },
        ],
      })
    );
  }
);

const creditHistoryRegistry = rest.get("/registry.pb", (req, res, ctx) => {
  return res(
    ctx.status(200), 
    ctx.set('Content-Type', 'application/octet-stream'),
    ctx.body(registry));
});

export { projectsListWithDefaultProject, creditHistoryRegistry };
