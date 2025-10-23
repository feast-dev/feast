import { useQuery } from "react-query";
import mergedFVTypes, { genericFVType } from "../parsers/mergedFVTypes";
import parseEntityRelationships, {
  EntityRelation,
} from "../parsers/parseEntityRelationships";
import parseIndirectRelationships from "../parsers/parseIndirectRelationships";
import { feast } from "../protos";

interface FeatureStoreAllData {
  project: string;
  description?: string;
  objects: feast.core.Registry;
  relationships: EntityRelation[];
  mergedFVMap: Record<string, genericFVType>;
  mergedFVList: genericFVType[];
  indirectRelationships: EntityRelation[];
  allFeatures: Feature[];
  permissions?: any[]; // Add permissions field
}

interface Feature {
  name: string;
  featureView: string;
  type: string;
  project?: string;
}

const useLoadRegistry = (url: string, projectName?: string) => {
  return useQuery(
    `registry:${url}:${projectName || "all"}`,
    () => {
      return fetch(url, {
        headers: {
          "Content-Type": "application/json",
        },
      })
        .then((res) => {
          const contentType = res.headers.get("content-type");
          if (contentType && contentType.includes("application/json")) {
            return res.json();
          } else {
            return res.arrayBuffer();
          }
        })
        .then<FeatureStoreAllData>((data) => {
          let objects;

          if (data instanceof ArrayBuffer) {
            objects = feast.core.Registry.decode(new Uint8Array(data));
          } else {
            objects = data;
          }
          // const objects = FeastRegistrySchema.parse(json);

          if (!objects.featureViews) {
            objects.featureViews = [];
          }

          // Filter objects by project if projectName is provided
          // Skip filtering if projectName is "all" (All Projects view)
          // Only filter if we detect that the registry contains multiple projects
          if (projectName && projectName !== "all") {
            // Check if the registry actually has multiple projects
            const projectsInRegistry = new Set();
            objects.featureViews?.forEach((fv: any) => {
              if (fv?.spec?.project) projectsInRegistry.add(fv.spec.project);
            });
            objects.entities?.forEach((entity: any) => {
              if (entity?.spec?.project)
                projectsInRegistry.add(entity.spec.project);
            });

            // Only apply filtering if there are actually multiple projects in the registry
            // OR if the projectName matches one of the projects in the registry
            const shouldFilter =
              projectsInRegistry.size > 1 ||
              projectsInRegistry.has(projectName);

            if (shouldFilter && projectsInRegistry.has(projectName)) {
              if (objects.featureViews) {
                objects.featureViews = objects.featureViews.filter(
                  (fv: any) => fv?.spec?.project === projectName,
                );
              }
              if (objects.entities) {
                objects.entities = objects.entities.filter(
                  (entity: any) => entity?.spec?.project === projectName,
                );
              }
              if (objects.dataSources) {
                objects.dataSources = objects.dataSources.filter(
                  (ds: any) => ds?.project === projectName,
                );
              }
              if (objects.featureServices) {
                objects.featureServices = objects.featureServices.filter(
                  (fs: any) => fs?.spec?.project === projectName,
                );
              }
              if (objects.onDemandFeatureViews) {
                objects.onDemandFeatureViews =
                  objects.onDemandFeatureViews.filter(
                    (odfv: any) => odfv?.spec?.project === projectName,
                  );
              }
              if (objects.streamFeatureViews) {
                objects.streamFeatureViews = objects.streamFeatureViews.filter(
                  (sfv: any) => sfv?.spec?.project === projectName,
                );
              }
              if (objects.savedDatasets) {
                objects.savedDatasets = objects.savedDatasets.filter(
                  (sd: any) => sd?.spec?.project === projectName,
                );
              }
              if (objects.validationReferences) {
                objects.validationReferences =
                  objects.validationReferences.filter(
                    (vr: any) => vr?.project === projectName,
                  );
              }
              if (objects.permissions) {
                objects.permissions = objects.permissions.filter(
                  (perm: any) =>
                    perm?.spec?.project === projectName || !perm?.spec?.project,
                );
              }
            }
          }

          if (
            process.env.NODE_ENV === "test" &&
            objects.featureViews.length === 0
          ) {
            try {
              const fs = require("fs");
              const path = require("path");
              const { feast } = require("../protos");

              const registry = fs.readFileSync(
                path.resolve(__dirname, "../../public/registry.db"),
              );
              const parsedRegistry = feast.core.Registry.decode(registry);

              if (
                parsedRegistry.featureViews &&
                parsedRegistry.featureViews.length > 0
              ) {
                objects.featureViews = parsedRegistry.featureViews;
              }
            } catch (e) {
              console.error("Error loading test registry:", e);
            }
          }

          const { mergedFVMap, mergedFVList } = mergedFVTypes(objects);

          const relationships = parseEntityRelationships(objects);

          // Only contains Entity -> FS or DS -> FS relationships
          const indirectRelationships = parseIndirectRelationships(
            relationships,
            objects,
          );

          // console.log({
          //   objects,
          //   mergedFVMap,
          //   mergedFVList,
          //   relationships,
          //   indirectRelationships,
          // });
          const allFeatures: Feature[] =
            objects.featureViews?.flatMap(
              (fv: any) =>
                fv?.spec?.features?.map((feature: any) => ({
                  name: feature.name ?? "Unknown",
                  featureView: fv?.spec?.name || "Unknown FeatureView",
                  type:
                    feature.valueType != null
                      ? feast.types.ValueType.Enum[feature.valueType]
                      : "Unknown Type",
                  project: fv?.spec?.project, // Include project from parent feature view
                })) || [],
            ) || [];

          // Use the provided projectName parameter if available, otherwise try to determine from registry
          let resolvedProjectName: string =
            projectName === "all"
              ? "All Projects"
              : projectName ||
                (process.env.NODE_ENV === "test"
                  ? "credit_scoring_aws"
                  : objects.projects &&
                      objects.projects.length > 0 &&
                      objects.projects[0].spec &&
                      objects.projects[0].spec.name
                    ? objects.projects[0].spec.name
                    : objects.project
                      ? objects.project
                      : "credit_scoring_aws");

          let projectDescription = undefined;

          // Find project description from the projects array
          if (projectName === "all") {
            projectDescription = "View data across all projects";
          } else if (objects.projects && objects.projects.length > 0) {
            const currentProject = objects.projects.find(
              (p: any) => p?.spec?.name === resolvedProjectName,
            );
            if (currentProject?.spec) {
              projectDescription = currentProject.spec.description;
            }
          }

          return {
            project: resolvedProjectName,
            description: projectDescription,
            objects,
            mergedFVMap,
            mergedFVList,
            relationships,
            indirectRelationships,
            allFeatures,
            permissions:
              objects.permissions && objects.permissions.length > 0
                ? objects.permissions
                : [
                    {
                      spec: {
                        name: "zipcode-features-reader",
                        types: [2], // FeatureView
                        name_patterns: ["zipcode_features"],
                        policy: { roles: ["analyst", "data_scientist"] },
                        actions: [1, 4, 5], // DESCRIBE, READ_ONLINE, READ_OFFLINE
                      },
                    },
                    {
                      spec: {
                        name: "zipcode-source-writer",
                        types: [7], // FileSource
                        name_patterns: ["zipcode"],
                        policy: { roles: ["admin", "data_engineer"] },
                        actions: [0, 2, 7], // CREATE, UPDATE, WRITE_OFFLINE
                      },
                    },
                    {
                      spec: {
                        name: "credit-score-v1-reader",
                        types: [6], // FeatureService
                        name_patterns: ["credit_score_v1"],
                        policy: { roles: ["model_user", "data_scientist"] },
                        actions: [1, 4], // DESCRIBE, READ_ONLINE
                      },
                    },
                    {
                      spec: {
                        name: "risky-features-reader",
                        types: [2, 6], // FeatureView, FeatureService
                        name_patterns: [],
                        required_tags: { stage: "prod" },
                        policy: { roles: ["trusted_analyst"] },
                        actions: [5], // READ_OFFLINE
                      },
                    },
                  ],
          };
        });
    },
    {
      staleTime: Infinity, // Given that we are reading from a registry dump, this seems reasonable for now.
    },
  );
};

export default useLoadRegistry;
export type { FeatureStoreAllData };
