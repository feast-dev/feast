import { http, HttpResponse } from "msw";
import { readFileSync } from "fs";
import path from "path";
import { feast } from "../protos";

const registryBuf = readFileSync(
  path.resolve(__dirname, "../../public/registry.db"),
);
const parsedRegistry = feast.core.Registry.decode(registryBuf);

const toJSON = (obj: any) => (obj && obj.toJSON ? obj.toJSON() : obj);

const entitiesJSON = (parsedRegistry.entities || []).map(toJSON);
const featureViewsJSON = (parsedRegistry.featureViews || []).map((fv) => ({
  ...toJSON(fv),
  type: "featureView",
}));
const onDemandFVsJSON = (parsedRegistry.onDemandFeatureViews || []).map(
  (fv) => ({
    ...toJSON(fv),
    type: "onDemandFeatureView",
  }),
);
const streamFVsJSON = (parsedRegistry.streamFeatureViews || []).map((fv) => ({
  ...toJSON(fv),
  type: "streamFeatureView",
}));
const allFeatureViewsJSON = [
  ...featureViewsJSON,
  ...onDemandFVsJSON,
  ...streamFVsJSON,
];
const featureServicesJSON = (parsedRegistry.featureServices || []).map(toJSON);
const dataSourcesJSON = (parsedRegistry.dataSources || []).map(toJSON);
const savedDatasetsJSON = (parsedRegistry.savedDatasets || []).map(toJSON);
const projectsJSON = (parsedRegistry.projects || []).map(toJSON);

const allFeatures = featureViewsJSON.flatMap((fv: any) =>
  (fv?.spec?.features || []).map((f: any) => ({
    name: f.name,
    featureViewName: fv.spec?.name,
    valueType: f.valueType,
    project: fv.spec?.project,
  })),
);

const projectsListWithDefaultProject = http.get("/projects-list.json", () =>
  HttpResponse.json({
    default: "credit_scoring_aws",
    projects: [
      {
        name: "Credit Score Project",
        description: "Project for credit scoring team and associated models.",
        id: "credit_scoring_aws",
        registryPath: "/api/v1",
      },
    ],
  }),
);

// REST API list endpoints
const restEntities = http.get("/api/v1/entities", () =>
  HttpResponse.json({
    entities: entitiesJSON,
    pagination: {},
    relationships: {},
  }),
);

const restFeatureViews = http.get("/api/v1/feature_views", () =>
  HttpResponse.json({
    featureViews: allFeatureViewsJSON,
    pagination: {},
    relationships: {},
  }),
);

const restFeatureServices = http.get("/api/v1/feature_services", () =>
  HttpResponse.json({
    featureServices: featureServicesJSON,
    pagination: {},
    relationships: {},
  }),
);

const restDataSources = http.get("/api/v1/data_sources", () =>
  HttpResponse.json({
    dataSources: dataSourcesJSON,
    pagination: {},
    relationships: {},
  }),
);

const restSavedDatasets = http.get("/api/v1/saved_datasets", () =>
  HttpResponse.json({
    savedDatasets: savedDatasetsJSON,
    pagination: {},
  }),
);

const restProjects = http.get("/api/v1/projects", () =>
  HttpResponse.json({
    projects: projectsJSON,
    pagination: {},
  }),
);

const restFeatures = http.get("/api/v1/features", () =>
  HttpResponse.json({
    features: allFeatures,
    pagination: {},
  }),
);

const restPermissions = http.get("/api/v1/permissions", () =>
  HttpResponse.json({
    permissions: [],
    pagination: {},
  }),
);

// Detail endpoints
const restFeatureViewDetail = http.get(
  "/api/v1/feature_views/:name",
  ({ params }) => {
    const name = params.name as string;
    const fv = allFeatureViewsJSON.find((f: any) => f.spec?.name === name);
    if (!fv) return HttpResponse.json({ detail: "Not found" }, { status: 404 });
    return HttpResponse.json(fv);
  },
);

const restEntityDetail = http.get("/api/v1/entities/:name", ({ params }) => {
  const name = params.name as string;
  const entity = entitiesJSON.find((e: any) => e.spec?.name === name);
  if (!entity)
    return HttpResponse.json({ detail: "Not found" }, { status: 404 });
  return HttpResponse.json(entity);
});

const restFeatureServiceDetail = http.get(
  "/api/v1/feature_services/:name",
  ({ params }) => {
    const name = params.name as string;
    const fs = featureServicesJSON.find((f: any) => f.spec?.name === name);
    if (!fs) return HttpResponse.json({ detail: "Not found" }, { status: 404 });
    return HttpResponse.json(fs);
  },
);

const restDataSourceDetail = http.get(
  "/api/v1/data_sources/:name",
  ({ params }) => {
    const name = params.name as string;
    const ds = dataSourcesJSON.find((d: any) => d.name === name);
    if (!ds) return HttpResponse.json({ detail: "Not found" }, { status: 404 });
    return HttpResponse.json(ds);
  },
);

const restFeatureDetail = http.get(
  "/api/v1/features/:fvName/:featureName",
  ({ params }) => {
    const fvName = params.fvName as string;
    const featureName = params.featureName as string;
    const fv = allFeatureViewsJSON.find((f: any) => f.spec?.name === fvName);
    if (!fv) return HttpResponse.json({ detail: "Not found" }, { status: 404 });
    const feature = (fv as any).spec?.features?.find(
      (f: any) => f.name === featureName,
    );
    if (!feature)
      return HttpResponse.json({ detail: "Not found" }, { status: 404 });
    return HttpResponse.json({
      featureViewName: fvName,
      featureName,
      feature,
      featureView: fv,
    });
  },
);

// "all" endpoints (for global search / all-projects view)
const restEntitiesAll = http.get("/api/v1/entities/all", () =>
  HttpResponse.json({
    entities: entitiesJSON.map((e: any) => ({
      ...e,
      project: e.spec?.project,
    })),
    pagination: {},
    relationships: {},
  }),
);

const restFeatureViewsAll = http.get("/api/v1/feature_views/all", () =>
  HttpResponse.json({
    featureViews: allFeatureViewsJSON.map((fv: any) => ({
      ...fv,
      project: fv.spec?.project,
    })),
    pagination: {},
    relationships: {},
  }),
);

const restFeatureServicesAll = http.get("/api/v1/feature_services/all", () =>
  HttpResponse.json({
    featureServices: featureServicesJSON.map((fs: any) => ({
      ...fs,
      project: fs.spec?.project,
    })),
    pagination: {},
    relationships: {},
  }),
);

const restDataSourcesAll = http.get("/api/v1/data_sources/all", () =>
  HttpResponse.json({
    dataSources: dataSourcesJSON.map((ds: any) => ({
      ...ds,
      project: ds.project,
    })),
    pagination: {},
    relationships: {},
  }),
);

const restSavedDatasetsAll = http.get("/api/v1/saved_datasets/all", () =>
  HttpResponse.json({
    savedDatasets: savedDatasetsJSON,
    pagination: {},
  }),
);

const restFeaturesAll = http.get("/api/v1/features/all", () =>
  HttpResponse.json({
    features: allFeatures,
    pagination: {},
  }),
);

const restSavedDatasetDetail = http.get(
  "/api/v1/saved_datasets/:name",
  ({ params }) => {
    const name = params.name as string;
    const sd = savedDatasetsJSON.find((d: any) => d.spec?.name === name);
    if (!sd) return HttpResponse.json({ detail: "Not found" }, { status: 404 });
    return HttpResponse.json(sd);
  },
);

const restMetrics = http.get("/api/v1/metrics/:type", () =>
  HttpResponse.json({}),
);

const allRestHandlers = [
  projectsListWithDefaultProject,
  // "all" endpoints must come before parameterized detail routes
  restEntitiesAll,
  restFeatureViewsAll,
  restFeatureServicesAll,
  restDataSourcesAll,
  restSavedDatasetsAll,
  restFeaturesAll,
  // List endpoints
  restEntities,
  restFeatureViews,
  restFeatureServices,
  restDataSources,
  restSavedDatasets,
  restProjects,
  restFeatures,
  restPermissions,
  // Detail endpoints
  restFeatureViewDetail,
  restEntityDetail,
  restFeatureServiceDetail,
  restDataSourceDetail,
  restSavedDatasetDetail,
  restFeatureDetail,
  restMetrics,
];

export { projectsListWithDefaultProject, allRestHandlers };
