const fs = require("fs");
const path = require("path");
const express = require("express");
const { feast } = require("./protos");

const registryBuf = fs.readFileSync(
  path.resolve(__dirname, "../public/registry.db"),
);
const parsedRegistry = feast.core.Registry.decode(registryBuf);
const projectsList = JSON.parse(
  fs.readFileSync(path.resolve(__dirname, "../public/projects-list.json")),
);

const toJSON = (obj) => (obj && obj.toJSON ? obj.toJSON() : obj);

const withType = (type) => (fv) => ({
  ...toJSON(fv),
  type,
});

const state = {
  entities: (parsedRegistry.entities || []).map(toJSON),
  featureViews: (parsedRegistry.featureViews || []).map(
    withType("featureView"),
  ),
  onDemandFeatureViews: (parsedRegistry.onDemandFeatureViews || []).map(
    withType("onDemandFeatureView"),
  ),
  streamFeatureViews: (parsedRegistry.streamFeatureViews || []).map(
    withType("streamFeatureView"),
  ),
  featureServices: (parsedRegistry.featureServices || []).map(toJSON),
  dataSources: (parsedRegistry.dataSources || []).map(toJSON),
  savedDatasets: (parsedRegistry.savedDatasets || []).map(toJSON),
  projects: (parsedRegistry.projects || []).map(toJSON),
};

const allFeatureViews = () => [
  ...state.featureViews,
  ...state.onDemandFeatureViews,
  ...state.streamFeatureViews,
];

const objectProject = (obj) => obj?.spec?.project || obj?.project;

const filterByProject = (items, project) => {
  if (!project || project === "all") return items;
  return items.filter((item) => objectProject(item) === project);
};

const allFeatures = (project) =>
  filterByProject(allFeatureViews(), project).flatMap((fv) =>
    (fv?.spec?.features || []).map((feature) => ({
      name: feature.name,
      featureViewName: fv.spec?.name,
      valueType: feature.valueType,
      project: fv.spec?.project,
    })),
  );

const responseList = (res, key, items) => {
  res.json({
    [key]: items,
    pagination: {},
    relationships: {},
  });
};

const findByName = (items, name) =>
  items.find((item) => item?.spec?.name === name || item?.name === name);

const entityPayloadToResource = (payload) => ({
  spec: {
    name: payload.name,
    joinKey: payload.join_key || payload.name,
    valueType: payload.value_type,
    description: payload.description || "",
    tags: payload.tags || {},
    owner: payload.owner || "",
    project: payload.project,
  },
  meta: {},
});

const dataSourcePayloadToResource = (payload) => ({
  name: payload.name,
  type: payload.type,
  timestampField: payload.timestamp_field,
  fieldMapping: payload.field_mapping || {},
  description: payload.description || "",
  tags: payload.tags || {},
  owner: payload.owner || "",
  project: payload.project,
  fileOptions: payload.file_options,
  bigqueryOptions: payload.bigquery_options,
  snowflakeOptions: payload.snowflake_options,
  redshiftOptions: payload.redshift_options,
  kafkaOptions: payload.kafka_options,
  sparkOptions: payload.spark_options,
});

const featureViewPayloadToResource = (payload) => ({
  spec: {
    name: payload.name,
    description: payload.description || "",
    owner: payload.owner || "",
    entities: payload.entities || [],
    features: payload.features || [],
    ttl: payload.ttl,
    online: payload.online,
    tags: payload.tags || {},
    project: payload.project,
    batchSource: payload.batch_source
      ? { name: payload.batch_source }
      : undefined,
  },
  meta: {},
  type: "featureView",
});

module.exports = function setupProxy(app) {
  app.use("/api/v1", express.json());

  app.get("/projects-list.json", (_req, res) => {
    res.json({
      ...projectsList,
      projects: projectsList.projects.map((project) =>
        project.id === "credit_scoring_aws"
          ? { ...project, registryPath: "/api/v1" }
          : project,
      ),
    });
  });

  app.get("/api/v1/entities/all", (_req, res) =>
    responseList(res, "entities", state.entities),
  );
  app.get("/api/v1/feature_views/all", (_req, res) =>
    responseList(res, "featureViews", allFeatureViews()),
  );
  app.get("/api/v1/feature_services/all", (_req, res) =>
    responseList(res, "featureServices", state.featureServices),
  );
  app.get("/api/v1/data_sources/all", (_req, res) =>
    responseList(res, "dataSources", state.dataSources),
  );
  app.get("/api/v1/saved_datasets/all", (_req, res) =>
    responseList(res, "savedDatasets", state.savedDatasets),
  );
  app.get("/api/v1/features/all", (_req, res) =>
    responseList(res, "features", allFeatures()),
  );
  app.get("/api/v1/label_views/all", (_req, res) =>
    responseList(res, "featureViews", []),
  );

  app.get("/api/v1/entities", (req, res) =>
    responseList(
      res,
      "entities",
      filterByProject(state.entities, req.query.project),
    ),
  );
  app.get("/api/v1/feature_views", (req, res) =>
    responseList(
      res,
      "featureViews",
      filterByProject(allFeatureViews(), req.query.project),
    ),
  );
  app.get("/api/v1/feature_services", (req, res) =>
    responseList(
      res,
      "featureServices",
      filterByProject(state.featureServices, req.query.project),
    ),
  );
  app.get("/api/v1/data_sources", (req, res) =>
    responseList(
      res,
      "dataSources",
      filterByProject(state.dataSources, req.query.project),
    ),
  );
  app.get("/api/v1/saved_datasets", (req, res) =>
    responseList(
      res,
      "savedDatasets",
      filterByProject(state.savedDatasets, req.query.project),
    ),
  );
  app.get("/api/v1/features", (req, res) =>
    responseList(res, "features", allFeatures(req.query.project)),
  );
  app.get("/api/v1/label_views", (_req, res) =>
    responseList(res, "featureViews", []),
  );
  app.get("/api/v1/labels", (_req, res) => responseList(res, "labels", []));
  app.get("/api/v1/projects", (_req, res) =>
    responseList(res, "projects", state.projects),
  );
  app.get("/api/v1/permissions", (_req, res) =>
    responseList(res, "permissions", []),
  );
  app.get("/api/v1/metrics/:type", (_req, res) => res.json({}));

  app.get("/api/v1/entities/:name", (req, res) => {
    const entity = findByName(state.entities, req.params.name);
    if (!entity) return res.status(404).json({ detail: "Not found" });
    return res.json(entity);
  });
  app.get("/api/v1/feature_views/:name", (req, res) => {
    const featureView = findByName(allFeatureViews(), req.params.name);
    if (!featureView) return res.status(404).json({ detail: "Not found" });
    return res.json(featureView);
  });
  app.get("/api/v1/feature_services/:name", (req, res) => {
    const featureService = findByName(state.featureServices, req.params.name);
    if (!featureService) return res.status(404).json({ detail: "Not found" });
    return res.json(featureService);
  });
  app.get("/api/v1/data_sources/:name", (req, res) => {
    const dataSource = findByName(state.dataSources, req.params.name);
    if (!dataSource) return res.status(404).json({ detail: "Not found" });
    return res.json(dataSource);
  });
  app.get("/api/v1/saved_datasets/:name", (req, res) => {
    const savedDataset = findByName(state.savedDatasets, req.params.name);
    if (!savedDataset) return res.status(404).json({ detail: "Not found" });
    return res.json(savedDataset);
  });
  app.get("/api/v1/features/:fvName/:featureName", (req, res) => {
    const featureView = findByName(allFeatureViews(), req.params.fvName);
    const feature = featureView?.spec?.features?.find(
      (f) => f.name === req.params.featureName,
    );
    if (!feature) return res.status(404).json({ detail: "Not found" });
    return res.json({
      featureViewName: req.params.fvName,
      featureName: req.params.featureName,
      feature,
      featureView,
    });
  });

  app.post("/api/v1/entities", (req, res) => {
    const body = req.body || {};
    const existingIndex = state.entities.findIndex(
      (entity) => entity?.spec?.name === body.name,
    );
    const entity = entityPayloadToResource(body);
    if (existingIndex >= 0) {
      state.entities[existingIndex] = entity;
    } else {
      state.entities.push(entity);
    }
    res.json({
      name: body.name,
      project: body.project,
      status: "applied",
    });
  });

  app.post("/api/v1/data_sources", (req, res) => {
    const body = req.body || {};
    const existingIndex = state.dataSources.findIndex(
      (dataSource) => dataSource?.name === body.name,
    );
    const dataSource = dataSourcePayloadToResource(body);
    if (existingIndex >= 0) {
      state.dataSources[existingIndex] = dataSource;
    } else {
      state.dataSources.push(dataSource);
    }
    res.json({
      name: body.name,
      project: body.project,
      status: "applied",
    });
  });

  app.post("/api/v1/feature_views", (req, res) => {
    const body = req.body || {};
    const existingIndex = state.featureViews.findIndex(
      (featureView) => featureView?.spec?.name === body.name,
    );
    const featureView = featureViewPayloadToResource(body);
    if (existingIndex >= 0) {
      state.featureViews[existingIndex] = featureView;
    } else {
      state.featureViews.push(featureView);
    }
    res.json({
      name: body.name,
      project: body.project,
      status: "applied",
    });
  });

  app.delete("/api/v1/entities/:name", (req, res) => {
    state.entities = state.entities.filter(
      (entity) => entity?.spec?.name !== req.params.name,
    );
    res.json({
      name: req.params.name,
      project: req.query.project,
      status: "deleted",
    });
  });

  app.delete("/api/v1/data_sources/:name", (req, res) => {
    state.dataSources = state.dataSources.filter(
      (dataSource) => dataSource?.name !== req.params.name,
    );
    res.json({
      name: req.params.name,
      project: req.query.project,
      status: "deleted",
    });
  });

  app.delete("/api/v1/feature_views/:name", (req, res) => {
    state.featureViews = state.featureViews.filter(
      (featureView) => featureView?.spec?.name !== req.params.name,
    );
    res.json({
      name: req.params.name,
      project: req.query.project,
      status: "deleted",
    });
  });
};
