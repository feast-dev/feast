import React from "react";

import "@elastic/eui/dist/eui_theme_light.css";
import "./index.css";

import { Routes, Route } from "react-router-dom";
import { EuiProvider, EuiErrorBoundary } from "@elastic/eui";

import ProjectOverviewPage from "./pages/ProjectOverviewPage";
import Layout from "./pages/Layout";
import NoMatch from "./pages/NoMatch";
import DatasourceIndex from "./pages/data-sources/Index";
import DatasetIndex from "./pages/saved-data-sets/Index";
import EntityIndex from "./pages/entities/Index";
import EntityInstance from "./pages/entities/EntityInstance";
import FeatureListPage from "./pages/features/FeatureListPage";
import FeatureInstance from "./pages/features/FeatureInstance";
import FeatureServiceIndex from "./pages/feature-services/Index";
import FeatureViewIndex from "./pages/feature-views/Index";
import FeatureViewInstance from "./pages/feature-views/FeatureViewInstance";
import FeatureServiceInstance from "./pages/feature-services/FeatureServiceInstance";
import DataSourceInstance from "./pages/data-sources/DataSourceInstance";
import RootProjectSelectionPage from "./pages/RootProjectSelectionPage";
import DatasetInstance from "./pages/saved-data-sets/DatasetInstance";
import NoProjectGuard from "./components/NoProjectGuard";

import TabsRegistryContext, {
  FeastTabsRegistryInterface,
} from "./custom-tabs/TabsRegistryContext";
import FeatureFlagsContext, {
  FeatureFlags,
} from "./contexts/FeatureFlagsContext";
import {
  ProjectListContext,
  ProjectsListContextInterface,
} from "./contexts/ProjectListContext";

interface FeastUIConfigs {
  tabsRegistry?: FeastTabsRegistryInterface;
  featureFlags?: FeatureFlags;
  projectListPromise?: Promise<any>;
}

const defaultProjectListPromise = (basename: string) => {
  return fetch(`${basename}/projects-list.json`, {
    headers: {
      "Content-Type": "application/json",
    },
  }).then((res) => {
    return res.json();
  });
};

const FeastUISansProviders = ({
  basename = "",
  feastUIConfigs,
}: {
  basename?: string;
  feastUIConfigs?: FeastUIConfigs;
}) => {
  const projectListContext: ProjectsListContextInterface =
    feastUIConfigs?.projectListPromise
      ? {
          projectsListPromise: feastUIConfigs?.projectListPromise,
          isCustom: true,
        }
      : {
          projectsListPromise: defaultProjectListPromise(basename),
          isCustom: false,
        };

  return (
    <EuiProvider colorMode="light">
      <EuiErrorBoundary>
        <TabsRegistryContext.Provider
          value={feastUIConfigs?.tabsRegistry || {}}
        >
          <FeatureFlagsContext.Provider
            value={feastUIConfigs?.featureFlags || {}}
          >
            <ProjectListContext.Provider value={projectListContext}>
              <Routes>
                <Route path="/" element={<Layout />}>
                  <Route index element={<RootProjectSelectionPage />} />
                  <Route path="/p/:projectName/*" element={<NoProjectGuard />}>
                    <Route index element={<ProjectOverviewPage />} />
                    <Route path="data-source/" element={<DatasourceIndex />} />
                    <Route
                      path="data-source/:dataSourceName/*"
                      element={<DataSourceInstance />}
                    />
                    <Route path="features/" element={<FeatureListPage />} />
                    <Route
                      path="feature-view/"
                      element={<FeatureViewIndex />}
                    />
                    <Route
                      path="feature-view/:featureViewName/*"
                      element={<FeatureViewInstance />}
                    ></Route>
                    <Route
                      path="feature-view/:FeatureViewName/feature/:FeatureName/*"
                      element={<FeatureInstance />}
                    />
                    <Route
                      path="feature-service/"
                      element={<FeatureServiceIndex />}
                    />
                    <Route
                      path="feature-service/:featureServiceName/*"
                      element={<FeatureServiceInstance />}
                    />
                    <Route path="entity/" element={<EntityIndex />} />
                    <Route
                      path="entity/:entityName/*"
                      element={<EntityInstance />}
                    />

                    <Route path="data-set/" element={<DatasetIndex />} />
                    <Route
                      path="data-set/:datasetName/*"
                      element={<DatasetInstance />}
                    />
                  </Route>
                </Route>
                <Route path="*" element={<NoMatch />} />
              </Routes>
            </ProjectListContext.Provider>
          </FeatureFlagsContext.Provider>
        </TabsRegistryContext.Provider>
      </EuiErrorBoundary>
    </EuiProvider>
  );
};

export default FeastUISansProviders;
export type { FeastUIConfigs };
