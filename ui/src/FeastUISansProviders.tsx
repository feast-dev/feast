import React from "react";

import "./index.css";

import { Routes, Route } from "react-router-dom";
import { EuiProvider, EuiErrorBoundary } from "@elastic/eui";
import { ThemeProvider, useTheme } from "./contexts/ThemeContext";

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
import LabelViewIndex from "./pages/label-views/Index";
import LabelViewInstance from "./pages/label-views/LabelViewInstance";
import PermissionsIndex from "./pages/permissions/Index";
import LineageIndex from "./pages/lineage/Index";
import NoProjectGuard from "./components/NoProjectGuard";
import MonitoringIndex from "./pages/monitoring/Index";
import FeatureMetricsDetail from "./pages/monitoring/FeatureMetricsDetail";
import ComputeEngineIndex from "./pages/compute-engines/Index";

import TabsRegistryContext, {
  FeastTabsRegistryInterface,
} from "./custom-tabs/TabsRegistryContext";
import MonitoringContext, {
  MonitoringConfig,
} from "./contexts/MonitoringContext";
import CurlGeneratorTab from "./pages/feature-views/CurlGeneratorTab";
import FeatureFlagsContext, {
  FeatureFlags,
} from "./contexts/FeatureFlagsContext";
import {
  ProjectListContext,
  ProjectsListContextInterface,
} from "./contexts/ProjectListContext";
import DataModeContext from "./contexts/DataModeContext";
import type { DataModeConfig, FetchOptions } from "./contexts/DataModeContext";
import { AuthProvider, useAuth } from "./contexts/AuthContext";

interface FeastUIConfigs {
  tabsRegistry?: FeastTabsRegistryInterface;
  featureFlags?: FeatureFlags;
  projectListPromise?: Promise<any>;
  fetchOptions?: FetchOptions;
  monitoringConfig?: MonitoringConfig;
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
    <ThemeProvider>
      <AuthProvider>
        <FeastUISansProvidersInner
          basename={basename}
          projectListContext={projectListContext}
          feastUIConfigs={feastUIConfigs}
        />
      </AuthProvider>
    </ThemeProvider>
  );
};

const FeastUISansProvidersInner = ({
  basename,
  projectListContext,
  feastUIConfigs,
}: {
  basename: string;
  projectListContext: ProjectsListContextInterface;
  feastUIConfigs?: FeastUIConfigs;
}) => {
  const { colorMode } = useTheme();
  const { isInitializing } = useAuth();

  const dataModeConfig: DataModeConfig = {
    fetchOptions: feastUIConfigs?.fetchOptions,
  };

  const monitoringConfig: MonitoringConfig =
    feastUIConfigs?.monitoringConfig || {
      apiBaseUrl: "/api/v1",
      enabled: true,
    };

  if (isInitializing) {
    return (
      <EuiProvider colorMode={colorMode}>
        <div
          style={{
            display: "flex",
            justifyContent: "center",
            alignItems: "center",
            height: "100vh",
            flexDirection: "column",
            gap: 16,
          }}
        >
          <div className="euiLoadingSpinner euiLoadingSpinner--large" />
          <p style={{ color: "#69707D" }}>Connecting to identity provider...</p>
        </div>
      </EuiProvider>
    );
  }

  return (
    <EuiProvider colorMode={colorMode}>
      <EuiErrorBoundary>
        <DataModeContext.Provider value={dataModeConfig}>
          <TabsRegistryContext.Provider
            value={{
              RegularFeatureViewCustomTabs: [
                {
                  label: "CURL Generator",
                  path: "curl-generator",
                  Component: CurlGeneratorTab,
                },
                ...(feastUIConfigs?.tabsRegistry
                  ?.RegularFeatureViewCustomTabs || []),
              ],
              OnDemandFeatureViewCustomTabs:
                feastUIConfigs?.tabsRegistry?.OnDemandFeatureViewCustomTabs ||
                [],
              StreamFeatureViewCustomTabs:
                feastUIConfigs?.tabsRegistry?.StreamFeatureViewCustomTabs || [],
              FeatureServiceCustomTabs:
                feastUIConfigs?.tabsRegistry?.FeatureServiceCustomTabs || [],
              FeatureCustomTabs:
                feastUIConfigs?.tabsRegistry?.FeatureCustomTabs || [],
              DataSourceCustomTabs:
                feastUIConfigs?.tabsRegistry?.DataSourceCustomTabs || [],
              EntityCustomTabs:
                feastUIConfigs?.tabsRegistry?.EntityCustomTabs || [],
              DatasetCustomTabs:
                feastUIConfigs?.tabsRegistry?.DatasetCustomTabs || [],
            }}
          >
            <FeatureFlagsContext.Provider
              value={feastUIConfigs?.featureFlags || {}}
            >
              <MonitoringContext.Provider value={monitoringConfig}>
                <ProjectListContext.Provider value={projectListContext}>
                  <Routes>
                    <Route path="/" element={<Layout />}>
                      <Route index element={<RootProjectSelectionPage />} />
                      <Route
                        path="/p/:projectName/*"
                        element={<NoProjectGuard />}
                      >
                        <Route index element={<ProjectOverviewPage />} />
                        <Route
                          path="data-source/"
                          element={<DatasourceIndex />}
                        />
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
                        <Route
                          path="label-view/"
                          element={<LabelViewIndex />}
                        />
                        <Route
                          path="label-view/:labelViewName/*"
                          element={<LabelViewInstance />}
                        />
                        <Route
                          path="label-view/:FeatureViewName/label/:FeatureName/*"
                          element={<FeatureInstance />}
                        />
                        <Route path="data-set/" element={<DatasetIndex />} />
                        <Route
                          path="data-set/:datasetName/*"
                          element={<DatasetInstance />}
                        />
                        <Route
                          path="permissions/"
                          element={<PermissionsIndex />}
                        />
                        <Route path="lineage/" element={<LineageIndex />} />
                        <Route
                          path="monitoring/"
                          element={<MonitoringIndex />}
                        />
                        <Route
                          path="monitoring/feature/:featureViewName/:featureName"
                          element={<FeatureMetricsDetail />}
                        />
                        <Route
                          path="compute-engine/*"
                          element={<ComputeEngineIndex />}
                        />
                      </Route>
                    </Route>
                    <Route path="*" element={<NoMatch />} />
                  </Routes>
                </ProjectListContext.Provider>
              </MonitoringContext.Provider>
            </FeatureFlagsContext.Provider>
          </TabsRegistryContext.Provider>
        </DataModeContext.Provider>
      </EuiErrorBoundary>
    </EuiProvider>
  );
};

export default FeastUISansProviders;
export type { FeastUIConfigs };
