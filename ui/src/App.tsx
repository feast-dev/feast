import React from "react";
import "inter-ui/inter.css";
import "@elastic/eui/dist/eui_theme_light.css";

import { EuiProvider } from "@elastic/eui";
import ProjectOverviewPage from "./pages/ProjectOverviewPage";
import { Route, Routes } from "react-router";
import Layout from "./pages/Layout";
import NoMatch from "./pages/NoMatch";
import DatasourceIndex from "./pages/data-sources/Index";
import DatasetIndex from "./pages/saved-data-sets/Index";
import EntityIndex from "./pages/entities/Index";
import EntityInstance from "./pages/entities/EntityInstance";
import FeatureServiceIndex from "./pages/feature-services/Index";
import FeatureViewIndex from "./pages/feature-views/Index";
import FeatureViewInstance from "./pages/feature-views/FeatureViewInstance";
import FeatureServiceInstance from "./pages/feature-services/FeatureServiceInstance";
import DataSourceInstance from "./pages/data-sources/DataSourceInstance";
import RootProjectSelectionPage from "./pages/RootProjectSelectionPage";
import DatasetInstance from "./pages/saved-data-sets/DatasetInstance";
import NoProjectGuard from "./components/NoProjectGuard";

const App = () => {
  return (
    <EuiProvider colorMode="light">
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
            <Route path="feature-view/" element={<FeatureViewIndex />} />
            <Route
              path="feature-view/:featureViewName/*"
              element={<FeatureViewInstance />}
            />
            <Route path="feature-service/" element={<FeatureServiceIndex />} />
            <Route
              path="feature-service/:featureServiceName/*"
              element={<FeatureServiceInstance />}
            />
            <Route path="entity/" element={<EntityIndex />} />
            <Route path="entity/:entityName/*" element={<EntityInstance />} />

            <Route path="data-set/" element={<DatasetIndex />} />
            <Route
              path="data-set/:datasetName/*"
              element={<DatasetInstance />}
            />
          </Route>
        </Route>
        <Route path="*" element={<NoMatch />} />
      </Routes>
    </EuiProvider>
  );
};

export default App;
