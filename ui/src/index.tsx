import React from "react";
import ReactDOM from "react-dom";
import { QueryClient } from "react-query";
import FeastUI from "./FeastUI";

// How to add a Custom Tab
// 1. Pick which object type you want your tab
//    to be in. e.g. Feature View, Feature Service, etc.
//
// 2. Write a regular React Component for Tab Content.
//    It will be passed props with data about the Feast FCO
//    e.g. RegularFeatureViewCustomTabProps, FeatureServiceCustomTabProps
//    See: types.ts in this folder
//
// 3. Register the tab in the appropriate array below. Each entry
//    is a record with three keys: label, path, and Component.
//    Import your component and pass it as Component
import FSReportTab from "./custom-tabs/feature-service-report-tab/ProfilingReportTab";

const queryClient = new QueryClient();

const tabsRegistry = {
  FeatureServiceCustomTabs: [
    {
      label: "Profiling Report",
      path: "profiling-report",
      Component: FSReportTab,
    },
  ],
};

ReactDOM.render(
  <React.StrictMode>
    <FeastUI
      reactQueryClient={queryClient}
      feastUIConfigs={{
        tabsRegistry: tabsRegistry,
        projectListPromise: fetch(
          (process.env.PUBLIC_URL || "") + "/projects-list.json",
          {
            headers: {
              "Content-Type": "application/json",
            },
          }
        ).then((res) => {
          return res.json();
        }),
      }}
    />
  </React.StrictMode>,
  document.getElementById("root")
);
