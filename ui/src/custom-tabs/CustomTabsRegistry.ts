import {
  DatasetCustomTabRegistrationInterface,
  DataSourceCustomTabRegistrationInterface,
  EntityCustomTabRegistrationInterface,
  FeatureServiceCustomTabRegistrationInterface,
  OnDemandFeatureViewCustomTabRegistrationInterface,
  RegularFeatureViewCustomTabRegistrationInterface,
} from "./types";

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
import RFVDemoCustomTab from "./reguar-fv-demo-tab/DemoCustomTab";
import ODFVDemoCustomTab from "./ondemand-fv-demo-tab/DemoCustomTab";
import FSDemoCustomTab from "./feature-service-demo-tab/DemoCustomTab";
import DSDemoCustomTab from "./data-source-demo-tab/DemoCustomTab";
import EntDemoCustomTab from "./entity-demo-tab/DemoCustomTab";
import DatasetDemoCustomTab from "./dataset-demo-tab/DemoCustomTab";

// Registry of Custom Tabs
const RegularFeatureViewCustomTabs: RegularFeatureViewCustomTabRegistrationInterface[] =
  [
    {
      label: "Custom Tab Demo", // Navigation Label for the tab
      path: "demo-tab", // Subpath for the tab
      Component: RFVDemoCustomTab,
    },
  ];

const OnDemandFeatureViewCustomTabs: OnDemandFeatureViewCustomTabRegistrationInterface[] =
  [
    {
      label: "Custom Tab Demo",
      path: "demo-tab",
      Component: ODFVDemoCustomTab,
    },
  ];

const FeatureServiceCustomTabs: FeatureServiceCustomTabRegistrationInterface[] =
  [
    {
      label: "Custom Tab Demo",
      path: "fs-demo-tab",
      Component: FSDemoCustomTab,
    },
  ];

const DataSourceCustomTabs: DataSourceCustomTabRegistrationInterface[] = [
  {
    label: "Custom Tab Demo",
    path: "fs-demo-tab",
    Component: DSDemoCustomTab,
  },
];

const EntityCustomTabs: EntityCustomTabRegistrationInterface[] = [
  {
    label: "Custom Tab Demo",
    path: "demo-tab",
    Component: EntDemoCustomTab,
  },
];

const DatasetCustomTabs: DatasetCustomTabRegistrationInterface[] = [
  {
    label: "Custom Tab Demo",
    path: "demo-tab",
    Component: DatasetDemoCustomTab,
  },
];

export {
  RegularFeatureViewCustomTabs,
  OnDemandFeatureViewCustomTabs,
  FeatureServiceCustomTabs,
  DataSourceCustomTabs,
  EntityCustomTabs,
  DatasetCustomTabs,
};
