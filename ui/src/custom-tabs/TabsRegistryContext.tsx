import React, { useEffect, useState } from "react";

import {
  useResolvedPath,
  resolvePath,
  useLocation,
  NavigateFunction,
  Route,
} from "react-router-dom";

import RegularFeatureViewCustomTabLoadingWrapper from "../utils/custom-tabs/RegularFeatureViewCustomTabLoadingWrapper";
import OnDemandFeatureViewCustomTabLoadingWrapper from "../utils/custom-tabs/OnDemandFeatureViewCustomTabLoadingWrapper";
import FeatureServiceCustomTabLoadingWrapper from "../utils/custom-tabs/FeatureServiceCustomTabLoadingWrapper";
import DataSourceCustomTabLoadingWrapper from "../utils/custom-tabs/DataSourceCustomTabLoadingWrapper";
import EntityCustomTabLoadingWrapper from "../utils/custom-tabs/EntityCustomTabLoadingWrapper";
import DatasetCustomTabLoadingWrapper from "../utils/custom-tabs/DatasetCustomTabLoadingWrapper";

import {
  RegularFeatureViewCustomTabRegistrationInterface,
  OnDemandFeatureViewCustomTabRegistrationInterface,
  FeatureServiceCustomTabRegistrationInterface,
  DataSourceCustomTabRegistrationInterface,
  EntityCustomTabRegistrationInterface,
  DatasetCustomTabRegistrationInterface,
  CustomTabRegistrationInterface,
} from "./types";

interface FeastTabsRegistryInterface {
  RegularFeatureViewCustomTabs?: RegularFeatureViewCustomTabRegistrationInterface[];
  OnDemandFeatureViewCustomTabs?: OnDemandFeatureViewCustomTabRegistrationInterface[];
  FeatureServiceCustomTabs?: FeatureServiceCustomTabRegistrationInterface[];
  DataSourceCustomTabs?: DataSourceCustomTabRegistrationInterface[];
  EntityCustomTabs?: EntityCustomTabRegistrationInterface[];
  DatasetCustomTabs?: DatasetCustomTabRegistrationInterface[];
}

interface NavigationTabInterface {
  label: string;
  isSelected: boolean;
  onClick: () => void;
}

const TabsRegistryContext = React.createContext<FeastTabsRegistryInterface>({});

const useGenericCustomTabsNavigation = <
  T extends CustomTabRegistrationInterface
>(
  entries: T[],
  navigate: NavigateFunction
) => {
  // Check for Duplicates
  const arrayOfPaths = entries.map((tab) => tab.path);

  const duplicatedPaths = arrayOfPaths.filter(
    (item, index) => arrayOfPaths.indexOf(item) !== index
  );

  // Throw error if multiple custom tabs being registered to the same path
  if (duplicatedPaths.length) {
    throw new Error(
      `More than one tabs registered for path url: ${duplicatedPaths.join(
        ", "
      )}`
    );
  }

  const [customNavigationTabs, setTabs] = useState<NavigationTabInterface[]>(
    []
  );

  const featureViewRoot = useResolvedPath(""); // Root of Feature View Section
  const { pathname } = useLocation(); // Current Location

  useEffect(() => {
    setTabs(
      entries.map(({ label, path }) => {
        const resolvedTabPath = resolvePath(path, featureViewRoot.pathname);

        return {
          label,
          // Can't use the match hooks here b/c we're in a loop due
          // to React hooks needing a predictable number of
          // hooks to be run. See: https://reactjs.org/docs/hooks-rules.html
          isSelected: pathname === resolvedTabPath.pathname,
          onClick: () => {
            navigate(path);
          },
        };
      })
    );
  }, [pathname, navigate, featureViewRoot.pathname, entries]);

  return {
    customNavigationTabs,
  };
};

// Creating Routes
interface InnerComponent<T> {
  label: string;
  path: string;
  Component: (props: T) => JSX.Element;
}
type WrapperComponentType<T> = ({
  Component,
}: {
  Component: (props: T) => JSX.Element;
}) => JSX.Element;

const genericCustomTabRoutes = <T,>(
  tabs: InnerComponent<T>[],
  WrapperComponent: WrapperComponentType<T>
) => {
  return tabs.map(({ path, Component }) => {
    const WrappedComponent = () => {
      return <WrapperComponent Component={Component} />;
    };

    return (
      <Route key={path} path={`/${path}/*`} element={<WrappedComponent />} />
    );
  });
};

// Navigation Hooks for Each Custom Tab Type
const useRegularFeatureViewCustomTabs = (navigate: NavigateFunction) => {
  const { RegularFeatureViewCustomTabs } =
    React.useContext(TabsRegistryContext);

  return useGenericCustomTabsNavigation<RegularFeatureViewCustomTabRegistrationInterface>(
    RegularFeatureViewCustomTabs || [],
    navigate
  );
};

const useOnDemandFeatureViewCustomTabs = (navigate: NavigateFunction) => {
  const { OnDemandFeatureViewCustomTabs } =
    React.useContext(TabsRegistryContext);

  return useGenericCustomTabsNavigation<OnDemandFeatureViewCustomTabRegistrationInterface>(
    OnDemandFeatureViewCustomTabs || [],
    navigate
  );
};

const useFeatureServiceCustomTabs = (navigate: NavigateFunction) => {
  const { FeatureServiceCustomTabs } = React.useContext(TabsRegistryContext);

  return useGenericCustomTabsNavigation<FeatureServiceCustomTabRegistrationInterface>(
    FeatureServiceCustomTabs || [],
    navigate
  );
};

const useDataSourceCustomTabs = (navigate: NavigateFunction) => {
  const { DataSourceCustomTabs } = React.useContext(TabsRegistryContext);

  return useGenericCustomTabsNavigation<DataSourceCustomTabRegistrationInterface>(
    DataSourceCustomTabs || [],
    navigate
  );
};

const useEntityCustomTabs = (navigate: NavigateFunction) => {
  const { EntityCustomTabs } = React.useContext(TabsRegistryContext);

  return useGenericCustomTabsNavigation<EntityCustomTabRegistrationInterface>(
    EntityCustomTabs || [],
    navigate
  );
};

const useDatasetCustomTabs = (navigate: NavigateFunction) => {
  const { DatasetCustomTabs } = React.useContext(TabsRegistryContext);

  return useGenericCustomTabsNavigation<DatasetCustomTabRegistrationInterface>(
    DatasetCustomTabs || [],
    navigate
  );
};

// Routes for Each Custom Tab Type
const useRegularFeatureViewCustomTabRoutes = () => {
  const { RegularFeatureViewCustomTabs } =
    React.useContext(TabsRegistryContext);

  return genericCustomTabRoutes(
    RegularFeatureViewCustomTabs || [],
    RegularFeatureViewCustomTabLoadingWrapper
  );
};

const useOnDemandFeatureViewCustomTabRoutes = () => {
  const { OnDemandFeatureViewCustomTabs } =
    React.useContext(TabsRegistryContext);

  return genericCustomTabRoutes(
    OnDemandFeatureViewCustomTabs || [],
    OnDemandFeatureViewCustomTabLoadingWrapper
  );
};

const useFeatureServiceCustomTabRoutes = () => {
  const { FeatureServiceCustomTabs } = React.useContext(TabsRegistryContext);

  return genericCustomTabRoutes(
    FeatureServiceCustomTabs || [],
    FeatureServiceCustomTabLoadingWrapper
  );
};

const useDataSourceCustomTabRoutes = () => {
  const { DataSourceCustomTabs } = React.useContext(TabsRegistryContext);

  return genericCustomTabRoutes(
    DataSourceCustomTabs || [],
    DataSourceCustomTabLoadingWrapper
  );
};

const useEntityCustomTabRoutes = () => {
  const { EntityCustomTabs } = React.useContext(TabsRegistryContext);

  return genericCustomTabRoutes(
    EntityCustomTabs || [],
    EntityCustomTabLoadingWrapper
  );
};

const useDatasetCustomTabRoutes = () => {
  const { DatasetCustomTabs } = React.useContext(TabsRegistryContext);

  return genericCustomTabRoutes(
    DatasetCustomTabs || [],
    DatasetCustomTabLoadingWrapper
  );
};

export default TabsRegistryContext;
export {
  // Navigation
  useRegularFeatureViewCustomTabs,
  useOnDemandFeatureViewCustomTabs,
  useFeatureServiceCustomTabs,
  useDataSourceCustomTabs,
  useEntityCustomTabs,
  useDatasetCustomTabs,
  // Routes
  useRegularFeatureViewCustomTabRoutes,
  useOnDemandFeatureViewCustomTabRoutes,
  useFeatureServiceCustomTabRoutes,
  useDataSourceCustomTabRoutes,
  useEntityCustomTabRoutes,
  useDatasetCustomTabRoutes,
};

export type { FeastTabsRegistryInterface };
