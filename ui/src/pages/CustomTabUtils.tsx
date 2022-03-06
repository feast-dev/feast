import React, { useState, useEffect } from "react";

import {
  DataSourceCustomTabs,
  FeatureServiceCustomTabs,
  RegularFeatureViewCustomTabs,
  EntityCustomTabs,
  OnDemandFeatureViewCustomTabs,
  DatasetCustomTabs,
} from "../custom-tabs/CustomTabsRegistry";
import {
  useResolvedPath,
  resolvePath,
  useLocation,
  NavigateFunction,
  Route,
} from "react-router";

import {
  CustomTabRegistrationInterface,
  DataSourceCustomTabRegistrationInterface,
  EntityCustomTabRegistrationInterface,
  FeatureServiceCustomTabRegistrationInterface,
  RegularFeatureViewCustomTabRegistrationInterface,
  OnDemandFeatureViewCustomTabRegistrationInterface,
  DatasetCustomTabRegistrationInterface,
} from "../custom-tabs/types";

import RegularFeatureViewCustomTabLoadingWrapper from "../utils/custom-tabs/RegularFeatureViewCustomTabLoadingWrapper";
import OnDemandFeatureViewCustomTabLoadingWrapper from "../utils/custom-tabs/OnDemandFeatureViewCustomTabLoadingWrapper";
import FeatureServiceCustomTabLoadingWrapper from "../utils/custom-tabs/FeatureServiceCustomTabLoadingWrapper";
import DataSourceCustomTabLoadingWrapper from "../utils/custom-tabs/DataSourceCustomTabLoadingWrapper";
import EntityCustomTabLoadingWrapper from "../utils/custom-tabs/EntityCustomTabLoadingWrapper";
import DatasetCustomTabLoadingWrapper from "../utils/custom-tabs/DatasetCustomTabLoadingWrapper";

interface NavigationTabInterface {
  label: string;
  isSelected: boolean;
  onClick: () => void;
}

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

const useRegularFeatureViewCustomTabs = (navigate: NavigateFunction) => {
  return useGenericCustomTabsNavigation<RegularFeatureViewCustomTabRegistrationInterface>(
    RegularFeatureViewCustomTabs,
    navigate
  );
};

const useOnDemandFeatureViewCustomTabs = (navigate: NavigateFunction) => {
  return useGenericCustomTabsNavigation<OnDemandFeatureViewCustomTabRegistrationInterface>(
    OnDemandFeatureViewCustomTabs,
    navigate
  );
};

const useFeatureServiceCustomTabs = (navigate: NavigateFunction) => {
  return useGenericCustomTabsNavigation<FeatureServiceCustomTabRegistrationInterface>(
    FeatureServiceCustomTabs,
    navigate
  );
};

const useDataSourceCustomTabs = (navigate: NavigateFunction) => {
  return useGenericCustomTabsNavigation<DataSourceCustomTabRegistrationInterface>(
    DataSourceCustomTabs,
    navigate
  );
};

const useEntityCustomTabs = (navigate: NavigateFunction) => {
  return useGenericCustomTabsNavigation<EntityCustomTabRegistrationInterface>(
    EntityCustomTabs,
    navigate
  );
};

const useDatasetCustomTabs = (navigate: NavigateFunction) => {
  return useGenericCustomTabsNavigation<DatasetCustomTabRegistrationInterface>(
    DatasetCustomTabs,
    navigate
  );
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

const regularFeatureViewCustomTabRoutes = () => {
  return genericCustomTabRoutes(
    RegularFeatureViewCustomTabs,
    RegularFeatureViewCustomTabLoadingWrapper
  );
};

const onDemandFeatureViewCustomTabRoutes = () => {
  return genericCustomTabRoutes(
    OnDemandFeatureViewCustomTabs,
    OnDemandFeatureViewCustomTabLoadingWrapper
  );
};

const featureServiceCustomTabRoutes = () => {
  return genericCustomTabRoutes(
    FeatureServiceCustomTabs,
    FeatureServiceCustomTabLoadingWrapper
  );
};

const dataSourceCustomTabRoutes = () => {
  return genericCustomTabRoutes(
    DataSourceCustomTabs,
    DataSourceCustomTabLoadingWrapper
  );
};

const entityCustomTabRoutes = () => {
  return genericCustomTabRoutes(
    EntityCustomTabs,
    EntityCustomTabLoadingWrapper
  );
};

const datasetCustomTabRoutes = () => {
  return genericCustomTabRoutes(
    DatasetCustomTabs,
    DatasetCustomTabLoadingWrapper
  );
};

export {
  useRegularFeatureViewCustomTabs,
  regularFeatureViewCustomTabRoutes,
  useOnDemandFeatureViewCustomTabs,
  onDemandFeatureViewCustomTabRoutes,
  useFeatureServiceCustomTabs,
  featureServiceCustomTabRoutes,
  useDataSourceCustomTabs,
  dataSourceCustomTabRoutes,
  useEntityCustomTabs,
  entityCustomTabRoutes,
  useDatasetCustomTabs,
  datasetCustomTabRoutes,
};
