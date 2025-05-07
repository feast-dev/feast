import React, { useContext, useState } from "react";

import { EuiIcon, EuiSideNav, htmlIdGenerator } from "@elastic/eui";
import { Link, useParams } from "react-router-dom";
import { useMatchSubpath } from "../hooks/useMatchSubpath";
import useLoadRegistry from "../queries/useLoadRegistry";
import RegistryPathContext from "../contexts/RegistryPathContext";

import { DataSourceIcon } from "../graphics/DataSourceIcon";
import { EntityIcon } from "../graphics/EntityIcon";
import { FeatureViewIcon } from "../graphics/FeatureViewIcon";
import { FeatureServiceIcon } from "../graphics/FeatureServiceIcon";
import { DatasetIcon } from "../graphics/DatasetIcon";
import { FeatureIcon } from "../graphics/FeatureIcon";
import { HomeIcon } from "../graphics/HomeIcon";
import { LineageIcon } from "../graphics/LineageIcon";
import { PermissionsIcon } from "../graphics/PermissionsIcon";

const SideNav = () => {
  const registryUrl = useContext(RegistryPathContext);
  const { isSuccess, data } = useLoadRegistry(registryUrl);
  const { projectName } = useParams();

  const [isSideNavOpenOnMobile, setisSideNavOpenOnMobile] = useState(false);

  const toggleOpenOnMobile = () => {
    setisSideNavOpenOnMobile(!isSideNavOpenOnMobile);
  };

  const dataSourcesLabel = `Data Sources ${
    isSuccess && data?.objects.dataSources
      ? `(${data?.objects.dataSources?.length})`
      : ""
  }`;

  const entitiesLabel = `Entities ${
    isSuccess && data?.objects.entities
      ? `(${data?.objects.entities?.length})`
      : ""
  }`;

  const featureViewsLabel = `Feature Views ${
    isSuccess && data?.mergedFVList && data?.mergedFVList.length > 0
      ? `(${data?.mergedFVList.length})`
      : ""
  }`;

  const featureListLabel = `Features ${
    isSuccess && data?.allFeatures && data?.allFeatures.length > 0
      ? `(${data?.allFeatures.length})`
      : ""
  }`;

  const featureServicesLabel = `Feature Services ${
    isSuccess && data?.objects.featureServices
      ? `(${data?.objects.featureServices?.length})`
      : ""
  }`;

  const savedDatasetsLabel = `Datasets ${
    isSuccess && data?.objects.savedDatasets
      ? `(${data?.objects.savedDatasets?.length})`
      : ""
  }`;

  const baseUrl = `/p/${projectName}`;

  const sideNav: React.ComponentProps<typeof EuiSideNav>["items"] = [
    {
      name: "Home",
      id: htmlIdGenerator("home")(),
      icon: <EuiIcon type={HomeIcon} />,
      renderItem: (props) => <Link {...props} to={`${baseUrl}`} />,
      isSelected: useMatchSubpath(`${baseUrl}$`),
    },
    {
      name: "Resources",
      id: htmlIdGenerator("resources")(),
      items: [
        {
          name: "Lineage",
          id: htmlIdGenerator("lineage")(),
          icon: <EuiIcon type={LineageIcon} />,
          renderItem: (props) => <Link {...props} to={`${baseUrl}/lineage`} />,
          isSelected: useMatchSubpath(`${baseUrl}/lineage`),
        },
        {
          name: dataSourcesLabel,
          id: htmlIdGenerator("dataSources")(),
          icon: <EuiIcon type={DataSourceIcon} />,
          renderItem: (props) => (
            <Link {...props} to={`${baseUrl}/data-source`} />
          ),
          isSelected: useMatchSubpath(`${baseUrl}/data-source`),
        },
        {
          name: entitiesLabel,
          id: htmlIdGenerator("entities")(),
          icon: <EuiIcon type={EntityIcon} />,
          renderItem: (props) => <Link {...props} to={`${baseUrl}/entity`} />,
          isSelected: useMatchSubpath(`${baseUrl}/entity`),
        },
        {
          name: featureListLabel,
          id: htmlIdGenerator("featureList")(),
          icon: <EuiIcon type={FeatureIcon} />,
          renderItem: (props) => <Link {...props} to={`${baseUrl}/features`} />,
          isSelected: useMatchSubpath(`${baseUrl}/features`),
        },
        {
          name: featureViewsLabel,
          id: htmlIdGenerator("featureView")(),
          icon: <EuiIcon type={FeatureViewIcon} />,
          renderItem: (props) => (
            <Link {...props} to={`${baseUrl}/feature-view`} />
          ),
          isSelected: useMatchSubpath(`${baseUrl}/feature-view`),
        },
        {
          name: featureServicesLabel,
          id: htmlIdGenerator("featureService")(),
          icon: <EuiIcon type={FeatureServiceIcon} />,
          renderItem: (props) => (
            <Link {...props} to={`${baseUrl}/feature-service`} />
          ),
          isSelected: useMatchSubpath(`${baseUrl}/feature-service`),
        },
        {
          name: savedDatasetsLabel,
          id: htmlIdGenerator("savedDatasets")(),
          icon: <EuiIcon type={DatasetIcon} />,
          renderItem: (props) => <Link {...props} to={`${baseUrl}/data-set`} />,
          isSelected: useMatchSubpath(`${baseUrl}/data-set`),
        },
        {
          name: "Permissions",
          id: htmlIdGenerator("permissions")(),
          icon: <EuiIcon type={PermissionsIcon} />,
          renderItem: (props) => (
            <Link {...props} to={`${baseUrl}/permissions`} />
          ),
          isSelected: useMatchSubpath(`${baseUrl}/permissions`),
        },
      ],
    },
  ];

  return (
    <EuiSideNav
      aria-label="Project Level"
      mobileTitle="Feast"
      toggleOpenOnMobile={() => toggleOpenOnMobile()}
      isOpenOnMobile={isSideNavOpenOnMobile}
      items={sideNav}
    />
  );
};

export default SideNav;
