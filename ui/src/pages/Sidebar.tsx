import React, { useState } from "react";

import { EuiIcon, EuiSideNav, htmlIdGenerator } from "@elastic/eui";
import { Link, useParams } from "react-router-dom";
import { useMatchSubpath } from "../hooks/useMatchSubpath";
import useResourceQuery, {
  entityListPath,
  featureViewListPath,
  featureServiceListPath,
  dataSourceListPath,
  savedDatasetListPath,
  featuresListPath,
  restFeatureViewsToMergedList,
} from "../queries/useResourceQuery";

import { DataSourceIcon } from "../graphics/DataSourceIcon";
import { EntityIcon } from "../graphics/EntityIcon";
import { FeatureViewIcon } from "../graphics/FeatureViewIcon";
import { FeatureServiceIcon } from "../graphics/FeatureServiceIcon";
import { DatasetIcon } from "../graphics/DatasetIcon";
import { FeatureIcon } from "../graphics/FeatureIcon";
import { HomeIcon } from "../graphics/HomeIcon";
import { PermissionsIcon } from "../graphics/PermissionsIcon";
import type { genericFVType } from "../parsers/mergedFVTypes";

const SideNav = () => {
  const { projectName } = useParams();

  const { isSuccess: dsSuccess, data: dataSources } = useResourceQuery<any[]>({
    resourceType: "sidebar-ds",
    project: projectName,
    protoSelect: (d) => d.objects.dataSources,
    restPath: dataSourceListPath(projectName),
    restSelect: (d) => d.dataSources,
  });

  const { isSuccess: entSuccess, data: entities } = useResourceQuery<any[]>({
    resourceType: "sidebar-entities",
    project: projectName,
    protoSelect: (d) => d.objects.entities,
    restPath: entityListPath(projectName),
    restSelect: (d) => d.entities,
  });

  const { isSuccess: fvSuccess, data: featureViews } = useResourceQuery<
    genericFVType[]
  >({
    resourceType: "sidebar-fvs",
    project: projectName,
    protoSelect: (d) => d.mergedFVList,
    restPath: featureViewListPath(projectName),
    restSelect: restFeatureViewsToMergedList,
  });

  const { isSuccess: featSuccess, data: features } = useResourceQuery<any[]>({
    resourceType: "sidebar-features",
    project: projectName,
    protoSelect: (d) => d.allFeatures,
    restPath: featuresListPath(projectName),
    restSelect: (d) => d.features,
  });

  const { isSuccess: fsSuccess, data: featureServices } = useResourceQuery<
    any[]
  >({
    resourceType: "sidebar-fs",
    project: projectName,
    protoSelect: (d) => d.objects.featureServices,
    restPath: featureServiceListPath(projectName),
    restSelect: (d) => d.featureServices,
  });

  const { isSuccess: sdSuccess, data: savedDatasets } = useResourceQuery<
    any[]
  >({
    resourceType: "sidebar-sd",
    project: projectName,
    protoSelect: (d) => d.objects.savedDatasets,
    restPath: savedDatasetListPath(projectName),
    restSelect: (d) => d.savedDatasets,
  });

  const [isSideNavOpenOnMobile, setisSideNavOpenOnMobile] = useState(false);

  const toggleOpenOnMobile = () => {
    setisSideNavOpenOnMobile(!isSideNavOpenOnMobile);
  };

  const dataSourcesLabel = `Data Sources ${dsSuccess && dataSources ? `(${dataSources.length})` : ""}`;
  const entitiesLabel = `Entities ${entSuccess && entities ? `(${entities.length})` : ""}`;
  const featureViewsLabel = `Feature Views ${fvSuccess && featureViews && featureViews.length > 0 ? `(${featureViews.length})` : ""}`;
  const featureListLabel = `Features ${featSuccess && features && features.length > 0 ? `(${features.length})` : ""}`;
  const featureServicesLabel = `Feature Services ${fsSuccess && featureServices ? `(${featureServices.length})` : ""}`;
  const savedDatasetsLabel = `Datasets ${sdSuccess && savedDatasets ? `(${savedDatasets.length})` : ""}`;

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
          icon: <EuiIcon type="graphApp" />,
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
          renderItem: (props) => (
            <Link {...props} to={`${baseUrl}/features`} />
          ),
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
          renderItem: (props) => (
            <Link {...props} to={`${baseUrl}/data-set`} />
          ),
          isSelected: useMatchSubpath(`${baseUrl}/data-set`),
        },
        {
          name: "Data Labeling",
          id: htmlIdGenerator("dataLabeling")(),
          icon: <EuiIcon type="documentEdit" color="#006BB4" />,
          renderItem: (props) => (
            <Link {...props} to={`${baseUrl}/data-labeling`} />
          ),
          isSelected: useMatchSubpath(`${baseUrl}/data-labeling`),
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
