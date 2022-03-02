import React, { useContext, useState } from "react";

import { EuiIcon, EuiSideNav, htmlIdGenerator } from "@elastic/eui";
import { useNavigate, useParams } from "react-router-dom";
import { useMatchSubpath } from "../hooks/useMatchSubpath";
import useLoadRegistry from "../queries/useLoadRegistry";
import RegistryPathContext from "../contexts/RegistryPathContext";

import DataSourceIcon from "../data-source-icon.svg";
import EntityIcon from "../entity-icon.svg";
import FeatureViewIcon from "../feature-view.svg";
import FeatureServiceIcon from "../feature-service.svg";
import DatasetIcon from "../dataset-icon.svg";

const SideNav = () => {
  const registryUrl = useContext(RegistryPathContext);
  const { isSuccess, data } = useLoadRegistry(registryUrl);
  const { projectName } = useParams();

  const [isSideNavOpenOnMobile, setisSideNavOpenOnMobile] = useState(false);

  const navigate = useNavigate();

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

  const sideNav = [
    {
      name: "Home",
      id: htmlIdGenerator("basicExample")(),
      onClick: () => {
        navigate(`/p/${projectName}/`);
      },
      items: [
        {
          name: dataSourcesLabel,
          id: htmlIdGenerator("dataSources")(),
          icon: <EuiIcon type={DataSourceIcon} />,
          onClick: () => {
            navigate(`/p/${projectName}/data-source`);
          },
          isSelected: useMatchSubpath("data-source"),
        },
        {
          name: entitiesLabel,
          id: htmlIdGenerator("entities")(),
          icon: <EuiIcon type={EntityIcon} />,
          onClick: () => {
            navigate(`/p/${projectName}/entity`);
          },
          isSelected: useMatchSubpath("entity"),
        },
        {
          name: featureViewsLabel,
          id: htmlIdGenerator("featureView")(),
          icon: <EuiIcon type={FeatureViewIcon} />,
          onClick: () => {
            navigate(`/p/${projectName}/feature-view`);
          },
          isSelected: useMatchSubpath("feature-view"),
        },
        {
          name: featureServicesLabel,
          id: htmlIdGenerator("featureService")(),
          icon: <EuiIcon type={FeatureServiceIcon} />,
          onClick: () => {
            navigate(`/p/${projectName}/feature-service`);
          },
          isSelected: useMatchSubpath("feature-service"),
        },
        {
          name: savedDatasetsLabel,
          id: htmlIdGenerator("savedDatasets")(),
          icon: <EuiIcon type={DatasetIcon} />,
          onClick: () => {
            navigate(`/p/${projectName}/data-set`);
          },
          isSelected: useMatchSubpath("data-set"),
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
      style={{ width: 192 }}
      items={sideNav}
    />
  );
};

export default SideNav;
