import React, { useContext } from "react";
import {
  EuiPageTemplate,
  EuiText,
  EuiFlexGroup,
  EuiFlexItem,
  EuiTitle,
  EuiSpacer,
  EuiSkeletonText,
  EuiEmptyPrompt,
  EuiFieldSearch,
} from "@elastic/eui";

import { useDocumentTitle } from "../hooks/useDocumentTitle";
import ObjectsCountStats from "../components/ObjectsCountStats";
import ExplorePanel from "../components/ExplorePanel";
import useLoadRegistry from "../queries/useLoadRegistry";
import RegistryPathContext from "../contexts/RegistryPathContext";
import RegistryVisualizationTab from "../components/RegistryVisualizationTab";
import RegistrySearch from "../components/RegistrySearch";
import { useParams } from "react-router-dom";

const ProjectOverviewPage = () => {
  useDocumentTitle("Feast Home");
  const registryUrl = useContext(RegistryPathContext);
  const { isLoading, isSuccess, isError, data } = useLoadRegistry(registryUrl);

  const { projectName } = useParams<{ projectName: string }>();

  const categories = [
    {
      name: "Data Sources",
      data: data?.objects.dataSources || [],
      getLink: (item: any) => `/p/${projectName}/data-source/${item.name}`,
    },
    {
      name: "Entities",
      data: data?.objects.entities || [],
      getLink: (item: any) => `/p/${projectName}/entity/${item.name}`,
    },
    {
      name: "Features",
      data: data?.allFeatures || [],
      getLink: (item: any) => {
        const featureView = item?.featureView;
        return featureView
          ? `/p/${projectName}/feature-view/${featureView}/feature/${item.name}`
          : "#";
      },
    },
    {
      name: "Feature Views",
      data: data?.mergedFVList || [],
      getLink: (item: any) => `/p/${projectName}/feature-view/${item.name}`,
    },
    {
      name: "Feature Services",
      data: data?.objects.featureServices || [],
      getLink: (item: any) => {
        const serviceName = item?.name || item?.spec?.name;
        return serviceName
          ? `/p/${projectName}/feature-service/${serviceName}`
          : "#";
      },
    },
  ];

  return (
    <EuiPageTemplate panelled>
      <EuiPageTemplate.Section>
        <EuiTitle size="l">
          <h1>
            {isLoading && <EuiSkeletonText lines={1} />}
            {isSuccess && data?.project && `Project: ${data.project}`}
          </h1>
        </EuiTitle>
        <EuiSpacer />

        <EuiFlexGroup>
          <EuiFlexItem grow={2}>
            {isLoading && <EuiSkeletonText lines={4} />}
            {isError && (
              <EuiEmptyPrompt
                iconType="alert"
                color="danger"
                title={<h2>Error Loading Project Configs</h2>}
                body={
                  <p>
                    There was an error loading the Project Configurations.
                    Please check that <code>feature_store.yaml</code> file is
                    available and well-formed.
                  </p>
                }
              />
            )}
            {isSuccess &&
              (data?.description ? (
                <EuiText>
                  <pre>{data.description}</pre>
                </EuiText>
              ) : (
                <EuiText>
                  <p>
                    Welcome to your new Feast project. In this UI, you can see
                    Data Sources, Entities, Features, Feature Views, and Feature
                    Services registered in Feast.
                  </p>
                  <p>
                    It looks like this project already has some objects
                    registered. If you are new to this project, we suggest
                    starting by exploring the Feature Services, as they
                    represent the collection of Feature Views serving a
                    particular model.
                  </p>
                  <p>
                    <strong>Note</strong>: We encourage you to replace this
                    welcome message with more suitable content for your team.
                    You can do so by specifying a{" "}
                    <code>project_description</code> in your{" "}
                    <code>feature_store.yaml</code> file.
                  </p>
                </EuiText>
              ))}
            <ObjectsCountStats />
          </EuiFlexItem>
          <EuiFlexItem grow={1}>
            <ExplorePanel />
          </EuiFlexItem>
        </EuiFlexGroup>
      </EuiPageTemplate.Section>
      <EuiPageTemplate.Section>
        {isSuccess && <RegistrySearch categories={categories} />}
      </EuiPageTemplate.Section>
    </EuiPageTemplate>
  );
};

export default ProjectOverviewPage;
