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
  EuiPanel,
  EuiStat,
  EuiCard,
} from "@elastic/eui";

import { useDocumentTitle } from "../hooks/useDocumentTitle";
import ObjectsCountStats from "../components/ObjectsCountStats";
import ExplorePanel from "../components/ExplorePanel";
import useLoadRegistry from "../queries/useLoadRegistry";
import RegistryPathContext from "../contexts/RegistryPathContext";
import RegistryVisualizationTab from "../components/RegistryVisualizationTab";
import RegistrySearch from "../components/RegistrySearch";
import { useParams, useNavigate } from "react-router-dom";
import { useLoadProjectsList } from "../contexts/ProjectListContext";

// Component for "All Projects" view
const AllProjectsDashboard = () => {
  const registryUrl = useContext(RegistryPathContext);
  const navigate = useNavigate();
  const { data: projectsData } = useLoadProjectsList();
  const { data: registryData } = useLoadRegistry(registryUrl);

  if (!registryData) {
    return <EuiSkeletonText lines={10} />;
  }

  // Calculate total counts across all projects
  const totalCounts = {
    featureViews: registryData.objects.featureViews?.length || 0,
    entities: registryData.objects.entities?.length || 0,
    dataSources: registryData.objects.dataSources?.length || 0,
    featureServices: registryData.objects.featureServices?.length || 0,
    features: registryData.allFeatures?.length || 0,
  };

  // Get projects from registry and count their objects
  const projects = projectsData?.projects.filter((p) => p.id !== "all") || [];
  const projectStats = projects.map((project) => {
    const projectFVs =
      registryData.objects.featureViews?.filter(
        (fv: any) => fv?.spec?.project === project.id,
      ) || [];
    const projectEntities =
      registryData.objects.entities?.filter(
        (e: any) => e?.spec?.project === project.id,
      ) || [];
    const projectFeatures =
      registryData.allFeatures?.filter((f: any) => f?.project === project.id) ||
      [];

    return {
      ...project,
      counts: {
        featureViews: projectFVs.length,
        entities: projectEntities.length,
        features: projectFeatures.length,
      },
    };
  });

  return (
    <EuiPageTemplate panelled>
      <EuiPageTemplate.Section>
        <EuiTitle size="l">
          <h1>All Projects Overview</h1>
        </EuiTitle>
        <EuiSpacer />

        <EuiText>
          <p>
            View aggregated statistics and explore data across all your Feast
            projects.
          </p>
        </EuiText>
        <EuiSpacer size="l" />

        {/* Total Stats */}
        <EuiPanel hasBorder>
          <EuiTitle size="s">
            <h3>Total Across All Projects</h3>
          </EuiTitle>
          <EuiSpacer size="m" />
          <EuiFlexGroup>
            <EuiFlexItem>
              <EuiStat
                title={totalCounts.featureViews.toString()}
                description="Feature Views"
                titleSize="m"
                textAlign="center"
              />
            </EuiFlexItem>
            <EuiFlexItem>
              <EuiStat
                title={totalCounts.entities.toString()}
                description="Entities"
                titleSize="m"
                textAlign="center"
              />
            </EuiFlexItem>
            <EuiFlexItem>
              <EuiStat
                title={totalCounts.features.toString()}
                description="Features"
                titleSize="m"
                textAlign="center"
              />
            </EuiFlexItem>
            <EuiFlexItem>
              <EuiStat
                title={totalCounts.featureServices.toString()}
                description="Feature Services"
                titleSize="m"
                textAlign="center"
              />
            </EuiFlexItem>
            <EuiFlexItem>
              <EuiStat
                title={totalCounts.dataSources.toString()}
                description="Data Sources"
                titleSize="m"
                textAlign="center"
              />
            </EuiFlexItem>
          </EuiFlexGroup>
        </EuiPanel>

        <EuiSpacer size="l" />

        {/* Individual Projects */}
        <EuiTitle size="s">
          <h3>Projects ({projects.length})</h3>
        </EuiTitle>
        <EuiSpacer size="m" />
        <EuiFlexGroup gutterSize="l" wrap>
          {projectStats.map((project) => (
            <EuiFlexItem
              key={project.id}
              style={{ minWidth: "300px", maxWidth: "400px" }}
            >
              <EuiCard
                title={project.name}
                description={project.description}
                onClick={() => navigate(`/p/${project.id}`)}
                style={{ cursor: "pointer" }}
              >
                <EuiSpacer size="s" />
                <EuiFlexGroup justifyContent="spaceAround" gutterSize="s">
                  <EuiFlexItem grow={false}>
                    <EuiText size="s" textAlign="center">
                      <strong>{project.counts.featureViews}</strong>
                      <br />
                      <span style={{ fontSize: "0.85em", color: "#69707D" }}>
                        Feature Views
                      </span>
                    </EuiText>
                  </EuiFlexItem>
                  <EuiFlexItem grow={false}>
                    <EuiText size="s" textAlign="center">
                      <strong>{project.counts.entities}</strong>
                      <br />
                      <span style={{ fontSize: "0.85em", color: "#69707D" }}>
                        Entities
                      </span>
                    </EuiText>
                  </EuiFlexItem>
                  <EuiFlexItem grow={false}>
                    <EuiText size="s" textAlign="center">
                      <strong>{project.counts.features}</strong>
                      <br />
                      <span style={{ fontSize: "0.85em", color: "#69707D" }}>
                        Features
                      </span>
                    </EuiText>
                  </EuiFlexItem>
                </EuiFlexGroup>
              </EuiCard>
            </EuiFlexItem>
          ))}
        </EuiFlexGroup>
      </EuiPageTemplate.Section>
    </EuiPageTemplate>
  );
};

const ProjectOverviewPage = () => {
  useDocumentTitle("Feast Home");
  const registryUrl = useContext(RegistryPathContext);
  const { projectName } = useParams<{ projectName: string }>();
  const { isLoading, isSuccess, isError, data } = useLoadRegistry(
    registryUrl,
    projectName,
  );

  // Show aggregated dashboard for "All Projects" view
  if (projectName === "all") {
    return <AllProjectsDashboard />;
  }

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
    </EuiPageTemplate>
  );
};

export default ProjectOverviewPage;
