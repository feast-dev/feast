import React from "react";
import {
  EuiPageTemplate,
  EuiText,
  EuiFlexGroup,
  EuiFlexItem,
  EuiTitle,
  EuiSpacer,
  EuiSkeletonText,
  EuiPanel,
  EuiStat,
  EuiCard,
} from "@elastic/eui";

import { useDocumentTitle } from "../hooks/useDocumentTitle";
import ObjectsCountStats from "../components/ObjectsCountStats";
import ExplorePanel from "../components/ExplorePanel";
import useResourceQuery, {
  restFeatureViewsToMergedList,
} from "../queries/useResourceQuery";
import { useParams, useNavigate } from "react-router-dom";
import { useLoadProjectsList } from "../contexts/ProjectListContext";
import type { genericFVType } from "../parsers/mergedFVTypes";

const getItemProject = (item: any): string =>
  item?.project || item?.spec?.project || "";

// Component for "All Projects" view
const AllProjectsDashboard = () => {
  const navigate = useNavigate();
  const { data: projectsData } = useLoadProjectsList();

  const { data: allFVs } = useResourceQuery<genericFVType[]>({
    resourceType: "all-proj-fvs",
    protoSelect: (d) => d.mergedFVList,
    restPath: "/feature_views/all?limit=100&include_relationships=true",
    restSelect: restFeatureViewsToMergedList,
  });

  const { data: allEntities } = useResourceQuery<any[]>({
    resourceType: "all-proj-entities",
    protoSelect: (d) => d.objects.entities,
    restPath: "/entities/all?limit=100",
    restSelect: (d) => d.entities,
  });

  const { data: allDS } = useResourceQuery<any[]>({
    resourceType: "all-proj-ds",
    protoSelect: (d) => d.objects.dataSources,
    restPath: "/data_sources/all?limit=100",
    restSelect: (d) => d.dataSources,
  });

  const { data: allFS } = useResourceQuery<any[]>({
    resourceType: "all-proj-fs",
    protoSelect: (d) => d.objects.featureServices,
    restPath: "/feature_services/all?limit=100",
    restSelect: (d) => d.featureServices,
  });

  const { data: allFeatures } = useResourceQuery<any[]>({
    resourceType: "all-proj-features",
    protoSelect: (d) => d.allFeatures,
    restPath: "/features/all?limit=100",
    restSelect: (d) => d.features,
  });

  const loaded = allFVs && allEntities && allDS && allFS && allFeatures;

  if (!loaded) {
    return <EuiSkeletonText lines={10} />;
  }

  const totalCounts = {
    featureViews: allFVs.length,
    entities: allEntities.length,
    dataSources: allDS.length,
    featureServices: allFS.length,
    features: allFeatures.length,
  };

  const projects = projectsData?.projects.filter((p) => p.id !== "all") || [];
  const projectStats = projects.map((project) => {
    const matchesProject = (item: any) => getItemProject(item) === project.id;

    return {
      ...project,
      counts: {
        featureViews: allFVs.filter((fv) =>
          matchesProject(fv.object || fv),
        ).length,
        entities: allEntities.filter(matchesProject).length,
        features: allFeatures.filter(matchesProject).length,
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
  const { projectName } = useParams<{ projectName: string }>();
  const { data: projectsData } = useLoadProjectsList();

  // Show aggregated dashboard for "All Projects" view
  if (projectName === "all") {
    return <AllProjectsDashboard />;
  }

  const currentProject = projectsData?.projects.find(
    (p) => p.id === projectName,
  );

  return (
    <EuiPageTemplate panelled>
      <EuiPageTemplate.Section>
        <EuiTitle size="l">
          <h1>
            {currentProject
              ? `Project: ${currentProject.name}`
              : projectName
                ? `Project: ${projectName}`
                : ""}
          </h1>
        </EuiTitle>
        <EuiSpacer />

        <EuiFlexGroup>
          <EuiFlexItem grow={2}>
            {currentProject?.description ? (
              <EuiText>
                <pre>{currentProject.description}</pre>
              </EuiText>
            ) : (
              <EuiText>
                <p>
                  Welcome to your new Feast project. In this UI, you can see
                  Data Sources, Entities, Features, Feature Views, and Feature
                  Services registered in Feast.
                </p>
                <p>
                  It looks like this project already has some objects registered.
                  If you are new to this project, we suggest starting by
                  exploring the Feature Services, as they represent the
                  collection of Feature Views serving a particular model.
                </p>
                <p>
                  <strong>Note</strong>: We encourage you to replace this
                  welcome message with more suitable content for your team. You
                  can do so by specifying a <code>project_description</code> in
                  your <code>feature_store.yaml</code> file.
                </p>
              </EuiText>
            )}
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
