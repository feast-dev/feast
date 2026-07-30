import React, { useContext, useEffect, useState } from "react";
import {
  EuiButtonIcon,
  EuiCard,
  EuiFlexGrid,
  EuiFlexGroup,
  EuiFlexItem,
  EuiIcon,
  EuiSkeletonText,
  EuiPageTemplate,
  EuiText,
  EuiTitle,
  EuiHorizontalRule,
} from "@elastic/eui";
import {
  useLoadProjectsList,
  ProjectListContext,
  ProjectsListSchema,
} from "../contexts/ProjectListContext";
import { useNavigate } from "react-router-dom";
import { useQueryClient } from "react-query";
import FeastIconBlue from "../graphics/FeastIconBlue";

const RootProjectSelectionPage = () => {
  const { isLoading, isSuccess, data } = useLoadProjectsList();
  const navigate = useNavigate();
  const queryClient = useQueryClient();
  const projectListCtx = useContext(ProjectListContext);
  const basename = projectListCtx?.basename || "";
  const [refreshing, setRefreshing] = useState(false);

  useEffect(() => {
    if (data && data.default) {
      // If a default is set, redirect there.
      navigate(`/p/${data.default}`);
    }

    if (data && data.projects.length === 1) {
      // If there is only one project, redirect there.
      navigate(`/p/${data.projects[0].id}`);
    }
  }, [data, navigate]);

  const handleRefresh = async () => {
    setRefreshing(true);
    try {
      await fetch(`${basename}/api/registry/refresh`, { method: "POST" });
      const res = await fetch(`${basename}/projects-list.json`, {
        headers: { "Content-Type": "application/json" },
      });
      const json = await res.json();
      const parsed = ProjectsListSchema.parse(json);
      queryClient.setQueryData("feast-projects-list", parsed);
    } finally {
      setRefreshing(false);
    }
  };

  const projectCards = data?.projects.map((item, index) => {
    return (
      <EuiFlexItem key={index}>
        <EuiCard
          icon={<EuiIcon size="xxl" type={FeastIconBlue} />}
          title={`${item.name}`}
          description={item?.description || ""}
          onClick={() => {
            navigate(`/p/${item.id}`);
          }}
        />
      </EuiFlexItem>
    );
  });

  return (
    <EuiPageTemplate panelled>
      <EuiPageTemplate.Section>
        <EuiFlexGroup alignItems="center" justifyContent="spaceBetween">
          <EuiFlexItem grow={false}>
            <EuiTitle size="s">
              <h1>Welcome to Feast</h1>
            </EuiTitle>
          </EuiFlexItem>
          <EuiFlexItem grow={false}>
            <EuiButtonIcon
              iconType="refresh"
              aria-label="Refresh projects"
              onClick={handleRefresh}
              isLoading={refreshing}
              display="base"
              size="m"
            />
          </EuiFlexItem>
        </EuiFlexGroup>
        <EuiText>
          <p>Select one of the projects.</p>
        </EuiText>
        <EuiHorizontalRule margin="m" />
        {isLoading && <EuiSkeletonText lines={1} />}
        {isSuccess && data?.projects && (
          <EuiFlexGrid columns={3} gutterSize="l">
            {projectCards}
          </EuiFlexGrid>
        )}
      </EuiPageTemplate.Section>
    </EuiPageTemplate>
  );
};

export default RootProjectSelectionPage;
