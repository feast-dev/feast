import React, { useEffect } from "react";
import {
  EuiCard,
  EuiFlexGrid,
  EuiFlexItem,
  EuiIcon,
  EuiSkeletonText,
  EuiPageTemplate,
  EuiText,
  EuiTitle,
  EuiHorizontalRule,
} from "@elastic/eui";
import { useLoadProjectsList } from "../contexts/ProjectListContext";
import { useNavigate } from "react-router-dom";
import FeastIconBlue from "../graphics/FeastIconBlue";

const RootProjectSelectionPage = () => {
  const { isLoading, isSuccess, data } = useLoadProjectsList();
  const navigate = useNavigate();

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
        <EuiTitle size="s">
          <h1>Welcome to Feast</h1>
        </EuiTitle>
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
