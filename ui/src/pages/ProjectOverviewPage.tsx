import React, { useContext } from "react";

import {
  EuiPageContent,
  EuiPageContentBody,
  EuiText,
  EuiFlexGroup,
  EuiFlexItem,
  EuiTitle,
  EuiSpacer,
  EuiLoadingContent,
  EuiEmptyPrompt,
} from "@elastic/eui";

import { useDocumentTitle } from "../hooks/useDocumentTitle";
import ObjectsCountStats from "../components/ObjectsCountStats";
import ExplorePanel from "../components/ExplorePanel";
import useLoadRegistry from "../queries/useLoadRegistry";
import RegistryPathContext from "../contexts/RegistryPathContext";

const ProjectOverviewPage = () => {
  useDocumentTitle("Feast Home");
  const registryUrl = useContext(RegistryPathContext);
  const { isLoading, isSuccess, isError, data } = useLoadRegistry(registryUrl);

  return (
    <EuiPageContent
      hasBorder={false}
      hasShadow={false}
      paddingSize="none"
      color="transparent"
      borderRadius="none"
    >
      <EuiPageContentBody>
        <EuiTitle size="l">
          <h1>
            {isLoading && <EuiLoadingContent lines={1} />}
            {isSuccess && data?.project && `Project: ${data.project}`}
          </h1>
        </EuiTitle>
        <EuiSpacer />

        <EuiFlexGroup>
          <EuiFlexItem grow={2}>
            {isLoading && <EuiLoadingContent lines={4} />}
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
                    Data Sources, Entities, Feature Views and Feature Services
                    registered in Feast.
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
      </EuiPageContentBody>
    </EuiPageContent>
  );
};

export default ProjectOverviewPage;
