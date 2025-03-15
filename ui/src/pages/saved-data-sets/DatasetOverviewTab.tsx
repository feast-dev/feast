import {
  EuiFlexGroup,
  EuiHorizontalRule,
  EuiLoadingSpinner,
  EuiTitle,
  EuiPanel,
  EuiFlexItem,
  EuiSpacer,
  EuiDescriptionList,
  EuiDescriptionListTitle,
  EuiDescriptionListDescription,
} from "@elastic/eui";
import React from "react";
import { useParams } from "react-router-dom";
import DatasetFeaturesTable from "./DatasetFeaturesTable";
import DatasetJoinKeysTable from "./DatasetJoinKeysTable";
import useLoadDataset from "./useLoadDataset";
import { toDate } from "../../utils/timestamp";

const EntityOverviewTab = () => {
  let { datasetName } = useParams();

  if (!datasetName) {
    throw new Error(
      "Route doesn't have a 'datasetName' part. This route is likely rendering the wrong component.",
    );
  }

  const { isLoading, isSuccess, isError, data } = useLoadDataset(datasetName);
  const isEmpty = data === undefined;

  return (
    <React.Fragment>
      {isLoading && (
        <React.Fragment>
          <EuiLoadingSpinner size="m" /> Loading
        </React.Fragment>
      )}
      {isEmpty && <p>No dataset with name: {datasetName}</p>}
      {isError && <p>Error loading dataset: {datasetName}</p>}
      {isSuccess && data && (
        <React.Fragment>
          <EuiFlexGroup>
            <EuiFlexItem grow={2}>
              <EuiPanel hasBorder={true}>
                <EuiTitle size="xs">
                  <h2>Features</h2>
                </EuiTitle>
                <EuiHorizontalRule margin="xs" />
                <DatasetFeaturesTable
                  features={
                    data.spec?.features!.map((joinedName: string) => {
                      const [featureViewName, featureName] =
                        joinedName.split(":");

                      return {
                        featureViewName,
                        featureName,
                      };
                    })!
                  }
                />
              </EuiPanel>
              <EuiSpacer size="m" />
              <EuiPanel hasBorder={true}>
                <EuiTitle size="xs">
                  <h2>Join Keys</h2>
                </EuiTitle>
                <EuiHorizontalRule margin="xs" />
                <DatasetJoinKeysTable
                  joinKeys={
                    data?.spec?.joinKeys!.map((joinKey) => {
                      return { name: joinKey };
                    })!
                  }
                />
              </EuiPanel>
            </EuiFlexItem>
            <EuiFlexItem grow={1}>
              <EuiPanel hasBorder={true} grow={false}>
                <EuiTitle size="xs">
                  <h3>Properties</h3>
                </EuiTitle>
                <EuiHorizontalRule margin="xs" />
                <EuiDescriptionList>
                  <EuiDescriptionListTitle>
                    Source Feature Service
                  </EuiDescriptionListTitle>
                  <EuiDescriptionListDescription>
                    {data?.spec?.featureServiceName!}
                  </EuiDescriptionListDescription>
                </EuiDescriptionList>
              </EuiPanel>
              <EuiSpacer size="m" />
              <EuiPanel hasBorder={true} grow={false}>
                <EuiDescriptionList>
                  <EuiDescriptionListTitle>Created</EuiDescriptionListTitle>
                  <EuiDescriptionListDescription>
                    {toDate(data?.meta?.createdTimestamp!).toLocaleDateString(
                      "en-CA",
                    )}
                  </EuiDescriptionListDescription>
                </EuiDescriptionList>
              </EuiPanel>
            </EuiFlexItem>
          </EuiFlexGroup>
        </React.Fragment>
      )}
    </React.Fragment>
  );
};
export default EntityOverviewTab;
