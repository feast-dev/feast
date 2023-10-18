import {
  EuiFlexGroup,
  EuiHorizontalRule,
  EuiLoadingSpinner,
  EuiTitle,
} from "@elastic/eui";
import {
  EuiPanel,
  EuiText,
  EuiFlexItem,
  EuiSpacer,
  EuiDescriptionList,
  EuiDescriptionListTitle,
  EuiDescriptionListDescription,
} from "@elastic/eui";
import React from "react";
import { useParams } from "react-router-dom";
import TagsDisplay from "../../components/TagsDisplay";
import FeatureViewEdgesList from "./FeatureViewEdgesList";
import useFeatureViewEdgesByEntity from "./useFeatureViewEdgesByEntity";
import useLoadEntity from "./useLoadEntity";
import { toDate } from "../../utils/timestamp";
import { feast } from "../../protos";

const EntityOverviewTab = () => {
  let { entityName } = useParams();

  const eName = entityName === undefined ? "" : entityName;
  const { isLoading, isSuccess, isError, data } = useLoadEntity(eName);
  const isEmpty = data === undefined;

  const fvEdges = useFeatureViewEdgesByEntity();
  const fvEdgesSuccess = fvEdges.isSuccess;
  const fvEdgesData = fvEdges.data;

  return (
    <React.Fragment>
      {isLoading && (
        <React.Fragment>
          <EuiLoadingSpinner size="m" /> Loading
        </React.Fragment>
      )}
      {isEmpty && <p>No entity with name: {entityName}</p>}
      {isError && <p>Error loading entity: {entityName}</p>}
      {isSuccess && data && (
        <React.Fragment>
          <EuiFlexGroup>
            <EuiFlexItem>
              <EuiPanel hasBorder={true}>
                <EuiTitle size="xs">
                  <h3>Properties</h3>
                </EuiTitle>
                <EuiHorizontalRule margin="xs" />
                <EuiDescriptionList>
                  <EuiDescriptionListTitle>Join Key</EuiDescriptionListTitle>
                  <EuiDescriptionListDescription>
                    {data?.spec?.joinKey}
                  </EuiDescriptionListDescription>

                  <EuiDescriptionListTitle>Description</EuiDescriptionListTitle>
                  <EuiDescriptionListDescription>
                    {data?.spec?.description}
                  </EuiDescriptionListDescription>

                  <EuiDescriptionListTitle>Value Type</EuiDescriptionListTitle>
                  <EuiDescriptionListDescription>
                    {feast.types.ValueType.Enum[data?.spec?.valueType!]}
                  </EuiDescriptionListDescription>
                </EuiDescriptionList>
              </EuiPanel>
              <EuiSpacer size="m" />
              <EuiPanel hasBorder={true}>
                <EuiDescriptionList>
                  <EuiDescriptionListTitle>Created</EuiDescriptionListTitle>
                  <EuiDescriptionListDescription>
                    {data?.meta?.createdTimestamp ? (
                      toDate(data.meta.createdTimestamp).toLocaleDateString("en-CA")
                    ) : (
                      <EuiText>No createdTimestamp specified on this entity.</EuiText>
                    )}
                  </EuiDescriptionListDescription>

                  <EuiDescriptionListTitle>Updated</EuiDescriptionListTitle>
                  <EuiDescriptionListDescription>
                    {data?.meta?.lastUpdatedTimestamp ? (
                      toDate(data.meta.lastUpdatedTimestamp).toLocaleDateString("en-CA")
                    ) : (
                      <EuiText>No lastUpdatedTimestamp specified on this entity.</EuiText>
                    )}
                  </EuiDescriptionListDescription>
                </EuiDescriptionList>
              </EuiPanel>
            </EuiFlexItem>
            <EuiFlexItem>
              <EuiPanel hasBorder={true}>
                <EuiTitle size="xs">
                  <h3>Feature Views</h3>
                </EuiTitle>
                <EuiHorizontalRule margin="xs" />
                {fvEdgesSuccess && fvEdgesData ? (
                  fvEdgesData[eName] ? (
                    <FeatureViewEdgesList
                      fvNames={fvEdgesData[eName].map((r) => {
                        return r.target.name;
                      })}
                    />
                  ) : (
                    <EuiText>No feature views have this entity</EuiText>
                  )
                ) : (
                  <EuiText>
                    Error loading feature views that have this entity.
                  </EuiText>
                )}
              </EuiPanel>
              <EuiSpacer size="m" />
              <EuiPanel hasBorder={true} grow={false}>
                <EuiTitle size="xs">
                  <h3>Labels</h3>
                </EuiTitle>
                <EuiHorizontalRule margin="xs" />
                {data?.spec?.tags ? (
                  <TagsDisplay tags={data.spec.tags} />
                ) : (
                  <EuiText>No labels specified on this entity.</EuiText>
                )}
              </EuiPanel>
            </EuiFlexItem>
          </EuiFlexGroup>
        </React.Fragment>
      )}
    </React.Fragment>
  );
};
export default EntityOverviewTab;
