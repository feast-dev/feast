import {
  EuiBadge,
  EuiFlexGroup,
  EuiFlexItem,
  EuiHorizontalRule,
  EuiLoadingSpinner,
  EuiPanel,
  EuiSpacer,
  EuiStat,
  EuiText,
  EuiTextAlign,
  EuiTitle,
} from "@elastic/eui";
import React from "react";
import { useParams } from "react-router-dom";
import { useNavigate } from "react-router-dom";
import FeaturesInServiceList from "../../components/FeaturesInServiceDisplay";
import PermissionsDisplay from "../../components/PermissionsDisplay";
import TagsDisplay from "../../components/TagsDisplay";
import { encodeSearchQueryString } from "../../hooks/encodeSearchQueryString";
import FeatureViewEdgesList from "../entities/FeatureViewEdgesList";
import useLoadFeatureService from "./useLoadFeatureService";
import { toDate } from "../../utils/timestamp";
import { getEntityPermissions } from "../../utils/permissionUtils";
import { FEAST_FCO_TYPES } from "../../parsers/types";

const FeatureServiceOverviewTab = () => {
  let { featureServiceName, projectName } = useParams();

  const fsName = featureServiceName === undefined ? "" : featureServiceName;

  const { isLoading, isSuccess, isError, data, entities } =
    useLoadFeatureService(fsName);
  const isEmpty = data === undefined;

  let numFeatures = 0;
  let numFeatureViews = 0;
  if (data) {
    data?.spec?.features?.forEach((featureView) => {
      numFeatureViews += 1;
      numFeatures += featureView?.featureColumns!.length;
    });
  }

  const navigate = useNavigate();

  return (
    <React.Fragment>
      {isLoading && (
        <React.Fragment>
          <EuiLoadingSpinner size="m" /> Loading
        </React.Fragment>
      )}
      {isEmpty && <p>No feature service with name: {featureServiceName}</p>}
      {isError && <p>Error loading feature service: {featureServiceName}</p>}
      {isSuccess && data && (
        <EuiFlexGroup direction="column">
          <EuiFlexGroup alignItems="center">
            <EuiFlexItem grow={false}>
              <EuiStat title={`${numFeatures}`} description="Total Features" />
            </EuiFlexItem>
            <EuiFlexItem>
              <EuiTextAlign textAlign="center">
                <p>from</p>
              </EuiTextAlign>
            </EuiFlexItem>
            <EuiFlexItem>
              <EuiStat
                title={`${numFeatureViews}`}
                description="Feature Views"
              />
            </EuiFlexItem>
            {data?.meta?.lastUpdatedTimestamp ? (
              <EuiFlexItem>
                <EuiStat
                  title={`${toDate(data?.meta?.lastUpdatedTimestamp!).toLocaleDateString("en-CA")}`}
                  description="Last updated"
                />
              </EuiFlexItem>
            ) : (
              <EuiText>
                No last updated timestamp specified on this feature service.
              </EuiText>
            )}
          </EuiFlexGroup>
          <EuiFlexGroup>
            <EuiFlexItem grow={2}>
              <EuiPanel hasBorder={true}>
                <EuiTitle size="xs">
                  <h2>Features</h2>
                </EuiTitle>
                <EuiHorizontalRule margin="xs" />
                {data?.spec?.features ? (
                  <FeaturesInServiceList featureViews={data?.spec?.features} />
                ) : (
                  <EuiText>
                    No features specified for this feature service.
                  </EuiText>
                )}
              </EuiPanel>
            </EuiFlexItem>
            <EuiFlexItem grow={1}>
              <EuiPanel hasBorder={true} grow={false}>
                <EuiTitle size="xs">
                  <h3>Tags</h3>
                </EuiTitle>
                <EuiHorizontalRule margin="xs" />
                {data?.spec?.tags ? (
                  <TagsDisplay
                    tags={data.spec.tags}
                    createLink={(key, value) => {
                      return (
                        `/p/${projectName}/feature-service?` +
                        encodeSearchQueryString(`${key}:${value}`)
                      );
                    }}
                  />
                ) : (
                  <EuiText>No Tags specified on this feature service.</EuiText>
                )}
              </EuiPanel>
              <EuiSpacer size="m" />
              <EuiPanel hasBorder={true}>
                <EuiTitle size="xs">
                  <h3>Entities</h3>
                </EuiTitle>
                <EuiHorizontalRule margin="xs" />
                {entities ? (
                  <EuiFlexGroup wrap responsive={false} gutterSize="xs">
                    {entities.map((entity) => {
                      return (
                        <EuiFlexItem grow={false} key={entity.name}>
                          <EuiBadge
                            color="primary"
                            onClick={() => {
                              navigate(
                                `/p/${projectName}/entity/${entity.name}`,
                              );
                            }}
                            onClickAriaLabel={entity.name}
                            data-test-sub="testExample1"
                          >
                            {entity.name}
                          </EuiBadge>
                        </EuiFlexItem>
                      );
                    })}
                  </EuiFlexGroup>
                ) : (
                  <EuiText>No Entities.</EuiText>
                )}
              </EuiPanel>
              <EuiSpacer size="m" />
              <EuiPanel hasBorder={true}>
                <EuiTitle size="xs">
                  <h3>All Feature Views</h3>
                </EuiTitle>
                <EuiHorizontalRule margin="xs" />
                {data?.spec?.features?.length! > 0 ? (
                  <FeatureViewEdgesList
                    fvNames={
                      data?.spec?.features?.map((f) => {
                        return f.featureViewName!;
                      })!
                    }
                  />
                ) : (
                  <EuiText>No feature views in this feature service</EuiText>
                )}
              </EuiPanel>
              <EuiSpacer size="m" />
              <EuiPanel hasBorder={true}>
                <EuiTitle size="xs">
                  <h3>Permissions</h3>
                </EuiTitle>
                <EuiHorizontalRule margin="xs" />
                {data?.permissions ? (
                  <PermissionsDisplay 
                    permissions={getEntityPermissions(
                      data.permissions,
                      FEAST_FCO_TYPES.featureService,
                      fsName
                    )}
                  />
                ) : (
                  <EuiText>No permissions defined for this feature service.</EuiText>
                )}
              </EuiPanel>
            </EuiFlexItem>
          </EuiFlexGroup>
        </EuiFlexGroup>
      )}
    </React.Fragment>
  );
};

export default FeatureServiceOverviewTab;
