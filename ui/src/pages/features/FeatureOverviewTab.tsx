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
import useLoadFeature from "./useLoadFeature";

const FeatureOverviewTab = () => {
  let { FeatureViewName, FeatureName } = useParams();

  const eName = FeatureViewName === undefined ? "" : FeatureViewName;
  const fName = FeatureName === undefined ? "" : FeatureName;
  const { isLoading, isSuccess, isError, data, featureData } = useLoadFeature(eName, fName);
  const isEmpty = data === undefined || featureData === undefined;
  // const isEmpty = featureData === undefined;

  return (
    <React.Fragment>
      {isLoading && (
        <React.Fragment>
          <EuiLoadingSpinner size="m" /> Loading
        </React.Fragment>
      )}
      {isEmpty && <p>No Feature with name {FeatureName} in FeatureView {FeatureViewName}</p>}
      {isError && <p>Error loading Feature {FeatureName} in FeatureView {FeatureViewName}</p>}
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
                  <EuiDescriptionListTitle>Name</EuiDescriptionListTitle>
                  <EuiDescriptionListDescription>
                    {featureData?.name}
                  </EuiDescriptionListDescription>

                  <EuiDescriptionListTitle>Value Type</EuiDescriptionListTitle>
                  <EuiDescriptionListDescription>
                    {featureData?.valueType}
                  </EuiDescriptionListDescription>

                  <EuiDescriptionListTitle>FeatureView</EuiDescriptionListTitle>
                  <EuiDescriptionListDescription>
                  {FeatureViewName} 
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
export default FeatureOverviewTab;
