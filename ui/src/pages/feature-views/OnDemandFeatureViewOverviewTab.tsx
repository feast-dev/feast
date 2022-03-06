import {
  EuiFlexGroup,
  EuiFlexItem,
  EuiHorizontalRule,
  EuiText,
  EuiTitle,
  EuiPanel,
  EuiCodeBlock,
  EuiSpacer,
} from "@elastic/eui";
import React from "react";
import FeaturesListDisplay from "../../components/FeaturesListDisplay";
import {
  FeastODFVType,
  RequestDataSourceType,
  FeatureViewProjectionType,
} from "../../parsers/feastODFVS";
import { EntityRelation } from "../../parsers/parseEntityRelationships";
import { FEAST_FCO_TYPES } from "../../parsers/types";
import useLoadRelationshipData from "../../queries/useLoadRelationshipsData";
import FeatureViewProjectionDisplayPanel from "./components/FeatureViewProjectionDisplayPanel";
import RequestDataDisplayPanel from "./components/RequestDataDisplayPanel";
import ConsumingFeatureServicesList from "./ConsumingFeatureServicesList";

interface OnDemandFeatureViewOverviewTabProps {
  data: FeastODFVType;
}

const whereFSconsumesThisFv = (fvName: string) => {
  return (r: EntityRelation) => {
    return (
      r.source.name === fvName &&
      r.target.type === FEAST_FCO_TYPES.featureService
    );
  };
};

const OnDemandFeatureViewOverviewTab = ({
  data,
}: OnDemandFeatureViewOverviewTabProps) => {
  const inputs = Object.entries(data.spec.inputs);

  const relationshipQuery = useLoadRelationshipData();
  const fsNames = relationshipQuery.data
    ? relationshipQuery.data
        .filter(whereFSconsumesThisFv(data.spec.name))
        .map((fs) => {
          return fs.target.name;
        })
    : [];

  return (
    <React.Fragment>
      <EuiFlexGroup>
        <EuiFlexItem>
          <EuiPanel hasBorder={true}>
            <EuiTitle size="s">
              <h3>Transformation</h3>
            </EuiTitle>
            <EuiHorizontalRule margin="xs" />
            <EuiCodeBlock language="py" fontSize="m" paddingSize="m">
              {data.spec.userDefinedFunction.body}
            </EuiCodeBlock>
          </EuiPanel>
        </EuiFlexItem>
      </EuiFlexGroup>
      <EuiFlexGroup>
        <EuiFlexItem>
          <EuiPanel hasBorder={true}>
            <EuiTitle size="xs">
              <h3>Features ({data.spec.features.length})</h3>
            </EuiTitle>
            <EuiHorizontalRule margin="xs" />
            {data.spec.features ? (
              <FeaturesListDisplay
                featureViewName={data.spec.name}
                features={data.spec.features}
              />
            ) : (
              <EuiText>No Tags sepcified on this feature view.</EuiText>
            )}
          </EuiPanel>
        </EuiFlexItem>
        <EuiFlexItem>
          <EuiPanel hasBorder={true}>
            <EuiTitle size="xs">
              <h3>Inputs ({inputs.length})</h3>
            </EuiTitle>
            <EuiHorizontalRule margin="xs" />
            <EuiFlexGroup direction="column">
              {inputs.map(([key, inputGroup]) => {
                if ((inputGroup as RequestDataSourceType).requestDataSource) {
                  return (
                    <EuiFlexItem key={key}>
                      <RequestDataDisplayPanel
                        {...(inputGroup as RequestDataSourceType)}
                      />
                    </EuiFlexItem>
                  );
                }

                if (inputGroup as FeatureViewProjectionType) {
                  return (
                    <EuiFlexItem key={key}>
                      <FeatureViewProjectionDisplayPanel
                        {...(inputGroup as FeatureViewProjectionType)}
                      />
                    </EuiFlexItem>
                  );
                }

                return (
                  <EuiFlexItem key={key}>
                    <EuiCodeBlock language="json" fontSize="m" paddingSize="m">
                      {JSON.stringify(inputGroup, null, 2)}
                    </EuiCodeBlock>
                  </EuiFlexItem>
                );
              })}
            </EuiFlexGroup>
          </EuiPanel>
          <EuiSpacer size="m" />
          <EuiPanel hasBorder={true}>
            <EuiTitle size="xs">
              <h3>Consuming Feature Services</h3>
            </EuiTitle>
            <EuiHorizontalRule margin="xs" />
            {fsNames.length > 0 ? (
              <ConsumingFeatureServicesList fsNames={fsNames} />
            ) : (
              <EuiText>No services consume this feature view</EuiText>
            )}
          </EuiPanel>
        </EuiFlexItem>
      </EuiFlexGroup>
    </React.Fragment>
  );
};

export default OnDemandFeatureViewOverviewTab;
