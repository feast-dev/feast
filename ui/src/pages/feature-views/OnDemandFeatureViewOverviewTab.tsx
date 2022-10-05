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
import { useParams } from "react-router-dom";
import { EntityRelation } from "../../parsers/parseEntityRelationships";
import { FEAST_FCO_TYPES } from "../../parsers/types";
import useLoadRelationshipData from "../../queries/useLoadRelationshipsData";
import FeatureViewProjectionDisplayPanel from "./components/FeatureViewProjectionDisplayPanel";
import RequestDataDisplayPanel from "./components/RequestDataDisplayPanel";
import ConsumingFeatureServicesList from "./ConsumingFeatureServicesList";
import { feast } from "../../protos";

interface OnDemandFeatureViewOverviewTabProps {
  data: feast.core.IOnDemandFeatureView;
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
  const inputs = Object.entries(data?.spec?.sources!);
  const { projectName } = useParams();

  const relationshipQuery = useLoadRelationshipData();
  const fsNames = relationshipQuery.data
    ? relationshipQuery.data
        .filter(whereFSconsumesThisFv(data?.spec?.name!))
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
              {data?.spec?.userDefinedFunction?.bodyText}
            </EuiCodeBlock>
          </EuiPanel>
        </EuiFlexItem>
      </EuiFlexGroup>
      <EuiFlexGroup>
        <EuiFlexItem>
          <EuiPanel hasBorder={true}>
            <EuiTitle size="xs">
              <h3>Features ({data?.spec?.features!.length})</h3>
            </EuiTitle>
            <EuiHorizontalRule margin="xs" />
            {projectName && data?.spec?.features ? (
              <FeaturesListDisplay
                projectName={projectName}
                featureViewName={data?.spec?.name!}
                features={data.spec.features}
                link={false}
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
                if (
                  (inputGroup as feast.core.IOnDemandSource).requestDataSource
                ) {
                  return (
                    <EuiFlexItem key={key}>
                      <RequestDataDisplayPanel
                        {...(inputGroup as feast.core.IOnDemandSource)}
                      />
                    </EuiFlexItem>
                  );
                }

                if (
                  (inputGroup as feast.core.IOnDemandSource)
                    .featureViewProjection
                ) {
                  return (
                    <EuiFlexItem key={key}>
                      <FeatureViewProjectionDisplayPanel
                        {...(inputGroup.featureViewProjection as feast.core.IFeatureViewProjection)}
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
