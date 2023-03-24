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
import ConsumingFeatureServicesList from "./ConsumingFeatureServicesList";
import EuiCustomLink from "../../components/EuiCustomLink";
import { feast } from "../../protos";

interface StreamFeatureViewOverviewTabProps {
  data: feast.core.IStreamFeatureView;
}

const whereFSconsumesThisFv = (fvName: string) => {
  return (r: EntityRelation) => {
    return (
      r.source.name === fvName &&
      r.target.type === FEAST_FCO_TYPES.featureService
    );
  };
};

const StreamFeatureViewOverviewTab = ({
  data,
}: StreamFeatureViewOverviewTabProps) => {
  const inputs = Object.entries([data.spec?.streamSource]);
  const { projectName } = useParams();

  const relationshipQuery = useLoadRelationshipData();
  const fsNames = relationshipQuery.data
    ? relationshipQuery.data
        .filter(whereFSconsumesThisFv(data.spec?.name!))
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
              {data.spec?.userDefinedFunction?.bodyText}
            </EuiCodeBlock>
          </EuiPanel>
        </EuiFlexItem>
      </EuiFlexGroup>
      <EuiFlexGroup>
        <EuiFlexItem>
          <EuiPanel hasBorder={true}>
            <EuiTitle size="xs">
              <h3>Features ({data.spec?.features?.length})</h3>
            </EuiTitle>
            <EuiHorizontalRule margin="xs" />
            {projectName && data.spec?.features ? (
              <FeaturesListDisplay
                projectName={projectName}
                featureViewName={data.spec.name!}
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

                return (
                  <EuiPanel hasBorder={true} key={key}>
                    <EuiText size="xs">
                      <span>Stream Source</span>
                    </EuiText>
                    <EuiTitle size="s">
                      <EuiCustomLink
                        href={`${process.env.PUBLIC_URL || ""}/p/${projectName}/data-source/${inputGroup?.name}`}
                        to={`${process.env.PUBLIC_URL || ""}/p/${projectName}/data-source/${inputGroup?.name}`}
                      >
                        {inputGroup?.name}
                      </EuiCustomLink>
                    </EuiTitle>
                    <EuiFlexItem key={key}>
                      <EuiCodeBlock language="json" fontSize="m" paddingSize="m">
                        {JSON.stringify(inputGroup, null, 2)}
                      </EuiCodeBlock>
                    </EuiFlexItem>
                  </EuiPanel>
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

export default StreamFeatureViewOverviewTab;
