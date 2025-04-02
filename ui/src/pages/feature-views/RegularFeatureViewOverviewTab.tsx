import {
  EuiBadge,
  EuiFlexGroup,
  EuiFlexItem,
  EuiHorizontalRule,
  EuiPanel,
  EuiSpacer,
  EuiStat,
  EuiText,
  EuiTitle,
} from "@elastic/eui";
import React from "react";

import { useNavigate, useParams } from "react-router-dom";
import FeaturesListDisplay from "../../components/FeaturesListDisplay";
import TagsDisplay from "../../components/TagsDisplay";
import { encodeSearchQueryString } from "../../hooks/encodeSearchQueryString";
import { EntityRelation } from "../../parsers/parseEntityRelationships";
import { FEAST_FCO_TYPES } from "../../parsers/types";
import useLoadRelationshipData from "../../queries/useLoadRelationshipsData";
import BatchSourcePropertiesView from "../data-sources/BatchSourcePropertiesView";
import ConsumingFeatureServicesList from "./ConsumingFeatureServicesList";
import { feast } from "../../protos";
import { toDate } from "../../utils/timestamp";

const whereFSconsumesThisFv = (fvName: string) => {
  return (r: EntityRelation) => {
    return (
      r.source.name === fvName &&
      r.target.type === FEAST_FCO_TYPES.featureService
    );
  };
};

interface RegularFeatureViewOverviewTabProps {
  data: feast.core.IFeatureView;
}

const RegularFeatureViewOverviewTab = ({
  data,
}: RegularFeatureViewOverviewTabProps) => {
  const navigate = useNavigate();

  const { projectName } = useParams();
  const { featureViewName } = useParams();

  const fvName = featureViewName === undefined ? "" : featureViewName;

  const relationshipQuery = useLoadRelationshipData();

  const fsNames = relationshipQuery.data
    ? relationshipQuery.data.filter(whereFSconsumesThisFv(fvName)).map((fs) => {
        return fs.target.name;
      })
    : [];
  const numOfFs = fsNames.length;

  return (
    <React.Fragment>
      <EuiFlexGroup>
        <EuiFlexItem>
          <EuiStat title={`${numOfFs}`} description="Consuming Services" />
        </EuiFlexItem>
      </EuiFlexGroup>
      <EuiSpacer size="l" />
      <EuiFlexGroup>
        <EuiFlexItem>
          <EuiPanel hasBorder={true}>
            <EuiTitle size="xs">
              <h3>Features ({data?.spec?.features?.length})</h3>
            </EuiTitle>
            <EuiHorizontalRule margin="xs" />
            {projectName && data?.spec?.features ? (
              <FeaturesListDisplay
                projectName={projectName}
                featureViewName={data?.spec?.name!}
                features={data.spec.features}
                link={true}
              />
            ) : (
              <EuiText>No features specified on this feature view.</EuiText>
            )}
          </EuiPanel>
        </EuiFlexItem>
        <EuiFlexItem>
          <EuiPanel hasBorder={true} grow={false}>
            <EuiTitle size="xs">
              <h3>Entities</h3>
            </EuiTitle>
            <EuiHorizontalRule margin="xs" />
            {data?.spec?.entities ? (
              <EuiFlexGroup wrap responsive={false} gutterSize="xs">
                {data.spec.entities.map((entity) => {
                  return (
                    <EuiFlexItem grow={false} key={entity}>
                      <EuiBadge
                        color="primary"
                        onClick={() => {
                          navigate(`/p/${projectName}/entity/${entity}`);
                        }}
                        onClickAriaLabel={entity}
                        data-test-sub="testExample1"
                      >
                        {entity}
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
              <h3>Consuming Feature Services</h3>
            </EuiTitle>
            <EuiHorizontalRule margin="xs" />
            {fsNames.length > 0 ? (
              <ConsumingFeatureServicesList fsNames={fsNames} />
            ) : (
              <EuiText>No services consume this feature view</EuiText>
            )}
          </EuiPanel>
          <EuiSpacer size="m" />
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
                    `/p/${projectName}/feature-view?` +
                    encodeSearchQueryString(`${key}:${value}`)
                  );
                }}
                owner={data?.spec?.owner!}
                description={data?.spec?.description!}
              />
            ) : (
              <EuiText>No Tags specified on this feature view.</EuiText>
            )}
          </EuiPanel>
        </EuiFlexItem>
      </EuiFlexGroup>
      <EuiSpacer size="l" />
      <EuiFlexGroup>
        <EuiFlexItem>
          <EuiPanel hasBorder={true}>
            <EuiTitle size="xs">
              <h3>Batch Source</h3>
            </EuiTitle>
            <EuiHorizontalRule margin="xs" />
            <BatchSourcePropertiesView batchSource={data?.spec?.batchSource!} />
          </EuiPanel>
        </EuiFlexItem>
      </EuiFlexGroup>
      <EuiSpacer size="l" />
      <EuiPanel>
        <EuiTitle size="xs">
          <h3>Materialization Intervals</h3>
        </EuiTitle>
        <React.Fragment>
          {data?.meta?.materializationIntervals?.map((interval, i) => {
            return (
              <p key={i}>
                {toDate(interval.startTime!).toLocaleDateString("en-CA")} to{" "}
                {toDate(interval.endTime!).toLocaleDateString("en-CA")}
              </p>
            );
          })}
        </React.Fragment>
      </EuiPanel>
    </React.Fragment>
  );
};

export default RegularFeatureViewOverviewTab;
