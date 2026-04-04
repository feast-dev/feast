import React from "react";
import {
  EuiFlexGroup,
  EuiFlexItem,
  EuiStat,
  EuiHorizontalRule,
  EuiTitle,
  EuiSpacer,
} from "@elastic/eui";
import { useNavigate, useParams } from "react-router-dom";
import useResourceQuery, {
  entityListPath,
  featureViewListPath,
  featureServiceListPath,
  dataSourceListPath,
  restFeatureViewsToMergedList,
} from "../queries/useResourceQuery";
import type { genericFVType } from "../parsers/mergedFVTypes";

const statStyle = { cursor: "pointer" };

const ObjectsCountStats = () => {
  const { projectName } = useParams();
  const navigate = useNavigate();

  const { data: featureServices, isSuccess: fsOk } = useResourceQuery<any[]>({
    resourceType: "stats-fs",
    project: projectName,
    protoSelect: (d) => d.objects.featureServices,
    restPath: featureServiceListPath(projectName),
    restSelect: (d) => d.featureServices,
  });

  const { data: featureViews, isSuccess: fvOk } = useResourceQuery<
    genericFVType[]
  >({
    resourceType: "stats-fvs",
    project: projectName,
    protoSelect: (d) => d.mergedFVList,
    restPath: featureViewListPath(projectName),
    restSelect: restFeatureViewsToMergedList,
  });

  const { data: entities, isSuccess: entOk } = useResourceQuery<any[]>({
    resourceType: "stats-ent",
    project: projectName,
    protoSelect: (d) => d.objects.entities,
    restPath: entityListPath(projectName),
    restSelect: (d) => d.entities,
  });

  const { data: dataSources, isSuccess: dsOk } = useResourceQuery<any[]>({
    resourceType: "stats-ds",
    project: projectName,
    protoSelect: (d) => d.objects.dataSources,
    restPath: dataSourceListPath(projectName),
    restSelect: (d) => d.dataSources,
  });

  const allOk = fsOk && fvOk && entOk && dsOk;

  return (
    <React.Fragment>
      <EuiSpacer size="l" />
      <EuiHorizontalRule margin="xs" />
      {!allOk && <p>Loading</p>}
      {allOk && (
        <React.Fragment>
          <EuiTitle size="xs">
            <h3>Registered in this Feast project are &hellip;</h3>
          </EuiTitle>
          <EuiSpacer size="s" />
          <EuiFlexGroup>
            <EuiFlexItem>
              <EuiStat
                style={statStyle}
                onClick={() => navigate(`/p/${projectName}/feature-service`)}
                description="Feature Services→"
                title={featureServices?.length || 0}
                reverse
              />
            </EuiFlexItem>
            <EuiFlexItem>
              <EuiStat
                style={statStyle}
                description="Feature Views→"
                onClick={() => navigate(`/p/${projectName}/feature-view`)}
                title={featureViews?.length || 0}
                reverse
              />
            </EuiFlexItem>
            <EuiFlexItem>
              <EuiStat
                style={statStyle}
                description="Entities→"
                onClick={() => navigate(`/p/${projectName}/entity`)}
                title={entities?.length || 0}
                reverse
              />
            </EuiFlexItem>
            <EuiFlexItem>
              <EuiStat
                style={statStyle}
                description="Data Sources→"
                onClick={() => navigate(`/p/${projectName}/data-source`)}
                title={dataSources?.length || 0}
                reverse
              />
            </EuiFlexItem>
          </EuiFlexGroup>
        </React.Fragment>
      )}
    </React.Fragment>
  );
};

export default ObjectsCountStats;
