import React from "react";
import {
  EuiFlexGroup,
  EuiFlexItem,
  EuiStat,
  EuiHorizontalRule,
  EuiSpacer,
} from "@elastic/eui";
import { useNavigate, useParams } from "react-router-dom";
import useResourceQuery, {
  entityListPath,
  featureViewListPath,
  featureServiceListPath,
  dataSourceListPath,
  labelViewListPath,
  featuresListPath,
  restFeatureViewsToMergedList,
  restLabelViewsFromResponse,
} from "../queries/useResourceQuery";
import type { genericFVType } from "../parsers/mergedFVTypes";

const statStyle = { cursor: "pointer" };

const ObjectsCountStats = () => {
  const { projectName } = useParams();
  const navigate = useNavigate();

  const fsQuery = useResourceQuery<any[]>({
    resourceType: "stats-fs",
    project: projectName,
    restPath: featureServiceListPath(projectName),
    restSelect: (d) => d.featureServices,
  });

  const fvQuery = useResourceQuery<genericFVType[]>({
    resourceType: "stats-fvs",
    project: projectName,
    restPath: featureViewListPath(projectName),
    restSelect: restFeatureViewsToMergedList,
  });

  const entQuery = useResourceQuery<any[]>({
    resourceType: "stats-ent",
    project: projectName,
    restPath: entityListPath(projectName),
    restSelect: (d) => d.entities,
  });

  const dsQuery = useResourceQuery<any[]>({
    resourceType: "stats-ds",
    project: projectName,
    restPath: dataSourceListPath(projectName),
    restSelect: (d) => d.dataSources,
  });

  const featQuery = useResourceQuery<any[]>({
    resourceType: "stats-feat",
    project: projectName,
    restPath: featuresListPath(projectName),
    restSelect: (d) => d.features || [],
  });

  const lvQuery = useResourceQuery<any[]>({
    resourceType: "stats-lv",
    project: projectName,
    restPath: labelViewListPath(projectName),
    restSelect: restLabelViewsFromResponse,
  });

  const settled = (q: { isSuccess: boolean; isError: boolean }) =>
    q.isSuccess || q.isError;
  const allSettled =
    settled(fsQuery) &&
    settled(fvQuery) &&
    settled(entQuery) &&
    settled(dsQuery) &&
    settled(featQuery) &&
    settled(lvQuery);

  const stat = (
    q: { data?: any[]; isPermissionDenied: boolean },
    label: string,
    path: string,
  ) => {
    if (q.isPermissionDenied) return null;
    return (
      <EuiFlexItem>
        <EuiStat
          style={statStyle}
          onClick={() => navigate(`/p/${projectName}/${path}`)}
          description={`${label}→`}
          title={q.data?.length || 0}
          reverse
        />
      </EuiFlexItem>
    );
  };

  const hasLabelViews =
    !lvQuery.isPermissionDenied && (lvQuery.data?.length || 0) > 0;

  const queries = [fsQuery, fvQuery, featQuery, entQuery, dsQuery, lvQuery];
  const hasAnyAccessible = queries.some((q) => !q.isPermissionDenied);

  if (allSettled && !hasAnyAccessible) return null;

  return (
    <React.Fragment>
      <EuiSpacer size="l" />
      <EuiHorizontalRule margin="xs" />
      {!allSettled && <p>Loading</p>}
      {allSettled && (
        <React.Fragment>
          <EuiFlexGroup>
            {stat(fsQuery, "Feature Services", "feature-service")}
            {stat(fvQuery, "Feature Views", "feature-view")}
            {stat(featQuery, "Features", "features")}
            {stat(entQuery, "Entities", "entity")}
            {stat(dsQuery, "Data Sources", "data-source")}
          </EuiFlexGroup>
          {hasLabelViews && (
            <React.Fragment>
              <EuiSpacer size="m" />
              <EuiFlexGroup>
                {stat(lvQuery, "Label Views", "label-view")}
                <EuiFlexItem />
                <EuiFlexItem />
                <EuiFlexItem />
              </EuiFlexGroup>
            </React.Fragment>
          )}
        </React.Fragment>
      )}
    </React.Fragment>
  );
};

export default ObjectsCountStats;
