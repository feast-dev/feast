import React, { useContext } from "react";
import {
  EuiFlexGroup,
  EuiFlexItem,
  EuiStat,
  EuiHorizontalRule,
  EuiTitle,
  EuiSpacer,
} from "@elastic/eui";
import useLoadRegistry from "../queries/useLoadRegistry";
import { useNavigate, useParams } from "react-router-dom";
import RegistryPathContext from "../contexts/RegistryPathContext";

const useLoadObjectStats = () => {
  const registryUrl = useContext(RegistryPathContext);
  const query = useLoadRegistry(registryUrl);

  const data =
    query.isSuccess && query.data
      ? {
          featureServices: query.data.objects.featureServices?.length || 0,
          featureViews: query.data.mergedFVList.length,
          entities: query.data.objects.entities?.length || 0,
          dataSources: query.data.objects.dataSources?.length || 0,
        }
      : undefined;

  return {
    ...query,
    data,
  };
};

const statStyle = { cursor: "pointer" };

const ObjectsCountStats = () => {
  const { isLoading, isSuccess, isError, data } = useLoadObjectStats();
  const { projectName } = useParams();

  const navigate = useNavigate();

  return (
    <React.Fragment>
      <EuiSpacer size="l" />
      <EuiHorizontalRule margin="xs" />
      {isLoading && <p>Loading</p>}
      {isError && <p>There was an error in loading registry information.</p>}
      {isSuccess && data && (
        <React.Fragment>
          <EuiTitle size="xs">
            <h3>Registered in this Feast project are &hellip;</h3>
          </EuiTitle>
          <EuiSpacer size="s" />
          <EuiFlexGroup>
            <EuiFlexItem>
              <EuiStat
                style={statStyle}
                onClick={() => navigate(`${process.env.PUBLIC_URL || ""}/p/${projectName}/feature-service`)}
                description="Feature Services→"
                title={data.featureServices}
                reverse
              />
            </EuiFlexItem>
            <EuiFlexItem>
              <EuiStat
                style={statStyle}
                description="Feature Views→"
                onClick={() => navigate(`${process.env.PUBLIC_URL || ""}/p/${projectName}/feature-view`)}
                title={data.featureViews}
                reverse
              />
            </EuiFlexItem>
            <EuiFlexItem>
              <EuiStat
                style={statStyle}
                description="Entities→"
                onClick={() => navigate(`${process.env.PUBLIC_URL || ""}/p/${projectName}/entity`)}
                title={data.entities}
                reverse
              />
            </EuiFlexItem>
            <EuiFlexItem>
              <EuiStat
                style={statStyle}
                description="Data Sources→"
                onClick={() => navigate(`${process.env.PUBLIC_URL || ""}/p/${projectName}/data-source`)}
                title={data.dataSources}
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
