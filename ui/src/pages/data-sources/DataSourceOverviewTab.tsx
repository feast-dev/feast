import {
  EuiFlexGroup,
  EuiHorizontalRule,
  EuiLoadingSpinner,
  EuiText,
  EuiTitle,
} from "@elastic/eui";
import {
  EuiPanel,
  EuiFlexItem,
  EuiDescriptionList,
  EuiDescriptionListTitle,
  EuiDescriptionListDescription,
  EuiSpacer,
} from "@elastic/eui";
import React, { useContext } from "react";
import { useParams } from "react-router-dom";
import PermissionsDisplay from "../../components/PermissionsDisplay";
import RegistryPathContext from "../../contexts/RegistryPathContext";
import { FEAST_FCO_TYPES } from "../../parsers/types";
import { feast } from "../../protos";
import useLoadRegistry from "../../queries/useLoadRegistry";
import { getEntityPermissions } from "../../utils/permissionUtils";
import BatchSourcePropertiesView from "./BatchSourcePropertiesView";
import FeatureViewEdgesList from "../entities/FeatureViewEdgesList";
import RequestDataSourceSchemaTable from "./RequestDataSourceSchemaTable";
import useLoadDataSource from "./useLoadDataSource";

const DataSourceOverviewTab = () => {
  let { dataSourceName, projectName } = useParams();
  const registryUrl = useContext(RegistryPathContext);
  const registryQuery = useLoadRegistry(registryUrl, projectName);

  const dsName = dataSourceName === undefined ? "" : dataSourceName;
  const { isLoading, isSuccess, isError, data, consumingFeatureViews } =
    useLoadDataSource(dsName);
  const isEmpty = data === undefined;

  return (
    <React.Fragment>
      {isLoading && (
        <React.Fragment>
          <EuiLoadingSpinner size="m" /> Loading
        </React.Fragment>
      )}
      {isEmpty && <p>No data source with name: {dataSourceName}</p>}
      {isError && <p>Error loading data source: {dataSourceName}</p>}
      {isSuccess && data && (
        <React.Fragment>
          <EuiFlexGroup>
            <EuiFlexItem>
              <EuiFlexGroup>
                <EuiFlexItem>
                  <EuiPanel hasBorder={true} hasShadow={false}>
                    <EuiTitle size="xs">
                      <h2>Properties</h2>
                    </EuiTitle>
                    <EuiHorizontalRule margin="xs" />
                    {data.fileOptions || data.bigqueryOptions ? (
                      <BatchSourcePropertiesView batchSource={data} />
                    ) : data.type ? (
                      <React.Fragment>
                        <EuiDescriptionList>
                          <EuiDescriptionListTitle>
                            Source Type
                          </EuiDescriptionListTitle>
                          <EuiDescriptionListDescription>
                            {feast.core.DataSource.SourceType[data.type]}
                          </EuiDescriptionListDescription>
                        </EuiDescriptionList>
                      </React.Fragment>
                    ) : (
                      ""
                    )}
                  </EuiPanel>
                </EuiFlexItem>
              </EuiFlexGroup>
              <EuiSpacer size="m" />
              <EuiFlexGroup>
                <EuiFlexItem>
                  {data.requestDataOptions ? (
                    <EuiPanel hasBorder={true}>
                      <EuiTitle size="xs">
                        <h2>Request Source Schema</h2>
                      </EuiTitle>
                      <EuiHorizontalRule margin="xs"></EuiHorizontalRule>
                      <RequestDataSourceSchemaTable
                        fields={
                          data?.requestDataOptions?.schema!.map((obj) => {
                            return {
                              fieldName: obj.name!,
                              valueType: obj.valueType!,
                            };
                          })!
                        }
                      />
                    </EuiPanel>
                  ) : (
                    ""
                  )}
                </EuiFlexItem>
              </EuiFlexGroup>
            </EuiFlexItem>
            <EuiFlexItem>
              <EuiPanel hasBorder={true}>
                <EuiTitle size="xs">
                  <h2>Consuming Feature Views</h2>
                </EuiTitle>
                <EuiHorizontalRule margin="xs"></EuiHorizontalRule>
                {consumingFeatureViews && consumingFeatureViews.length > 0 ? (
                  <FeatureViewEdgesList
                    fvNames={consumingFeatureViews.map((f) => {
                      return f.target.name;
                    })}
                  />
                ) : (
                  <EuiText>No consuming feature views</EuiText>
                )}
              </EuiPanel>
              <EuiSpacer size="m" />
              <EuiPanel hasBorder={true}>
                <EuiTitle size="xs">
                  <h2>Permissions</h2>
                </EuiTitle>
                <EuiHorizontalRule margin="xs"></EuiHorizontalRule>
                {registryQuery.data?.permissions ? (
                  <PermissionsDisplay
                    permissions={getEntityPermissions(
                      registryQuery.data.permissions,
                      FEAST_FCO_TYPES.dataSource,
                      dsName,
                    )}
                  />
                ) : (
                  <EuiText>
                    No permissions defined for this data source.
                  </EuiText>
                )}
              </EuiPanel>
            </EuiFlexItem>
          </EuiFlexGroup>
        </React.Fragment>
      )}
    </React.Fragment>
  );
};
export default DataSourceOverviewTab;
