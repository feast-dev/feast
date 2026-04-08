import {
  EuiFlexGroup,
  EuiHorizontalRule,
  EuiLoadingSpinner,
  EuiText,
  EuiTitle,
  EuiButtonEmpty,
  EuiCallOut,
} from "@elastic/eui";
import {
  EuiPanel,
  EuiFlexItem,
  EuiDescriptionList,
  EuiDescriptionListTitle,
  EuiDescriptionListDescription,
  EuiSpacer,
} from "@elastic/eui";
import React, { useContext, useState } from "react";
import { useParams } from "react-router-dom";
import PermissionsDisplay from "../../components/PermissionsDisplay";
import DataSourceFormModal, {
  DataSourceFormData,
} from "../../components/DataSourceFormModal";
import RegistryPathContext from "../../contexts/RegistryPathContext";
import { FEAST_FCO_TYPES } from "../../parsers/types";
import { feast } from "../../protos";
import useLoadRegistry from "../../queries/useLoadRegistry";
import { getEntityPermissions } from "../../utils/permissionUtils";
import BatchSourcePropertiesView from "./BatchSourcePropertiesView";
import FeatureViewEdgesList from "../entities/FeatureViewEdgesList";
import RequestDataSourceSchemaTable from "./RequestDataSourceSchemaTable";
import useLoadDataSource from "./useLoadDataSource";

const buildEditFormData = (ds: feast.core.IDataSource): DataSourceFormData => {
  const tags = ds.tags
    ? Object.entries(ds.tags).map(([key, value]) => ({ key, value }))
    : [];

  return {
    name: ds.name || "",
    description: ds.description || "",
    owner: ds.owner || "",
    sourceType: String(ds.type ?? 0),
    timestampField: ds.timestampField || "",
    createdTimestampColumn: ds.createdTimestampColumn || "",
    tags,
    fileUri: ds.fileOptions?.uri || "",
    bigqueryTable: ds.bigqueryOptions?.table || "",
    bigqueryQuery: ds.bigqueryOptions?.query || "",
    snowflakeTable: ds.snowflakeOptions?.table || "",
    snowflakeDatabase: ds.snowflakeOptions?.database || "",
    snowflakeSchema: ds.snowflakeOptions?.schema || "",
    redshiftTable: ds.redshiftOptions?.table || "",
    redshiftDatabase: ds.redshiftOptions?.database || "",
    redshiftSchema: ds.redshiftOptions?.schema || "",
    kafkaBootstrapServers: ds.kafkaOptions?.kafkaBootstrapServers || "",
    kafkaTopic: ds.kafkaOptions?.topic || "",
    sparkTable: ds.sparkOptions?.table || "",
    sparkPath: ds.sparkOptions?.path || "",
  };
};

const DataSourceOverviewTab = () => {
  let { dataSourceName, projectName } = useParams();
  const registryUrl = useContext(RegistryPathContext);
  const registryQuery = useLoadRegistry(registryUrl, projectName);

  const dsName = dataSourceName === undefined ? "" : dataSourceName;
  const { isLoading, isSuccess, isError, data, consumingFeatureViews } =
    useLoadDataSource(dsName);
  const isEmpty = data === undefined;

  const [isEditModalOpen, setIsEditModalOpen] = useState(false);
  const [successMessage, setSuccessMessage] = useState<string | null>(null);

  const handleEditSubmit = (formData: DataSourceFormData) => {
    console.log("Data source edit payload:", formData);
    setIsEditModalOpen(false);
    setSuccessMessage(
      `Changes to "${formData.name}" are ready to apply. Backend integration coming soon.`,
    );
    setTimeout(() => setSuccessMessage(null), 5000);
  };

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
          {successMessage && (
            <>
              <EuiCallOut
                title={successMessage}
                color="success"
                iconType="check"
                size="s"
              />
              <EuiSpacer size="m" />
            </>
          )}
          <EuiFlexGroup justifyContent="flexEnd">
            <EuiFlexItem grow={false}>
              <EuiButtonEmpty
                iconType="pencil"
                onClick={() => setIsEditModalOpen(true)}
              >
                Edit Data Source
              </EuiButtonEmpty>
            </EuiFlexItem>
          </EuiFlexGroup>
          <EuiSpacer size="s" />
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
                          data?.requestDataOptions?.schema!.map((obj: any) => {
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
                    fvNames={consumingFeatureViews.map((f: any) => {
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

      {isEditModalOpen && data && (
        <DataSourceFormModal
          onClose={() => setIsEditModalOpen(false)}
          onSubmit={handleEditSubmit}
          initialData={buildEditFormData(data)}
          isEdit
        />
      )}
    </React.Fragment>
  );
};
export default DataSourceOverviewTab;
