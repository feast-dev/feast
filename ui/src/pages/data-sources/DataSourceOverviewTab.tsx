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
import React from "react";
import { useParams } from "react-router-dom";
import BatchSourcePropertiesView from "./BatchSourcePropertiesView";
import FeatureViewEdgesList from "../entities/FeatureViewEdgesList";
import RequestDataSourceSchemaTable from "./RequestDataSourceSchemaTable";
import useLoadDataSource from "./useLoadDataSource";
import { feast } from "../../protos";

const DataSourceOverviewTab = () => {
  const { dataSourceName } = useParams();

  const dsName = dataSourceName === undefined ? "" : dataSourceName;
  const { isLoading, isSuccess, isError, data, consumingFeatureViews } =
    useLoadDataSource(dsName);
  const isEmpty = data === undefined;

  const viewTypesForDs: Record<string, string> | undefined =
    consumingFeatureViews && consumingFeatureViews.length > 0
      ? consumingFeatureViews.reduce((acc: Record<string, string>, f: any) => {
          acc[f.target.name] =
            f.target.type === "labelView" ? "labelView" : "featureView";
          return acc;
        }, {})
      : undefined;

  const spec = data?.spec || data;
  const sourceType = spec?.type;

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
                    {spec?.fileOptions || spec?.bigqueryOptions ? (
                      <BatchSourcePropertiesView batchSource={spec} />
                    ) : String(sourceType) ===
                      String(feast.core.DataSource.SourceType.BATCH_ICEBERG) ? (
                      (() => {
                        let cfg: any = {};
                        try {
                          cfg = JSON.parse(
                            spec?.customOptions?.configuration || "{}",
                          );
                        } catch {
                          /* ignore */
                        }
                        return (
                          <EuiDescriptionList>
                            <EuiDescriptionListTitle>
                              Source Type
                            </EuiDescriptionListTitle>
                            <EuiDescriptionListDescription>
                              Iceberg / Unity Catalog
                            </EuiDescriptionListDescription>
                            <EuiDescriptionListTitle>
                              Catalog Type
                            </EuiDescriptionListTitle>
                            <EuiDescriptionListDescription>
                              {cfg.catalog_type || "rest"}
                            </EuiDescriptionListDescription>
                            {cfg.endpoint && (
                              <>
                                <EuiDescriptionListTitle>
                                  Endpoint
                                </EuiDescriptionListTitle>
                                <EuiDescriptionListDescription>
                                  {cfg.endpoint}
                                </EuiDescriptionListDescription>
                              </>
                            )}
                            <EuiDescriptionListTitle>
                              Warehouse
                            </EuiDescriptionListTitle>
                            <EuiDescriptionListDescription>
                              {cfg.warehouse || "—"}
                            </EuiDescriptionListDescription>
                            <EuiDescriptionListTitle>
                              Namespace
                            </EuiDescriptionListTitle>
                            <EuiDescriptionListDescription>
                              {cfg.namespace || "—"}
                            </EuiDescriptionListDescription>
                            <EuiDescriptionListTitle>
                              Table
                            </EuiDescriptionListTitle>
                            <EuiDescriptionListDescription>
                              {cfg.table || "—"}
                            </EuiDescriptionListDescription>
                            {cfg.token_env_var && (
                              <>
                                <EuiDescriptionListTitle>
                                  Token Env Variable
                                </EuiDescriptionListTitle>
                                <EuiDescriptionListDescription>
                                  {cfg.token_env_var}
                                </EuiDescriptionListDescription>
                              </>
                            )}
                          </EuiDescriptionList>
                        );
                      })()
                    ) : sourceType ? (
                      <React.Fragment>
                        <EuiDescriptionList>
                          <EuiDescriptionListTitle>
                            Source Type
                          </EuiDescriptionListTitle>
                          <EuiDescriptionListDescription>
                            {sourceType}
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
                  {spec?.requestDataOptions ? (
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
                  <h2>Consuming Views</h2>
                </EuiTitle>
                <EuiHorizontalRule margin="xs"></EuiHorizontalRule>
                {consumingFeatureViews && consumingFeatureViews.length > 0 ? (
                  <FeatureViewEdgesList
                    fvNames={consumingFeatureViews.map((f: any) => {
                      return f.target.name;
                    })}
                    viewTypes={viewTypesForDs}
                  />
                ) : (
                  <EuiText>No consuming views</EuiText>
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
