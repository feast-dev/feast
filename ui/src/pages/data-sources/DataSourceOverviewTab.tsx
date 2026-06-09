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
import React, { useState } from "react";
import { useParams } from "react-router-dom";
import DataSourceFormModal, {
  DataSourceFormData,
} from "../../components/DataSourceFormModal";
import { feast } from "../../protos";
import { useApplyDataSource } from "../../queries/mutations/useDataSourceMutations";
import BatchSourcePropertiesView from "./BatchSourcePropertiesView";
import FeatureViewEdgesList from "../entities/FeatureViewEdgesList";
import RequestDataSourceSchemaTable from "./RequestDataSourceSchemaTable";
import useLoadDataSource from "./useLoadDataSource";

const buildEditFormData = (ds: any): DataSourceFormData => {
  const spec = ds.spec || ds;
  const tags = spec.tags
    ? Object.entries(spec.tags).map(([key, value]) => ({
        key,
        value: value as string,
      }))
    : [];

  return {
    name: spec.name || ds.name || "",
    description: spec.description || ds.description || "",
    owner: spec.owner || ds.owner || "",
    sourceType: String(spec.type ?? ds.type ?? 0),
    timestampField: spec.timestampField || ds.timestampField || "",
    createdTimestampColumn:
      spec.createdTimestampColumn || ds.createdTimestampColumn || "",
    tags,
    fileUri: spec.fileOptions?.uri || ds.fileOptions?.uri || "",
    bigqueryTable:
      spec.bigqueryOptions?.table || ds.bigqueryOptions?.table || "",
    bigqueryQuery:
      spec.bigqueryOptions?.query || ds.bigqueryOptions?.query || "",
    snowflakeTable:
      spec.snowflakeOptions?.table || ds.snowflakeOptions?.table || "",
    snowflakeDatabase:
      spec.snowflakeOptions?.database || ds.snowflakeOptions?.database || "",
    snowflakeSchema:
      spec.snowflakeOptions?.schema || ds.snowflakeOptions?.schema || "",
    redshiftTable:
      spec.redshiftOptions?.table || ds.redshiftOptions?.table || "",
    redshiftDatabase:
      spec.redshiftOptions?.database || ds.redshiftOptions?.database || "",
    redshiftSchema:
      spec.redshiftOptions?.schema || ds.redshiftOptions?.schema || "",
    kafkaBootstrapServers:
      spec.kafkaOptions?.kafkaBootstrapServers ||
      ds.kafkaOptions?.kafkaBootstrapServers ||
      "",
    kafkaTopic: spec.kafkaOptions?.topic || ds.kafkaOptions?.topic || "",
    sparkTable: spec.sparkOptions?.table || ds.sparkOptions?.table || "",
    sparkPath: spec.sparkOptions?.path || ds.sparkOptions?.path || "",
  };
};

const formDataToPayload = (formData: DataSourceFormData, project: string) => {
  const payload: Record<string, any> = {
    name: formData.name,
    project,
    type: parseInt(formData.sourceType, 10),
    timestamp_field: formData.timestampField,
    created_timestamp_column: formData.createdTimestampColumn,
    description: formData.description,
    owner: formData.owner,
    tags: Object.fromEntries(
      formData.tags.filter((t) => t.key.trim()).map((t) => [t.key, t.value]),
    ),
  };

  const st = formData.sourceType;
  if (st === String(feast.core.DataSource.SourceType.BATCH_FILE)) {
    payload.file_options = { uri: formData.fileUri };
  } else if (st === String(feast.core.DataSource.SourceType.BATCH_BIGQUERY)) {
    payload.bigquery_options = {
      table: formData.bigqueryTable,
      query: formData.bigqueryQuery,
    };
  } else if (st === String(feast.core.DataSource.SourceType.BATCH_SNOWFLAKE)) {
    payload.snowflake_options = {
      table: formData.snowflakeTable,
      database: formData.snowflakeDatabase,
      schema_: formData.snowflakeSchema,
    };
  } else if (st === String(feast.core.DataSource.SourceType.BATCH_REDSHIFT)) {
    payload.redshift_options = {
      table: formData.redshiftTable,
      database: formData.redshiftDatabase,
      schema_: formData.redshiftSchema,
    };
  } else if (st === String(feast.core.DataSource.SourceType.STREAM_KAFKA)) {
    payload.kafka_options = {
      kafka_bootstrap_servers: formData.kafkaBootstrapServers,
      topic: formData.kafkaTopic,
    };
  } else if (st === String(feast.core.DataSource.SourceType.BATCH_SPARK)) {
    payload.spark_options = {
      table: formData.sparkTable,
      path: formData.sparkPath,
    };
  }

  return payload;
};

const DataSourceOverviewTab = () => {
  const { dataSourceName, projectName } = useParams();

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

  const [isEditModalOpen, setIsEditModalOpen] = useState(false);
  const [successMessage, setSuccessMessage] = useState<string | null>(null);
  const [errorMessage, setErrorMessage] = useState<string | null>(null);
  const applyDataSource = useApplyDataSource();

  const handleEditSubmit = (formData: DataSourceFormData) => {
    const payload = formDataToPayload(formData, projectName || "");
    applyDataSource.mutate(payload as any, {
      onSuccess: () => {
        setIsEditModalOpen(false);
        setErrorMessage(null);
        setSuccessMessage(
          `Data source "${formData.name}" updated successfully.`,
        );
        setTimeout(() => setSuccessMessage(null), 5000);
      },
      onError: (err: unknown) => {
        // Error shown inside the modal via submitError prop
        const message =
          err instanceof Error ? err.message : "An unexpected error occurred.";
        setErrorMessage(message);
      },
    });
  };

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
          {errorMessage && (
            <>
              <EuiCallOut
                title={errorMessage}
                color="danger"
                iconType="alert"
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
                    {spec?.fileOptions || spec?.bigqueryOptions ? (
                      <BatchSourcePropertiesView batchSource={spec} />
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

      {isEditModalOpen && data && (
        <DataSourceFormModal
          onClose={() => {
            setIsEditModalOpen(false);
            setErrorMessage(null);
          }}
          onSubmit={handleEditSubmit}
          initialData={buildEditFormData(data)}
          isEdit
          isSubmitting={applyDataSource.isLoading}
          submitError={errorMessage}
        />
      )}
    </React.Fragment>
  );
};
export default DataSourceOverviewTab;
