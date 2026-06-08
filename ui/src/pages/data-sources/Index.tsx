import React, { useState } from "react";
import { useParams } from "react-router-dom";

import {
  EuiPageTemplate,
  EuiLoadingSpinner,
  EuiFlexGroup,
  EuiFlexItem,
  EuiTitle,
  EuiFieldSearch,
  EuiSpacer,
  EuiButton,
  EuiCallOut,
} from "@elastic/eui";

import DatasourcesListingTable from "./DataSourcesListingTable";
import { useDocumentTitle } from "../../hooks/useDocumentTitle";
import DataSourceIndexEmptyState from "./DataSourceIndexEmptyState";
import { DataSourceIcon } from "../../graphics/DataSourceIcon";
import { useSearchQuery } from "../../hooks/useSearchInputWithTags";
import { feast } from "../../protos";
import ExportButton from "../../components/ExportButton";
import DataSourceFormModal, {
  DataSourceFormData,
} from "../../components/DataSourceFormModal";
import { useApplyDataSource } from "../../queries/mutations/useDataSourceMutations";
import useResourceQuery, {
  dataSourceListPath,
} from "../../queries/useResourceQuery";

const useLoadDatasources = () => {
  const { projectName } = useParams();
  return useResourceQuery<any[]>({
    resourceType: "data-sources-list",
    project: projectName,
    restPath: dataSourceListPath(projectName),
    restSelect: (d) => d.dataSources,
  });
};

const filterFn = (data: any[], searchTokens: string[]) => {
  if (searchTokens.length) {
    return data.filter((entry) => {
      const name = entry.name || entry.spec?.name || "";
      return searchTokens.find((token) => {
        return token.length >= 3 && name.indexOf(token) >= 0;
      });
    });
  }

  return data;
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

const Index = () => {
  const { projectName } = useParams();
  const { isLoading, isSuccess, isError, data } = useLoadDatasources();
  const isAllProjects = projectName === "all";

  const [isModalOpen, setIsModalOpen] = useState(false);
  const [successMessage, setSuccessMessage] = useState<string | null>(null);
  const [errorMessage, setErrorMessage] = useState<string | null>(null);
  const applyDataSource = useApplyDataSource();

  useDocumentTitle(`Data Sources | Feast`);

  const { searchString, searchTokens, setSearchString } = useSearchQuery();

  const filterResult = data ? filterFn(data, searchTokens) : data;

  const handleCreateSubmit = (formData: DataSourceFormData) => {
    const payload = formDataToPayload(formData, projectName || "");
    applyDataSource.mutate(payload as any, {
      onSuccess: () => {
        setIsModalOpen(false);
        setErrorMessage(null);
        setSuccessMessage(
          `Data source "${formData.name}" created successfully.`,
        );
        setTimeout(() => setSuccessMessage(null), 5000);
      },
      onError: (err: unknown) => {
        const message =
          err instanceof Error ? err.message : "An unexpected error occurred.";
        setErrorMessage(message);
        setTimeout(() => setErrorMessage(null), 8000);
      },
    });
  };

  return (
    <EuiPageTemplate panelled>
      <EuiPageTemplate.Header
        restrictWidth
        iconType={DataSourceIcon}
        pageTitle="Data Sources"
        rightSideItems={[
          ...(isAllProjects
            ? []
            : [
                <EuiButton
                  fill
                  iconType="plus"
                  onClick={() => setIsModalOpen(true)}
                  key="create"
                >
                  Create Data Source
                </EuiButton>,
              ]),
          <ExportButton
            data={filterResult ?? []}
            fileName="data_sources"
            formats={["json"]}
            key="export"
          />,
        ]}
      />
      <EuiPageTemplate.Section>
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
        {isLoading && (
          <p>
            <EuiLoadingSpinner size="m" /> Loading
          </p>
        )}
        {isError && <p>We encountered an error while loading.</p>}
        {isSuccess && !data && <DataSourceIndexEmptyState />}
        {isSuccess && data && data.length > 0 && filterResult && (
          <React.Fragment>
            <EuiFlexGroup>
              <EuiFlexItem grow={2}>
                <EuiTitle size="xs">
                  <h2>Search</h2>
                </EuiTitle>
                <EuiFieldSearch
                  value={searchString}
                  fullWidth={true}
                  onChange={(e) => {
                    setSearchString(e.target.value);
                  }}
                />
              </EuiFlexItem>
            </EuiFlexGroup>
            <EuiSpacer size="m" />
            <DatasourcesListingTable dataSources={filterResult} />
          </React.Fragment>
        )}
      </EuiPageTemplate.Section>

      {isModalOpen && (
        <DataSourceFormModal
          onClose={() => setIsModalOpen(false)}
          onSubmit={handleCreateSubmit}
        />
      )}
    </EuiPageTemplate>
  );
};

export default Index;
