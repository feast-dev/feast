import React from "react";
import {
  EuiFlexGroup,
  EuiHorizontalRule,
  EuiLoadingSpinner,
  EuiTitle,
  EuiPanel,
  EuiFlexItem,
  EuiSpacer,
  EuiDescriptionList,
  EuiDescriptionListTitle,
  EuiDescriptionListDescription,
  EuiBadge,
  EuiText,
  EuiCallOut,
} from "@elastic/eui";
import { useParams } from "react-router-dom";
import DatasetFeaturesTable from "./DatasetFeaturesTable";
import DatasetJoinKeysTable from "./DatasetJoinKeysTable";
import useLoadDataset from "./useLoadDataset";

function extractStorageInfo(data: any): { type: string; path: string } {
  const storage = data?.spec?.storage;
  if (!storage) return { type: "Unknown", path: "—" };
  if (storage.fileStorage?.uri)
    return { type: "File", path: storage.fileStorage.uri };
  if (storage.bigqueryStorage?.table)
    return { type: "BigQuery", path: storage.bigqueryStorage.table };
  if (storage.snowflakeStorage?.table)
    return { type: "Snowflake", path: storage.snowflakeStorage.table };
  if (storage.redshiftStorage?.table)
    return { type: "Redshift", path: storage.redshiftStorage.table };
  if (storage.sparkStorage?.path)
    return { type: "Spark", path: storage.sparkStorage.path };
  if (storage.sparkStorage?.table)
    return { type: "Spark", path: storage.sparkStorage.table };
  if (storage.trinoStorage?.table)
    return { type: "Trino", path: storage.trinoStorage.table };
  if (storage.athenaStorage?.table)
    return { type: "Athena", path: storage.athenaStorage.table };
  if (storage.customStorage?.configuration)
    return { type: "Custom", path: storage.customStorage.configuration };
  return { type: "Unknown", path: "—" };
}

function formatTimestamp(ts: any): string {
  if (!ts) return "—";
  try {
    return new Date(ts).toLocaleString("en-US", {
      dateStyle: "medium",
      timeStyle: "short",
    });
  } catch {
    return "—";
  }
}

const DatasetOverviewTab = () => {
  let { datasetName } = useParams();

  if (!datasetName) {
    throw new Error(
      "Route doesn't have a 'datasetName' part. This route is likely rendering the wrong component.",
    );
  }

  const { isLoading, isError, data } = useLoadDataset(datasetName);

  if (isLoading) {
    return (
      <EuiFlexGroup justifyContent="center" alignItems="center">
        <EuiFlexItem grow={false}>
          <EuiLoadingSpinner size="l" />
        </EuiFlexItem>
        <EuiFlexItem grow={false}>
          <EuiText>Loading dataset details...</EuiText>
        </EuiFlexItem>
      </EuiFlexGroup>
    );
  }

  if (isError || !data) {
    return (
      <EuiCallOut title="Dataset not found" color="warning" iconType="alert">
        <p>
          Could not load dataset <strong>{datasetName}</strong>. It may have
          been deleted or the registry may be unavailable.
        </p>
      </EuiCallOut>
    );
  }

  const storageInfo = extractStorageInfo(data);
  const features = data.spec?.features || [];
  const joinKeys = data.spec?.joinKeys || data.spec?.join_keys || [];
  const tags = data.spec?.tags || {};
  const featureServiceName =
    data.spec?.featureServiceName || data.spec?.feature_service_name;
  const createdTs = data.meta?.createdTimestamp || data.meta?.created_timestamp;
  const minEventTs =
    data.meta?.minEventTimestamp || data.meta?.min_event_timestamp;
  const maxEventTs =
    data.meta?.maxEventTimestamp || data.meta?.max_event_timestamp;

  // Determine provenance: if min/max event timestamps exist, the dataset
  // was likely created via SDK (persist), otherwise it was linked manually.
  const provenance = minEventTs
    ? "Created (feature retrieval)"
    : "Linked (existing data)";

  return (
    <EuiFlexGroup gutterSize="l">
      {/* Left: Schema */}
      <EuiFlexItem grow={2}>
        <EuiPanel hasBorder>
          <EuiTitle size="xs">
            <h2>Features ({features.length})</h2>
          </EuiTitle>
          <EuiHorizontalRule margin="xs" />
          {features.length > 0 ? (
            <DatasetFeaturesTable
              features={features.map((joinedName: string) => {
                const parts = joinedName.split(":");
                return {
                  featureViewName: parts.length > 1 ? parts[0] : "—",
                  featureName: parts.length > 1 ? parts[1] : joinedName,
                };
              })}
            />
          ) : (
            <EuiText size="s" color="subdued">
              No features defined
            </EuiText>
          )}
        </EuiPanel>

        <EuiSpacer size="m" />

        <EuiPanel hasBorder>
          <EuiTitle size="xs">
            <h2>Retrieval Keys ({joinKeys.length})</h2>
          </EuiTitle>
          <EuiHorizontalRule margin="xs" />
          {joinKeys.length > 0 ? (
            <DatasetJoinKeysTable
              joinKeys={joinKeys.map((joinKey: any) => ({ name: joinKey }))}
            />
          ) : (
            <EuiText size="s" color="subdued">
              No retrieval keys defined
            </EuiText>
          )}
        </EuiPanel>
      </EuiFlexItem>

      {/* Right: Properties */}
      <EuiFlexItem grow={1}>
        <EuiPanel hasBorder grow={false}>
          <EuiTitle size="xs">
            <h3>Properties</h3>
          </EuiTitle>
          <EuiHorizontalRule margin="xs" />
          <EuiDescriptionList compressed>
            <EuiDescriptionListTitle>Origin</EuiDescriptionListTitle>
            <EuiDescriptionListDescription>
              <EuiBadge color={minEventTs ? "success" : "default"}>
                {provenance}
              </EuiBadge>
            </EuiDescriptionListDescription>

            <EuiDescriptionListTitle>Storage Type</EuiDescriptionListTitle>
            <EuiDescriptionListDescription>
              <EuiBadge color="hollow">{storageInfo.type}</EuiBadge>
            </EuiDescriptionListDescription>

            <EuiDescriptionListTitle>Storage Path</EuiDescriptionListTitle>
            <EuiDescriptionListDescription>
              <EuiText size="xs">
                <code>{storageInfo.path}</code>
              </EuiText>
            </EuiDescriptionListDescription>

            {featureServiceName && (
              <>
                <EuiDescriptionListTitle>
                  Feature Service
                </EuiDescriptionListTitle>
                <EuiDescriptionListDescription>
                  {featureServiceName}
                </EuiDescriptionListDescription>
              </>
            )}

            <EuiDescriptionListTitle>Created</EuiDescriptionListTitle>
            <EuiDescriptionListDescription>
              {formatTimestamp(createdTs)}
            </EuiDescriptionListDescription>

            {minEventTs && (
              <>
                <EuiDescriptionListTitle>
                  Event Time Range
                </EuiDescriptionListTitle>
                <EuiDescriptionListDescription>
                  {formatTimestamp(minEventTs)} → {formatTimestamp(maxEventTs)}
                </EuiDescriptionListDescription>
              </>
            )}
          </EuiDescriptionList>
        </EuiPanel>

        {Object.keys(tags).length > 0 && (
          <>
            <EuiSpacer size="m" />
            <EuiPanel hasBorder grow={false}>
              <EuiTitle size="xs">
                <h3>Tags</h3>
              </EuiTitle>
              <EuiHorizontalRule margin="xs" />
              <EuiDescriptionList compressed>
                {Object.entries(tags).map(([key, value]) => (
                  <React.Fragment key={key}>
                    <EuiDescriptionListTitle>{key}</EuiDescriptionListTitle>
                    <EuiDescriptionListDescription>
                      {value as string}
                    </EuiDescriptionListDescription>
                  </React.Fragment>
                ))}
              </EuiDescriptionList>
            </EuiPanel>
          </>
        )}
      </EuiFlexItem>
    </EuiFlexGroup>
  );
};

export default DatasetOverviewTab;
