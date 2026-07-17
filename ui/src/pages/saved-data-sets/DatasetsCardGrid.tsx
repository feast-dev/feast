import React, { useState } from "react";
import {
  EuiFlexGroup,
  EuiFlexItem,
  EuiPanel,
  EuiTitle,
  EuiText,
  EuiBadge,
  EuiSpacer,
  EuiButtonIcon,
  EuiToolTip,
  EuiIcon,
  EuiCopy,
} from "@elastic/eui";
import { useNavigate, useParams } from "react-router-dom";

const STORAGE_TYPE_CONFIG: Record<
  string,
  { label: string; color: string; icon: string }
> = {
  file: { label: "File", color: "#4CAF50", icon: "document" },
  bigquery: { label: "BigQuery", color: "#4285F4", icon: "storage" },
  snowflake: { label: "Snowflake", color: "#29B5E8", icon: "snowflake" },
  redshift: { label: "Redshift", color: "#205B97", icon: "compute" },
  spark: { label: "Spark", color: "#E25A1C", icon: "bolt" },
  trino: { label: "Trino", color: "#DD00A1", icon: "database" },
  athena: { label: "Athena", color: "#8C4FFF", icon: "database" },
  custom: { label: "Custom", color: "#607D8B", icon: "gear" },
  unknown: { label: "Storage", color: "#98A2B3", icon: "database" },
};

function detectStorageType(dataset: any): string {
  const storage = dataset?.spec?.storage;
  if (!storage) return "unknown";
  if (storage.fileStorage) return "file";
  if (storage.bigqueryStorage) return "bigquery";
  if (storage.snowflakeStorage) return "snowflake";
  if (storage.redshiftStorage) return "redshift";
  if (storage.sparkStorage) return "spark";
  if (storage.trinoStorage) return "trino";
  if (storage.athenaStorage) return "athena";
  if (storage.customStorage) return "custom";
  return "unknown";
}

function extractStoragePath(dataset: any): string | undefined {
  const storage = dataset?.spec?.storage;
  if (!storage) return undefined;
  if (storage.fileStorage?.uri) return storage.fileStorage.uri;
  if (storage.bigqueryStorage?.table) return storage.bigqueryStorage.table;
  if (storage.snowflakeStorage?.table) return storage.snowflakeStorage.table;
  if (storage.redshiftStorage?.table) return storage.redshiftStorage.table;
  if (storage.sparkStorage?.path) return storage.sparkStorage.path;
  if (storage.sparkStorage?.table) return storage.sparkStorage.table;
  if (storage.trinoStorage?.table) return storage.trinoStorage.table;
  if (storage.athenaStorage?.table) return storage.athenaStorage.table;
  if (storage.customStorage?.configuration)
    return storage.customStorage.configuration;
  return undefined;
}

function truncatePath(path: string, maxLen: number = 42): string {
  if (path.length <= maxLen) return path;
  const start = path.slice(0, 18);
  const end = path.slice(-20);
  return `${start}...${end}`;
}

function getRelativeTime(date: Date): string {
  const now = new Date();
  const diffMs = now.getTime() - date.getTime();
  const diffDays = Math.floor(diffMs / (1000 * 60 * 60 * 24));

  if (diffDays === 0) return "Today";
  if (diffDays === 1) return "Yesterday";
  if (diffDays < 7) return `${diffDays}d ago`;
  if (diffDays < 30) return `${Math.floor(diffDays / 7)}w ago`;
  if (diffDays < 365) return `${Math.floor(diffDays / 30)}mo ago`;
  return `${Math.floor(diffDays / 365)}y ago`;
}

interface DatasetCardProps {
  dataset: any;
  isHovered: boolean;
  onHover: (name: string | null) => void;
  onDelete?: (name: string) => void;
}

const DatasetCard: React.FC<DatasetCardProps> = ({
  dataset,
  isHovered,
  onHover,
  onDelete,
}) => {
  const { projectName } = useParams();
  const navigate = useNavigate();
  const spec = dataset.spec || dataset;
  const meta = dataset.meta || {};
  const name = spec.name || "unknown";
  const datasetProject = dataset.project || spec.project || projectName;
  const features = spec.features || [];
  const joinKeys = spec.joinKeys || spec.join_keys || [];
  const tags = spec.tags || {};
  const featureServiceName =
    spec.featureServiceName || spec.feature_service_name;
  const createdTimestamp = meta.createdTimestamp || meta.created_timestamp;
  const storagePath = extractStoragePath(dataset);
  const storageType = detectStorageType(dataset);
  const storageInfo =
    STORAGE_TYPE_CONFIG[storageType] || STORAGE_TYPE_CONFIG.unknown;

  const formattedDate = createdTimestamp
    ? new Date(createdTimestamp).toLocaleDateString("en-US", {
        month: "short",
        day: "numeric",
        year: "numeric",
      })
    : null;
  const relativeTime = createdTimestamp
    ? getRelativeTime(new Date(createdTimestamp))
    : null;

  const codeSnippet = `dataset = store.get_saved_dataset("${name}")\ndf = dataset.to_df()`;

  return (
    <EuiFlexItem grow={false} style={{ width: 360, minWidth: 300 }}>
      <EuiPanel
        hasBorder
        hasShadow={isHovered}
        paddingSize="l"
        onMouseEnter={() => onHover(name)}
        onMouseLeave={() => onHover(null)}
        onClick={() => navigate(`/p/${datasetProject}/data-set/${name}`)}
        style={{
          height: "100%",
          display: "flex",
          flexDirection: "column",
          transition: "all 0.2s ease",
          transform: isHovered ? "translateY(-2px)" : "none",
          borderTop: `3px solid ${storageInfo.color}`,
          cursor: "pointer",
        }}
      >
        {/* Header: Name + Storage badge */}
        <EuiFlexGroup
          justifyContent="spaceBetween"
          alignItems="center"
          responsive={false}
          gutterSize="s"
        >
          <EuiFlexItem grow={false} style={{ minWidth: 0 }}>
            <EuiTitle size="xs">
              <h3
                style={{
                  overflow: "hidden",
                  textOverflow: "ellipsis",
                  whiteSpace: "nowrap",
                  margin: 0,
                }}
              >
                {name}
              </h3>
            </EuiTitle>
          </EuiFlexItem>
          <EuiFlexItem grow={false}>
            <EuiBadge color={storageInfo.color} iconType={storageInfo.icon}>
              {storageInfo.label}
            </EuiBadge>
          </EuiFlexItem>
        </EuiFlexGroup>

        <EuiSpacer size="m" />

        {/* Storage path */}
        {storagePath && (
          <EuiToolTip content={storagePath} position="bottom">
            <EuiText size="xs" color="subdued">
              <EuiIcon type="folderOpen" size="s" />{" "}
              <code style={{ fontSize: "11px" }}>
                {truncatePath(storagePath)}
              </code>
            </EuiText>
          </EuiToolTip>
        )}

        <EuiSpacer size="m" />

        {/* Metrics */}
        <EuiFlexGroup gutterSize="l" responsive={false} wrap>
          <EuiFlexItem grow={false}>
            <EuiText size="xs" color="subdued">
              Features
            </EuiText>
            <EuiText size="s">
              <strong>{features.length}</strong>
            </EuiText>
          </EuiFlexItem>
          <EuiFlexItem grow={false}>
            <EuiText size="xs" color="subdued">
              Retrieval Keys
            </EuiText>
            <EuiText size="s">
              <strong>{joinKeys.length}</strong>
            </EuiText>
          </EuiFlexItem>
          {featureServiceName && (
            <EuiFlexItem grow={false}>
              <EuiText size="xs" color="subdued">
                Service
              </EuiText>
              <EuiText size="s">
                <strong>{featureServiceName}</strong>
              </EuiText>
            </EuiFlexItem>
          )}
        </EuiFlexGroup>

        {/* Spacer to push footer */}
        <div style={{ flex: 1 }} />

        <EuiSpacer size="m" />

        {/* Tags row */}
        {Object.keys(tags).length > 0 && (
          <>
            <EuiFlexGroup gutterSize="xs" wrap responsive={false}>
              {Object.entries(tags)
                .slice(0, 3)
                .map(([key, value]) => (
                  <EuiFlexItem grow={false} key={key}>
                    <EuiBadge color="hollow">
                      {key}: {value as string}
                    </EuiBadge>
                  </EuiFlexItem>
                ))}
              {Object.keys(tags).length > 3 && (
                <EuiFlexItem grow={false}>
                  <EuiBadge color="hollow">
                    +{Object.keys(tags).length - 3} more
                  </EuiBadge>
                </EuiFlexItem>
              )}
            </EuiFlexGroup>
            <EuiSpacer size="s" />
          </>
        )}

        {/* Footer: Timestamp + actions */}
        <EuiFlexGroup
          justifyContent="spaceBetween"
          alignItems="center"
          responsive={false}
        >
          <EuiFlexItem grow={false}>
            {formattedDate && (
              <EuiToolTip content={formattedDate}>
                <EuiText size="xs" color="subdued">
                  <EuiIcon type="clock" size="s" /> {relativeTime}
                </EuiText>
              </EuiToolTip>
            )}
          </EuiFlexItem>
          <EuiFlexItem grow={false}>
            <EuiFlexGroup
              gutterSize="xs"
              alignItems="center"
              responsive={false}
            >
              <EuiFlexItem grow={false}>
                <EuiCopy textToCopy={codeSnippet}>
                  {(copy) => (
                    <EuiToolTip content="Copy Python load code">
                      <EuiButtonIcon
                        iconType="copyClipboard"
                        aria-label="Copy load code"
                        size="s"
                        onClick={(e: React.MouseEvent) => {
                          e.stopPropagation();
                          copy();
                        }}
                      />
                    </EuiToolTip>
                  )}
                </EuiCopy>
              </EuiFlexItem>
              {onDelete && (
                <EuiFlexItem grow={false}>
                  <EuiToolTip content="Delete dataset">
                    <EuiButtonIcon
                      iconType="trash"
                      color="danger"
                      size="s"
                      aria-label="Delete dataset"
                      onClick={(e: React.MouseEvent) => {
                        e.stopPropagation();
                        onDelete(name);
                      }}
                    />
                  </EuiToolTip>
                </EuiFlexItem>
              )}
            </EuiFlexGroup>
          </EuiFlexItem>
        </EuiFlexGroup>
      </EuiPanel>
    </EuiFlexItem>
  );
};

interface DatasetsCardGridProps {
  datasets: any[];
  onDelete?: (name: string) => void;
}

const DatasetsCardGrid = ({ datasets, onDelete }: DatasetsCardGridProps) => {
  const [hoveredId, setHoveredId] = useState<string | null>(null);

  if (datasets.length === 0) {
    return (
      <EuiText textAlign="center" color="subdued">
        <p>No datasets match your search.</p>
      </EuiText>
    );
  }

  return (
    <EuiFlexGroup wrap responsive gutterSize="l">
      {datasets.map((dataset: any) => {
        const name = dataset.spec?.name || dataset.name || "unknown";
        return (
          <DatasetCard
            key={name}
            dataset={dataset}
            isHovered={hoveredId === name}
            onHover={setHoveredId}
            onDelete={onDelete}
          />
        );
      })}
    </EuiFlexGroup>
  );
};

export default DatasetsCardGrid;
