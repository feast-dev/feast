import React, { useCallback, useMemo, useState } from "react";
import {
  EuiFlexGroup,
  EuiFlexItem,
  EuiPanel,
  EuiTitle,
  EuiText,
  EuiBadge,
  EuiSpacer,
  EuiIcon,
  EuiBreadcrumbs,
  EuiEmptyPrompt,
  EuiTreeView,
  EuiToolTip,
  EuiButtonIcon,
  EuiCopy,
} from "@elastic/eui";
import type { Node as EuiTreeNode } from "@elastic/eui/src/components/tree_view/tree_view";
import { useNavigate, useParams } from "react-router-dom";

/* ────────────────────────── types ────────────────────────── */

interface BrowsePath {
  namespace?: string;
  collection?: string;
}

interface DatasetCatalogBrowserProps {
  datasets: any[];
  onDelete?: (name: string) => void;
}

/* ──────────────────── hierarchy builder ──────────────────── */

function buildHierarchy(datasets: any[]) {
  const tree: Record<string, Record<string, any[]>> = {};
  for (const ds of datasets) {
    const ns = ds.spec?.namespace || "";
    const col = ds.spec?.collection || "";
    if (!tree[ns]) tree[ns] = {};
    if (!tree[ns][col]) tree[ns][col] = [];
    tree[ns][col].push(ds);
  }
  return tree;
}

/* ──────────────────── colors ──────────────────── */

const NS_COLOR = "#0077CC";
const COL_COLOR = "#8B5CF6";

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
  return `${path.slice(0, 18)}...${path.slice(-20)}`;
}

function getRelativeTime(date: Date): string {
  const diffMs = Date.now() - date.getTime();
  const diffDays = Math.floor(diffMs / 86400000);
  if (diffDays === 0) return "Today";
  if (diffDays === 1) return "Yesterday";
  if (diffDays < 7) return `${diffDays}d ago`;
  if (diffDays < 30) return `${Math.floor(diffDays / 7)}w ago`;
  if (diffDays < 365) return `${Math.floor(diffDays / 30)}mo ago`;
  return `${Math.floor(diffDays / 365)}y ago`;
}

/* ──────────────────── tile: dataset card ──────────────────── */

const DatasetCard: React.FC<{
  dataset: any;
  onNavigate: () => void;
  onDelete?: (name: string) => void;
}> = ({ dataset, onNavigate, onDelete }) => {
  const [isHovered, setIsHovered] = useState(false);
  const spec = dataset.spec || {};
  const meta = dataset.meta || {};
  const name = spec.name || "unknown";
  const features = spec.features || [];
  const joinKeys = spec.joinKeys || spec.join_keys || [];
  const tags = spec.tags || {};
  const description = spec.description || "";
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
        onMouseEnter={() => setIsHovered(true)}
        onMouseLeave={() => setIsHovered(false)}
        onClick={onNavigate}
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
        {/* Header */}
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

        {/* Description */}
        {description && (
          <EuiText size="xs" color="subdued" style={{ marginTop: 4 }}>
            <p style={{ margin: 0, lineHeight: 1.4 }}>{description}</p>
          </EuiText>
        )}

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

        <div style={{ flex: 1 }} />
        <EuiSpacer size="m" />

        {/* Tags */}
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

        {/* Footer */}
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

/* ──────────────────── tile: folder card (same width as dataset) ──────────────────── */

const FolderCard: React.FC<{
  name: string;
  type: "namespace" | "collection";
  datasetCount: number;
  collectionCount?: number;
  onClick: () => void;
}> = ({ name, type, datasetCount, collectionCount, onClick }) => {
  const [isHovered, setIsHovered] = useState(false);
  const color = type === "namespace" ? NS_COLOR : COL_COLOR;

  return (
    <EuiFlexItem grow={false} style={{ width: 360, minWidth: 300 }}>
      <EuiPanel
        hasBorder
        hasShadow={isHovered}
        paddingSize="l"
        onMouseEnter={() => setIsHovered(true)}
        onMouseLeave={() => setIsHovered(false)}
        onClick={onClick}
        style={{
          cursor: "pointer",
          transition: "all 0.2s ease",
          transform: isHovered ? "translateY(-2px)" : "none",
          borderTop: `3px solid ${color}`,
          display: "flex",
          flexDirection: "column",
        }}
      >
        <EuiFlexGroup gutterSize="m" alignItems="center" responsive={false}>
          <EuiFlexItem grow={false}>
            <EuiIcon
              type={type === "namespace" ? "folderClosed" : "folderOpen"}
              size="xxl"
              color={color}
            />
          </EuiFlexItem>
          <EuiFlexItem>
            <EuiTitle size="xs">
              <h3 style={{ margin: 0, wordBreak: "break-word" }}>{name}</h3>
            </EuiTitle>
          </EuiFlexItem>
        </EuiFlexGroup>

        <EuiSpacer size="m" />

        <EuiFlexGroup gutterSize="l" responsive={false} wrap>
          {type === "namespace" &&
            collectionCount !== undefined &&
            collectionCount > 0 && (
              <EuiFlexItem grow={false}>
                <EuiText size="xs" color="subdued">
                  Collections
                </EuiText>
                <EuiText size="s">
                  <strong>{collectionCount}</strong>
                </EuiText>
              </EuiFlexItem>
            )}
          <EuiFlexItem grow={false}>
            <EuiText size="xs" color="subdued">
              Datasets
            </EuiText>
            <EuiText size="s">
              <strong>{datasetCount}</strong>
            </EuiText>
          </EuiFlexItem>
        </EuiFlexGroup>
      </EuiPanel>
    </EuiFlexItem>
  );
};

/* ──────────────────── tree styles ──────────────────── */

const TREE_CSS = `
  .catalogTree .euiTreeView__node {
    margin-bottom: 1px;
  }
  .catalogTree .euiTreeView__node--expanded > .euiTreeView__nodeInner {
    background: rgba(0, 119, 204, 0.05);
    border-radius: 4px;
  }
  .catalogTree .euiTreeView__nodeInner {
    padding: 4px 6px;
    border-radius: 4px;
    transition: background 0.15s ease;
  }
  .catalogTree .euiTreeView__nodeInner:hover {
    background: rgba(0, 119, 204, 0.08);
  }
  .catalogTree .euiTreeView__nodeInner--withArrow .euiTreeView__nodeInner__arrow {
    margin-right: 2px;
  }
  .catalogTree .euiTreeView__nodeLabel {
    font-size: 13px;
  }
`;

/* ──────────────────── main component ──────────────────── */

const DatasetCatalogBrowser: React.FC<DatasetCatalogBrowserProps> = ({
  datasets,
  onDelete,
}) => {
  const { projectName } = useParams();
  const navigate = useNavigate();
  const [browsePath, setBrowsePath] = useState<BrowsePath>({});
  const [showTree, setShowTree] = useState(true);

  const hierarchy = useMemo(() => buildHierarchy(datasets), [datasets]);

  const namespaceList = useMemo(() => {
    const entries: Array<{
      name: string;
      collections: string[];
      totalDatasets: number;
    }> = [];
    for (const [ns, cols] of Object.entries(hierarchy)) {
      if (ns === "") continue;
      const colNames = Object.keys(cols)
        .filter((c) => c !== "")
        .sort();
      const total = Object.values(cols).reduce((s, a) => s + a.length, 0);
      entries.push({ name: ns, collections: colNames, totalDatasets: total });
    }
    entries.sort((a, b) => a.name.localeCompare(b.name));
    return entries;
  }, [hierarchy]);

  const rootDatasets = useMemo(() => {
    const cols = hierarchy[""];
    if (!cols) return [];
    const all: any[] = [];
    for (const arr of Object.values(cols)) all.push(...arr);
    return all;
  }, [hierarchy]);

  const goToDataset = useCallback(
    (dataset: any) => {
      const p = dataset.project || dataset.spec?.project || projectName;
      const n = dataset.spec?.name || dataset.name;
      navigate(`/p/${p}/data-set/${n}`);
    },
    [navigate, projectName],
  );

  /* ── tree sidebar (EuiTreeView) ── */

  // Clickable label for parent nodes — onClick navigates, stopPropagation prevents toggle
  const navLabel = useCallback(
    (
      text: string,
      onClick: () => void,
      isActive: boolean,
      activeColor?: string,
    ) => (
      <span
        onClick={(e) => {
          e.stopPropagation();
          onClick();
        }}
        style={{
          cursor: "pointer",
          fontWeight: isActive ? 600 : 400,
          color: isActive ? activeColor || NS_COLOR : undefined,
        }}
      >
        {text}
      </span>
    ),
    [],
  );

  const treeItems: EuiTreeNode[] = useMemo(() => {
    const isRootSelected = browsePath.namespace === undefined;
    const items: EuiTreeNode[] = [
      {
        id: "_root",
        label: navLabel(
          "All Datasets",
          () => setBrowsePath({}),
          isRootSelected,
        ),
        icon: <EuiIcon type="layers" size="s" />,
        isExpanded: true,
      },
    ];

    for (const ns of namespaceList) {
      const nsData = hierarchy[ns.name] || {};
      const nsChildren: EuiTreeNode[] = [];
      const nsSelected =
        browsePath.namespace === ns.name && !browsePath.collection;

      // Collection sub-folders
      for (const col of ns.collections) {
        const colDatasets = nsData[col] || [];
        const colSelected =
          browsePath.namespace === ns.name && browsePath.collection === col;

        const dsLeaves: EuiTreeNode[] = colDatasets.map((ds: any) => ({
          id: `ds:${ns.name}/${col}/${ds.spec?.name || ds.name}`,
          label: ds.spec?.name || ds.name || "unknown",
          icon: <EuiIcon type="document" size="s" />,
          callback: () => {
            goToDataset(ds);
            return `ds:${ns.name}/${col}/${ds.spec?.name}`;
          },
        }));

        nsChildren.push({
          id: `col:${ns.name}/${col}`,
          label: navLabel(
            `${col} (${colDatasets.length})`,
            () => setBrowsePath({ namespace: ns.name, collection: col }),
            colSelected,
            COL_COLOR,
          ),
          icon: <EuiIcon type="folderClosed" size="s" color={COL_COLOR} />,
          iconWhenExpanded: (
            <EuiIcon type="folderOpen" size="s" color={COL_COLOR} />
          ),
          children: dsLeaves.length > 0 ? dsLeaves : undefined,
        });
      }

      // Direct datasets under namespace (no collection)
      const directDatasets = nsData[""] || [];
      for (const ds of directDatasets) {
        nsChildren.push({
          id: `ds:${ns.name}/_/${ds.spec?.name || ds.name}`,
          label: ds.spec?.name || ds.name || "unknown",
          icon: <EuiIcon type="document" size="s" />,
          callback: () => {
            goToDataset(ds);
            return `ds:${ns.name}/_/${ds.spec?.name}`;
          },
        });
      }

      items.push({
        id: `ns:${ns.name}`,
        label: navLabel(
          `${ns.name} (${ns.totalDatasets})`,
          () => setBrowsePath({ namespace: ns.name }),
          nsSelected,
          NS_COLOR,
        ),
        icon: <EuiIcon type="folderClosed" size="s" color={NS_COLOR} />,
        iconWhenExpanded: (
          <EuiIcon type="folderOpen" size="s" color={NS_COLOR} />
        ),
        children: nsChildren.length > 0 ? nsChildren : undefined,
      });
    }

    // Ungrouped datasets
    if (rootDatasets.length > 0) {
      const ungroupedLeaves: EuiTreeNode[] = rootDatasets.map((ds: any) => ({
        id: `ds:_ungrouped/${ds.spec?.name || ds.name}`,
        label: ds.spec?.name || ds.name || "unknown",
        icon: <EuiIcon type="document" size="s" />,
        callback: () => {
          goToDataset(ds);
          return `ds:_ungrouped/${ds.spec?.name}`;
        },
      }));

      items.push({
        id: "_ungrouped",
        label: navLabel(
          `Ungrouped (${rootDatasets.length})`,
          () => setBrowsePath({}),
          false,
          "#98A2B3",
        ),
        icon: <EuiIcon type="folderOpen" size="s" color="#98A2B3" />,
        children: ungroupedLeaves,
      });
    }

    return items;
  }, [
    namespaceList,
    rootDatasets,
    hierarchy,
    browsePath,
    goToDataset,
    navLabel,
  ]);

  /* ── breadcrumbs ── */
  const breadcrumbs = useMemo(() => {
    const crumbs: Array<{ text: string; onClick?: () => void }> = [
      {
        text: "All Datasets",
        onClick:
          browsePath.namespace !== undefined
            ? () => setBrowsePath({})
            : undefined,
      },
    ];
    if (browsePath.namespace) {
      crumbs.push({
        text: browsePath.namespace,
        onClick: browsePath.collection
          ? () => setBrowsePath({ namespace: browsePath.namespace })
          : undefined,
      });
      if (browsePath.collection) {
        crumbs.push({ text: browsePath.collection });
      }
    }
    return crumbs;
  }, [browsePath]);

  /* ── content ── */
  const renderContent = () => {
    // ─── Root level: namespace folders + datasets mixed in one grid ───
    if (browsePath.namespace === undefined) {
      if (namespaceList.length === 0 && rootDatasets.length === 0) {
        return (
          <EuiEmptyPrompt
            iconType="folderClosed"
            title={<h3>No datasets yet</h3>}
            body={
              <p>
                Add datasets and organize them into namespaces and collections.
              </p>
            }
          />
        );
      }

      return (
        <EuiFlexGroup wrap responsive gutterSize="l">
          {namespaceList.map((ns) => (
            <FolderCard
              key={ns.name}
              name={ns.name}
              type="namespace"
              datasetCount={ns.totalDatasets}
              collectionCount={ns.collections.length}
              onClick={() => setBrowsePath({ namespace: ns.name })}
            />
          ))}
          {rootDatasets.map((d: any) => (
            <DatasetCard
              key={d.spec?.name}
              dataset={d}
              onNavigate={() => goToDataset(d)}
              onDelete={onDelete}
            />
          ))}
        </EuiFlexGroup>
      );
    }

    // ─── Namespace level: collection folders + direct datasets mixed ───
    if (browsePath.namespace && !browsePath.collection) {
      const ns = browsePath.namespace;
      const cols = hierarchy[ns] || {};
      const colNames = Object.keys(cols)
        .filter((c) => c !== "")
        .sort();
      const directDs = cols[""] || [];

      return (
        <EuiFlexGroup wrap responsive gutterSize="l">
          {colNames.map((col) => (
            <FolderCard
              key={col}
              name={col}
              type="collection"
              datasetCount={(cols[col] || []).length}
              onClick={() => setBrowsePath({ namespace: ns, collection: col })}
            />
          ))}
          {directDs.map((d: any) => (
            <DatasetCard
              key={d.spec?.name}
              dataset={d}
              onNavigate={() => goToDataset(d)}
              onDelete={onDelete}
            />
          ))}
          {colNames.length === 0 && directDs.length === 0 && (
            <EuiFlexItem>
              <EuiText size="s" color="subdued" textAlign="center">
                This namespace is empty.
              </EuiText>
            </EuiFlexItem>
          )}
        </EuiFlexGroup>
      );
    }

    // ─── Collection level: datasets only ───
    if (browsePath.namespace && browsePath.collection) {
      const dsList =
        hierarchy[browsePath.namespace]?.[browsePath.collection] || [];

      return (
        <EuiFlexGroup wrap responsive gutterSize="l">
          {dsList.map((d: any) => (
            <DatasetCard
              key={d.spec?.name}
              dataset={d}
              onNavigate={() => goToDataset(d)}
              onDelete={onDelete}
            />
          ))}
          {dsList.length === 0 && (
            <EuiFlexItem>
              <EuiText size="s" color="subdued" textAlign="center">
                No datasets in this collection.
              </EuiText>
            </EuiFlexItem>
          )}
        </EuiFlexGroup>
      );
    }

    return null;
  };

  /* ──────────────────── render ──────────────────── */

  const hasTree = namespaceList.length > 0;

  return (
    <div style={{ display: "flex", gap: 0 }}>
      {/* Tree sidebar */}
      {hasTree && (
        <div
          style={{
            width: showTree ? 230 : 40,
            minWidth: showTree ? 200 : 40,
            flexShrink: 0,
            borderRight: "1px solid #D3DAE6",
            paddingRight: showTree ? 12 : 0,
            marginRight: 16,
          }}
        >
          {showTree ? (
            <>
              <div
                style={{
                  display: "flex",
                  alignItems: "center",
                  justifyContent: "space-between",
                }}
              >
                <EuiText size="xs" color="subdued">
                  <strong
                    style={{ textTransform: "uppercase", letterSpacing: 0.5 }}
                  >
                    Catalog
                  </strong>
                </EuiText>
                <EuiToolTip content="Hide catalog tree">
                  <EuiButtonIcon
                    iconType="menuLeft"
                    aria-label="Hide catalog tree"
                    size="s"
                    color="text"
                    onClick={() => setShowTree(false)}
                  />
                </EuiToolTip>
              </div>
              <EuiSpacer size="xs" />
              <div className="catalogTree">
                <style>{TREE_CSS}</style>
                <EuiTreeView
                  items={treeItems}
                  display="default"
                  showExpansionArrows
                  aria-label="Dataset catalog tree"
                />
              </div>
            </>
          ) : (
            <EuiToolTip content="Show catalog tree" position="right">
              <EuiButtonIcon
                iconType="menuRight"
                aria-label="Show catalog tree"
                size="s"
                color="text"
                onClick={() => setShowTree(true)}
              />
            </EuiToolTip>
          )}
        </div>
      )}

      {/* Content */}
      <div style={{ flex: 1, minWidth: 0 }}>
        <EuiBreadcrumbs breadcrumbs={breadcrumbs} truncate={false} max={5} />
        <EuiSpacer size="s" />
        {renderContent()}
      </div>
    </div>
  );
};

export default DatasetCatalogBrowser;
