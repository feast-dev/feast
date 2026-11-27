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
import {
  getDataSourceTypeMeta,
  getConnectionTarget,
  truncateMiddle,
  getRelativeTime,
  CATEGORY_META,
  getTagBadge,
} from "./dataSourceTypeUtils";

function pythonSnippet(name: string): string {
  return `store.get_data_source("${name}")`;
}

interface DataSourceCardProps {
  dataSource: any;
  isHovered: boolean;
  onHover: (name: string | null) => void;
}

const DataSourceCard: React.FC<DataSourceCardProps> = ({
  dataSource,
  isHovered,
  onHover,
}) => {
  const { projectName } = useParams();
  const navigate = useNavigate();
  const ds = dataSource.spec || dataSource;
  const name = ds.name || dataSource.name || "unknown";
  const dsProject = dataSource.project || ds.project || projectName;
  const description = ds.description || "";
  const owner = ds.owner || "";
  const tags: Record<string, string> = ds.tags || {};
  const timestampField = ds.timestampField || ds.timestamp_field || "";
  const meta = getDataSourceTypeMeta(dataSource);
  const categoryInfo = CATEGORY_META[meta.category];
  const connection = getConnectionTarget(dataSource);

  const createdTs =
    dataSource.meta?.createdTimestamp ||
    dataSource.meta?.created_timestamp ||
    ds.meta?.createdTimestamp;
  const formattedDate = createdTs
    ? new Date(createdTs).toLocaleDateString("en-US", {
        month: "short",
        day: "numeric",
        year: "numeric",
      })
    : null;
  const relativeTime = createdTs ? getRelativeTime(new Date(createdTs)) : null;

  const Icon = meta.icon;

  return (
    <EuiFlexItem grow={false} style={{ width: 360, minWidth: 300 }}>
      <EuiPanel
        hasBorder
        hasShadow={isHovered}
        paddingSize="l"
        onMouseEnter={() => onHover(name)}
        onMouseLeave={() => onHover(null)}
        onClick={() => navigate(`/p/${dsProject}/data-source/${name}`)}
        style={{
          height: "100%",
          display: "flex",
          flexDirection: "column",
          transition: "all 0.2s ease",
          transform: isHovered ? "translateY(-2px)" : "none",
          borderTop: `3px solid ${meta.color}`,
          cursor: "pointer",
        }}
      >
        {/* Header: icon + name + type */}
        <EuiFlexGroup alignItems="center" gutterSize="m" responsive={false}>
          <EuiFlexItem grow={false}>
            <div
              style={{
                width: 44,
                height: 44,
                display: "flex",
                alignItems: "center",
                justifyContent: "center",
                borderRadius: 8,
                backgroundColor: `${meta.color}14`,
                flexShrink: 0,
              }}
            >
              <Icon width={28} height={28} />
            </div>
          </EuiFlexItem>
          <EuiFlexItem grow style={{ minWidth: 0 }}>
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
            <EuiText size="xs" color="subdued">
              {meta.label}
            </EuiText>
          </EuiFlexItem>
          <EuiFlexItem grow={false}>
            <EuiBadge color={categoryInfo.color as any}>
              {categoryInfo.label}
            </EuiBadge>
          </EuiFlexItem>
        </EuiFlexGroup>

        {/* Description */}
        {description && (
          <>
            <EuiSpacer size="s" />
            <EuiText size="xs" color="subdued">
              <p
                style={{
                  margin: 0,
                  lineHeight: 1.4,
                  display: "-webkit-box",
                  WebkitLineClamp: 2,
                  WebkitBoxOrient: "vertical",
                  overflow: "hidden",
                }}
              >
                {description}
              </p>
            </EuiText>
          </>
        )}

        <EuiSpacer size="m" />

        {/* Connection target */}
        {connection && (
          <EuiToolTip content={connection.value} position="bottom">
            <EuiText size="xs" color="subdued">
              <EuiIcon type="link" size="s" /> {connection.label}:{" "}
              <code style={{ fontSize: 11 }}>
                {truncateMiddle(connection.value)}
              </code>
            </EuiText>
          </EuiToolTip>
        )}

        {/* Metrics row */}
        <EuiSpacer size="m" />
        <EuiFlexGroup gutterSize="l" responsive={false} wrap>
          {timestampField && (
            <EuiFlexItem grow={false}>
              <EuiText size="xs" color="subdued">
                Event timestamp
              </EuiText>
              <EuiText size="s">
                <strong>{timestampField}</strong>
              </EuiText>
            </EuiFlexItem>
          )}
          {owner && (
            <EuiFlexItem grow={false}>
              <EuiText size="xs" color="subdued">
                Owner
              </EuiText>
              <EuiText size="s">
                <strong>{owner}</strong>
              </EuiText>
            </EuiFlexItem>
          )}
        </EuiFlexGroup>

        {/* Push footer down */}
        <div style={{ flex: 1 }} />
        <EuiSpacer size="m" />

        {/* Tags */}
        {Object.keys(tags).length > 0 && (
          <>
            <EuiFlexGroup gutterSize="xs" wrap responsive={false}>
              {Object.entries(tags)
                .slice(0, 3)
                .map(([key, value]) => {
                  const badge = getTagBadge(key, value as string);
                  return (
                    <EuiFlexItem grow={false} key={key}>
                      <EuiBadge color={badge.color as any}>
                        {badge.label}
                      </EuiBadge>
                    </EuiFlexItem>
                  );
                })}
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

        {/* Footer: timestamp + copy */}
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
            <EuiCopy textToCopy={pythonSnippet(name)}>
              {(copy) => (
                <EuiToolTip content="Copy Python accessor">
                  <EuiButtonIcon
                    iconType="copyClipboard"
                    aria-label="Copy Python accessor"
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
        </EuiFlexGroup>
      </EuiPanel>
    </EuiFlexItem>
  );
};

interface DataSourcesCardGridProps {
  dataSources: any[];
}

const DataSourcesCardGrid = ({ dataSources }: DataSourcesCardGridProps) => {
  const [hoveredId, setHoveredId] = useState<string | null>(null);

  if (dataSources.length === 0) {
    return (
      <EuiText textAlign="center" color="subdued">
        <p>No data sources match your search.</p>
      </EuiText>
    );
  }

  return (
    <EuiFlexGroup wrap responsive gutterSize="l">
      {dataSources.map((dataSource: any) => {
        const name =
          dataSource.spec?.name || dataSource.name || Math.random().toString();
        return (
          <DataSourceCard
            key={name}
            dataSource={dataSource}
            isHovered={hoveredId === name}
            onHover={setHoveredId}
          />
        );
      })}
    </EuiFlexGroup>
  );
};

export default DataSourcesCardGrid;
