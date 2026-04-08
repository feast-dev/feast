import React, { useContext, useState, useMemo } from "react";
import {
  EuiBasicTable,
  EuiText,
  EuiPanel,
  EuiTitle,
  EuiHorizontalRule,
  EuiCodeBlock,
  EuiSpacer,
  EuiFlexGroup,
  EuiFlexItem,
  EuiBadge,
} from "@elastic/eui";
import RegistryPathContext from "../../contexts/RegistryPathContext";
import useLoadRegistry from "../../queries/useLoadRegistry";
import { feast } from "../../protos";
import { toDate } from "../../utils/timestamp";
import { useParams } from "react-router-dom";

interface FeatureViewVersionsTabProps {
  featureViewName: string;
}

interface DecodedVersion {
  record: feast.core.IFeatureViewVersionRecord;
  features: feast.core.IFeatureSpecV2[];
  entities: string[];
  description: string;
  udfBody: string | null;
}

const decodeVersionProto = (
  record: feast.core.IFeatureViewVersionRecord,
): DecodedVersion => {
  const result: DecodedVersion = {
    record,
    features: [],
    entities: [],
    description: "",
    udfBody: null,
  };

  if (!record.featureViewProto || record.featureViewProto.length === 0) {
    return result;
  }

  try {
    const bytes =
      record.featureViewProto instanceof Uint8Array
        ? record.featureViewProto
        : new Uint8Array(record.featureViewProto);

    if (record.featureViewType === "on_demand_feature_view") {
      const odfv = feast.core.OnDemandFeatureView.decode(bytes);
      result.features = odfv.spec?.features || [];
      result.description = odfv.spec?.description || "";
      result.udfBody =
        odfv.spec?.featureTransformation?.userDefinedFunction?.bodyText ||
        odfv.spec?.userDefinedFunction?.bodyText ||
        null;
    } else if (record.featureViewType === "stream_feature_view") {
      const sfv = feast.core.StreamFeatureView.decode(bytes);
      result.features = sfv.spec?.features || [];
      result.entities = sfv.spec?.entities || [];
      result.description = sfv.spec?.description || "";
    } else {
      const fv = feast.core.FeatureView.decode(bytes);
      result.features = fv.spec?.features || [];
      result.entities = fv.spec?.entities || [];
      result.description = fv.spec?.description || "";
    }
  } catch (e) {
    console.error("Failed to decode version proto:", e);
  }

  return result;
};

const VersionDetail = ({ decoded }: { decoded: DecodedVersion }) => {
  return (
    <EuiFlexGroup direction="column" gutterSize="m">
      {decoded.description && (
        <EuiFlexItem>
          <EuiText size="s" color="subdued">
            {decoded.description}
          </EuiText>
        </EuiFlexItem>
      )}
      {decoded.udfBody && (
        <EuiFlexItem>
          <EuiPanel hasBorder paddingSize="s">
            <EuiTitle size="xxs">
              <h4>Transformation</h4>
            </EuiTitle>
            <EuiHorizontalRule margin="xs" />
            <EuiCodeBlock language="py" fontSize="s" paddingSize="s">
              {decoded.udfBody}
            </EuiCodeBlock>
          </EuiPanel>
        </EuiFlexItem>
      )}
      <EuiFlexGroup>
        <EuiFlexItem>
          <EuiPanel hasBorder paddingSize="s">
            <EuiTitle size="xxs">
              <h4>Features ({decoded.features.length})</h4>
            </EuiTitle>
            <EuiHorizontalRule margin="xs" />
            {decoded.features.length > 0 ? (
              <EuiBasicTable
                items={decoded.features}
                columns={[
                  { field: "name", name: "Name" },
                  {
                    field: "valueType",
                    name: "Value Type",
                    render: (vt: feast.types.ValueType.Enum) =>
                      feast.types.ValueType.Enum[vt],
                  },
                ]}
              />
            ) : (
              <EuiText size="s">No features in this version.</EuiText>
            )}
          </EuiPanel>
        </EuiFlexItem>
        {decoded.entities.length > 0 && (
          <EuiFlexItem grow={false}>
            <EuiPanel hasBorder paddingSize="s">
              <EuiTitle size="xxs">
                <h4>Entities</h4>
              </EuiTitle>
              <EuiHorizontalRule margin="xs" />
              <EuiFlexGroup wrap responsive={false} gutterSize="xs">
                {decoded.entities.map((entity) => (
                  <EuiFlexItem grow={false} key={entity}>
                    <EuiBadge color="primary">{entity}</EuiBadge>
                  </EuiFlexItem>
                ))}
              </EuiFlexGroup>
            </EuiPanel>
          </EuiFlexItem>
        )}
      </EuiFlexGroup>
    </EuiFlexGroup>
  );
};

const FeatureViewVersionsTab = ({
  featureViewName,
}: FeatureViewVersionsTabProps) => {
  const registryUrl = useContext(RegistryPathContext);
  const { projectName } = useParams();
  const registryQuery = useLoadRegistry(registryUrl, projectName);
  const [expandedRows, setExpandedRows] = useState<Record<number, boolean>>({});

  const records =
    registryQuery.data?.objects?.featureViewVersionHistory?.records?.filter(
      (r: feast.core.IFeatureViewVersionRecord) =>
        r.featureViewName === featureViewName,
    ) || [];

  const decodedVersions = useMemo(
    () => records.map(decodeVersionProto),
    [records],
  );

  if (records.length === 0) {
    return <EuiText>No version history for this feature view.</EuiText>;
  }

  const toggleRow = (versionNumber: number) => {
    setExpandedRows((prev) => ({
      ...prev,
      [versionNumber]: !prev[versionNumber],
    }));
  };

  const columns = [
    {
      field: "record.versionNumber",
      name: "Version",
      render: (_: unknown, item: DecodedVersion) =>
        `v${item.record.versionNumber}`,
      sortable: true,
      width: "80px",
    },
    {
      name: "Features",
      render: (item: DecodedVersion) => `${item.features.length}`,
      width: "80px",
    },
    {
      field: "record.featureViewType",
      name: "Type",
      render: (_: unknown, item: DecodedVersion) =>
        item.record.featureViewType || "—",
    },
    {
      field: "record.createdTimestamp",
      name: "Created",
      render: (_: unknown, item: DecodedVersion) =>
        item.record.createdTimestamp
          ? toDate(item.record.createdTimestamp).toLocaleString()
          : "—",
    },
    {
      field: "record.versionId",
      name: "Version ID",
      render: (_: unknown, item: DecodedVersion) =>
        item.record.versionId || "—",
    },
  ];

  const itemIdToExpandedRowMap: Record<string, React.ReactNode> = {};
  decodedVersions.forEach((decoded) => {
    const vn = decoded.record.versionNumber!;
    if (expandedRows[vn]) {
      itemIdToExpandedRowMap[String(vn)] = <VersionDetail decoded={decoded} />;
    }
  });

  return (
    <EuiBasicTable
      items={decodedVersions}
      itemId={(item: DecodedVersion) => String(item.record.versionNumber)}
      itemIdToExpandedRowMap={itemIdToExpandedRowMap}
      columns={[
        {
          isExpander: true,
          width: "40px",
          render: (item: DecodedVersion) => {
            const vn = item.record.versionNumber!;
            return (
              <button
                onClick={() => toggleRow(vn)}
                aria-label={expandedRows[vn] ? "Collapse" : "Expand"}
                style={{
                  background: "none",
                  border: "none",
                  cursor: "pointer",
                  fontSize: "16px",
                }}
              >
                {expandedRows[vn] ? "▾" : "▸"}
              </button>
            );
          },
        },
        ...columns,
      ]}
    />
  );
};

export default FeatureViewVersionsTab;
