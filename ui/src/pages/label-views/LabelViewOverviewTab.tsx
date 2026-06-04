import React from "react";
import { useParams } from "react-router-dom";
import {
  EuiFlexGroup,
  EuiFlexItem,
  EuiPanel,
  EuiTitle,
  EuiHorizontalRule,
  EuiDescriptionList,
  EuiDescriptionListTitle,
  EuiDescriptionListDescription,
  EuiText,
  EuiSpacer,
  EuiBadge,
  EuiLoadingSpinner,
  EuiBasicTable,
  EuiBasicTableColumn,
  EuiCallOut,
  EuiLink,
} from "@elastic/eui";

import useLoadLabelView from "./useLoadLabelView";

const CONFLICT_POLICY_MAP: Record<string, string> = {
  "0": "LAST_WRITE_WINS",
  "1": "LABELER_PRIORITY",
  "2": "MAJORITY_VOTE",
  LAST_WRITE_WINS: "LAST_WRITE_WINS",
  LABELER_PRIORITY: "LABELER_PRIORITY",
  MAJORITY_VOTE: "MAJORITY_VOTE",
};

interface SchemaField {
  name: string;
  valueType: string;
}

const LabelViewOverviewTab = () => {
  const { labelViewName, projectName } = useParams();
  const name = labelViewName || "";
  const { isLoading, isSuccess, isError, data } = useLoadLabelView(name);

  if (isLoading) {
    return (
      <p>
        <EuiLoadingSpinner size="m" /> Loading
      </p>
    );
  }
  if (isError) {
    return <p>Error loading label view: {name}</p>;
  }
  if (!isSuccess || !data) {
    return <p>No label view found with name: {name}</p>;
  }

  const spec = data.spec || {};
  const meta = data.meta || {};
  const conflictPolicy =
    CONFLICT_POLICY_MAP[spec.conflictPolicy] ||
    spec.conflictPolicy ||
    "LAST_WRITE_WINS";
  const retainHistory = spec.retainHistory ?? false;
  const labelerField = spec.labelerField || "labeler";
  const entities: string[] = spec.entities || [];
  const features: any[] = spec.features || [];

  const schemaColumns: EuiBasicTableColumn<SchemaField>[] = [
    { field: "name", name: "Field Name", sortable: true },
    { field: "valueType", name: "Value Type" },
  ];

  const schemaRows: SchemaField[] = features.map((f: any) => ({
    name: f.name || "Unknown",
    valueType: f.valueType != null ? String(f.valueType) : "Unknown",
  }));

  return (
    <React.Fragment>
      <EuiCallOut title="Label View" color="success" iconType="check" size="s">
        <p>
          <strong>conflict_policy</strong> is enforced for offline store reads
          (training data, Browse, Quality). <strong>retain_history</strong> is
          inherent to the offline store — all writes are appended. The online
          store uses last-write-wins for serving.
        </p>
      </EuiCallOut>
      <EuiSpacer size="m" />
      <EuiFlexGroup>
        <EuiFlexItem>
          <EuiPanel hasBorder>
            <EuiTitle size="xs">
              <h3>Properties</h3>
            </EuiTitle>
            <EuiHorizontalRule margin="xs" />
            <EuiDescriptionList>
              <EuiDescriptionListTitle>Conflict Policy</EuiDescriptionListTitle>
              <EuiDescriptionListDescription>
                <EuiBadge
                  color={
                    conflictPolicy === "LAST_WRITE_WINS"
                      ? "default"
                      : conflictPolicy === "MAJORITY_VOTE"
                        ? "primary"
                        : "accent"
                  }
                >
                  {conflictPolicy}
                </EuiBadge>
              </EuiDescriptionListDescription>

              <EuiDescriptionListTitle>Labeler Field</EuiDescriptionListTitle>
              <EuiDescriptionListDescription>
                {labelerField}
              </EuiDescriptionListDescription>

              <EuiDescriptionListTitle>Retain History</EuiDescriptionListTitle>
              <EuiDescriptionListDescription>
                <EuiBadge color={retainHistory ? "success" : "default"}>
                  {retainHistory ? "Yes" : "No"}
                </EuiBadge>
              </EuiDescriptionListDescription>

              <EuiDescriptionListTitle>Entities</EuiDescriptionListTitle>
              <EuiDescriptionListDescription>
                {entities.length > 0
                  ? entities.map((ent: string, i: number) => (
                      <React.Fragment key={ent}>
                        {i > 0 && ", "}
                        <EuiLink href={`/p/${projectName}/entity/${ent}`}>
                          {ent}
                        </EuiLink>
                      </React.Fragment>
                    ))
                  : "-"}
              </EuiDescriptionListDescription>

              {spec.description && (
                <>
                  <EuiDescriptionListTitle>Description</EuiDescriptionListTitle>
                  <EuiDescriptionListDescription>
                    {spec.description}
                  </EuiDescriptionListDescription>
                </>
              )}
            </EuiDescriptionList>
          </EuiPanel>
          <EuiSpacer size="m" />
          <EuiPanel hasBorder>
            <EuiTitle size="xs">
              <h3>Metadata</h3>
            </EuiTitle>
            <EuiHorizontalRule margin="xs" />
            <EuiDescriptionList>
              <EuiDescriptionListTitle>Created</EuiDescriptionListTitle>
              <EuiDescriptionListDescription>
                {meta.createdTimestamp
                  ? new Date(
                      typeof meta.createdTimestamp === "string"
                        ? meta.createdTimestamp
                        : Number(meta.createdTimestamp.seconds) * 1000,
                    ).toLocaleDateString("en-CA")
                  : "N/A"}
              </EuiDescriptionListDescription>

              <EuiDescriptionListTitle>Last Updated</EuiDescriptionListTitle>
              <EuiDescriptionListDescription>
                {meta.lastUpdatedTimestamp
                  ? new Date(
                      typeof meta.lastUpdatedTimestamp === "string"
                        ? meta.lastUpdatedTimestamp
                        : Number(meta.lastUpdatedTimestamp.seconds) * 1000,
                    ).toLocaleDateString("en-CA")
                  : "N/A"}
              </EuiDescriptionListDescription>
            </EuiDescriptionList>
          </EuiPanel>
        </EuiFlexItem>
        <EuiFlexItem>
          <EuiPanel hasBorder>
            <EuiTitle size="xs">
              <h3>Labels</h3>
            </EuiTitle>
            <EuiHorizontalRule margin="xs" />
            {schemaRows.length > 0 ? (
              <EuiBasicTable<SchemaField>
                items={schemaRows}
                columns={[
                  {
                    field: "name",
                    name: "Label Name",
                    sortable: true,
                    render: (labelName: string) => (
                      <EuiLink
                        href={`/p/${projectName}/label-view/${name}/label/${labelName}`}
                      >
                        {labelName}
                      </EuiLink>
                    ),
                  },
                  { field: "valueType", name: "Value Type" },
                ]}
                tableLayout="auto"
              />
            ) : (
              <EuiText>No labels defined.</EuiText>
            )}
          </EuiPanel>
          <EuiSpacer size="m" />
          {spec.source && (
            <EuiPanel hasBorder>
              <EuiTitle size="xs">
                <h3>Data Source</h3>
              </EuiTitle>
              <EuiHorizontalRule margin="xs" />
              <EuiDescriptionList>
                <EuiDescriptionListTitle>Source Type</EuiDescriptionListTitle>
                <EuiDescriptionListDescription>
                  {spec.source.type || "PushSource"}
                </EuiDescriptionListDescription>
                {spec.source.name && (
                  <>
                    <EuiDescriptionListTitle>
                      Source Name
                    </EuiDescriptionListTitle>
                    <EuiDescriptionListDescription>
                      <EuiLink
                        href={`/p/${projectName}/data-source/${spec.source.name}`}
                      >
                        {spec.source.name}
                      </EuiLink>
                    </EuiDescriptionListDescription>
                  </>
                )}
              </EuiDescriptionList>
            </EuiPanel>
          )}
        </EuiFlexItem>
      </EuiFlexGroup>
    </React.Fragment>
  );
};

export default LabelViewOverviewTab;
