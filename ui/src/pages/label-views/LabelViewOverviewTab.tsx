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
  EuiCode,
} from "@elastic/eui";

import useLoadLabelView from "./useLoadLabelView";
import useAnnotationConfig from "./useAnnotationConfig";

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

const PROFILE_COLORS: Record<string, string> = {
  "document-span": "primary",
  table: "default",
  "entity-form": "accent",
  "active-learning": "success",
};

interface FieldRoleRow {
  field: string;
  role: string;
  values?: string;
  widget?: string;
}

const LabelViewOverviewTab = () => {
  const { labelViewName, projectName } = useParams();
  const name = labelViewName || "";
  const { isLoading, isSuccess, isError, data } = useLoadLabelView(name);
  const { data: annotationConfig } = useAnnotationConfig(name);

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
  const labelerField = spec.labelerField || "labeler";
  const entities: string[] = spec.entityColumns?.length
    ? spec.entityColumns.map((ec: { name: string }) => ec.name)
    : spec.entities || [];
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
          (training data, Browse, Quality). The offline store always retains
          full write history. The online store uses last-write-wins for serving.
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
          {annotationConfig && (
            <>
              <EuiSpacer size="m" />
              <EuiPanel hasBorder>
                <EuiTitle size="xs">
                  <h3>Labeling Method</h3>
                </EuiTitle>
                <EuiHorizontalRule margin="xs" />
                <EuiDescriptionList>
                  <EuiDescriptionListTitle>Profile</EuiDescriptionListTitle>
                  <EuiDescriptionListDescription>
                    <EuiBadge
                      color={
                        PROFILE_COLORS[annotationConfig.profile] || "default"
                      }
                    >
                      {annotationConfig.profile}
                    </EuiBadge>
                  </EuiDescriptionListDescription>

                  <EuiDescriptionListTitle>Push Source</EuiDescriptionListTitle>
                  <EuiDescriptionListDescription>
                    <EuiCode transparentBackground>
                      {annotationConfig.push_source_name || "N/A"}
                    </EuiCode>
                  </EuiDescriptionListDescription>
                </EuiDescriptionList>

                {Object.keys(annotationConfig.field_roles).length > 0 && (
                  <>
                    <EuiSpacer size="s" />
                    <EuiText size="xs" color="subdued">
                      <strong>Field Roles</strong>
                    </EuiText>
                    <EuiSpacer size="xs" />
                    <EuiBasicTable<FieldRoleRow>
                      items={Object.entries(annotationConfig.field_roles).map(
                        ([field, role]) => ({
                          field,
                          role,
                          values:
                            annotationConfig.label_values[field]?.join(", "),
                          widget: annotationConfig.label_widgets[field],
                        }),
                      )}
                      columns={[
                        { field: "field", name: "Field", width: "30%" },
                        {
                          field: "role",
                          name: "Role",
                          width: "25%",
                          render: (role: string) => (
                            <EuiBadge
                              color={
                                role === "label"
                                  ? "primary"
                                  : role === "content_ref"
                                    ? "success"
                                    : role === "content"
                                      ? "success"
                                      : "hollow"
                              }
                            >
                              {role}
                            </EuiBadge>
                          ),
                        },
                        {
                          field: "values",
                          name: "Values",
                          render: (v: string) => v || "\u2014",
                        },
                        {
                          field: "widget",
                          name: "Widget",
                          render: (w: string) => w || "\u2014",
                        },
                      ]}
                      tableLayout="auto"
                      compressed
                    />
                  </>
                )}
              </EuiPanel>
            </>
          )}
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
