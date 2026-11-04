import React, { useState, useContext, useMemo } from "react";
import { useParams } from "react-router-dom";
import {
  EuiCallOut,
  EuiSpacer,
  EuiFlexGroup,
  EuiFlexItem,
  EuiFormRow,
  EuiFieldText,
  EuiButton,
  EuiPanel,
  EuiTitle,
  EuiText,
  EuiLoadingSpinner,
  EuiButtonGroup,
  EuiCode,
  EuiTextArea,
  EuiModal,
  EuiModalHeader,
  EuiModalHeaderTitle,
  EuiModalBody,
  EuiModalFooter,
  EuiOverlayMask,
  EuiForm,
  EuiIcon,
  EuiBasicTable,
  EuiBasicTableColumn,
  EuiBadge,
  EuiDescriptionList,
  EuiDescriptionListTitle,
  EuiDescriptionListDescription,
} from "@elastic/eui";
import { useTheme } from "../../contexts/ThemeContext";
import RegistryPathContext from "../../contexts/RegistryPathContext";
import type { AnnotationConfig } from "./useAnnotationConfig";

interface TextSelection {
  text: string;
  start: number;
  end: number;
}

interface DocumentLabel {
  text: string;
  start: number;
  end: number;
  label: string;
  timestamp: number;
}

interface RagLabelingMethodProps {
  annotationConfig: AnnotationConfig;
}

const RagLabelingMethod = ({ annotationConfig }: RagLabelingMethodProps) => {
  const { labelViewName } = useParams();
  const { colorMode } = useTheme();
  const registryUrl = useContext(RegistryPathContext);

  const fieldRoles = annotationConfig.field_roles;
  const labelValues = annotationConfig.label_values;
  const labelWidgets = annotationConfig.label_widgets;

  const contentRefField = useMemo(
    () =>
      Object.entries(fieldRoles).find(
        ([, role]) => role === "content_ref",
      )?.[0] || null,
    [fieldRoles],
  );
  const contentField = useMemo(
    () =>
      Object.entries(fieldRoles).find(([, role]) => role === "content")?.[0] ||
      null,
    [fieldRoles],
  );
  const spanStartField = useMemo(
    () =>
      Object.entries(fieldRoles).find(
        ([, role]) => role === "span_start",
      )?.[0] || null,
    [fieldRoles],
  );
  const spanEndField = useMemo(
    () =>
      Object.entries(fieldRoles).find(([, role]) => role === "span_end")?.[0] ||
      null,
    [fieldRoles],
  );

  const labelFieldEntries = useMemo(
    () => Object.entries(fieldRoles).filter(([, role]) => role === "label"),
    [fieldRoles],
  );
  const primaryLabelField = labelFieldEntries[0]?.[0] || null;
  const secondaryLabelFields = labelFieldEntries.slice(1).map(([name]) => name);

  const primaryLabelOptions = useMemo(() => {
    if (!primaryLabelField)
      return [
        { id: "relevant", label: "Relevant" },
        { id: "irrelevant", label: "Irrelevant" },
      ];
    const values = labelValues[primaryLabelField];
    if (values && values.length > 0) {
      return values.map((v) => ({
        id: v,
        label: v.charAt(0).toUpperCase() + v.slice(1),
      }));
    }
    return [
      { id: "relevant", label: "Relevant" },
      { id: "irrelevant", label: "Irrelevant" },
    ];
  }, [primaryLabelField, labelValues]);

  const [filePath, setFilePath] = useState("");
  const [selectedText, setSelectedText] = useState<TextSelection | null>(null);
  const [labelingMode, setLabelingMode] = useState(
    primaryLabelOptions[0]?.id || "relevant",
  );
  const [labels, setLabels] = useState<DocumentLabel[]>([]);
  const [isLoading, setIsLoading] = useState(false);
  const [documentContent, setDocumentContent] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [groundTruthLabel, setGroundTruthLabel] = useState("");
  const [isSaving, setIsSaving] = useState(false);
  const [pushSuccess, setPushSuccess] = useState<string | null>(null);
  const [isModalOpen, setIsModalOpen] = useState(false);
  const [extraFieldValues, setExtraFieldValues] = useState<
    Record<string, string>
  >({});

  const loadDocument = async () => {
    if (!filePath) return;
    setIsLoading(true);
    setError(null);

    try {
      const baseUrl = registryUrl?.replace(/\/$/, "") || "/api/v1";
      const response = await fetch(
        `${baseUrl}/document-content?path=${encodeURIComponent(filePath)}`,
      );
      if (response.ok) {
        const result = await response.json();
        setDocumentContent(result.content);
      } else {
        setDocumentContent(
          `This is a sample document for testing RAG labeling in Feast UI.

The document contains multiple paragraphs that can be used to test text highlighting and labeling.

This paragraph discusses machine learning and artificial intelligence concepts. It covers topics like neural networks, deep learning, and natural language processing. Users should be able to select and label relevant portions of this text for RAG retrieval systems.

Another section focuses on data engineering and ETL pipelines. This content explains how to process large datasets and build scalable data infrastructure.

The final paragraph contains information about feature stores and real-time machine learning systems.`,
        );
      }
    } catch {
      setDocumentContent(
        `This is a sample document for testing RAG labeling in Feast UI.

The document contains multiple paragraphs that can be used to test text highlighting and labeling.

This paragraph discusses machine learning and artificial intelligence concepts. It covers topics like neural networks, deep learning, and natural language processing.

Another section focuses on data engineering and ETL pipelines. This content explains how to process large datasets and build scalable data infrastructure.

The final paragraph contains information about feature stores and real-time machine learning systems.`,
      );
    } finally {
      setIsLoading(false);
    }
  };

  const handleTextSelection = () => {
    const selection = window.getSelection();
    if (selection && selection.toString().trim() && documentContent) {
      const selectedTextContent = selection.toString().trim();
      const range = selection.getRangeAt(0);
      const rangeText = range.toString();
      if (rangeText) {
        const startIndex = documentContent.indexOf(rangeText);
        if (startIndex !== -1) {
          setSelectedText({
            text: selectedTextContent,
            start: startIndex,
            end: startIndex + rangeText.length,
          });
        }
      }
    }
  };

  const handleLabelSelection = () => {
    if (selectedText) {
      const newLabel: DocumentLabel = {
        text: selectedText.text,
        start: selectedText.start,
        end: selectedText.end,
        label: labelingMode,
        timestamp: Date.now(),
      };
      setLabels([...labels, newLabel]);
      setSelectedText(null);
      const selection = window.getSelection();
      if (selection) selection.removeAllRanges();
    }
  };

  const handleRemoveLabel = (index: number) => {
    setLabels(labels.filter((_: DocumentLabel, i: number) => i !== index));
  };

  const generateChunkId = (docName: string, start: number, end: number) => {
    const raw = `${docName}:${start}:${end}`;
    let hash = 0;
    for (let i = 0; i < raw.length; i++) {
      const char = raw.charCodeAt(i);
      hash = (hash << 5) - hash + char;
      hash |= 0;
    }
    return `chunk_${Math.abs(hash).toString(36)}`;
  };

  const openSubmitModal = () => {
    const defaults: Record<string, string> = {};
    if (annotationConfig.labeler_field) {
      defaults[annotationConfig.labeler_field] = "rag_labeling_ui";
    }
    setExtraFieldValues(defaults);
    setIsModalOpen(true);
  };

  const buildPushRows = () => {
    const docName = filePath || "document";
    const entityField = annotationConfig.entities[0] || "entity_id";
    const labelerField = annotationConfig.labeler_field;

    return labels.map((label) => {
      const row: Record<string, any> = {};

      row[entityField] = generateChunkId(docName, label.start, label.end);

      if (annotationConfig.entities.length > 1) {
        for (let i = 1; i < annotationConfig.entities.length; i++) {
          const ent = annotationConfig.entities[i];
          if (extraFieldValues[ent]) {
            row[ent] = extraFieldValues[ent];
          }
        }
      }

      if (contentRefField) row[contentRefField] = filePath;
      if (contentField) row[contentField] = label.text;
      if (spanStartField) row[spanStartField] = label.start;
      if (spanEndField) row[spanEndField] = label.end;
      if (primaryLabelField) row[primaryLabelField] = label.label;

      for (const secField of secondaryLabelFields) {
        const widget = labelWidgets[secField];
        if (widget === "text" && groundTruthLabel) {
          row[secField] = groundTruthLabel;
        } else if (extraFieldValues[secField]) {
          row[secField] = extraFieldValues[secField];
        }
      }

      if (labelerField) {
        row[labelerField] = extraFieldValues[labelerField] || "rag_labeling_ui";
      }

      row["event_timestamp"] = new Date().toISOString();
      return row;
    });
  };

  const saveToLabelView = async () => {
    if (labels.length === 0) return;
    setIsSaving(true);
    setError(null);
    setPushSuccess(null);

    try {
      const baseUrl = registryUrl?.replace(/\/$/, "") || "/api/v1";
      const pushSourceName =
        annotationConfig.push_source_name || `${labelViewName}_push_source`;
      const rows = buildPushRows();

      const columnar: Record<string, any[]> = {};
      if (rows.length > 0) {
        for (const key of Object.keys(rows[0])) {
          columnar[key] = rows.map((r) => r[key]);
        }
      }

      const response = await fetch(`${baseUrl}/push`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          push_source_name: pushSourceName,
          df: columnar,
          to: "online_and_offline",
        }),
      });

      if (response.ok) {
        setPushSuccess(
          `Pushed ${labels.length} span labels to ${labelViewName}`,
        );
        setLabels([]);
        setIsModalOpen(false);
      } else {
        const errData = await response.json().catch(() => null);
        setError(
          typeof errData?.detail === "string"
            ? errData.detail
            : `Push failed (${response.status})`,
        );
      }
    } catch (e: any) {
      setError(e.message || "Network error");
    } finally {
      setIsSaving(false);
    }
  };

  const exportJSON = () => {
    const rows = buildPushRows();
    const saveData = {
      labelView: labelViewName,
      profile: annotationConfig.profile,
      filePath,
      groundTruthLabel,
      rows,
      timestamp: new Date().toISOString(),
    };
    const blob = new Blob([JSON.stringify(saveData, null, 2)], {
      type: "application/json",
    });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = `${labelViewName}_rag_labels.json`;
    a.click();
    URL.revokeObjectURL(url);
  };

  const renderDocumentWithHighlights = (
    content: string,
  ): (string | React.ReactElement)[] => {
    const allHighlights = [...labels];
    if (selectedText) {
      allHighlights.push({
        text: selectedText.text,
        start: selectedText.start,
        end: selectedText.end,
        label: "temp-selection",
        timestamp: 0,
      });
    }
    if (allHighlights.length === 0) return [content];

    const sortedHighlights = [...allHighlights].sort(
      (a, b) => a.start - b.start,
    );
    const result: (string | React.ReactElement)[] = [];
    let lastIndex = 0;

    const positiveValues = new Set(
      primaryLabelOptions.length > 0
        ? [primaryLabelOptions[0].id]
        : ["relevant"],
    );

    sortedHighlights.forEach((highlight, index) => {
      result.push(content.slice(lastIndex, highlight.start));
      let highlightColor: string;
      let borderColor: string;

      if (highlight.label === "temp-selection") {
        highlightColor = colorMode === "dark" ? "#1a4d66" : "#add8e6";
        borderColor = colorMode === "dark" ? "#2d6b8a" : "#87ceeb";
      } else if (!positiveValues.has(highlight.label)) {
        highlightColor = colorMode === "dark" ? "#4d1a1a" : "#f8d7da";
        borderColor = colorMode === "dark" ? "#6b2d2d" : "#f5c6cb";
      } else {
        highlightColor = colorMode === "dark" ? "#1a4d1a" : "#d4edda";
        borderColor = colorMode === "dark" ? "#2d6b2d" : "#c3e6cb";
      }

      result.push(
        <span
          key={`highlight-${index}`}
          style={{
            backgroundColor: highlightColor,
            padding: "2px 4px",
            borderRadius: "3px",
            border: `1px solid ${borderColor}`,
          }}
          title={
            highlight.label === "temp-selection"
              ? "Selected text"
              : `${primaryLabelField || "label"}: ${highlight.label}`
          }
        >
          {highlight.text}
        </span>,
      );
      lastIndex = highlight.end;
    });

    result.push(content.slice(lastIndex));
    return result;
  };

  const chunkColumns: EuiBasicTableColumn<DocumentLabel>[] = [
    {
      field: "label",
      name: primaryLabelField || "Label",
      width: "120px",
      render: (value: string) => {
        const isPositive =
          primaryLabelOptions.length > 0 && value === primaryLabelOptions[0].id;
        return (
          <EuiBadge color={isPositive ? "success" : "danger"}>{value}</EuiBadge>
        );
      },
    },
    {
      field: "text",
      name: "Span Text",
      truncateText: true,
      render: (value: string) =>
        value.substring(0, 100) + (value.length > 100 ? "..." : ""),
    },
    {
      name: "Offsets",
      width: "100px",
      render: (item: DocumentLabel) => (
        <EuiCode transparentBackground>
          {item.start}–{item.end}
        </EuiCode>
      ),
    },
  ];

  const unmappedFeatures = annotationConfig.features.filter((f) => {
    if (f === annotationConfig.labeler_field) return false;
    if (fieldRoles[f]) return false;
    return true;
  });

  return (
    <React.Fragment>
      <EuiCallOut
        title="Label document spans"
        color="primary"
        iconType="iInCircle"
        size="s"
      >
        <p>
          Load a document, highlight text spans, and label them. Span positions
          and labels are auto-mapped to your schema fields and pushed to{" "}
          <strong>{labelViewName}</strong> via its PushSource.
        </p>
      </EuiCallOut>

      <EuiSpacer size="s" />

      <EuiPanel paddingSize="s" color="subdued">
        <EuiTitle size="xxs">
          <h4>Field Mapping</h4>
        </EuiTitle>
        <EuiSpacer size="xs" />
        <EuiDescriptionList compressed type="column" style={{ maxWidth: 500 }}>
          {contentRefField && (
            <>
              <EuiDescriptionListTitle>Document path</EuiDescriptionListTitle>
              <EuiDescriptionListDescription>
                <EuiCode transparentBackground>{contentRefField}</EuiCode>
              </EuiDescriptionListDescription>
            </>
          )}
          {contentField && (
            <>
              <EuiDescriptionListTitle>Span text</EuiDescriptionListTitle>
              <EuiDescriptionListDescription>
                <EuiCode transparentBackground>{contentField}</EuiCode>
              </EuiDescriptionListDescription>
            </>
          )}
          {spanStartField && spanEndField && (
            <>
              <EuiDescriptionListTitle>Offsets</EuiDescriptionListTitle>
              <EuiDescriptionListDescription>
                <EuiCode transparentBackground>{spanStartField}</EuiCode>,{" "}
                <EuiCode transparentBackground>{spanEndField}</EuiCode>
              </EuiDescriptionListDescription>
            </>
          )}
          {primaryLabelField && (
            <>
              <EuiDescriptionListTitle>Primary label</EuiDescriptionListTitle>
              <EuiDescriptionListDescription>
                <EuiCode transparentBackground>{primaryLabelField}</EuiCode>
                {" → "}
                {primaryLabelOptions.map((o) => o.id).join(", ")}
              </EuiDescriptionListDescription>
            </>
          )}
          {secondaryLabelFields.map((f) => (
            <React.Fragment key={f}>
              <EuiDescriptionListTitle>{f}</EuiDescriptionListTitle>
              <EuiDescriptionListDescription>
                <EuiBadge color="hollow">{labelWidgets[f] || "text"}</EuiBadge>
              </EuiDescriptionListDescription>
            </React.Fragment>
          ))}
        </EuiDescriptionList>
      </EuiPanel>

      <EuiSpacer size="l" />

      <EuiFlexGroup>
        <EuiFlexItem>
          <EuiFormRow label="Document file path">
            <EuiFieldText
              placeholder="./path/to/document.txt"
              value={filePath}
              onChange={(e) => setFilePath(e.target.value)}
            />
          </EuiFormRow>
        </EuiFlexItem>
        <EuiFlexItem grow={false}>
          <EuiFormRow hasEmptyLabelSpace>
            <EuiButton
              fill
              onClick={loadDocument}
              disabled={!filePath}
              isLoading={isLoading}
            >
              Load Document
            </EuiButton>
          </EuiFormRow>
        </EuiFlexItem>
      </EuiFlexGroup>

      {isLoading && (
        <>
          <EuiSpacer size="m" />
          <EuiFlexGroup alignItems="center" gutterSize="s">
            <EuiFlexItem grow={false}>
              <EuiLoadingSpinner size="m" />
            </EuiFlexItem>
            <EuiFlexItem>
              <EuiText>Loading document...</EuiText>
            </EuiFlexItem>
          </EuiFlexGroup>
        </>
      )}

      {error && (
        <>
          <EuiSpacer size="m" />
          <EuiCallOut title="Error" color="danger" iconType="alert">
            <p>{error}</p>
          </EuiCallOut>
        </>
      )}

      {pushSuccess && (
        <>
          <EuiSpacer size="m" />
          <EuiCallOut title={pushSuccess} color="success" iconType="check" />
        </>
      )}

      {documentContent && (
        <>
          <EuiSpacer size="l" />

          <EuiFlexGroup alignItems="center">
            <EuiFlexItem>
              <EuiFormRow
                label={
                  primaryLabelField
                    ? `Span label (${primaryLabelField})`
                    : "Span label"
                }
              >
                <EuiButtonGroup
                  legend="Choose span label"
                  options={primaryLabelOptions}
                  idSelected={labelingMode}
                  onChange={(id) => setLabelingMode(id)}
                  buttonSize="s"
                />
              </EuiFormRow>
            </EuiFlexItem>
            <EuiFlexItem grow={false}>
              <EuiFormRow hasEmptyLabelSpace>
                <EuiButton
                  fill
                  onClick={handleLabelSelection}
                  disabled={!selectedText}
                >
                  Label Selected Text
                </EuiButton>
              </EuiFormRow>
            </EuiFlexItem>
          </EuiFlexGroup>

          {selectedText && (
            <>
              <EuiSpacer size="s" />
              <EuiCallOut
                title="Text selected"
                color="primary"
                size="s"
                iconType="quote"
              >
                <EuiCode>{selectedText.text.substring(0, 120)}</EuiCode>
              </EuiCallOut>
            </>
          )}

          <EuiSpacer size="m" />

          <EuiPanel paddingSize="l">
            <EuiTitle size="s">
              <h3>Document Content</h3>
            </EuiTitle>
            <EuiSpacer size="m" />
            <EuiText>
              <div
                style={{
                  whiteSpace: "pre-wrap",
                  lineHeight: "1.6",
                  userSelect: "text",
                  cursor: "text",
                }}
                onMouseUp={handleTextSelection}
              >
                {renderDocumentWithHighlights(documentContent)}
              </div>
            </EuiText>
          </EuiPanel>

          {secondaryLabelFields.length > 0 && (
            <>
              <EuiSpacer size="l" />
              {secondaryLabelFields.map((f) => {
                const widget = labelWidgets[f] || "text";
                if (widget === "text") {
                  return (
                    <EuiFormRow
                      key={f}
                      label={f}
                      helpText={`Secondary label field (${widget})`}
                    >
                      <EuiTextArea
                        placeholder={`Enter ${f}...`}
                        value={
                          f === secondaryLabelFields[0]
                            ? groundTruthLabel
                            : extraFieldValues[f] || ""
                        }
                        onChange={(e) => {
                          if (f === secondaryLabelFields[0]) {
                            setGroundTruthLabel(e.target.value);
                          } else {
                            setExtraFieldValues((prev) => ({
                              ...prev,
                              [f]: e.target.value,
                            }));
                          }
                        }}
                        rows={3}
                      />
                    </EuiFormRow>
                  );
                }
                return null;
              })}
            </>
          )}

          <EuiSpacer size="m" />

          <EuiFlexGroup justifyContent="flexEnd" gutterSize="s">
            <EuiFlexItem grow={false}>
              <EuiButton
                onClick={exportJSON}
                iconType="exportAction"
                disabled={labels.length === 0}
              >
                Export JSON
              </EuiButton>
            </EuiFlexItem>
            <EuiFlexItem grow={false}>
              <EuiButton
                fill
                color="success"
                onClick={openSubmitModal}
                iconType="push"
                disabled={labels.length === 0}
              >
                Save to LabelView ({labels.length})
              </EuiButton>
            </EuiFlexItem>
          </EuiFlexGroup>

          {labels.length > 0 && (
            <>
              <EuiSpacer size="l" />
              <EuiPanel paddingSize="l">
                <EuiTitle size="s">
                  <h3>Labeled Spans ({labels.length})</h3>
                </EuiTitle>
                <EuiSpacer size="m" />
                {labels.map((label, index) => (
                  <EuiFlexGroup
                    key={index}
                    alignItems="center"
                    gutterSize="s"
                    style={{ marginBottom: 4 }}
                  >
                    <EuiFlexItem grow={false}>
                      <EuiBadge
                        color={
                          label.label === primaryLabelOptions[0]?.id
                            ? "success"
                            : "danger"
                        }
                      >
                        {label.label}
                      </EuiBadge>
                    </EuiFlexItem>
                    <EuiFlexItem grow={false}>
                      <EuiCode transparentBackground>
                        {label.start}–{label.end}
                      </EuiCode>
                    </EuiFlexItem>
                    <EuiFlexItem>
                      <EuiText size="s">
                        &quot;{label.text.substring(0, 80)}
                        {label.text.length > 80 ? "..." : ""}&quot;
                      </EuiText>
                    </EuiFlexItem>
                    <EuiFlexItem grow={false}>
                      <EuiButton
                        size="s"
                        color="danger"
                        onClick={() => handleRemoveLabel(index)}
                      >
                        Remove
                      </EuiButton>
                    </EuiFlexItem>
                  </EuiFlexGroup>
                ))}
              </EuiPanel>
            </>
          )}
        </>
      )}

      {isModalOpen && (
        <EuiOverlayMask>
          <EuiModal onClose={() => setIsModalOpen(false)} maxWidth={600}>
            <EuiModalHeader>
              <EuiModalHeaderTitle>
                <EuiIcon type="push" /> Push {labels.length} Span
                {labels.length !== 1 ? "s" : ""} to {labelViewName}
              </EuiModalHeaderTitle>
            </EuiModalHeader>
            <EuiModalBody>
              <EuiText size="s" color="subdued">
                <p>
                  Each span will be pushed as a row. Entity key (
                  <strong>{annotationConfig.entities[0] || "entity_id"}</strong>
                  ) is auto-generated from document path + span offsets. Fields
                  are mapped from the annotation profile.
                </p>
              </EuiText>
              <EuiSpacer size="m" />

              <EuiPanel color="subdued" paddingSize="s">
                <EuiTitle size="xxs">
                  <h4>Spans to push:</h4>
                </EuiTitle>
                <EuiSpacer size="xs" />
                <EuiBasicTable
                  items={labels}
                  columns={chunkColumns}
                  tableLayout="auto"
                  compressed
                />
              </EuiPanel>

              <EuiSpacer size="l" />
              <EuiTitle size="xs">
                <h4>Additional Fields</h4>
              </EuiTitle>
              <EuiSpacer size="s" />
              <EuiForm>
                <EuiFormRow
                  label={annotationConfig.labeler_field || "labeler"}
                  helpText="Your identity as the labeler"
                >
                  <EuiFieldText
                    placeholder="Enter labeler ID"
                    value={
                      extraFieldValues[annotationConfig.labeler_field] || ""
                    }
                    onChange={(e) =>
                      setExtraFieldValues((prev) => ({
                        ...prev,
                        [annotationConfig.labeler_field]: e.target.value,
                      }))
                    }
                  />
                </EuiFormRow>

                {annotationConfig.entities.length > 1 &&
                  annotationConfig.entities.slice(1).map((ent) => (
                    <EuiFormRow key={ent} label={ent}>
                      <EuiFieldText
                        placeholder={`Enter ${ent}`}
                        value={extraFieldValues[ent] || ""}
                        onChange={(e) =>
                          setExtraFieldValues((prev) => ({
                            ...prev,
                            [ent]: e.target.value,
                          }))
                        }
                      />
                    </EuiFormRow>
                  ))}

                {unmappedFeatures.map((f) => (
                  <EuiFormRow key={f} label={f}>
                    <EuiFieldText
                      placeholder={`Enter ${f}`}
                      value={extraFieldValues[f] || ""}
                      onChange={(e) =>
                        setExtraFieldValues((prev) => ({
                          ...prev,
                          [f]: e.target.value,
                        }))
                      }
                    />
                  </EuiFormRow>
                ))}
              </EuiForm>

              {error && (
                <>
                  <EuiSpacer size="m" />
                  <EuiCallOut
                    title="Push Error"
                    color="danger"
                    iconType="alert"
                    size="s"
                  >
                    {error}
                  </EuiCallOut>
                </>
              )}
            </EuiModalBody>
            <EuiModalFooter>
              <EuiButton
                color="text"
                onClick={() => {
                  setIsModalOpen(false);
                  setError(null);
                }}
              >
                Cancel
              </EuiButton>
              <EuiButton
                fill
                color="success"
                onClick={saveToLabelView}
                isLoading={isSaving}
                iconType="check"
              >
                Push Labels
              </EuiButton>
            </EuiModalFooter>
          </EuiModal>
        </EuiOverlayMask>
      )}
    </React.Fragment>
  );
};

export default RagLabelingMethod;
