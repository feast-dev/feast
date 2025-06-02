import React, { useState } from "react";
import {
  EuiPageSection,
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
} from "@elastic/eui";
import { useTheme } from "../../contexts/ThemeContext";

interface DocumentContent {
  content: string;
  file_path: string;
}

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
  groundTruthLabel: string;
}

const RagTab = () => {
  const { colorMode } = useTheme();
  const [filePath, setFilePath] = useState("./src/test-document.txt");
  const [selectedText, setSelectedText] = useState<TextSelection | null>(null);
  const [labelingMode, setLabelingMode] = useState("relevant");
  const [labels, setLabels] = useState<DocumentLabel[]>([]);
  const [isLoading, setIsLoading] = useState(false);
  const [documentContent, setDocumentContent] =
    useState<DocumentContent | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [prompt, setPrompt] = useState("");
  const [query, setQuery] = useState("");
  const [groundTruthLabel, setGroundTruthLabel] = useState("");
  const [isSaving, setIsSaving] = useState(false);
  const [hasUnsavedChanges, setHasUnsavedChanges] = useState(false);

  const loadDocument = async () => {
    if (!filePath) return;

    setIsLoading(true);
    setError(null);

    try {
      if (filePath === "./src/test-document.txt") {
        const testContent = `This is a sample document for testing the data labeling functionality in Feast UI.

The document contains multiple paragraphs and sections that can be used to test the text highlighting and labeling features.

This paragraph discusses machine learning and artificial intelligence concepts. It covers topics like neural networks, deep learning, and natural language processing. Users should be able to select and label relevant portions of this text for RAG retrieval systems.

Another section focuses on data engineering and ETL pipelines. This content explains how to process large datasets and build scalable data infrastructure. The labeling system should allow users to mark this as relevant or irrelevant for their specific use cases.

The final paragraph contains information about feature stores and real-time machine learning systems. This text can be used to test the highlighting functionality and ensure that labels are properly stored and displayed in the user interface.`;

        setDocumentContent({
          content: testContent,
          file_path: filePath,
        });

        loadSavedLabels();
      } else {
        throw new Error(
          "Document not found. Please use the test document path: ./src/test-document.txt",
        );
      }
    } catch (err) {
      setError(
        err instanceof Error
          ? err.message
          : "An error occurred while loading the document",
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

      const textContent = documentContent.content;

      let startIndex = -1;
      let endIndex = -1;

      const rangeText = range.toString();
      if (rangeText) {
        startIndex = textContent.indexOf(rangeText);
        if (startIndex !== -1) {
          endIndex = startIndex + rangeText.length;
        }
      }

      if (startIndex !== -1 && endIndex !== -1) {
        setSelectedText({
          text: selectedTextContent,
          start: startIndex,
          end: endIndex,
        });
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
        groundTruthLabel: groundTruthLabel,
      };

      setLabels([...labels, newLabel]);
      setSelectedText(null);
      setHasUnsavedChanges(true);

      const selection = window.getSelection();
      if (selection) {
        selection.removeAllRanges();
      }
    }
  };

  const handleRemoveLabel = (index: number) => {
    setLabels(labels.filter((_: DocumentLabel, i: number) => i !== index));
    setHasUnsavedChanges(true);
  };

  const saveLabels = () => {
    setIsSaving(true);

    setTimeout(() => {
      try {
        const saveData = {
          filePath: filePath,
          prompt: prompt,
          query: query,
          groundTruthLabel: groundTruthLabel,
          labels: labels,
          timestamp: new Date().toISOString(),
        };

        const pathParts = filePath.split("/");
        const filename = pathParts[pathParts.length - 1];
        const nameWithoutExt = filename.replace(/\.[^/.]+$/, "");
        const downloadFilename = `${nameWithoutExt}-labels.json`;

        const jsonString = JSON.stringify(saveData, null, 2);
        const blob = new Blob([jsonString], { type: "application/json" });
        const url = URL.createObjectURL(blob);

        const link = document.createElement("a");
        link.href = url;
        link.download = downloadFilename;
        link.style.display = "none";

        document.body.appendChild(link);
        link.click();
        document.body.removeChild(link);
        URL.revokeObjectURL(url);

        setHasUnsavedChanges(false);
        alert(
          `Successfully saved ${labels.length} labels. File downloaded as ${downloadFilename}`,
        );
      } catch (error) {
        console.error("Error saving labels:", error);
        alert("Error saving labels. Please try again.");
      } finally {
        setIsSaving(false);
      }
    }, 100);
  };

  const loadSavedLabels = () => {
    try {
      const savedData = JSON.parse(localStorage.getItem("ragLabels") || "[]");
      const fileData = savedData.find(
        (item: any) => item.filePath === filePath,
      );

      if (fileData) {
        setPrompt(fileData.prompt || "");
        setQuery(fileData.query || "");
        setGroundTruthLabel(fileData.groundTruthLabel || "");
        setLabels(fileData.labels || []);
        setHasUnsavedChanges(false);
      }
    } catch (error) {
      console.error("Error loading saved labels:", error);
    }
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
        groundTruthLabel: "",
      });
    }

    if (allHighlights.length === 0) {
      return [content];
    }

    const sortedHighlights = [...allHighlights].sort(
      (a, b) => a.start - b.start,
    );
    const result: (string | React.ReactElement)[] = [];
    let lastIndex = 0;

    sortedHighlights.forEach((highlight, index) => {
      result.push(content.slice(lastIndex, highlight.start));

      let highlightColor, borderColor;

      if (highlight.label === "temp-selection") {
        if (colorMode === "dark") {
          highlightColor = "#1a4d66";
          borderColor = "#2d6b8a";
        } else {
          highlightColor = "#add8e6";
          borderColor = "#87ceeb";
        }
      } else if (highlight.label === "irrelevant") {
        if (colorMode === "dark") {
          highlightColor = "#4d1a1a";
          borderColor = "#6b2d2d";
        } else {
          highlightColor = "#f8d7da";
          borderColor = "#f5c6cb";
        }
      } else {
        if (colorMode === "dark") {
          highlightColor = "#1a4d1a";
          borderColor = "#2d6b2d";
        } else {
          highlightColor = "#d4edda";
          borderColor = "#c3e6cb";
        }
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
              : `Chunk: ${highlight.label}${highlight.groundTruthLabel ? `, Ground Truth: ${highlight.groundTruthLabel}` : ""}`
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

  const labelingOptions = [
    {
      id: "relevant",
      label: "Relevant",
    },
    {
      id: "irrelevant",
      label: "Irrelevant",
    },
  ];

  return (
    <EuiPageSection>
      <EuiCallOut
        title="Label document chunks for RAG"
        color="primary"
        iconType="iInCircle"
      >
        <p>
          Load a document and highlight text chunks to label them for chunk
          extraction/retrieval. Add prompt and query context, then provide
          ground truth labels for generation evaluation.
        </p>
      </EuiCallOut>

      <EuiSpacer size="l" />

      <EuiFlexGroup>
        <EuiFlexItem>
          <EuiFormRow label="Document file path">
            <EuiFieldText
              placeholder="./src/your-document.txt"
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

      <EuiSpacer size="l" />

      {isLoading && (
        <EuiFlexGroup alignItems="center" gutterSize="s">
          <EuiFlexItem grow={false}>
            <EuiLoadingSpinner size="m" />
          </EuiFlexItem>
          <EuiFlexItem>
            <EuiText>Loading document...</EuiText>
          </EuiFlexItem>
        </EuiFlexGroup>
      )}

      {error && (
        <EuiCallOut
          title="Error loading document"
          color="danger"
          iconType="alert"
        >
          <p>{error}</p>
        </EuiCallOut>
      )}

      {documentContent && (
        <>
          <EuiPanel paddingSize="l">
            <EuiTitle size="s">
              <h3>RAG Context</h3>
            </EuiTitle>
            <EuiSpacer size="m" />
            <EuiFlexGroup>
              <EuiFlexItem>
                <EuiFormRow
                  label="Prompt"
                  helpText="System context for the RAG system"
                >
                  <EuiTextArea
                    placeholder="Enter system prompt..."
                    value={prompt}
                    onChange={(e) => {
                      setPrompt(e.target.value);
                      setHasUnsavedChanges(true);
                    }}
                    rows={3}
                  />
                </EuiFormRow>
              </EuiFlexItem>
              <EuiFlexItem>
                <EuiFormRow
                  label="Query"
                  helpText="User query for retrieval testing"
                >
                  <EuiTextArea
                    placeholder="Enter user query..."
                    value={query}
                    onChange={(e) => {
                      setQuery(e.target.value);
                      setHasUnsavedChanges(true);
                    }}
                    rows={3}
                  />
                </EuiFormRow>
              </EuiFlexItem>
            </EuiFlexGroup>
          </EuiPanel>

          <EuiSpacer size="l" />

          <EuiTitle size="m">
            <h2>Step 1: Label for Chunk Extraction</h2>
          </EuiTitle>
          <EuiSpacer size="m" />

          <EuiFlexGroup>
            <EuiFlexItem>
              <EuiFormRow label="Chunk extraction label">
                <EuiButtonGroup
                  legend="Choose chunk extraction label"
                  options={labelingOptions}
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

          <EuiSpacer size="l" />

          {selectedText && (
            <EuiCallOut
              title="Text selected for labeling"
              color="primary"
              size="s"
            >
              <EuiCode>{selectedText.text}</EuiCode>
            </EuiCallOut>
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
                {renderDocumentWithHighlights(documentContent.content)}
              </div>
            </EuiText>
          </EuiPanel>

          <EuiSpacer size="l" />

          <EuiTitle size="m">
            <h2>Step 2: Label for Generation</h2>
          </EuiTitle>
          <EuiSpacer size="m" />

          <EuiFormRow
            label="Ground Truth Label"
            helpText="Text for generation evaluation"
          >
            <EuiTextArea
              placeholder="Enter ground truth label for generation evaluation..."
              value={groundTruthLabel}
              onChange={(e) => {
                setGroundTruthLabel(e.target.value);
                setHasUnsavedChanges(true);
              }}
              rows={3}
            />
          </EuiFormRow>

          <EuiSpacer size="m" />

          <EuiFlexGroup justifyContent="flexEnd">
            <EuiFlexItem grow={false}>
              <EuiButton
                fill
                color="success"
                onClick={saveLabels}
                disabled={
                  labels.length === 0 && !groundTruthLabel && !prompt && !query
                }
                isLoading={isSaving}
                iconType="save"
              >
                Save Labels
              </EuiButton>
            </EuiFlexItem>
          </EuiFlexGroup>

          <EuiSpacer size="m" />

          {(labels.length > 0 || groundTruthLabel || prompt || query) && (
            <>
              <EuiCallOut
                title="Ready to save"
                color="success"
                iconType="check"
                size="s"
              >
                <p>
                  Click "Save Labels" to download your labeled data as a JSON
                  file.
                </p>
              </EuiCallOut>
              <EuiSpacer size="m" />
            </>
          )}

          <EuiSpacer size="m" />

          {hasUnsavedChanges && (
            <>
              <EuiCallOut
                title="Unsaved changes"
                color="warning"
                iconType="alert"
                size="s"
              >
                <p>
                  You have unsaved changes. Click "Save Labels" to persist your
                  work.
                </p>
              </EuiCallOut>
              <EuiSpacer size="m" />
            </>
          )}

          {labels.length > 0 && (
            <>
              <EuiSpacer size="l" />
              <EuiPanel paddingSize="l">
                <EuiTitle size="s">
                  <h3>Extracted Chunk Labels ({labels.length})</h3>
                </EuiTitle>
                <EuiSpacer size="m" />
                {labels.map((label, index) => (
                  <EuiFlexGroup key={index} alignItems="center" gutterSize="s">
                    <EuiFlexItem grow={false}>
                      <EuiCode
                        color={
                          label.label === "relevant" ? "success" : "danger"
                        }
                      >
                        Chunk: {label.label}
                      </EuiCode>
                    </EuiFlexItem>
                    {label.groundTruthLabel && (
                      <EuiFlexItem grow={false}>
                        <EuiCode color="primary">
                          GT: {label.groundTruthLabel}
                        </EuiCode>
                      </EuiFlexItem>
                    )}
                    <EuiFlexItem>
                      <EuiText size="s">
                        "{label.text.substring(0, 80)}
                        {label.text.length > 80 ? "..." : ""}"
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
    </EuiPageSection>
  );
};

export default RagTab;
