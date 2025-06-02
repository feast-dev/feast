import React, { useState } from "react";
import {
  EuiPage,
  EuiPageBody,
  EuiPageSection,
  EuiPageHeader,
  EuiTitle,
  EuiSpacer,
  EuiFlexGroup,
  EuiFlexItem,
  EuiButton,
  EuiFieldText,
  EuiFormRow,
  EuiPanel,
  EuiText,
  EuiCallOut,
  EuiLoadingSpinner,
  EuiButtonGroup,
  EuiCode,
} from "@elastic/eui";

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
}

const DocumentLabelingPage = () => {
  const [filePath, setFilePath] = useState("./src/test-document.txt");
  const [selectedText, setSelectedText] = useState<TextSelection | null>(null);
  const [labelingMode, setLabelingMode] = useState("relevant");
  const [labels, setLabels] = useState<DocumentLabel[]>([]);
  const [isLoading, setIsLoading] = useState(false);
  const [documentContent, setDocumentContent] =
    useState<DocumentContent | null>(null);
  const [error, setError] = useState<string | null>(null);

  const loadDocument = async () => {
    if (!filePath) return;

    setIsLoading(true);
    setError(null);

    try {
      if (filePath === "./src/test-document.txt") {
        const testContent = `This is a sample document for testing the document labeling functionality in Feast UI.

The document contains multiple paragraphs and sections that can be used to test the text highlighting and labeling features.

This paragraph discusses machine learning and artificial intelligence concepts. It covers topics like neural networks, deep learning, and natural language processing. Users should be able to select and label relevant portions of this text for RAG retrieval systems.

Another section focuses on data engineering and ETL pipelines. This content explains how to process large datasets and build scalable data infrastructure. The labeling system should allow users to mark this as relevant or irrelevant for their specific use cases.

The final paragraph contains information about feature stores and real-time machine learning systems. This text can be used to test the highlighting functionality and ensure that labels are properly stored and displayed in the user interface.`;

        setDocumentContent({
          content: testContent,
          file_path: filePath,
        });
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
      };

      setLabels([...labels, newLabel]);
      setSelectedText(null);

      const selection = window.getSelection();
      if (selection) {
        selection.removeAllRanges();
      }
    }
  };

  const handleRemoveLabel = (index: number) => {
    setLabels(labels.filter((_: DocumentLabel, i: number) => i !== index));
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

      let highlightColor = "#d4edda";
      let borderColor = "#c3e6cb";

      if (highlight.label === "temp-selection") {
        highlightColor = "#add8e6";
        borderColor = "#87ceeb";
      } else if (highlight.label === "irrelevant") {
        highlightColor = "#f8d7da";
        borderColor = "#f5c6cb";
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
              : `Label: ${highlight.label}`
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
    <EuiPage>
      <EuiPageBody>
        <EuiPageHeader>
          <EuiTitle size="l">
            <h1>Data Labeling for RAG</h1>
          </EuiTitle>
        </EuiPageHeader>

        <EuiPageSection>
          <EuiPageSection>
            <EuiCallOut
              title="Label document text for RAG retrieval"
              color="primary"
              iconType="iInCircle"
            >
              <p>
                Load a document file and highlight text chunks to label them as
                relevant or irrelevant for RAG retrieval. This helps improve the
                quality of your retrieval system by providing human feedback.
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
                <EuiFlexGroup alignItems="center" gutterSize="m">
                  <EuiFlexItem grow={false}>
                    <EuiText size="s">
                      <strong>Labeling mode:</strong>
                    </EuiText>
                  </EuiFlexItem>
                  <EuiFlexItem grow={false}>
                    <EuiButtonGroup
                      legend="Choose labeling mode"
                      options={labelingOptions}
                      idSelected={labelingMode}
                      onChange={(id) => setLabelingMode(id)}
                      buttonSize="s"
                    />
                  </EuiFlexItem>
                  <EuiFlexItem grow={false}>
                    <EuiButton
                      size="s"
                      fill
                      onClick={handleLabelSelection}
                      disabled={!selectedText}
                    >
                      Label Selected Text
                    </EuiButton>
                  </EuiFlexItem>
                </EuiFlexGroup>

                <EuiSpacer size="m" />

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

                {labels.length > 0 && (
                  <>
                    <EuiSpacer size="l" />
                    <EuiPanel paddingSize="l">
                      <EuiTitle size="s">
                        <h3>Labels ({labels.length})</h3>
                      </EuiTitle>
                      <EuiSpacer size="m" />
                      {labels.map((label, index) => (
                        <EuiFlexGroup
                          key={index}
                          alignItems="center"
                          gutterSize="s"
                        >
                          <EuiFlexItem>
                            <EuiCode
                              color={
                                label.label === "relevant"
                                  ? "success"
                                  : "danger"
                              }
                            >
                              {label.label}
                            </EuiCode>
                          </EuiFlexItem>
                          <EuiFlexItem>
                            <EuiText size="s">
                              "{label.text.substring(0, 100)}
                              {label.text.length > 100 ? "..." : ""}"
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
        </EuiPageSection>
      </EuiPageBody>
    </EuiPage>
  );
};

export default DocumentLabelingPage;
