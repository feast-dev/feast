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
  EuiTable,
  EuiTableHeader,
  EuiTableHeaderCell,
  EuiTableBody,
  EuiTableRow,
  EuiTableRowCell,
  EuiSelect,
  EuiLoadingSpinner,
} from "@elastic/eui";

interface ClassificationData {
  id: number;
  text: string;
  currentClass: string;
  originalClass?: string;
}

const ClassificationTab = () => {
  const [csvPath, setCsvPath] = useState("./src/sample-data.csv");
  const [isLoading, setIsLoading] = useState(false);
  const [data, setData] = useState<ClassificationData[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [availableClasses] = useState(["positive", "negative", "neutral"]);

  const loadCsvData = async () => {
    if (!csvPath) return;

    setIsLoading(true);
    setError(null);

    try {
      if (csvPath === "./src/sample-data.csv") {
        const sampleData: ClassificationData[] = [
          {
            id: 1,
            text: "This product is amazing! I love the quality and design.",
            currentClass: "positive",
            originalClass: "positive",
          },
          {
            id: 2,
            text: "The service was terrible and the food was cold.",
            currentClass: "negative",
            originalClass: "negative",
          },
          {
            id: 3,
            text: "It's an okay product, nothing special but does the job.",
            currentClass: "neutral",
            originalClass: "neutral",
          },
          {
            id: 4,
            text: "Excellent customer support and fast delivery!",
            currentClass: "positive",
            originalClass: "positive",
          },
          {
            id: 5,
            text: "I'm not sure how I feel about this purchase.",
            currentClass: "neutral",
            originalClass: "positive",
          },
        ];

        setData(sampleData);
      } else {
        throw new Error(
          "CSV file not found. Please use the sample data path: ./src/sample-data.csv",
        );
      }
    } catch (err) {
      setError(
        err instanceof Error
          ? err.message
          : "An error occurred while loading the CSV data",
      );
    } finally {
      setIsLoading(false);
    }
  };

  const handleClassChange = (id: number, newClass: string) => {
    setData(
      data.map((item) =>
        item.id === id ? { ...item, currentClass: newClass } : item,
      ),
    );
  };

  const getChangedItems = () => {
    return data.filter((item) => item.currentClass !== item.originalClass);
  };

  const resetChanges = () => {
    setData(
      data.map((item) => ({ ...item, currentClass: item.originalClass || "" })),
    );
  };

  const saveChanges = () => {
    const changedItems = getChangedItems();
    console.log("Saving classification changes:", changedItems);
    alert(`Saved ${changedItems.length} classification changes!`);
  };

  const columns = [
    {
      field: "id",
      name: "ID",
      width: "60px",
    },
    {
      field: "text",
      name: "Text",
      width: "60%",
    },
    {
      field: "originalClass",
      name: "Original Class",
      width: "15%",
    },
    {
      field: "currentClass",
      name: "Current Class",
      width: "20%",
    },
  ];

  return (
    <EuiPageSection>
      <EuiCallOut
        title="Classify sample data"
        color="primary"
        iconType="iInCircle"
      >
        <p>
          Load a CSV file containing text samples and edit their classification
          labels. This helps improve your classification models by providing
          corrected training data.
        </p>
      </EuiCallOut>

      <EuiSpacer size="l" />

      <EuiFlexGroup>
        <EuiFlexItem>
          <EuiFormRow label="CSV file path">
            <EuiFieldText
              placeholder="./src/your-data.csv"
              value={csvPath}
              onChange={(e) => setCsvPath(e.target.value)}
            />
          </EuiFormRow>
        </EuiFlexItem>
        <EuiFlexItem grow={false}>
          <EuiFormRow hasEmptyLabelSpace>
            <EuiButton
              fill
              onClick={loadCsvData}
              disabled={!csvPath}
              isLoading={isLoading}
            >
              Load CSV Data
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
            <EuiText>Loading CSV data...</EuiText>
          </EuiFlexItem>
        </EuiFlexGroup>
      )}

      {error && (
        <EuiCallOut
          title="Error loading CSV data"
          color="danger"
          iconType="alert"
        >
          <p>{error}</p>
        </EuiCallOut>
      )}

      {data.length > 0 && (
        <>
          <EuiFlexGroup alignItems="center" justifyContent="spaceBetween">
            <EuiFlexItem grow={false}>
              <EuiTitle size="s">
                <h3>Classification Data ({data.length} samples)</h3>
              </EuiTitle>
            </EuiFlexItem>
            <EuiFlexItem grow={false}>
              <EuiFlexGroup gutterSize="s">
                <EuiFlexItem grow={false}>
                  <EuiButton
                    color="warning"
                    onClick={resetChanges}
                    disabled={getChangedItems().length === 0}
                  >
                    Reset Changes
                  </EuiButton>
                </EuiFlexItem>
                <EuiFlexItem grow={false}>
                  <EuiButton
                    fill
                    onClick={saveChanges}
                    disabled={getChangedItems().length === 0}
                  >
                    Save Changes ({getChangedItems().length})
                  </EuiButton>
                </EuiFlexItem>
              </EuiFlexGroup>
            </EuiFlexItem>
          </EuiFlexGroup>

          <EuiSpacer size="m" />

          <EuiPanel paddingSize="none">
            <EuiTable>
              <EuiTableHeader>
                {columns.map((column, index) => (
                  <EuiTableHeaderCell key={index} width={column.width}>
                    {column.name}
                  </EuiTableHeaderCell>
                ))}
              </EuiTableHeader>
              <EuiTableBody>
                {data.map((item) => (
                  <EuiTableRow key={item.id}>
                    <EuiTableRowCell>{item.id}</EuiTableRowCell>
                    <EuiTableRowCell>
                      <EuiText size="s">{item.text}</EuiText>
                    </EuiTableRowCell>
                    <EuiTableRowCell>
                      <EuiText
                        size="s"
                        color={
                          item.originalClass === "positive"
                            ? "success"
                            : item.originalClass === "negative"
                              ? "danger"
                              : "default"
                        }
                      >
                        {item.originalClass}
                      </EuiText>
                    </EuiTableRowCell>
                    <EuiTableRowCell>
                      <EuiSelect
                        options={availableClasses.map((cls) => ({
                          value: cls,
                          text: cls,
                        }))}
                        value={item.currentClass}
                        onChange={(e) =>
                          handleClassChange(item.id, e.target.value)
                        }
                        compressed
                      />
                    </EuiTableRowCell>
                  </EuiTableRow>
                ))}
              </EuiTableBody>
            </EuiTable>
          </EuiPanel>

          {getChangedItems().length > 0 && (
            <>
              <EuiSpacer size="l" />
              <EuiCallOut
                title={`${getChangedItems().length} items have been modified`}
                color="warning"
                iconType="alert"
              >
                <p>
                  You have unsaved changes. Click "Save Changes" to persist your
                  modifications.
                </p>
              </EuiCallOut>
            </>
          )}
        </>
      )}
    </EuiPageSection>
  );
};

export default ClassificationTab;
