import React, { useState, useEffect } from "react";
import {
  EuiPanel,
  EuiTitle,
  EuiHorizontalRule,
  EuiSpacer,
  EuiText,
  EuiButton,
  EuiFlexGroup,
  EuiFlexItem,
  EuiFormRow,
  EuiFieldText,
  EuiCopy,
  EuiCheckbox,
} from "@elastic/eui";
import { CodeBlock, github } from "react-code-blocks";
import { RegularFeatureViewCustomTabProps } from "../../custom-tabs/types";

const CurlGeneratorTab = ({
  feastObjectQuery,
}: RegularFeatureViewCustomTabProps) => {
  const data = feastObjectQuery.data as any;
  const [serverUrl, setServerUrl] = useState(() => {
    const savedUrl = localStorage.getItem("feast-feature-server-url");
    return savedUrl || "http://localhost:6566";
  });
  const [entityValues, setEntityValues] = useState<Record<string, string>>({});
  const [selectedFeatures, setSelectedFeatures] = useState<
    Record<string, boolean>
  >({});

  useEffect(() => {
    localStorage.setItem("feast-feature-server-url", serverUrl);
  }, [serverUrl]);

  if (feastObjectQuery.isLoading) {
    return <EuiText>Loading...</EuiText>;
  }

  if (feastObjectQuery.isError || !data) {
    return <EuiText>Error loading feature view data.</EuiText>;
  }

  const generateFeatureNames = () => {
    if (!data?.name || !data?.features) return [];

    return data.features
      .filter((feature: any) => selectedFeatures[feature.name] !== false)
      .map((feature: any) => `${data.name}:${feature.name}`);
  };

  const generateEntityObject = () => {
    if (!data?.object?.spec?.entities) return {};

    const entities: Record<string, number[]> = {};
    data.object.spec.entities.forEach((entityName: string) => {
      const userValue = entityValues[entityName];
      if (userValue) {
        const values = userValue.split(",").map((v) => {
          const num = parseInt(v.trim());
          return isNaN(num) ? 1001 : num;
        });
        entities[entityName] = values;
      } else {
        entities[entityName] = [1001, 1002, 1003];
      }
    });
    return entities;
  };

  const generateCurlCommand = () => {
    const features = generateFeatureNames();
    const entities = generateEntityObject();

    const payload = {
      features,
      entities,
    };

    const curlCommand = `curl -X POST \\
  "${serverUrl}/get-online-features" \\
  -H "Content-Type: application/json" \\
  -d '${JSON.stringify(payload, null, 2)}'`;

    return curlCommand;
  };

  const curlCommand = generateCurlCommand();

  return (
    <React.Fragment>
      <EuiPanel hasBorder={true}>
        <EuiTitle size="s">
          <h2>Feature Server CURL Generator</h2>
        </EuiTitle>
        <EuiHorizontalRule margin="s" />
        <EuiText size="s" color="subdued">
          <p>
            Generate a CURL command to fetch online features from the feature
            server. The command is pre-populated with all features and entities
            from this feature view.
          </p>
        </EuiText>
        <EuiSpacer size="m" />

        <EuiFormRow label="Feature Server URL">
          <EuiFieldText
            value={serverUrl}
            onChange={(e) => setServerUrl(e.target.value)}
            placeholder="http://localhost:6566"
          />
        </EuiFormRow>

        <EuiSpacer size="m" />

        {data?.features && data.features.length > 0 && (
          <>
            <EuiFlexGroup justifyContent="spaceBetween" alignItems="center">
              <EuiFlexItem grow={false}>
                <EuiTitle size="xs">
                  <h3>
                    Features to Include (
                    {
                      Object.values(selectedFeatures).filter((v) => v !== false)
                        .length
                    }
                    /{data.features.length})
                  </h3>
                </EuiTitle>
              </EuiFlexItem>
              <EuiFlexItem grow={false}>
                <EuiFlexGroup gutterSize="s">
                  <EuiFlexItem grow={false}>
                    <EuiButton
                      size="s"
                      onClick={() => {
                        const allSelected: Record<string, boolean> = {};
                        data.features.forEach((feature: any) => {
                          allSelected[feature.name] = true;
                        });
                        setSelectedFeatures(allSelected);
                      }}
                    >
                      Select All
                    </EuiButton>
                  </EuiFlexItem>
                  <EuiFlexItem grow={false}>
                    <EuiButton
                      size="s"
                      onClick={() => {
                        const noneSelected: Record<string, boolean> = {};
                        data.features.forEach((feature: any) => {
                          noneSelected[feature.name] = false;
                        });
                        setSelectedFeatures(noneSelected);
                      }}
                    >
                      Select None
                    </EuiButton>
                  </EuiFlexItem>
                </EuiFlexGroup>
              </EuiFlexItem>
            </EuiFlexGroup>
            <EuiSpacer size="s" />
            <EuiPanel color="subdued" paddingSize="s">
              {Array.from(
                { length: Math.ceil(data.features.length / 5) },
                (_, rowIndex) => (
                  <EuiFlexGroup
                    key={rowIndex}
                    direction="row"
                    gutterSize="s"
                    style={{
                      marginBottom:
                        rowIndex < Math.ceil(data.features.length / 5) - 1
                          ? "8px"
                          : "0",
                    }}
                  >
                    {data.features
                      .slice(rowIndex * 5, (rowIndex + 1) * 5)
                      .map((feature: any) => (
                        <EuiFlexItem
                          key={feature.name}
                          grow={false}
                          style={{ minWidth: "180px" }}
                        >
                          <EuiCheckbox
                            id={`feature-${feature.name}`}
                            label={feature.name}
                            checked={selectedFeatures[feature.name] !== false}
                            onChange={(e) =>
                              setSelectedFeatures((prev) => ({
                                ...prev,
                                [feature.name]: e.target.checked,
                              }))
                            }
                          />
                        </EuiFlexItem>
                      ))}
                  </EuiFlexGroup>
                ),
              )}
            </EuiPanel>
            <EuiSpacer size="m" />
          </>
        )}

        {data?.object?.spec?.entities &&
          data.object.spec.entities.length > 0 && (
            <>
              <EuiTitle size="xs">
                <h3>Entity Values (comma-separated)</h3>
              </EuiTitle>
              <EuiSpacer size="s" />
              {data.object.spec.entities.map((entityName: string) => (
                <EuiFormRow key={entityName} label={entityName}>
                  <EuiFieldText
                    value={entityValues[entityName] || ""}
                    onChange={(e) =>
                      setEntityValues((prev) => ({
                        ...prev,
                        [entityName]: e.target.value,
                      }))
                    }
                    placeholder="1001, 1002, 1003"
                  />
                </EuiFormRow>
              ))}
              <EuiSpacer size="m" />
            </>
          )}

        <EuiFlexGroup justifyContent="spaceBetween" alignItems="center">
          <EuiFlexItem grow={false}>
            <EuiTitle size="xs">
              <h3>Generated CURL Command</h3>
            </EuiTitle>
          </EuiFlexItem>
          <EuiFlexItem grow={false}>
            <EuiCopy textToCopy={curlCommand}>
              {(copy) => (
                <EuiButton onClick={copy} size="s" iconType="copy">
                  Copy to Clipboard
                </EuiButton>
              )}
            </EuiCopy>
          </EuiFlexItem>
        </EuiFlexGroup>

        <EuiSpacer size="s" />

        <CodeBlock
          text={curlCommand}
          language="bash"
          showLineNumbers={false}
          theme={github}
        />

        <EuiSpacer size="m" />

        <EuiText size="s" color="subdued">
          <p>
            <strong>Features included:</strong>{" "}
            {generateFeatureNames().join(", ")}
          </p>
          {data?.object?.spec?.entities && (
            <p>
              <strong>Entities:</strong> {data.object.spec.entities.join(", ")}
            </p>
          )}
        </EuiText>
      </EuiPanel>
    </React.Fragment>
  );
};

export default CurlGeneratorTab;
