import React, { useState, useContext, useEffect } from "react";
import {
  EuiPageTemplate,
  EuiTitle,
  EuiForm,
  EuiFormRow,
  EuiFieldText,
  EuiSelect,
  EuiTextArea,
  EuiButton,
  EuiSpacer,
  EuiCallOut,
  EuiFlexGroup,
  EuiFlexItem,
  EuiComboBox,
  EuiFieldNumber,
  EuiAccordion,
  EuiPanel,
  EuiSwitch,
} from "@elastic/eui";
import { useNavigate, useParams } from "react-router-dom";
import { FeatureViewIcon } from "../../graphics/FeatureViewIcon";
import { useDocumentTitle } from "../../hooks/useDocumentTitle";
import RegistryPathContext from "../../contexts/RegistryPathContext";
import useLoadRegistry from "../../queries/useLoadRegistry";
import { writeToLocalRegistry, generateCliCommand } from "../../utils/localRegistryWriter";

const valueTypeOptions = [
  { value: "INT32", text: "INT32" },
  { value: "INT64", text: "INT64" },
  { value: "FLOAT", text: "FLOAT" },
  { value: "DOUBLE", text: "DOUBLE" },
  { value: "STRING", text: "STRING" },
  { value: "BYTES", text: "BYTES" },
  { value: "BOOL", text: "BOOL" },
  { value: "UNIX_TIMESTAMP", text: "UNIX_TIMESTAMP" },
  { value: "TIMESTAMP", text: "TIMESTAMP" },
];

const CreateFeatureViewPage = () => {
  const { projectName } = useParams<{ projectName: string }>();
  const registryUrl = useContext(RegistryPathContext);
  const navigate = useNavigate();

  useDocumentTitle(`Create Feature View | Feast`);

  const [isSubmitting, setIsSubmitting] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [success, setSuccess] = useState(false);

  const [name, setName] = useState("");
  const [description, setDescription] = useState("");
  const [tags, setTags] = useState("");
  const [owner, setOwner] = useState("");
  const [cliCommandDisplay, setCliCommandDisplay] = useState("");
  const [writeToRegistry, setWriteToRegistry] = useState(false);
  
  const [ttlSeconds, setTtlSeconds] = useState("86400");
  
  const [features, setFeatures] = useState([{ name: "", valueType: "FLOAT" }]);
  
  const [selectedEntities, setSelectedEntities] = useState<Array<{ label: string }>>([]);
  const [entityOptions, setEntityOptions] = useState<Array<{ label: string }>>([]);
  
  const [selectedDataSource, setSelectedDataSource] = useState<{ label: string } | null>(null);
  const [dataSourceOptions, setDataSourceOptions] = useState<Array<{ label: string }>>([]);
  
  const registryQuery = useLoadRegistry(registryUrl);
  
  useEffect(() => {
    if (registryQuery.isSuccess && registryQuery.data) {
      const entities = registryQuery.data.objects.entities || [];
      setEntityOptions(
        entities.map((entity: any) => ({
          label: entity.spec?.name || "",
        }))
      );
      
      const dataSources = registryQuery.data.objects.dataSources || [];
      setDataSourceOptions(
        dataSources.map((ds: any) => ({
          label: ds.name || "",
        }))
      );
    }
  }, [registryQuery.isSuccess, registryQuery.data]);

  const addFeature = () => {
    setFeatures([...features, { name: "", valueType: "FLOAT" }]);
  };

  const removeFeature = (index: number) => {
    const newFeatures = [...features];
    newFeatures.splice(index, 1);
    setFeatures(newFeatures);
  };

  const updateFeature = (index: number, field: string, value: string) => {
    const newFeatures = [...features];
    newFeatures[index] = { ...newFeatures[index], [field]: value };
    setFeatures(newFeatures);
  };

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setIsSubmitting(true);
    setError(null);
    setSuccess(false);

    try {
      const tagsObject: Record<string, string> = {};
      tags.split(",").forEach((tag) => {
        const [key, value] = tag.trim().split(":");
        if (key && value) {
          tagsObject[key] = value;
        }
      });

      if (!name) throw new Error("Name is required");
      if (!selectedDataSource) throw new Error("Data source is required");
      if (selectedEntities.length === 0) throw new Error("At least one entity is required");
      if (features.length === 0) throw new Error("At least one feature is required");
      
      for (const feature of features) {
        if (!feature.name) throw new Error("All features must have a name");
      }

      const featureViewData = {
        name,
        description,
        tags: tagsObject,
        owner,
        ttl_seconds: parseInt(ttlSeconds),
        entities: selectedEntities.map(e => e.label),
        features: features.map(f => ({
          name: f.name,
          value_type: f.valueType
        })),
        data_source: selectedDataSource.label,
        project: projectName,
      };

      console.log("Creating feature view with data:", featureViewData);
      
      const cliCommand = generateCliCommand("feature_view", featureViewData);
      
      console.log("CLI Command to create this feature view:");
      console.log(cliCommand);
      
      setCliCommandDisplay(cliCommand);
      
      if (writeToRegistry) {
        try {
          const result = await writeToLocalRegistry("feature_view", featureViewData, registryUrl);
          
          if (result.success) {
            setSuccess(true);
            console.log("Feature view written to registry:", result.message);
          } else {
            throw new Error(result.message);
          }
        } catch (err) {
          console.error("Error writing to registry:", err);
          throw err;
        }
      } else {
        await new Promise(resolve => setTimeout(resolve, 500));
        setSuccess(true);
      }
      setName("");
      setDescription("");
      setTags("");
      setOwner("");
      setTtlSeconds("86400");
      setFeatures([{ name: "", valueType: "FLOAT" }]);
      setSelectedEntities([]);
      setSelectedDataSource(null);
    } catch (err) {
      setError(err instanceof Error ? err.message : "An unknown error occurred");
    } finally {
      setIsSubmitting(false);
    }
  };

  return (
    <EuiPageTemplate panelled>
      <EuiPageTemplate.Header
        restrictWidth
        iconType={FeatureViewIcon}
        pageTitle="Create Feature View"
      />
      <EuiPageTemplate.Section>
        {error && (
          <>
            <EuiCallOut title="Error creating feature view" color="danger">
              <p>{error}</p>
            </EuiCallOut>
            <EuiSpacer />
          </>
        )}
        {success && (
          <>
            <EuiCallOut title="Feature view creation instructions" color="success">
              <p>To create this feature view in your local Feast registry, use the following CLI command:</p>
              <pre style={{ marginTop: '10px', backgroundColor: '#f5f5f5', padding: '10px', borderRadius: '4px', overflowX: 'auto' }}>
                {cliCommandDisplay || `# Create a Python file named feature_view_example.py with the following content:

from feast import FeatureView, Feature
from feast.value_type import ValueType
from datetime import timedelta

# You'll need to get references to your entities and data source
# This is a simplified example - you may need to adjust based on your actual setup
data_source = feast.get_data_source("example_source")
entities = [feast.get_entity(e) for e in ["example_entity"]]

feature_view = FeatureView(
    name="example_feature_view",
    entities=entities,
    ttl=timedelta(seconds=86400),
    features=[
        Feature(name="example_feature", dtype=FLOAT)
    ],
    online=True,
    source=data_source,
    tags={},
    owner="",
    description=""
)

# Then apply it using the Feast CLI:
# feast apply feature_view_example.py`}
              </pre>
            </EuiCallOut>
            <EuiSpacer />
          </>
        )}
        <EuiForm component="form" onSubmit={handleSubmit}>
          <EuiFormRow label="Name" helpText="Unique name for the feature view">
            <EuiFieldText
              name="name"
              value={name}
              onChange={(e) => setName(e.target.value)}
              required
            />
          </EuiFormRow>

          <EuiFormRow label="Description" helpText="Human-readable description">
            <EuiTextArea
              name="description"
              value={description}
              onChange={(e) => setDescription(e.target.value)}
              rows={3}
            />
          </EuiFormRow>

          <EuiFormRow label="TTL (seconds)" helpText="Time to live in seconds">
            <EuiFieldNumber
              name="ttlSeconds"
              value={ttlSeconds}
              onChange={(e) => setTtlSeconds(e.target.value)}
              min={1}
              required
            />
          </EuiFormRow>

          <EuiFormRow label="Entities" helpText="Select one or more entities">
            <EuiComboBox
              placeholder="Select entities"
              options={entityOptions}
              selectedOptions={selectedEntities}
              onChange={(selected) => setSelectedEntities(selected)}
              isClearable={true}
              isInvalid={isSubmitting && selectedEntities.length === 0}
            />
          </EuiFormRow>

          <EuiFormRow label="Data Source" helpText="Select a data source">
            <EuiComboBox
              placeholder="Select data source"
              options={dataSourceOptions}
              selectedOptions={selectedDataSource ? [selectedDataSource] : []}
              onChange={(selected) => setSelectedDataSource(selected[0] || null)}
              singleSelection={{ asPlainText: true }}
              isClearable={true}
              isInvalid={isSubmitting && !selectedDataSource}
            />
          </EuiFormRow>

          <EuiSpacer />
          <EuiTitle size="xs">
            <h3>Features</h3>
          </EuiTitle>
          <EuiSpacer size="s" />

          {features.map((feature, index) => (
            <EuiFlexGroup key={index} alignItems="center">
              <EuiFlexItem>
                <EuiFormRow label="Feature Name">
                  <EuiFieldText
                    value={feature.name}
                    onChange={(e) => updateFeature(index, "name", e.target.value)}
                    required
                  />
                </EuiFormRow>
              </EuiFlexItem>
              <EuiFlexItem>
                <EuiFormRow label="Value Type">
                  <EuiSelect
                    options={valueTypeOptions}
                    value={feature.valueType}
                    onChange={(e) => updateFeature(index, "valueType", e.target.value)}
                    required
                  />
                </EuiFormRow>
              </EuiFlexItem>
              <EuiFlexItem grow={false}>
                <EuiFormRow hasEmptyLabelSpace>
                  <EuiButton
                    color="danger"
                    onClick={() => removeFeature(index)}
                    disabled={features.length === 1}
                    iconType="trash"
                    size="s"
                  >
                    Remove
                  </EuiButton>
                </EuiFormRow>
              </EuiFlexItem>
            </EuiFlexGroup>
          ))}

          <EuiSpacer size="s" />
          <EuiButton onClick={addFeature} iconType="plusInCircle" size="s">
            Add Feature
          </EuiButton>

          <EuiSpacer />

          <EuiAccordion id="additionalFields" buttonContent="Additional Fields">
            <EuiPanel>
              <EuiFormRow
                label="Tags"
                helpText="Comma-separated list of key:value pairs (e.g., team:analytics,owner:data)"
              >
                <EuiFieldText
                  name="tags"
                  value={tags}
                  onChange={(e) => setTags(e.target.value)}
                />
              </EuiFormRow>

              <EuiFormRow label="Owner" helpText="Email of the primary maintainer">
                <EuiFieldText
                  name="owner"
                  value={owner}
                  onChange={(e) => setOwner(e.target.value)}
                />
              </EuiFormRow>
            </EuiPanel>
          </EuiAccordion>

          <EuiSpacer />
          
          <EuiFormRow>
            <EuiSwitch
              label="Write directly to registry"
              checked={writeToRegistry}
              onChange={(e) => setWriteToRegistry(e.target.checked)}
              disabled={isSubmitting}
              data-test-subj="writeToRegistrySwitch"
            />
          </EuiFormRow>
          
          <EuiSpacer />

          <EuiFlexGroup>
            <EuiFlexItem grow={false}>
              <EuiButton
                type="submit"
                fill
                isLoading={isSubmitting}
                disabled={
                  isSubmitting ||
                  !name ||
                  !ttlSeconds ||
                  selectedEntities.length === 0 ||
                  !selectedDataSource ||
                  features.some(f => !f.name)
                }
              >
                {writeToRegistry ? 'Create Feature View in Registry' : 'Generate CLI Command'}
              </EuiButton>
            </EuiFlexItem>
            <EuiFlexItem grow={false}>
              <EuiButton
                onClick={() => navigate(`/p/${projectName}/feature-view/`)}
                disabled={isSubmitting}
              >
                Cancel
              </EuiButton>
            </EuiFlexItem>
          </EuiFlexGroup>
        </EuiForm>
      </EuiPageTemplate.Section>
    </EuiPageTemplate>
  );
};

export default CreateFeatureViewPage;
