import React, { useState, useContext, useEffect } from "react";
import {
  EuiPageTemplate,
  EuiTitle,
  EuiForm,
  EuiFormRow,
  EuiFieldText,
  EuiTextArea,
  EuiButton,
  EuiSpacer,
  EuiCallOut,
  EuiFlexGroup,
  EuiFlexItem,
  EuiComboBox,
  EuiAccordion,
  EuiPanel,
  EuiDragDropContext,
  EuiDraggable,
  EuiDroppable,
  EuiCard,
  EuiBadge,
  EuiIcon,
  EuiText,
  EuiSwitch,
} from "@elastic/eui";
import { useNavigate, useParams } from "react-router-dom";
import { FeatureServiceIcon } from "../../graphics/FeatureServiceIcon";
import { useDocumentTitle } from "../../hooks/useDocumentTitle";
import RegistryPathContext from "../../contexts/RegistryPathContext";
import useLoadRegistry from "../../queries/useLoadRegistry";
import { writeToLocalRegistry, generateCliCommand } from "../../utils/localRegistryWriter";

const CreateFeatureServicePage = () => {
  const { projectName } = useParams<{ projectName: string }>();
  const registryUrl = useContext(RegistryPathContext);
  const navigate = useNavigate();

  useDocumentTitle(`Create Feature Service | Feast`);

  const [isSubmitting, setIsSubmitting] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [success, setSuccess] = useState(false);

  const [name, setName] = useState("");
  const [description, setDescription] = useState("");
  const [tags, setTags] = useState("");
  const [owner, setOwner] = useState("");
  const [cliCommandDisplay, setCliCommandDisplay] = useState("");
  
  const [featureViewOptions, setFeatureViewOptions] = useState<Array<{ label: string, features: string[] }>>([]);
  const [selectedFeatures, setSelectedFeatures] = useState<Array<{ featureView: string, feature: string }>>([]);
  
  const registryQuery = useLoadRegistry(registryUrl);
  
  useEffect(() => {
    if (registryQuery.isSuccess && registryQuery.data) {
      const featureViews = registryQuery.data.mergedFVList || [];
      const options = featureViews
        .filter((fv: any) => fv.type === "regular")
        .map((fv: any) => {
          const features = fv.object?.spec?.features?.map((f: any) => f.name) || [];
          return {
            label: fv.name,
            features
          };
        });
      
      setFeatureViewOptions(options);
    }
  }, [registryQuery.isSuccess, registryQuery.data]);

  const handleFeatureViewSelect = (selectedOptions: Array<{ label: string }>, featureView: { label: string, features: string[] }) => {
    if (selectedOptions.length === 0) {
      return;
    }
    
    const newFeatures = selectedOptions.map(option => ({
      featureView: featureView.label,
      feature: option.label
    }));
    
    setSelectedFeatures([...selectedFeatures, ...newFeatures]);
  };

  const removeFeature = (index: number) => {
    const newFeatures = [...selectedFeatures];
    newFeatures.splice(index, 1);
    setSelectedFeatures(newFeatures);
  };

  const onDragEnd = ({ source, destination }: any) => {
    if (!destination) {
      return;
    }

    const items = Array.from(selectedFeatures);
    const [removed] = items.splice(source.index, 1);
    items.splice(destination.index, 0, removed);

    setSelectedFeatures(items);
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
      if (selectedFeatures.length === 0) throw new Error("At least one feature is required");
      
      const featuresByView: Record<string, string[]> = {};
      selectedFeatures.forEach(({ featureView, feature }) => {
        if (!featuresByView[featureView]) {
          featuresByView[featureView] = [];
        }
        featuresByView[featureView].push(feature);
      });
      
      const featureReferences = Object.entries(featuresByView).map(([featureView, features]) => ({
        feature_view_name: featureView,
        feature_names: features
      }));

      const featureServiceData = {
        name,
        description,
        tags: tagsObject,
        owner,
        feature_references: featureReferences,
        project: projectName,
      };

      console.log("Creating feature service with data:", featureServiceData);
      
      const featureServiceDataForCli = {
        name,
        description,
        tags: tagsObject,
        owner,
        featureReferences: featuresByView,
        project: projectName,
      };
      
      const cliCommand = generateCliCommand("feature_service", featureServiceDataForCli);
      
      console.log("CLI Command to create this feature service:");
      console.log(cliCommand);
      
      setCliCommandDisplay(cliCommand);
      
      await new Promise(resolve => setTimeout(resolve, 500));
      setSuccess(true);
      setName("");
      setDescription("");
      setTags("");
      setOwner("");
      setSelectedFeatures([]);
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
        iconType={FeatureServiceIcon}
        pageTitle="Create Feature Service"
      />
      <EuiPageTemplate.Section>
        {error && (
          <>
            <EuiCallOut title="Error creating feature service" color="danger">
              <p>{error}</p>
            </EuiCallOut>
            <EuiSpacer />
          </>
        )}
        {success && (
          <>
            <EuiCallOut title="Feature service creation instructions" color="success">
              <p>To create this feature service in your local Feast registry, use the following CLI command:</p>
              <pre style={{ marginTop: '10px', backgroundColor: '#f5f5f5', padding: '10px', borderRadius: '4px', overflowX: 'auto' }}>
                {cliCommandDisplay || `# Create a Python file named feature_service_example.py with the following content:

from feast import FeatureService, FeatureReference

feature_service = FeatureService(
    name="example_service",
    features=[
        FeatureReference(name="example_feature_view", features=["example_feature"])
    ],
    tags={},
    owner="",
    description=""
)

# Then apply it using the Feast CLI:
# feast apply feature_service_example.py`}
              </pre>
            </EuiCallOut>
            <EuiSpacer />
          </>
        )}
        <EuiForm component="form" onSubmit={handleSubmit}>
          <EuiFormRow label="Name" helpText="Unique name for the feature service">
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

          <EuiSpacer />
          <EuiTitle size="xs">
            <h3>Features</h3>
          </EuiTitle>
          <EuiSpacer size="s" />

          {featureViewOptions.map((featureView) => (
            <EuiFormRow
              key={featureView.label}
              label={`Features from ${featureView.label}`}
              helpText="Select features to include in this service"
            >
              <EuiComboBox
                placeholder="Select features"
                options={featureView.features.map(f => ({ label: f }))}
                onChange={(selected) => handleFeatureViewSelect(selected, featureView)}
                isClearable={true}
              />
            </EuiFormRow>
          ))}

          <EuiSpacer />
          <EuiTitle size="xs">
            <h3>Selected Features</h3>
          </EuiTitle>
          <EuiSpacer size="s" />

          {selectedFeatures.length === 0 ? (
            <EuiCallOut title="No features selected" color="warning">
              <p>Please select at least one feature to include in this service.</p>
            </EuiCallOut>
          ) : (
            <EuiDragDropContext onDragEnd={onDragEnd}>
              <EuiDroppable droppableId="FEATURES_DROPPABLE" spacing="m">
                {selectedFeatures.map((feature, idx) => (
                  <EuiDraggable
                    key={`${feature.featureView}-${feature.feature}-${idx}`}
                    index={idx}
                    draggableId={`${feature.featureView}-${feature.feature}-${idx}`}
                    customDragHandle={true}
                  >
                    {(provided) => (
                      <EuiFlexGroup alignItems="center" gutterSize="s">
                        <EuiFlexItem grow={false}>
                          <div {...provided.dragHandleProps}>
                            <EuiIcon type="grab" />
                          </div>
                        </EuiFlexItem>
                        <EuiFlexItem>
                          <EuiPanel>
                            <EuiFlexGroup alignItems="center" justifyContent="spaceBetween">
                              <EuiFlexItem>
                                <EuiBadge color="primary">{feature.featureView}</EuiBadge>
                                <EuiSpacer size="xs" />
                                <EuiText size="s">{feature.feature}</EuiText>
                              </EuiFlexItem>
                              <EuiFlexItem grow={false}>
                                <EuiButton
                                  color="danger"
                                  onClick={() => removeFeature(idx)}
                                  iconType="trash"
                                  size="s"
                                >
                                  Remove
                                </EuiButton>
                              </EuiFlexItem>
                            </EuiFlexGroup>
                          </EuiPanel>
                        </EuiFlexItem>
                      </EuiFlexGroup>
                    )}
                  </EuiDraggable>
                ))}
              </EuiDroppable>
            </EuiDragDropContext>
          )}

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

          <EuiFlexGroup>
            <EuiFlexItem grow={false}>
              <EuiButton
                type="submit"
                fill
                isLoading={isSubmitting}
                disabled={
                  isSubmitting ||
                  !name ||
                  selectedFeatures.length === 0
                }
              >
                Generate CLI Command
              </EuiButton>
            </EuiFlexItem>
            <EuiFlexItem grow={false}>
              <EuiButton
                onClick={() => navigate(`/p/${projectName}/feature-service/`)}
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

export default CreateFeatureServicePage;
