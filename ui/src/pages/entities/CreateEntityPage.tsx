import React, { useState, useContext } from "react";
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
  EuiSwitch,
} from "@elastic/eui";
import { useNavigate, useParams } from "react-router-dom";
import { EntityIcon } from "../../graphics/EntityIcon";
import { useDocumentTitle } from "../../hooks/useDocumentTitle";
import RegistryPathContext from "../../contexts/RegistryPathContext";
import { feast } from "../../protos";
import {
  writeToLocalRegistry,
  generateCliCommand,
} from "../../utils/localRegistryWriter";

const valueTypeOptions = Object.keys(feast.types.ValueType.Enum)
  .filter((key) => isNaN(Number(key)))
  .map((key) => ({
    value: key,
    text: key,
  }));

const CreateEntityPage = () => {
  const { projectName } = useParams<{ projectName: string }>();
  const registryUrl = useContext(RegistryPathContext);
  const navigate = useNavigate();

  useDocumentTitle(`Create Entity | Feast`);

  const [isSubmitting, setIsSubmitting] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [success, setSuccess] = useState(false);

  const [name, setName] = useState("");
  const [valueType, setValueType] = useState(valueTypeOptions[0].value);
  const [joinKey, setJoinKey] = useState("");
  const [description, setDescription] = useState("");
  const [tags, setTags] = useState("");
  const [owner, setOwner] = useState("");
  const [cliCommandDisplay, setCliCommandDisplay] = useState("");

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

      const entityData = {
        name,
        valueType,
        joinKey: joinKey || name, // Default to name if not provided
        description,
        tags: tagsObject,
        owner,
        project: projectName,
      };

      console.log("Creating entity with data:", entityData);

      const cliCommand = generateCliCommand("entity", entityData);
      console.log("CLI Command to create this entity:");
      console.log(cliCommand);

      setCliCommandDisplay(cliCommand);

      await new Promise((resolve) => setTimeout(resolve, 500));
      setSuccess(true);

      setName("");
      setValueType(valueTypeOptions[0].value);
      setJoinKey("");
      setDescription("");
      setTags("");
      setOwner("");

      /* 
      const response = await fetch(`${registryUrl}/api/entities`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify(entityData),
      });

      if (!response.ok) {
        const errorData = await response.json();
        throw new Error(errorData.detail || "Failed to create entity");
      }
      */
    } catch (err) {
      setError(
        err instanceof Error ? err.message : "An unknown error occurred",
      );
    } finally {
      setIsSubmitting(false);
    }
  };

  return (
    <EuiPageTemplate panelled>
      <EuiPageTemplate.Header
        restrictWidth
        iconType={EntityIcon}
        pageTitle="Create Entity"
      />
      <EuiPageTemplate.Section>
        {error && (
          <>
            <EuiCallOut title="Error creating entity" color="danger">
              <p>{error}</p>
            </EuiCallOut>
            <EuiSpacer />
          </>
        )}
        {success && (
          <>
            <EuiCallOut title="Entity creation instructions" color="success">
              <p>
                To create this entity in your local Feast registry, use the
                following CLI command:
              </p>
              <pre
                style={{
                  marginTop: "10px",
                  backgroundColor: "#f5f5f5",
                  padding: "10px",
                  borderRadius: "4px",
                  overflowX: "auto",
                }}
              >
                {cliCommandDisplay ||
                  `# Create a Python file named entity_example.py with the following content:

from feast import Entity
from feast.value_type import ValueType

entity = Entity(
    name="example_entity",
    value_type=ValueType.${valueTypeOptions[0].value},
    join_key="example_entity",
    description="",
    tags={},
    owner=""
)

# Then apply it using the Feast CLI:
# feast apply entity_example.py`}
              </pre>
            </EuiCallOut>
            <EuiSpacer />
          </>
        )}
        <EuiForm component="form" onSubmit={handleSubmit}>
          <EuiFormRow label="Name" helpText="Unique name for the entity">
            <EuiFieldText
              name="name"
              value={name}
              onChange={(e) => setName(e.target.value)}
              required
            />
          </EuiFormRow>

          <EuiFormRow label="Value Type" helpText="The type of the entity">
            <EuiSelect
              options={valueTypeOptions}
              value={valueType}
              onChange={(e) => setValueType(e.target.value)}
              required
            />
          </EuiFormRow>

          <EuiFormRow
            label="Join Key"
            helpText="Property that uniquely identifies entities (defaults to name if empty)"
          >
            <EuiFieldText
              name="joinKey"
              value={joinKey}
              onChange={(e) => setJoinKey(e.target.value)}
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

          <EuiSpacer />

          <EuiFlexGroup>
            <EuiFlexItem grow={false}>
              <EuiButton
                type="submit"
                fill
                isLoading={isSubmitting}
                disabled={isSubmitting || !name || !valueType}
              >
                Generate CLI Command
              </EuiButton>
            </EuiFlexItem>
            <EuiFlexItem grow={false}>
              <EuiButton
                onClick={() => navigate(`/p/${projectName}/entity/`)}
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

export default CreateEntityPage;
