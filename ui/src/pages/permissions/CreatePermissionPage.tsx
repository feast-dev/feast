import React, { useState, useContext } from "react";
import {
  EuiPageTemplate,
  EuiTitle,
  EuiForm,
  EuiFormRow,
  EuiFieldText,
  EuiSelect,
  EuiButton,
  EuiSpacer,
  EuiCallOut,
  EuiFlexGroup,
  EuiFlexItem,
  EuiAccordion,
  EuiPanel,
} from "@elastic/eui";
import { useNavigate, useParams } from "react-router-dom";
import { useDocumentTitle } from "../../hooks/useDocumentTitle";
import RegistryPathContext from "../../contexts/RegistryPathContext";
import useLoadRegistry from "../../queries/useLoadRegistry";

const CreatePermissionPage = () => {
  const { projectName } = useParams<{ projectName: string }>();
  const registryUrl = useContext(RegistryPathContext);
  const navigate = useNavigate();

  useDocumentTitle(`Create Permission | Feast`);

  const [isSubmitting, setIsSubmitting] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [success, setSuccess] = useState(false);

  const [name, setName] = useState("");
  const [description, setDescription] = useState("");
  const [principal, setPrincipal] = useState("");
  const [resource, setResource] = useState("");
  const [action, setAction] = useState("READ");
  const [tags, setTags] = useState("");
  const [owner, setOwner] = useState("");
  const [cliCommandDisplay, setCliCommandDisplay] = useState("");

  const registryQuery = useLoadRegistry(registryUrl);

  const actionOptions = [
    { value: "READ", text: "Read" },
    { value: "WRITE", text: "Write" },
    { value: "ADMIN", text: "Admin" },
  ];

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
      if (!principal) throw new Error("Principal is required");
      if (!resource) throw new Error("Resource is required");
      if (!action) throw new Error("Action is required");

      const permissionData = {
        name,
        description,
        principal,
        resource,
        action,
        tags: tagsObject,
        owner,
        project: projectName,
      };

      console.log("Creating permission with data:", permissionData);
      
      const cliCommand = `# Create a Python file named permission_${name.toLowerCase().replace(/\s+/g, '_')}.py with the following content:

from feast import Permission
from feast.permissions.action import Action

permission = Permission(
    name="${name}",
    principal="${principal}",
    resource="${resource}",
    action=Action.${action},
    description="${description}",
    tags=${JSON.stringify(tagsObject)},
    owner="${owner}"
)

# Then apply it using the Feast CLI:
# feast apply permission_${name.toLowerCase().replace(/\s+/g, '_')}.py`;
      
      console.log("CLI Command to create this permission:");
      console.log(cliCommand);
      
      await new Promise(resolve => setTimeout(resolve, 500));
      
      setCliCommandDisplay(cliCommand);
      
      /* 
      const response = await fetch(`${registryUrl}/api/permissions`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify(permissionData),
      });

      if (!response.ok) {
        const errorData = await response.json();
        throw new Error(errorData.detail || "Failed to create permission");
      }
      */

      setSuccess(true);
      setName("");
      setDescription("");
      setPrincipal("");
      setResource("");
      setAction("READ");
      setTags("");
      setOwner("");
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
        iconType="lock"
        pageTitle="Create Permission"
      />
      <EuiPageTemplate.Section>
        {error && (
          <>
            <EuiCallOut title="Error creating permission" color="danger">
              <p>{error}</p>
            </EuiCallOut>
            <EuiSpacer />
          </>
        )}
        {success && (
          <>
            <EuiCallOut title="Permission creation instructions" color="success">
              <p>To create this permission in your local Feast registry, use the following CLI command:</p>
              <pre style={{ marginTop: '10px', backgroundColor: '#f5f5f5', padding: '10px', borderRadius: '4px', overflowX: 'auto' }}>
                {cliCommandDisplay || `# Create a Python file named permission_example.py with the following content:

from feast import Permission
from feast.permissions.action import Action

permission = Permission(
    name="example_permission",
    principal="user:example@example.com",
    resource="project:default:entity:*",
    action=Action.READ,
    description="",
    tags={},
    owner=""
)

# Then apply it using the Feast CLI:
# feast apply permission_example.py`}
              </pre>
            </EuiCallOut>
            <EuiSpacer />
          </>
        )}
        <EuiForm component="form" onSubmit={handleSubmit}>
          <EuiFormRow label="Name" helpText="Unique name for the permission">
            <EuiFieldText
              name="name"
              value={name}
              onChange={(e) => setName(e.target.value)}
              required
            />
          </EuiFormRow>

          <EuiFormRow label="Principal" helpText="User or role that is granted the permission">
            <EuiFieldText
              name="principal"
              value={principal}
              onChange={(e) => setPrincipal(e.target.value)}
              required
            />
          </EuiFormRow>

          <EuiFormRow label="Resource" helpText="Resource that the permission applies to">
            <EuiFieldText
              name="resource"
              value={resource}
              onChange={(e) => setResource(e.target.value)}
              required
            />
          </EuiFormRow>

          <EuiFormRow label="Action" helpText="Type of access granted">
            <EuiSelect
              options={actionOptions}
              value={action}
              onChange={(e) => setAction(e.target.value)}
              required
            />
          </EuiFormRow>

          <EuiFormRow label="Description" helpText="Human-readable description">
            <EuiFieldText
              name="description"
              value={description}
              onChange={(e) => setDescription(e.target.value)}
            />
          </EuiFormRow>

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
                  !principal ||
                  !resource ||
                  !action
                }
              >
                Create Permission
              </EuiButton>
            </EuiFlexItem>
            <EuiFlexItem grow={false}>
              <EuiButton
                onClick={() => navigate(`/p/${projectName}/permissions/`)}
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

export default CreatePermissionPage;
