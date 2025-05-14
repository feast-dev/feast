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
  EuiPanel,
  EuiAccordion,
} from "@elastic/eui";
import { useNavigate, useParams } from "react-router-dom";
import { DataSourceIcon } from "../../graphics/DataSourceIcon";
import { useDocumentTitle } from "../../hooks/useDocumentTitle";
import RegistryPathContext from "../../contexts/RegistryPathContext";

const dataSourceTypeOptions = [
  { value: "file", text: "File Source" },
  { value: "bigquery", text: "BigQuery Source" },
  { value: "kafka", text: "Kafka Source" },
  { value: "kinesis", text: "Kinesis Source" },
  { value: "push", text: "Push Source" },
];

const CreateDataSourcePage = () => {
  const { projectName } = useParams<{ projectName: string }>();
  const registryUrl = useContext(RegistryPathContext);
  const navigate = useNavigate();

  useDocumentTitle(`Create Data Source | Feast`);

  const [isSubmitting, setIsSubmitting] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [success, setSuccess] = useState(false);

  const [name, setName] = useState("");
  const [description, setDescription] = useState("");
  const [sourceType, setSourceType] = useState(dataSourceTypeOptions[0].value);
  const [path, setPath] = useState("");
  const [timestampField, setTimestampField] = useState("");
  const [createdTimestampColumn, setCreatedTimestampColumn] = useState("");
  const [eventTimestampColumn, setEventTimestampColumn] = useState("");
  const [tags, setTags] = useState("");
  const [owner, setOwner] = useState("");

  const [kafkaBootstrapServers, setKafkaBootstrapServers] = useState("");
  const [kafkaTopic, setKafkaTopic] = useState("");

  const [kinesisRegion, setKinesisRegion] = useState("");
  const [kinesisStreamName, setKinesisStreamName] = useState("");

  const [bigqueryTable, setBigqueryTable] = useState("");

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

      let dataSourceData: any = {
        name,
        description,
        type: sourceType,
        tags: tagsObject,
        owner,
        project: projectName,
      };

      switch (sourceType) {
        case "file":
          dataSourceData.file_options = {
            path,
            timestamp_field: timestampField,
            created_timestamp_column: createdTimestampColumn,
            event_timestamp_column: eventTimestampColumn,
          };
          break;
        case "bigquery":
          dataSourceData.bigquery_options = {
            table: bigqueryTable,
            timestamp_field: timestampField,
            created_timestamp_column: createdTimestampColumn,
            event_timestamp_column: eventTimestampColumn,
          };
          break;
        case "kafka":
          dataSourceData.kafka_options = {
            bootstrap_servers: kafkaBootstrapServers,
            topic: kafkaTopic,
            timestamp_field: timestampField,
          };
          break;
        case "kinesis":
          dataSourceData.kinesis_options = {
            region: kinesisRegion,
            stream_name: kinesisStreamName,
            timestamp_field: timestampField,
          };
          break;
        case "push":
          dataSourceData.push_options = {
            timestamp_field: timestampField,
          };
          break;
      }

      const response = await fetch(`${registryUrl}/api/data-sources`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify(dataSourceData),
      });

      if (!response.ok) {
        const errorData = await response.json();
        throw new Error(errorData.detail || "Failed to create data source");
      }

      setSuccess(true);
      setName("");
      setDescription("");
      setSourceType(dataSourceTypeOptions[0].value);
      setPath("");
      setTimestampField("");
      setCreatedTimestampColumn("");
      setEventTimestampColumn("");
      setTags("");
      setOwner("");
      setKafkaBootstrapServers("");
      setKafkaTopic("");
      setKinesisRegion("");
      setKinesisStreamName("");
      setBigqueryTable("");
    } catch (err) {
      setError(err instanceof Error ? err.message : "An unknown error occurred");
    } finally {
      setIsSubmitting(false);
    }
  };

  const renderSourceSpecificFields = () => {
    switch (sourceType) {
      case "file":
        return (
          <>
            <EuiFormRow label="File Path" required>
              <EuiFieldText
                name="path"
                value={path}
                onChange={(e) => setPath(e.target.value)}
                required
              />
            </EuiFormRow>
          </>
        );
      case "bigquery":
        return (
          <>
            <EuiFormRow label="BigQuery Table" required>
              <EuiFieldText
                name="bigqueryTable"
                value={bigqueryTable}
                onChange={(e) => setBigqueryTable(e.target.value)}
                required
                placeholder="project.dataset.table"
              />
            </EuiFormRow>
          </>
        );
      case "kafka":
        return (
          <>
            <EuiFormRow label="Bootstrap Servers" required>
              <EuiFieldText
                name="kafkaBootstrapServers"
                value={kafkaBootstrapServers}
                onChange={(e) => setKafkaBootstrapServers(e.target.value)}
                required
                placeholder="localhost:9092"
              />
            </EuiFormRow>
            <EuiFormRow label="Topic" required>
              <EuiFieldText
                name="kafkaTopic"
                value={kafkaTopic}
                onChange={(e) => setKafkaTopic(e.target.value)}
                required
              />
            </EuiFormRow>
          </>
        );
      case "kinesis":
        return (
          <>
            <EuiFormRow label="Region" required>
              <EuiFieldText
                name="kinesisRegion"
                value={kinesisRegion}
                onChange={(e) => setKinesisRegion(e.target.value)}
                required
                placeholder="us-west-1"
              />
            </EuiFormRow>
            <EuiFormRow label="Stream Name" required>
              <EuiFieldText
                name="kinesisStreamName"
                value={kinesisStreamName}
                onChange={(e) => setKinesisStreamName(e.target.value)}
                required
              />
            </EuiFormRow>
          </>
        );
      case "push":
        return null;
      default:
        return null;
    }
  };

  return (
    <EuiPageTemplate panelled>
      <EuiPageTemplate.Header
        restrictWidth
        iconType={DataSourceIcon}
        pageTitle="Create Data Source"
      />
      <EuiPageTemplate.Section>
        {error && (
          <>
            <EuiCallOut title="Error creating data source" color="danger">
              <p>{error}</p>
            </EuiCallOut>
            <EuiSpacer />
          </>
        )}
        {success && (
          <>
            <EuiCallOut title="Data source created successfully" color="success">
              <p>The data source has been created successfully.</p>
            </EuiCallOut>
            <EuiSpacer />
          </>
        )}
        <EuiForm component="form" onSubmit={handleSubmit}>
          <EuiFormRow label="Name" helpText="Unique name for the data source" required>
            <EuiFieldText
              name="name"
              value={name}
              onChange={(e) => setName(e.target.value)}
              required
            />
          </EuiFormRow>

          <EuiFormRow label="Source Type" required>
            <EuiSelect
              options={dataSourceTypeOptions}
              value={sourceType}
              onChange={(e) => setSourceType(e.target.value)}
              required
            />
          </EuiFormRow>

          {renderSourceSpecificFields()}

          <EuiFormRow label="Timestamp Field" required>
            <EuiFieldText
              name="timestampField"
              value={timestampField}
              onChange={(e) => setTimestampField(e.target.value)}
              required
              placeholder="event_timestamp"
            />
          </EuiFormRow>

          <EuiAccordion id="additionalFields" buttonContent="Additional Fields">
            <EuiPanel>
              <EuiFormRow label="Created Timestamp Column">
                <EuiFieldText
                  name="createdTimestampColumn"
                  value={createdTimestampColumn}
                  onChange={(e) => setCreatedTimestampColumn(e.target.value)}
                  placeholder="created_timestamp"
                />
              </EuiFormRow>

              <EuiFormRow label="Event Timestamp Column">
                <EuiFieldText
                  name="eventTimestampColumn"
                  value={eventTimestampColumn}
                  onChange={(e) => setEventTimestampColumn(e.target.value)}
                  placeholder="event_timestamp"
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
            </EuiPanel>
          </EuiAccordion>

          <EuiSpacer />

          <EuiFlexGroup>
            <EuiFlexItem grow={false}>
              <EuiButton
                type="submit"
                fill
                isLoading={isSubmitting}
                disabled={isSubmitting || !name || !sourceType || !timestampField}
              >
                Create Data Source
              </EuiButton>
            </EuiFlexItem>
            <EuiFlexItem grow={false}>
              <EuiButton
                onClick={() => navigate(`/p/${projectName}/data-source/`)}
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

export default CreateDataSourcePage;
