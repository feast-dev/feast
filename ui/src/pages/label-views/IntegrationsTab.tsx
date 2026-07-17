import React, { useContext, useEffect, useState } from "react";
import { useParams } from "react-router-dom";
import {
  EuiPanel,
  EuiTitle,
  EuiText,
  EuiLoadingSpinner,
  EuiCallOut,
  EuiCodeBlock,
  EuiSpacer,
  EuiFlexGroup,
  EuiFlexItem,
  EuiBadge,
  EuiIcon,
} from "@elastic/eui";
import RegistryPathContext from "../../contexts/RegistryPathContext";
import useLoadLabelView from "./useLoadLabelView";

const IntegrationsTab = () => {
  const { labelViewName } = useParams();
  const registryUrl = useContext(RegistryPathContext);
  const name = labelViewName || "";
  const { isLoading, isSuccess, data } = useLoadLabelView(name);

  const [webhookConfig, setWebhookConfig] = useState<any>(null);
  const [configLoading, setConfigLoading] = useState(true);

  useEffect(() => {
    if (isSuccess && data) {
      const baseUrl = registryUrl?.replace(/\/$/, "") || "/api/v1";
      fetch(`${baseUrl}/webhook/config/${name}`)
        .then((r) => r.json())
        .then((d) => {
          setWebhookConfig(d);
          setConfigLoading(false);
        })
        .catch(() => setConfigLoading(false));
    }
  }, [isSuccess, data, name, registryUrl]);

  if (isLoading || configLoading) {
    return (
      <p>
        <EuiLoadingSpinner size="m" /> Loading integration config...
      </p>
    );
  }

  if (!webhookConfig) {
    return (
      <EuiCallOut title="Failed to load config" color="danger" iconType="alert">
        Could not fetch webhook configuration.
      </EuiCallOut>
    );
  }

  const webhookPayload = JSON.stringify(webhookConfig.payload_example, null, 2);
  const baseUrl = window.location.origin;
  const webhookFullUrl = `${baseUrl}${webhookConfig.webhook_url}`;
  const batchFullUrl = `${baseUrl}${webhookConfig.batch_url}`;

  const argillaPython = `import argilla as rg
import requests
from datetime import datetime, timezone

# After annotation is complete, export and push to Feast LabelView
dataset = rg.load("your_dataset_name")
submitted = dataset.records(status="submitted").to_list(flatten=True)

records = []
for record in submitted:
    records.append({
        ${webhookConfig.entity_fields.map((e: string) => `"${e}": record.metadata["${e}"],`).join("\n        ")}
        ${webhookConfig.label_fields.map((f: string) => `"${f}": record.responses["${f}"],`).join("\n        ")}
        ${webhookConfig.labeler_field ? `"${webhookConfig.labeler_field}": record.user_id,` : ""}
        "event_timestamp": datetime.now(timezone.utc).isoformat(),
    })

response = requests.post(
    "${webhookFullUrl}",
    json={
        "push_source_name": "${webhookConfig.push_source_name}",
        "records": records,
    },
)
print(f"Pushed {len(records)} labels: {response.json()}")`;

  const labelStudioPython = `import requests
from datetime import datetime, timezone
from label_studio_sdk import Client

ls = Client(url="http://localhost:8080", api_key="YOUR_KEY")
project = ls.get_project(PROJECT_ID)

# Export completed annotations
tasks = project.get_labeled_tasks()

records = []
for task in tasks:
    annotation = task["annotations"][0]["result"][0]
    records.append({
        ${webhookConfig.entity_fields.map((e: string) => `"${e}": task["data"]["${e}"],`).join("\n        ")}
        ${webhookConfig.label_fields.map((f: string) => `"${f}": annotation["value"].get("${f}", ""),`).join("\n        ")}
        ${webhookConfig.labeler_field ? `"${webhookConfig.labeler_field}": str(task["annotations"][0]["completed_by"]),` : ""}
        "event_timestamp": datetime.now(timezone.utc).isoformat(),
    })

response = requests.post(
    "${webhookFullUrl}",
    json={
        "push_source_name": "${webhookConfig.push_source_name}",
        "records": records,
    },
)
print(f"Pushed {len(records)} labels: {response.json()}")`;

  const curlExample = `curl -X POST "${webhookFullUrl}" \\
  -H "Content-Type: application/json" \\
  -d '${webhookPayload}'`;

  return (
    <React.Fragment>
      <EuiCallOut
        title="External Annotation Tool Integration"
        color="primary"
        iconType="link"
      >
        <EuiText size="s">
          Connect Argilla, Label Studio, or any annotation tool to push labels
          into this LabelView via webhook or batch API.
        </EuiText>
      </EuiCallOut>

      <EuiSpacer size="l" />

      {/* Webhook Configuration */}
      <EuiPanel>
        <EuiFlexGroup alignItems="center" gutterSize="s">
          <EuiFlexItem grow={false}>
            <EuiIcon type="bolt" size="l" color="success" />
          </EuiFlexItem>
          <EuiFlexItem>
            <EuiTitle size="xs">
              <h3>Webhook Endpoint</h3>
            </EuiTitle>
          </EuiFlexItem>
          <EuiFlexItem grow={false}>
            <EuiBadge color="success">POST</EuiBadge>
          </EuiFlexItem>
        </EuiFlexGroup>
        <EuiSpacer size="s" />
        <EuiText size="s" color="subdued">
          Real-time label ingestion from annotation tools. Automatically adds
          timestamps if not provided.
        </EuiText>
        <EuiSpacer size="m" />
        <EuiFlexGroup>
          <EuiFlexItem>
            <strong>URL:</strong>
            <EuiCodeBlock language="text" paddingSize="s" isCopyable>
              {webhookFullUrl}
            </EuiCodeBlock>
          </EuiFlexItem>
        </EuiFlexGroup>
        <EuiSpacer size="s" />
        <EuiFlexGroup gutterSize="s" wrap>
          <EuiFlexItem grow={false}>
            <strong>Required fields:</strong>
          </EuiFlexItem>
          {webhookConfig.entity_fields.map((f: string) => (
            <EuiFlexItem grow={false} key={f}>
              <EuiBadge color="warning">{f} (entity)</EuiBadge>
            </EuiFlexItem>
          ))}
          {webhookConfig.label_fields.map((f: string) => (
            <EuiFlexItem grow={false} key={f}>
              <EuiBadge color="primary">{f} (label)</EuiBadge>
            </EuiFlexItem>
          ))}
          {webhookConfig.labeler_field && (
            <EuiFlexItem grow={false}>
              <EuiBadge color="accent">
                {webhookConfig.labeler_field} (labeler)
              </EuiBadge>
            </EuiFlexItem>
          )}
        </EuiFlexGroup>
        <EuiSpacer size="m" />
        <EuiTitle size="xxs">
          <h4>Example payload:</h4>
        </EuiTitle>
        <EuiCodeBlock language="json" paddingSize="s" isCopyable>
          {webhookPayload}
        </EuiCodeBlock>
      </EuiPanel>

      <EuiSpacer size="l" />

      {/* Batch Push */}
      <EuiPanel>
        <EuiFlexGroup alignItems="center" gutterSize="s">
          <EuiFlexItem grow={false}>
            <EuiIcon type="importAction" size="l" color="primary" />
          </EuiFlexItem>
          <EuiFlexItem>
            <EuiTitle size="xs">
              <h3>Batch Push Endpoint</h3>
            </EuiTitle>
          </EuiFlexItem>
          <EuiFlexItem grow={false}>
            <EuiBadge color="primary">POST</EuiBadge>
          </EuiFlexItem>
        </EuiFlexGroup>
        <EuiSpacer size="s" />
        <EuiText size="s" color="subdued">
          Upload bulk labels from CSV/parquet exports. Same schema as webhook.
        </EuiText>
        <EuiSpacer size="m" />
        <EuiCodeBlock language="text" paddingSize="s" isCopyable>
          {batchFullUrl}
        </EuiCodeBlock>
      </EuiPanel>

      <EuiSpacer size="l" />

      {/* cURL Example */}
      <EuiPanel>
        <EuiTitle size="xs">
          <h3>cURL Example</h3>
        </EuiTitle>
        <EuiSpacer size="s" />
        <EuiCodeBlock language="bash" paddingSize="s" isCopyable>
          {curlExample}
        </EuiCodeBlock>
      </EuiPanel>

      <EuiSpacer size="l" />

      {/* Argilla Integration */}
      <EuiPanel>
        <EuiFlexGroup alignItems="center" gutterSize="s">
          <EuiFlexItem grow={false}>
            <EuiIcon type="logoElastic" size="l" />
          </EuiFlexItem>
          <EuiFlexItem>
            <EuiTitle size="xs">
              <h3>Argilla Integration</h3>
            </EuiTitle>
          </EuiFlexItem>
        </EuiFlexGroup>
        <EuiSpacer size="s" />
        <EuiText size="s" color="subdued">
          Export annotated records from Argilla and push to this LabelView.
        </EuiText>
        <EuiSpacer size="m" />
        <EuiCodeBlock language="python" paddingSize="s" isCopyable>
          {argillaPython}
        </EuiCodeBlock>
      </EuiPanel>

      <EuiSpacer size="l" />

      {/* Label Studio Integration */}
      <EuiPanel>
        <EuiFlexGroup alignItems="center" gutterSize="s">
          <EuiFlexItem grow={false}>
            <EuiIcon type="tag" size="l" />
          </EuiFlexItem>
          <EuiFlexItem>
            <EuiTitle size="xs">
              <h3>Label Studio Integration</h3>
            </EuiTitle>
          </EuiFlexItem>
        </EuiFlexGroup>
        <EuiSpacer size="s" />
        <EuiText size="s" color="subdued">
          Export completed annotations from Label Studio and ingest via webhook.
        </EuiText>
        <EuiSpacer size="m" />
        <EuiCodeBlock language="python" paddingSize="s" isCopyable>
          {labelStudioPython}
        </EuiCodeBlock>
      </EuiPanel>
    </React.Fragment>
  );
};

export default IntegrationsTab;
