import React, { useEffect, useRef, useState } from "react";
import {
  EuiPanel,
  EuiTitle,
  EuiHorizontalRule,
  EuiButton,
  EuiHealth,
  EuiText,
  EuiSpacer,
  EuiBadge,
  EuiBasicTable,
} from "@elastic/eui";
import {
  useValidateDataSource,
  ValidateDataSourceResult,
} from "../../queries/mutations/useDataSourceMutations";

interface ConnectionStatusPanelProps {
  dataSourceName: string;
  projectName: string;
}

const STATUS_META: Record<string, { color: string; label: string }> = {
  success: { color: "success", label: "Connected" },
  failed: { color: "danger", label: "Connection failed" },
  not_applicable: { color: "subdued", label: "Not testable" },
  unavailable: { color: "subdued", label: "Unavailable" },
};

function formatTestedAt(iso?: string): string {
  if (!iso) return "";
  try {
    return new Date(iso).toLocaleString("en-US", {
      dateStyle: "medium",
      timeStyle: "short",
    });
  } catch {
    return "";
  }
}

function storageKey(project: string, name: string): string {
  return `feast:ds-test:${project}:${name}`;
}

function readStored(key: string): ValidateDataSourceResult | null {
  try {
    const raw = window.localStorage.getItem(key);
    return raw ? (JSON.parse(raw) as ValidateDataSourceResult) : null;
  } catch {
    return null;
  }
}

const ConnectionStatusPanel: React.FC<ConnectionStatusPanelProps> = ({
  dataSourceName,
  projectName,
}) => {
  const validate = useValidateDataSource();
  const { reset } = validate;
  const key = storageKey(projectName, dataSourceName);

  const testedKeyRef = useRef<string | null>(null);

  const [persisted, setPersisted] = useState<ValidateDataSourceResult | null>(
    () => readStored(key),
  );

  useEffect(() => {
    reset();
    testedKeyRef.current = null;
    setPersisted(readStored(key));
  }, [key, reset]);

  const belongsToCurrent = testedKeyRef.current === key;

  useEffect(() => {
    if (belongsToCurrent && validate.isSuccess && validate.data) {
      const r = validate.data as ValidateDataSourceResult;
      setPersisted(r);
      try {
        window.localStorage.setItem(key, JSON.stringify(r));
      } catch {
        void 0;
      }
    }
  }, [belongsToCurrent, validate.isSuccess, validate.data, key]);

  const liveResult =
    belongsToCurrent && validate.isSuccess
      ? (validate.data as ValidateDataSourceResult | undefined)
      : undefined;
  const result = liveResult ?? persisted ?? undefined;
  const statusMeta = result ? STATUS_META[result.status] : undefined;

  const isTesting = validate.isLoading && belongsToCurrent;
  const showError = validate.isError && belongsToCurrent;

  const runTest = () => {
    testedKeyRef.current = key;
    validate.mutate({ name: dataSourceName, project: projectName });
  };

  return (
    <EuiPanel hasBorder grow={false}>
      <EuiTitle size="xs">
        <h3>Connection Status</h3>
      </EuiTitle>
      <EuiHorizontalRule margin="xs" />
      <EuiButton
        size="s"
        iconType="namespace"
        onClick={runTest}
        isLoading={isTesting}
        fullWidth
      >
        {result ? "Re-test connection" : "Test connection"}
      </EuiButton>
      <EuiSpacer size="m" />

      {isTesting && <EuiHealth color="subdued">Testing connection…</EuiHealth>}

      {!isTesting && showError && (
        <EuiHealth color="danger">
          {(validate.error as Error)?.message || "Connection test failed"}
        </EuiHealth>
      )}

      {!isTesting && result && statusMeta && (
        <>
          <EuiHealth color={statusMeta.color as any}>
            {statusMeta.label}
          </EuiHealth>
          <EuiSpacer size="xs" />
          <EuiText size="xs" color="subdued">
            {result.message}
          </EuiText>

          {result.columns && result.columns.length > 0 && (
            <>
              <EuiSpacer size="s" />
              <EuiText size="xs">
                <strong>Schema ({result.columns.length})</strong>
              </EuiText>
              <EuiSpacer size="xs" />
              <div style={{ maxHeight: 220, overflowY: "auto" }}>
                <EuiBasicTable
                  compressed
                  items={result.columns}
                  columns={[
                    {
                      name: "Column",
                      field: "name",
                      render: (name: string) => (
                        <code style={{ fontSize: 11 }}>{name}</code>
                      ),
                    },
                    {
                      name: "Type",
                      field: "type",
                      render: (type: string) => (
                        <EuiBadge color="hollow">{type}</EuiBadge>
                      ),
                    },
                  ]}
                />
              </div>
            </>
          )}

          {result.testedAt && (
            <>
              <EuiSpacer size="xs" />
              <EuiText size="xs" color="subdued">
                Last tested: {formatTestedAt(result.testedAt)}
              </EuiText>
            </>
          )}
        </>
      )}

      {!isTesting && !result && !showError && (
        <EuiText size="xs" color="subdued">
          Run a test to check connectivity to the underlying storage.
        </EuiText>
      )}
    </EuiPanel>
  );
};

export default ConnectionStatusPanel;
