import React, { useEffect, useState, useContext, useCallback } from "react";
import {
  EuiPanel,
  EuiFlexGroup,
  EuiFlexItem,
  EuiLoadingSpinner,
  EuiText,
  EuiCallOut,
  EuiButton,
  EuiButtonEmpty,
  EuiSpacer,
  EuiProgress,
} from "@elastic/eui";
import { useNavigate, useParams } from "react-router-dom";
import RegistryPathContext from "../../contexts/RegistryPathContext";
import { useDataMode } from "../../contexts/DataModeContext";

interface JobStatusPanelProps {
  jobId: string;
  datasetName: string;
  onComplete?: () => void;
  onClose: () => void;
  onRetry?: () => void;
}

interface JobStatus {
  job_id: string;
  status: "pending" | "running" | "completed" | "failed";
  dataset_name?: string;
  error?: string;
  created_at?: string;
  completed_at?: string;
}

const JobStatusPanel = ({
  jobId,
  datasetName,
  onComplete,
  onClose,
  onRetry,
}: JobStatusPanelProps) => {
  const { projectName } = useParams();
  const registryUrl = useContext(RegistryPathContext);
  const { fetchOptions } = useDataMode();
  const navigate = useNavigate();

  const [status, setStatus] = useState<JobStatus>({
    job_id: jobId,
    status: "pending",
    dataset_name: datasetName,
  });
  const [pollError, setPollError] = useState<string | null>(null);

  const pollStatus = useCallback(async () => {
    try {
      const response = await fetch(
        `${registryUrl}/saved_datasets/jobs/${encodeURIComponent(jobId)}`,
        { method: "GET", ...fetchOptions },
      );
      if (!response.ok) {
        const err = await response
          .json()
          .catch(() => ({ detail: "Unknown error" }));
        throw new Error(
          err.detail || `Status check failed: ${response.status}`,
        );
      }
      const data: JobStatus = await response.json();
      setStatus(data);

      if (data.status === "completed" && onComplete) {
        onComplete();
      }
    } catch (err: any) {
      setPollError(err.message);
    }
  }, [jobId, registryUrl, fetchOptions, onComplete]);

  useEffect(() => {
    const interval = setInterval(() => {
      if (status.status === "pending" || status.status === "running") {
        pollStatus();
      }
    }, 3000);

    pollStatus();

    return () => clearInterval(interval);
  }, [pollStatus, status.status]);

  const isRunning = status.status === "pending" || status.status === "running";
  const isCompleted = status.status === "completed";
  const isFailed = status.status === "failed";

  return (
    <EuiPanel paddingSize="l" hasBorder>
      {isRunning && (
        <>
          <EuiFlexGroup alignItems="center" gutterSize="m">
            <EuiFlexItem grow={false}>
              <EuiLoadingSpinner size="l" />
            </EuiFlexItem>
            <EuiFlexItem>
              <EuiText>
                <h4>Creating dataset: {datasetName}</h4>
                <p>
                  <EuiText size="s" color="subdued">
                    Running feature retrieval and persisting results...
                  </EuiText>
                </p>
              </EuiText>
            </EuiFlexItem>
          </EuiFlexGroup>
          <EuiSpacer size="m" />
          <EuiProgress size="xs" color="primary" />
          <EuiSpacer size="m" />
          <EuiText size="xs" color="subdued">
            Job ID: {jobId} | Status: {status.status}
          </EuiText>
          <EuiSpacer size="m" />
          <EuiButtonEmpty onClick={onClose} size="s">
            Close (job continues in background)
          </EuiButtonEmpty>
        </>
      )}

      {isCompleted && (
        <>
          <EuiCallOut
            title="Dataset created successfully!"
            color="success"
            iconType="check"
          >
            <p>
              <strong>{datasetName}</strong> has been created and registered in
              the catalog.
            </p>
          </EuiCallOut>
          <EuiSpacer size="m" />
          <EuiFlexGroup gutterSize="s">
            <EuiFlexItem grow={false}>
              <EuiButton
                fill
                iconType="eye"
                onClick={() => {
                  onClose();
                  navigate(`/p/${projectName}/data-set/${datasetName}`);
                }}
              >
                View Dataset
              </EuiButton>
            </EuiFlexItem>
            <EuiFlexItem grow={false}>
              <EuiButtonEmpty onClick={onClose}>Close</EuiButtonEmpty>
            </EuiFlexItem>
          </EuiFlexGroup>
        </>
      )}

      {isFailed && (
        <>
          <EuiCallOut
            title="Dataset creation failed"
            color="danger"
            iconType="alert"
          >
            <p>
              {status.error ||
                "An unknown error occurred during dataset creation."}
            </p>
          </EuiCallOut>
          <EuiSpacer size="m" />
          <EuiText size="xs" color="subdued">
            Job ID: {jobId}
          </EuiText>
          <EuiSpacer size="m" />
          <EuiFlexGroup gutterSize="s">
            {onRetry && (
              <EuiFlexItem grow={false}>
                <EuiButton onClick={onRetry} iconType="arrowLeft">
                  Back to Form
                </EuiButton>
              </EuiFlexItem>
            )}
            <EuiFlexItem grow={false}>
              <EuiButtonEmpty onClick={onClose}>Close</EuiButtonEmpty>
            </EuiFlexItem>
          </EuiFlexGroup>
        </>
      )}

      {pollError && (
        <>
          <EuiSpacer size="s" />
          <EuiCallOut
            title="Error checking job status"
            color="warning"
            size="s"
          >
            <p>{pollError}</p>
          </EuiCallOut>
        </>
      )}
    </EuiPanel>
  );
};

export default JobStatusPanel;
