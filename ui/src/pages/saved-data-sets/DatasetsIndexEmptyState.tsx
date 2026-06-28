import React from "react";
import {
  EuiEmptyPrompt,
  EuiTitle,
  EuiLink,
  EuiButton,
  EuiFlexGroup,
  EuiFlexItem,
} from "@elastic/eui";
import FeastIconBlue from "../../graphics/FeastIconBlue";

interface DatasetsIndexEmptyStateProps {
  onRegister?: () => void;
}

const DatasetsIndexEmptyState = ({
  onRegister,
}: DatasetsIndexEmptyStateProps) => {
  return (
    <EuiEmptyPrompt
      iconType={FeastIconBlue}
      title={<h2>Your Data Catalog is empty</h2>}
      body={
        <p>
          The Data Catalog lets you create, link, share, and reuse curated
          feature datasets for model training and validation. Link an existing
          data artifact or create a new one by running a feature retrieval job.
        </p>
      }
      actions={
        <EuiFlexGroup gutterSize="s" justifyContent="center">
          {onRegister && (
            <EuiFlexItem grow={false}>
              <EuiButton fill iconType="plus" onClick={onRegister}>
                Add to Catalog
              </EuiButton>
            </EuiFlexItem>
          )}
          <EuiFlexItem grow={false}>
            <EuiButton
              onClick={() => {
                window.open(
                  "https://docs.feast.dev/getting-started/concepts/dataset#creating-saved-dataset-from-historical-retrieval",
                  "_blank",
                );
              }}
            >
              Open Dataset Docs
            </EuiButton>
          </EuiFlexItem>
        </EuiFlexGroup>
      }
      footer={
        <>
          <EuiTitle size="xxs">
            <h3>Want to learn more?</h3>
          </EuiTitle>
          <EuiLink href="https://docs.feast.dev/" target="_blank">
            Read Feast documentation
          </EuiLink>
        </>
      }
    />
  );
};

export default DatasetsIndexEmptyState;
