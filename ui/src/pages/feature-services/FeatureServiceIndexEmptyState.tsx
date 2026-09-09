import React from "react";
import { EuiEmptyPrompt, EuiTitle, EuiLink, EuiButton } from "@elastic/eui";
import FeastIconBlue from "../../graphics/FeastIconBlue";

interface FeatureServiceIndexEmptyStateProps {
  onCreate?: () => void;
}

const FeatureServiceIndexEmptyState: React.FC<
  FeatureServiceIndexEmptyStateProps
> = ({ onCreate }) => {
  return (
    <EuiEmptyPrompt
      iconType={FeastIconBlue}
      title={<h2>There are no feature services</h2>}
      body={
        <p>
          Feature services group related features from one or more feature views
          for training or online serving. Create your first feature service to
          get started.
        </p>
      }
      actions={
        onCreate ? (
          <EuiButton fill iconType="plus" onClick={onCreate}>
            Create Feature Service
          </EuiButton>
        ) : (
          <EuiButton
            onClick={() => {
              window.open(
                "https://docs.feast.dev/getting-started/concepts/feature-retrieval#feature-services",
                "_blank",
              );
            }}
          >
            Open Feature Services Docs
          </EuiButton>
        )
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

export default FeatureServiceIndexEmptyState;
