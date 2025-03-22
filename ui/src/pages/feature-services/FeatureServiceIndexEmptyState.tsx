import React from "react";
import { EuiEmptyPrompt, EuiTitle, EuiLink, EuiButton } from "@elastic/eui";
import FeastIconBlue from "../../graphics/FeastIconBlue";

const FeatureServiceIndexEmptyState = () => {
  return (
    <EuiEmptyPrompt
      iconType={FeastIconBlue}
      title={<h2>There are no feature services</h2>}
      body={
        <p>
          This project does not have any Feature Services. Learn more about
          creating Feature Services in Feast Docs.
        </p>
      }
      actions={
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
