import React from "react";
import { EuiEmptyPrompt, EuiTitle, EuiLink, EuiButton } from "@elastic/eui";
import FeastIconBlue from "../../graphics/FeastIconBlue";

const EntityIndexEmptyState = () => {
  return (
    <EuiEmptyPrompt
      iconType={FeastIconBlue}
      title={<h2>There are no entities</h2>}
      body={
        <p>
          This project does not have any Entities. Learn more about creating
          Entities in Feast Docs.
        </p>
      }
      actions={
        <EuiButton
          onClick={() => {
            window.open(
              "https://docs.feast.dev/getting-started/concepts/entity",
              "_blank"
            );
          }}
        >
          Open Entities Docs
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

export default EntityIndexEmptyState;
