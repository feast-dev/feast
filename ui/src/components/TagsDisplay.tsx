import React from "react";
import {
  EuiDescriptionList,
  EuiDescriptionListDescription,
  EuiDescriptionListTitle,
} from "@elastic/eui";
import EuiCustomLink from "./EuiCustomLink";

interface TagsDisplayProps {
  createLink?: (key: string, value: string) => string;
  tags: Record<string, string>;
}

const TagsDisplay = ({ tags, createLink }: TagsDisplayProps) => {
  return (
    <EuiDescriptionList textStyle="reverse">
      {Object.entries(tags).map(([key, value]) => {
        return (
          <React.Fragment key={key}>
            <EuiDescriptionListTitle>{key}</EuiDescriptionListTitle>
            <EuiDescriptionListDescription>
              {createLink ? (
                <EuiCustomLink to={createLink(key, value)}>
                  {value}
                </EuiCustomLink>
              ) : (
                value
              )}
            </EuiDescriptionListDescription>
          </React.Fragment>
        );
      })}
    </EuiDescriptionList>
  );
};

export default TagsDisplay;
