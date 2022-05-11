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
  owner?: string;
  description?: string;
}

const TagsDisplay = ({
  tags,
  createLink,
  owner,
  description,
}: TagsDisplayProps) => {
  return (
    <EuiDescriptionList textStyle="reverse">
      {owner ? (
        <React.Fragment key={"owner"}>
          <EuiDescriptionListTitle>owner</EuiDescriptionListTitle>
          <EuiDescriptionListDescription>{owner}</EuiDescriptionListDescription>
        </React.Fragment>
      ) : (
        ""
      )}
      {description ? (
        <React.Fragment key={"description"}>
          <EuiDescriptionListTitle>description</EuiDescriptionListTitle>
          <EuiDescriptionListDescription>
            {description}
          </EuiDescriptionListDescription>
        </React.Fragment>
      ) : (
        ""
      )}
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
