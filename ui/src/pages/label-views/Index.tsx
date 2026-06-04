import React from "react";
import { useParams } from "react-router-dom";

import {
  EuiPageTemplate,
  EuiLoadingSpinner,
  EuiBasicTable,
  EuiBasicTableColumn,
  EuiBadge,
  EuiEmptyPrompt,
  EuiTitle,
  EuiLink,
} from "@elastic/eui";

import { LabelViewIcon } from "../../graphics/LabelViewIcon";
import { useDocumentTitle } from "../../hooks/useDocumentTitle";
import useResourceQuery, {
  labelViewListPath,
  restLabelViewsFromResponse,
} from "../../queries/useResourceQuery";

const useLoadLabelViews = () => {
  const { projectName } = useParams();
  return useResourceQuery<any[]>({
    resourceType: "label-views-list",
    project: projectName,
    restPath: labelViewListPath(projectName),
    restSelect: restLabelViewsFromResponse,
  });
};

interface LabelViewRow {
  name: string;
  entities: string[];
  conflictPolicy: string;
  labelerField: string;
  online: boolean;
  description: string;
}

const LabelViewsListingTable = ({ labelViews }: { labelViews: any[] }) => {
  const { projectName } = useParams();

  const rows: LabelViewRow[] = labelViews.map((lv: any) => {
    const spec = lv.spec || {};
    return {
      name: spec.name || "Unknown",
      entities: spec.entities || [],
      conflictPolicy: spec.conflictPolicy || "LAST_WRITE_WINS",
      labelerField: spec.labelerField || "labeler",
      online: spec.online !== false,
      description: spec.description || "",
    };
  });

  const columns: EuiBasicTableColumn<LabelViewRow>[] = [
    {
      field: "name",
      name: "Name",
      sortable: true,
      render: (name: string) => (
        <EuiLink href={`/p/${projectName}/label-view/${name}`}>{name}</EuiLink>
      ),
    },
    {
      field: "entities",
      name: "Entities",
      render: (entities: string[]) =>
        entities.length > 0
          ? entities.map((e, i) => (
              <React.Fragment key={e}>
                {i > 0 && ", "}
                <EuiLink href={`/p/${projectName}/entity/${e}`}>{e}</EuiLink>
              </React.Fragment>
            ))
          : "-",
    },
    {
      field: "conflictPolicy",
      name: "Conflict Policy",
      render: (policy: string) => {
        const color =
          policy === "LAST_WRITE_WINS"
            ? "default"
            : policy === "MAJORITY_VOTE"
              ? "primary"
              : "accent";
        return <EuiBadge color={color}>{policy}</EuiBadge>;
      },
    },
    {
      field: "labelerField",
      name: "Labeler Field",
    },
    {
      field: "online",
      name: "Online",
      render: (online: boolean) => (
        <EuiBadge color={online ? "success" : "default"}>
          {online ? "Yes" : "No"}
        </EuiBadge>
      ),
    },
    {
      field: "description",
      name: "Description",
      truncateText: true,
    },
  ];

  return (
    <EuiBasicTable<LabelViewRow>
      items={rows}
      columns={columns}
      tableLayout="auto"
    />
  );
};

const LabelViewIndexEmptyState = () => (
  <EuiEmptyPrompt
    iconType={LabelViewIcon}
    title={
      <EuiTitle size="s">
        <h2>No Label Views</h2>
      </EuiTitle>
    }
    body={
      <p>
        Label views manage mutable labels and annotations for agent
        interactions, safety monitoring, and RLHF pipelines. Define a LabelView
        in your feature repository to get started.
      </p>
    }
  />
);

const Index = () => {
  const { isLoading, isSuccess, isError, data } = useLoadLabelViews();

  useDocumentTitle(`Label Views | Feast`);

  return (
    <EuiPageTemplate panelled>
      <EuiPageTemplate.Header
        restrictWidth
        iconType={LabelViewIcon}
        pageTitle="Label Views"
      />
      <EuiPageTemplate.Section>
        {isLoading && (
          <p>
            <EuiLoadingSpinner size="m" /> Loading
          </p>
        )}
        {isError && <p>We encountered an error while loading.</p>}
        {isSuccess && (!data || data.length === 0) && (
          <LabelViewIndexEmptyState />
        )}
        {isSuccess && data && data.length > 0 && (
          <LabelViewsListingTable labelViews={data} />
        )}
      </EuiPageTemplate.Section>
    </EuiPageTemplate>
  );
};

export default Index;
