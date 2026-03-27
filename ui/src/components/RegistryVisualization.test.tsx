import React from "react";
import { render, screen, within } from "../test-utils";
import { waitFor } from "@testing-library/react";
import RegistryVisualization from "./RegistryVisualization";
import { feast } from "../protos";
import { FEAST_FCO_TYPES } from "../parsers/types";
import { EntityRelation } from "../parsers/parseEntityRelationships";
import { ThemeProvider } from "../contexts/ThemeContext";

// ReactFlow requires ResizeObserver
class ResizeObserverMock {
  observe() {}
  unobserve() {}
  disconnect() {}
}
(global as any).ResizeObserver = ResizeObserverMock;

// Helper to build minimal registry data
const makeRegistry = (
  overrides: Partial<feast.core.IRegistry> = {},
): feast.core.Registry => {
  return feast.core.Registry.create({
    featureViews: [],
    onDemandFeatureViews: [],
    streamFeatureViews: [],
    featureServices: [],
    entities: [],
    dataSources: [],
    ...overrides,
  });
};

const makeRelationship = (
  sourceName: string,
  sourceType: FEAST_FCO_TYPES,
  targetName: string,
  targetType: FEAST_FCO_TYPES,
): EntityRelation => ({
  source: { name: sourceName, type: sourceType },
  target: { name: targetName, type: targetType },
});

const renderVisualization = (
  registryData: feast.core.Registry,
  relationships: EntityRelation[] = [],
  indirectRelationships: EntityRelation[] = [],
) => {
  return render(
    <ThemeProvider>
      <RegistryVisualization
        registryData={registryData}
        relationships={relationships}
        indirectRelationships={indirectRelationships}
      />
    </ThemeProvider>,
  );
};

describe("RegistryVisualization version indicators", () => {
  test("renders version badge on feature view with currentVersionNumber > 0", async () => {
    const registry = makeRegistry({
      featureViews: [
        feast.core.FeatureView.create({
          spec: { name: "my_feature_view" },
          meta: { currentVersionNumber: 3 },
        }),
      ],
      entities: [
        feast.core.Entity.create({
          spec: { name: "my_entity" },
        }),
      ],
    });

    const relationships = [
      makeRelationship(
        "my_feature_view",
        FEAST_FCO_TYPES.featureView,
        "my_entity",
        FEAST_FCO_TYPES.entity,
      ),
    ];

    renderVisualization(registry, relationships);

    await waitFor(() => {
      expect(screen.getByText("v3")).toBeInTheDocument();
    });
  });

  test("does not render version badge when currentVersionNumber is 0", async () => {
    const registry = makeRegistry({
      featureViews: [
        feast.core.FeatureView.create({
          spec: { name: "unversioned_fv" },
          meta: { currentVersionNumber: 0 },
        }),
      ],
      entities: [
        feast.core.Entity.create({
          spec: { name: "my_entity" },
        }),
      ],
    });

    const relationships = [
      makeRelationship(
        "unversioned_fv",
        FEAST_FCO_TYPES.featureView,
        "my_entity",
        FEAST_FCO_TYPES.entity,
      ),
    ];

    renderVisualization(registry, relationships);

    await waitFor(() => {
      expect(screen.getByText("unversioned_fv")).toBeInTheDocument();
    });

    expect(screen.queryByText("v0")).not.toBeInTheDocument();
  });

  test("does not render version badge when meta has no version", async () => {
    const registry = makeRegistry({
      featureViews: [
        feast.core.FeatureView.create({
          spec: { name: "no_version_fv" },
          meta: {},
        }),
      ],
      entities: [
        feast.core.Entity.create({
          spec: { name: "my_entity" },
        }),
      ],
    });

    const relationships = [
      makeRelationship(
        "no_version_fv",
        FEAST_FCO_TYPES.featureView,
        "my_entity",
        FEAST_FCO_TYPES.entity,
      ),
    ];

    renderVisualization(registry, relationships);

    await waitFor(() => {
      expect(screen.getByText("no_version_fv")).toBeInTheDocument();
    });

    // No version badges should exist at all
    const badges = screen.queryAllByText(/^v\d+$/);
    expect(badges).toHaveLength(0);
  });

  test("renders version badge on on-demand feature view", async () => {
    const registry = makeRegistry({
      featureViews: [
        feast.core.FeatureView.create({
          spec: { name: "regular_fv" },
        }),
      ],
      onDemandFeatureViews: [
        feast.core.OnDemandFeatureView.create({
          spec: { name: "odfv_versioned" },
          meta: { currentVersionNumber: 2 },
        }),
      ],
      entities: [
        feast.core.Entity.create({
          spec: { name: "my_entity" },
        }),
      ],
    });

    // Connect regular FV to entity so nodes render, and ODFV to regular FV
    const relationships = [
      makeRelationship(
        "regular_fv",
        FEAST_FCO_TYPES.featureView,
        "my_entity",
        FEAST_FCO_TYPES.entity,
      ),
    ];

    // Use the full component and toggle isolated nodes to show ODFV
    render(
      <ThemeProvider>
        <RegistryVisualization
          registryData={registry}
          relationships={relationships}
          indirectRelationships={[]}
        />
      </ThemeProvider>,
    );

    // Enable isolated nodes to show the ODFV
    const isolatedCheckbox = screen.getByLabelText(
      /Show Objects Without Relationships/i,
    );
    isolatedCheckbox.click();

    await waitFor(() => {
      expect(screen.getByText("v2")).toBeInTheDocument();
    });
  });

  test("renders version badge on stream feature view", async () => {
    const registry = makeRegistry({
      featureViews: [
        feast.core.FeatureView.create({
          spec: { name: "regular_fv" },
        }),
      ],
      streamFeatureViews: [
        feast.core.StreamFeatureView.create({
          spec: { name: "sfv_versioned" },
          meta: { currentVersionNumber: 5 },
        }),
      ],
      entities: [
        feast.core.Entity.create({
          spec: { name: "my_entity" },
        }),
      ],
    });

    const relationships = [
      makeRelationship(
        "regular_fv",
        FEAST_FCO_TYPES.featureView,
        "my_entity",
        FEAST_FCO_TYPES.entity,
      ),
    ];

    render(
      <ThemeProvider>
        <RegistryVisualization
          registryData={registry}
          relationships={relationships}
          indirectRelationships={[]}
        />
      </ThemeProvider>,
    );

    // Enable isolated nodes to show the SFV
    const isolatedCheckbox = screen.getByLabelText(
      /Show Objects Without Relationships/i,
    );
    isolatedCheckbox.click();

    await waitFor(() => {
      expect(screen.getByText("v5")).toBeInTheDocument();
    });
  });

  test("renders version history info from featureViewVersionHistory", async () => {
    const registry = makeRegistry({
      featureViews: [
        feast.core.FeatureView.create({
          spec: { name: "versioned_fv" },
          meta: { currentVersionNumber: 2 },
        }),
      ],
      entities: [
        feast.core.Entity.create({
          spec: { name: "my_entity" },
        }),
      ],
      featureViewVersionHistory:
        feast.core.FeatureViewVersionHistory.create({
          records: [
            feast.core.FeatureViewVersionRecord.create({
              featureViewName: "versioned_fv",
              versionNumber: 1,
              description: "Initial version",
            }),
            feast.core.FeatureViewVersionRecord.create({
              featureViewName: "versioned_fv",
              versionNumber: 2,
              description: "Added new features",
            }),
          ],
        }),
    });

    const relationships = [
      makeRelationship(
        "versioned_fv",
        FEAST_FCO_TYPES.featureView,
        "my_entity",
        FEAST_FCO_TYPES.entity,
      ),
    ];

    renderVisualization(registry, relationships);

    await waitFor(() => {
      expect(screen.getByText("v2")).toBeInTheDocument();
    });
  });
});

describe("RegistryVisualization legend", () => {
  test("renders Version Changed entry in legend", async () => {
    const registry = makeRegistry({
      featureViews: [
        feast.core.FeatureView.create({
          spec: { name: "some_fv" },
        }),
      ],
      entities: [
        feast.core.Entity.create({
          spec: { name: "some_entity" },
        }),
      ],
    });

    const relationships = [
      makeRelationship(
        "some_fv",
        FEAST_FCO_TYPES.featureView,
        "some_entity",
        FEAST_FCO_TYPES.entity,
      ),
    ];

    renderVisualization(registry, relationships);

    await waitFor(() => {
      expect(screen.getByText("Version Changed")).toBeInTheDocument();
    });

    expect(screen.getByText("vN")).toBeInTheDocument();
  });
});
