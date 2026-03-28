# ADR-0002: Component Refactor

## Status

Accepted

## Context

The Feast project originally existed as a single monolithic repository containing many tightly coupled components: Core Registry, Serving Service, Job Service, Client Libraries, Spark ingestion code, Helm charts, and Terraform configurations.

Two distinct user groups were identified:

- **Platform teams**: Capable of running a complete feature store on Kubernetes with Spark, managing large-scale infrastructure.
- **Solution teams**: Small data science or data engineering teams wanting to solve ML business problems without deploying and managing Kubernetes or Spark clusters.

Delivering a viable minimal product to solution teams required a lighter-weight approach. However, the monolithic codebase made this difficult due to tight coupling between components.

## Decision

Adopt a staged approach to decouple the Feast codebase into modular, composable components:

### Stage 1: Move Out Non-Core Components

Split the monorepo into focused repositories:

- **feast** (main repo): Feast Python SDK, Documentation, and Protos (starting at v0.10.0).
- **feast-java**: Core Registry, Serving, and Java Client.
- **feast-spark**: Spark Ingestion, Spark Python SDK, and Job Service.
- **feast-helm-charts**: Helm charts for Kubernetes deployments.

### Stage 2: Document Contracts

Document all component-level contracts (I/O), API specifications (Protobuf), data contracts, and architecture diagrams.

### Stage 3: Remove Coupling

Remove unnecessary coupling between components, keeping only service contracts (Protobuf), data contracts, and integration tests as shared dependencies.

### Stage 4: Converge

Reverse the relationship so the main Feast SDK can use Spark-related code as a specific compute provider, rather than requiring it.

### Key Principles

- The main Feast repository provides a fully functional Python-based feature store that works without infrastructure dependencies.
- Spark and Kubernetes-based components remain available for platform teams.
- All existing functionality is maintained with no breaking changes during the transition.

## Consequences

### Positive

- Enabled a super lightweight core framework for Feast that teams can start with in seconds.
- Made it possible for teams to pick and choose components they want to adopt.
- Teams with existing internal implementations (ingestion, registry, serving) can integrate more easily.
- The Python SDK became the primary entry point, significantly lowering the barrier to getting started.

### Negative

- Temporary divergence between Feast and Feast-Spark codebases during the transition.
- Multiple repositories added coordination overhead during the migration period.

### Neutral

- Components have since been reconverged into the main repository with a cleaner separation of concerns.
- The Go, Java, and Python SDKs coexist in the main repository under separate directories.

## References

- Original RFC: [Feast RFC-020: Component Refactor](https://docs.google.com/document/d/1CjR3Ph3l65hF5bRuchR9u9WSoirnIuEb7ILY9Ioh1Sk/edit)
- GitHub Discussion: [#1353](https://github.com/feast-dev/feast/discussions/1353)
