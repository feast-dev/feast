# Architecture Decision Records (ADR)

This directory contains Architecture Decision Records (ADRs) for the Feast project. ADRs document significant architectural decisions made during the development of Feast, providing context, rationale, and consequences for each decision.

## What is an ADR?

An Architecture Decision Record captures a single architectural decision, including the context in which it was made, the decision itself, and the expected consequences. ADRs serve as a historical record for current and future contributors to understand why the project is structured the way it is.

## ADR Index

| ADR | Title | Status | Original RFC |
|-----|-------|--------|-------------|
| [ADR-0001](ADR-0001-feature-services.md) | Feature Services | Accepted | RFC-015 |
| [ADR-0002](ADR-0002-component-refactor.md) | Component Refactor | Accepted | RFC-020 |
| [ADR-0003](ADR-0003-on-demand-transformations.md) | On-Demand Transformations | Accepted | RFC-021 |
| [ADR-0004](ADR-0004-entity-join-key-mapping.md) | Entity Join Key Mapping | Accepted | RFC-023 |
| [ADR-0005](ADR-0005-stream-transformations.md) | Stream Transformations | Accepted | RFC-036 |
| [ADR-0006](ADR-0006-kubernetes-operator.md) | Kubernetes Operator | Accepted | RFC-042 |
| [ADR-0007](ADR-0007-unified-feature-transformations.md) | Unified Feature Transformations and Feature Views | Accepted | RFC-043 |
| [ADR-0008](ADR-0008-feature-view-versioning.md) | Feature View Versioning | Accepted | Feature View Versioning RFC |
| [ADR-0009](ADR-0009-contribution-extensibility.md) | Contribution and Extensibility Architecture | Accepted | RFC-014 |
| [ADR-0010](ADR-0010-vector-database-integration.md) | Vector Database Integration for LLM/RAG Support | Accepted | RFC-040 |
| [ADR-0011](ADR-0011-data-quality-monitoring.md) | Data Quality Monitoring | Accepted | RFC-027 |

## Creating a New ADR

1. Copy the [ADR template](ADR-TEMPLATE.md) to a new file with the next sequential number.
2. Fill in all sections of the template.
3. Submit a pull request with the new ADR.
4. Once the RFC is finalized and approved, update the ADR status to "Accepted".

## ADR Statuses

- **Proposed**: The decision is under discussion.
- **Accepted**: The decision has been accepted and is being (or has been) implemented.
- **Deprecated**: The decision is no longer relevant due to changes in the project.
- **Superseded**: The decision has been replaced by a newer ADR.
