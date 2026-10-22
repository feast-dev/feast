# Project

Projects provide complete isolation of feature stores at the infrastructure level. This is accomplished through resource namespacing, e.g., prefixing table names with the associated project. Each project should be considered a completely separate universe of entities and features. It is not possible to retrieve features from multiple projects in a single request. We recommend having a single feature store and a single project per environment (`dev`, `staging`, `prod`).

![](<../../.gitbook/assets/image (7).png>)

Users define one or more [feature views](feature-view.md) within a project. Each feature view contains one or more [features](feature-view.md#field). These features typically relate to one or more [entities](entity.md). A feature view must always have a [data source](data-ingestion.md), which in turn is used during the generation of training [datasets](feature-retrieval.md#dataset) and when materializing feature values into the online store.

The concept of a "project" provide the following benefits:

**Logical Grouping**: Projects group related features together, making it easier to manage and track them.

**Feature Definitions**: Within a project, you can define features, including their metadata, types, and sources. This helps standardize how features are created and consumed.

**Isolation**: Projects provide a way to isolate different environments, such as development, testing, and production, ensuring that changes in one project do not affect others.

**Collaboration**: By organizing features within projects, teams can collaborate more effectively, with clear boundaries around the features they are responsible for.

**Access Control**: Projects can implement permissions, allowing different users or teams to access only the features relevant to their work.