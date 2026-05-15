# Feast Operator Configuration Guides

These guides cover the `FeatureStore` Custom Resource (CR) from an **operator perspective** —
what to put in the CR, how the operator translates it into Kubernetes objects, and where to
look for store-specific YAML options in the Feast SDK docs.

## How the docs are organised

| Layer | Source | What it covers |
|-------|--------|----------------|
| **CR field reference** | [`api/markdown/ref.md`](https://github.com/feast-dev/feast/blob/stable/infra/feast-operator/docs/api/markdown/ref.md) | Every field, type, validation constraint — auto-generated from Go types |
| **Operator guides** (this folder) | Narrative how-tos | CR→K8s behavior, Secret formats, PVC patterns, operator-specific trade-offs |
| **Feast SDK reference** | [`feast/docs/reference/`](../reference/) | Store-specific `feature_store.yaml` options (online/offline store drivers, registry drivers, etc.) |
| **Sample CRs** | [`config/samples/`](https://github.com/feast-dev/feast/blob/stable/infra/feast-operator/config/samples/) | Copy-paste starting points for common configurations |

> **Rule of thumb**: if you need to know *what to put in the CR and why*, read the operator
> guides. If you need to know *which keys a particular store driver accepts*, read the Feast
> SDK docs for that store.

---

## Guide index

| # | Guide | Topic |
|---|-------|-------|
| 1 | [Project Provisioning](01-project-provisioning.md) | `feastProjectDir`: cloning a git repo vs `feast init` templates |
| 2 | [Persistence](02-persistence.md) | File (path + PVC) vs DB store for offline/online/registry; Secret format |
| 3 | [Serving & Observability](03-serving-and-observability.md) | Feature server workers, log level, Prometheus metrics, offline push batching, MCP |
| 4 | [Registry Topology](04-registry-topology.md) | Local vs remote registry, cross-namespace `feastRef`, remote TLS |
| 5 | [Security](05-security.md) | Kubernetes RBAC roles vs OIDC auth; TLS for all servers |
| 6 | [Batch Jobs](06-batch-and-jobs.md) | `batchEngine` ConfigMap contract, `cronJob` for scheduled materialization |
| 7 | [OpenLineage & Materialization](07-openlineage-and-materialization.md) | Lineage transports, API key Secret, materialization batch size |

---

## Quick-start: which guide do I need?

- **"How do I point the operator at my existing git feature repo?"** → [Guide 1](01-project-provisioning.md)
- **"How do I wire Postgres/Redis/DuckDB as my store?"** → [Guide 2](02-persistence.md)
- **"How do I enable Prometheus scraping for the feature server?"** → [Guide 3](03-serving-and-observability.md)
- **"How do I make all services share a remote registry?"** → [Guide 4](04-registry-topology.md)
- **"How do I enable Kubernetes RBAC or OIDC auth?"** → [Guide 5](05-security.md)
- **"How do I schedule nightly materialization?"** → [Guide 6](06-batch-and-jobs.md)
- **"How do I send lineage events to Marquez?"** → [Guide 7](07-openlineage-and-materialization.md)
- **"What are all valid fields on `ServingConfig`?"** → [API ref](https://github.com/feast-dev/feast/blob/stable/infra/feast-operator/docs/api/markdown/ref.md#servingconfig)

---

## Scaling & HA

Horizontal scaling, HPA, PodDisruptionBudget, affinity, and topology spread constraints are
covered in the main Feast docs:

→ [Horizontal Scaling with the Feast Operator](../scaling-feast.md#horizontal-scaling-with-the-feast-operator)
