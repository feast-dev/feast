---
title: "Feast Meets Oracle: Bringing the World's #1 Enterprise Database to the Feature Store"
description: Oracle Database now has first-class support as a Feast offline store — complete with Kubernetes-native operator integration. Learn how to use your existing Oracle infrastructure for production ML feature engineering.
date: 2026-03-16
authors: ["Aniket Paluskar", "Srihari Venkataramaiah"]
---

<div class="hero-image">
  <img src="/images/blog/feast-oracle-offline-store.png" alt="Feast and Oracle Database" loading="lazy">
</div>

# Feast Meets Oracle: Bringing the World's #1 Enterprise Database to the Feature Store

## The Problem: Your Data Is Already in Oracle — Why Move It?

If you work in a Fortune 500 company, chances are your most valuable data lives in Oracle Database. It is the world's number one enterprise database for a reason — decades of battle-tested reliability, performance, and governance have made it the backbone of mission-critical systems across finance, healthcare, telecommunications, government, and retail.

But here's the friction: when ML teams want to build features for their models, they typically export data *out* of Oracle into some other system — a data lake, a warehouse, a CSV on someone's laptop. That data movement introduces latency, staleness bugs, security blind spots, and an entire class of silent failures that only surface when a model starts degrading in production.

**What if you didn't have to move your data at all?**

With Feast's new Oracle offline store support — now fully integrated into the Feast Kubernetes operator — you can define, compute, and serve ML features directly from your existing Oracle infrastructure. No data migration. No pipeline duct tape. No compromises.

---

## What's New

Oracle Database is now a first-class offline store in Feast, supported across the full stack:

| Layer | What Changed |
|---|---|
| **Python SDK** | `OracleOfflineStore` and `OracleSource` — a complete offline store implementation built on `ibis-framework[oracle]` |
| **Feast Operator (v1 API)** | `oracle` is a validated persistence type in the `FeatureStore` CRD, with Secret-backed credential management |
| **CRD & Validation** | Kubernetes validates `oracle` at admission time — bad configs are rejected before they ever reach the operator |
| **Type System** | Full Oracle-to-Feast type mapping covering `NUMBER`, `VARCHAR2`, `CLOB`, `BLOB`, `BINARY_FLOAT`, `TIMESTAMP`, and more |
| **Documentation** | Reference docs for the [Oracle offline store](https://docs.feast.dev/reference/offline-stores/oracle) and [Oracle data source](https://docs.feast.dev/reference/data-sources/oracle) |

This isn't a thin wrapper or a partial integration. The Oracle offline store supports the complete Feast offline store interface:

- **`get_historical_features`** — point-in-time correct feature retrieval for training datasets, preventing future data leakage
- **`pull_latest_from_table_or_query`** — fetch the most recent feature values
- **`pull_all_from_table_or_query`** — full table scans for batch processing
- **`offline_write_batch`** — write feature data back to Oracle
- **`write_logged_features`** — persist logged features for monitoring and debugging

---

## Why This Matters: Oracle Is Where the Enterprise Lives

Oracle Database isn't just another backend option. It is the database that runs the world's banks, hospitals, supply chains, and telecom networks. When we say "number one enterprise database," we mean it in terms of:

- **Installed base** — More Fortune 100 companies run Oracle than any other database
- **Data gravity** — Petabytes of the world's most regulated, most valuable data already sits in Oracle
- **Operational maturity** — Decades of enterprise features: partitioning, RAC, Data Guard, Advanced Security, Audit Vault

For ML teams in these organizations, the path to production has always involved a painful detour: extract data from Oracle, load it somewhere else, build features there, then figure out how to serve them. Every step in that chain is a potential point of failure, a security review, and a compliance headache.

Feast's Oracle integration eliminates the detour entirely. Your features are computed where your data already has governance, backup, encryption, and access controls in place.

---

## How It Works: From Oracle Table to Production Features

### Step 1: Configure your feature store

Point Feast at your Oracle database in `feature_store.yaml`:

```yaml
project: my_project
registry: data/registry.db
provider: local
offline_store:
  type: oracle
  host: oracle-db.example.com
  port: 1521
  user: feast_user
  password: ${DB_PASSWORD}
  service_name: ORCL
online_store:
  path: data/online_store.db
```

Feast supports three Oracle connection modes — `service_name`, `sid`, or `dsn` — so it fits however your DBA has set things up:

```yaml
# Using SID
offline_store:
  type: oracle
  host: oracle-db.example.com
  port: 1521
  user: feast_user
  password: ${DB_PASSWORD}
  sid: ORCL

# Using full DSN
offline_store:
  type: oracle
  host: oracle-db.example.com
  port: 1521
  user: feast_user
  password: ${DB_PASSWORD}
  dsn: "(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=oracle-db.example.com)(PORT=1521))(CONNECT_DATA=(SERVICE_NAME=ORCL)))"
```

### Step 2: Define features backed by Oracle tables

```python
from feast import FeatureView, Field, Entity
from feast.types import Float64, Int64
from feast.infra.offline_stores.contrib.oracle_offline_store.oracle_source import OracleSource
from datetime import timedelta

customer = Entity(name="customer_id", join_keys=["customer_id"])

customer_transactions = OracleSource(
    name="customer_txn_source",
    table_ref="ANALYTICS.CUSTOMER_TRANSACTIONS",
    timestamp_field="TXN_TIMESTAMP",
)

customer_features = FeatureView(
    name="customer_transaction_features",
    entities=[customer],
    ttl=timedelta(days=30),
    schema=[
        Field(name="avg_txn_amount_30d", dtype=Float64),
        Field(name="txn_count_7d", dtype=Int64),
        Field(name="max_txn_amount_90d", dtype=Float64),
    ],
    source=customer_transactions,
)
```

### Step 3: Retrieve features for training

```python
from feast import FeatureStore
import pandas as pd

store = FeatureStore(repo_path=".")

entity_df = pd.DataFrame({
    "customer_id": [101, 102, 103, 104],
    "event_timestamp": pd.to_datetime(["2026-01-15", "2026-01-16", "2026-02-01", "2026-02-15"]),
})

training_df = store.get_historical_features(
    entity_df=entity_df,
    features=[
        "customer_transaction_features:avg_txn_amount_30d",
        "customer_transaction_features:txn_count_7d",
        "customer_transaction_features:max_txn_amount_90d",
    ],
).to_df()
```

Feast performs point-in-time correct joins against Oracle — no future data leaks into your training set, and the query runs *inside* Oracle, not in some external compute engine.

### Step 4: Serve features in production

```python
store.materialize_incremental(end_date=datetime.utcnow())

features = store.get_online_features(
    features=[
        "customer_transaction_features:avg_txn_amount_30d",
        "customer_transaction_features:txn_count_7d",
    ],
    entity_rows=[{"customer_id": 101}],
).to_dict()
```

---

## Kubernetes-Native: The Feast Operator and Oracle

For teams running Feast on Kubernetes, the Feast operator now natively manages Oracle-backed feature stores through the `FeatureStore` custom resource.

### Create a Secret with your Oracle credentials

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: oracle-offline-store
type: Opaque
stringData:
  oracle: |
    host: oracle-db.example.com
    port: "1521"
    user: feast_user
    password: changeme
    service_name: ORCL
```

### Define a FeatureStore custom resource

```yaml
apiVersion: feast.dev/v1
kind: FeatureStore
metadata:
  name: production-feature-store
spec:
  services:
    offlineStore:
      persistence:
        store:
          type: oracle
          secretRef:
            name: oracle-offline-store
```

### Apply and let the operator do the rest

```bash
kubectl apply -f feature-store.yaml
```

The operator validates the configuration against the CRD schema (rejecting invalid types at admission), reads the Secret, merges the Oracle connection parameters into the generated Feast config, and deploys the offline store service. Credential rotation, version upgrades, and config changes are all handled through Kubernetes-native reconciliation — the same operational model your platform team already knows.

Because credentials live in Kubernetes Secrets, they integrate naturally with external secret managers like HashiCorp Vault, AWS Secrets Manager, or Azure Key Vault through standard Kubernetes mechanisms. Oracle credentials never appear in plain text in your manifests or CI/CD logs.

---

## Real-World Use Cases

### Financial Services: Fraud Detection Without Data Movement

A global bank running Oracle for core banking can now build fraud detection features — transaction velocity, merchant category patterns, geographic anomaly scores — directly from their existing Oracle tables. The features stay within the same security perimeter, audit trail, and encryption boundary as the source data. No ETL pipeline to a secondary warehouse means no replication lag and no additional attack surface.

### Healthcare: Predictive Models on Regulated Data

Hospitals and insurers with patient data in Oracle can compute ML features (readmission risk scores, treatment outcome signals, resource utilization patterns) without copying PHI into a less governed system. Feast's feature definitions become the documented lineage trail that compliance teams need.

### Telecommunications: Network Optimization at Scale

Telcos managing billions of CDRs and network metrics in Oracle can build churn prediction, capacity forecasting, and service quality features on top of the data they already have — avoiding the cost and latency of replicating to a separate analytical platform.

### Retail: Demand Forecasting from Point-of-Sale Data

Retailers with Oracle-backed inventory and transaction systems can build demand forecasting and recommendation features without standing up a parallel data infrastructure. Features computed in Oracle can be materialized to the online store for real-time serving at the edge.

---

## Under the Hood: Built on ibis

The Oracle offline store is built on the [ibis framework](https://ibis-project.org/), a portable Python dataframe API that compiles to native SQL for each backend. This means:

- **Queries execute inside Oracle** — ibis translates Feast's retrieval operations into Oracle SQL, pushing computation to where the data lives
- **No intermediate data movement** — results are streamed back as Arrow tables without staging in a temporary system
- **Full Oracle type fidelity** — the type mapping covers the complete spectrum of Oracle data types, including `NUMBER`, `VARCHAR2`, `NVARCHAR2`, `CHAR`, `CLOB`, `NCLOB`, `BLOB`, `RAW`, `BINARY_FLOAT`, `BINARY_DOUBLE`, `DATE`, `TIMESTAMP`, `INTEGER`, `SMALLINT`, and `FLOAT`
- **Automatic DATE-to-TIMESTAMP casting** — Oracle's `DATE` type (which includes time components, unlike SQL standard) is properly handled

---

## Getting Started

Install Feast with Oracle support:

```bash
pip install 'feast[oracle]'
```

For Kubernetes deployments, ensure you're running Feast operator v0.61.0+ with the v1 API.

The full configuration reference, functionality matrix, and data source documentation are available in the Feast docs:

- [Oracle Offline Store Reference](https://docs.feast.dev/reference/offline-stores/oracle)
- [Oracle Data Source Reference](https://docs.feast.dev/reference/data-sources/oracle)
- [Feast Operator Documentation](https://docs.feast.dev/)

---

*Get started with the [Feast documentation](https://docs.feast.dev/) and join the community on [GitHub](https://github.com/feast-dev/feast) and [Slack](https://feastopensource.slack.com/). We'd love to hear how you're using Feast with Oracle.*
