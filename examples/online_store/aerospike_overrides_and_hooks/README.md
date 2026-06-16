# Aerospike: per-feature-view overrides + prewriting hooks

A short companion to [`docs/reference/online-stores/aerospike.md`](../../../docs/reference/online-stores/aerospike.md)
demonstrating three deployment patterns the Aerospike online store supports
without needing any Feast extension code:

1. **Per-feature-view namespace overrides** — pin one view to a RAM-only
   namespace and another to an SSD-backed one without splitting the project.
2. **Per-feature-view set overrides** — isolate one view in its own set so
   `feast apply` deletions or admin truncates only touch that view.
3. **Prewriting hooks** — apply a project-wide write-side transformation
   (PII masking in this example) without sprinkling it through every
   materialization job.

Nothing here is Aerospike-specific *infrastructure* — it's all configured
in `feature_store.yaml`. This directory only adds the hook-target Python
module the YAML references.

## Files

| file | purpose |
|---|---|
| [`hooks.py`](hooks.py) | A pure-Python prewriting-hook module containing `hash_pii_string_features`, the same example used in the docs. Drop into any module on the writer's `PYTHONPATH`. |
| [`feature_store.yaml`](feature_store.yaml) | Reference `online_store` block showing all three features wired together. Copy the `online_store` section into your own `feature_store.yaml` — the rest is project-specific scaffolding. |

## Prerequisites

* Feast installed with the Aerospike extra (`pip install 'feast[aerospike]'`).
* An Aerospike cluster reachable from your writer process. The
  [Aerospike online-store reference](../../../docs/reference/online-stores/aerospike.md)
  shows a minimal local CE config (`127.0.0.1:3000`); run Aerospike however
  you normally would (Docker, Kubernetes, bare metal).
* On every process that calls `online_write_batch` through this store
  (materialization workers, the registry CLI host, the feature server if
  you run one), the `FEAST_PII_SALT` environment variable must be set
  before the first write — `hash_pii_string_features` raises rather than
  silently writing plaintext if the salt isn't configured.
* The two namespaces referenced by `namespace_overrides` (`feast_ram` and
  `feast_ssd` in the sample YAML) must already exist on the Aerospike
  cluster — Aerospike cannot create namespaces at runtime.

## Trying it out

1. Drop `hooks.py` into a module on your `PYTHONPATH` that the writer
   process can import (e.g. inside your existing feature-repo package).
   The example uses the qualified path
   `examples.online_store.aerospike_overrides_and_hooks.hooks.hash_pii_string_features`.
2. Copy the `online_store:` block from `feature_store.yaml` into your
   own feature repo, adjusting hosts / namespaces / the hook import path
   for your project.
3. `export FEAST_PII_SALT=...` (anything random and stable across
   processes — rotate by re-running materialization with a new salt).
4. `feast apply` — the new config is registered.
5. Materialize as usual — the hook runs once per `online_write_batch`,
   and any feature named `email`, `phone_number` or `ssn` lands in
   Aerospike as a salted SHA-256 hex digest instead of plaintext.

## Read-side note

Prewriting hooks are **only** invoked on the write path. If your hook is
a one-way transform (hashing, encryption-without-decryption-key) you
have to apply the same transform to the candidate value at read time
yourself. Two-way transforms (deterministic encryption, Base64) need a
matching post-read step in your serving code; the Aerospike store does
not currently expose a symmetric "postreading hook".
