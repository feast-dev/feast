---
paths:
  - sdk/python/feast/infra/online_stores/**
  - sdk/python/feast/infra/offline_stores/**
  - sdk/python/feast/infra/registry/**
  - sdk/python/tests/unit/infra/**
  - go/**
  - infra/feast-operator/**
---

Read `skills/feast-architecture/SKILL.md` for the relevant component section:

| Working in… | Read section |
|---|---|
| `online_stores/` | Online Store — config pattern, entity key serialization, async support, registration |
| `offline_stores/` | Offline Store — interface, PIT join, pull_latest, adding a new backend |
| `infra/registry/` | Registry — proto vs SQL backend, caching, adding new object types |
| `go/` | Go Feature Server — entry point, serving path, online store interface, build commands |
| `infra/feast-operator/` | Feast Operator — CRD spec, reconcile loop, RBAC markers, dev workflow |

For testing patterns and debugging, also read `skills/feast-testing/SKILL.md`.

## When making any component change

- **Unit tests**: add or update tests in `sdk/python/tests/unit/infra/<subsystem>/`
- **Integration tests**: run `make test-python-integration-local`; add a universal test case in `sdk/python/tests/integration/` if the change affects retrieval or materialization behavior
- **Protos**: if you add a field to a proto message, recompile with `make protos` and update serialization helpers in `proto_registry_utils.py`
- **Both SDKs**: if the change affects online serving, check whether the Go server (`go/`) also needs updating
- **Skills/Rules**: if the change introduces new patterns, interfaces, or conventions that agents should follow, update the relevant section in `skills/feast-architecture/SKILL.md` (and `skills/feast-testing/SKILL.md` if testing patterns changed)

## Documentation — where to add/update

| Change type | Doc location | Also update |
|---|---|---|
| New **online store** | `docs/reference/online-stores/<name>.md` (copy an existing one as template) | `docs/reference/online-stores/README.md`, `docs/SUMMARY.md` (under "Online stores") |
| New **offline store** | `docs/reference/offline-stores/<name>.md` | `docs/reference/offline-stores/README.md`, `docs/reference/offline-stores/overview.md`, `docs/SUMMARY.md` |
| New **registry backend** | `docs/reference/registries/<name>.md` | `docs/SUMMARY.md` |
| Config option change | `docs/reference/feature-store-yaml.md` | — |
| New CLI flag or command | `docs/reference/feast-cli-commands.md` | — |
| How-to / integration guide | `docs/how-to-guides/customizing-feast/` or `docs/how-to-guides/` | `docs/SUMMARY.md` |
| Architecture / concept | `docs/getting-started/architecture/` or `docs/getting-started/components/` | `docs/SUMMARY.md` |
| Blog post | `/infra/website/docs/blog/` (NOT `docs/blog/`) | — |

All `docs/` pages are rendered by GitBook via `docs/SUMMARY.md`. Any new page must be added to `SUMMARY.md` or it won't appear in the site navigation.
