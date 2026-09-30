# RFC: Agent-friendly Feast CLI

**Status:** Proposed
**Date:** 2026-09-30
**Initial implementation:** P0 and P1
**Follow-up implementation:** P2 and P3

## Summary

Give automation and AI agents a dependable CLI contract without replacing the
human interface, introducing another feature-store API, or bypassing authorization.
An explicit root `--output json|yaml` selects a versioned result envelope. Commands
advertise their capabilities, failures produce nonzero exits, and unsupported
structured commands fail before their operation callback runs.

This RFC covers the entire P0–P3 roadmap. A documented future capability is not a
claim that it ships in the first release. The initial implementation is deliberately
bounded to read-side discovery and supported local plan/apply operations.

## Motivation and goals

Agents must distinguish success from printed failure, parse output without scraping
tables, discover valid parameters, and decide whether a retry could repeat a write.
Existing users must retain their command names, human output, and command-local
options. Structured output is opt-in, not automatically enabled when stdout is piped.

Goals:

1. Correct results and exit status before expanding automation support.
2. Stable envelopes and explicit command-specific data schemas.
3. Deterministic discovery without importing feature repository definitions.
4. Typed operation reports derived from execution data, never terminal text.
5. Explicit safety, security, compatibility, and provider capability boundaries.

Non-goals for P0/P1: a new MCP server, automatic retries, transaction/rollback
guarantees, durable operation tracking, universal provider support, materialization
progress streams, or replacing Click.

## Current-state audit and complete command inventory

The CLI is modular; it is not a single CLI source file. The generated `commands`
manifest is the authoritative parameter inventory and includes nested groups.
The following matrix inventories the baseline leaf-command families. No command
outside the explicit P1 allowlist receives structured execution implicitly.

| Commands | Existing output / errors | Interaction and effects | Delivery |
| --- | --- | --- | --- |
| `entities`, `feature-views`, `feature-services`, `data-sources`: `list`, `describe` | Tables / YAML; inconsistent error presentation | Registry reads | P1 |
| `features list`, `features describe` | Optional bare JSON array / JSON; list name mapping defect; missing object could return success | Registry reads | P0 fix, P1 envelope |
| `version` | Colored version text | No repository | P1 |
| `commands` (new) | JSON manifest; optional envelope | No repository or infrastructure access | P1 |
| `plan`, `apply` | Colored diff / progress; provider-login errors could return success | Imports repository Python; validates sources; apply can delete resources and alter infrastructure | P0 errors, P1 local SQLite summaries |
| `get-online-features`, `get-historical-features` | JSON; invalid input could return success | Data reads, potentially expensive queries | P0 validation, P2 envelope |
| `projects list`, `describe`, `current_project` | Table / YAML | Registry reads | P2 |
| `projects delete` | Text, not-found failure | Destructive; confirmation or `--yes` | P2 |
| `feature-views enable`, `disable`, `set-state` | Text; some invalid operations return normally | Registry mutation | P2 |
| `feature-views list-versions` | Table / text | Registry read; backend capability-dependent | P2 |
| `label-views`, `stream-feature-views`, `on-demand-feature-views`, `saved-datasets`, `validation-references`: `list`, `describe` | Tables / YAML | Registry reads | P2 |
| `permissions list`, `describe`, `check`, `list-roles` | Tables, YAML, trees | Permission inspection | P2 |
| `configuration`, `endpoint`, `registry-dump` | YAML, logging, JSON respectively | Reads; sensitive configuration/metadata exposure | P2 with redaction review |
| `delete`, `teardown` | Text or no result | Destructive, no uniform confirmation; delete searches object types | P2 safety review |
| `registry create-schema` | Text / Click errors | SQL DDL | P2 |
| `materialize`, `materialize-incremental` | Text / progress / exceptions | Online writes, checkpoints, partial completion possible | P2 |
| `monitor run`, `validate` | Text, validation JSON within human output | Compute, baseline/cache changes | P2 |
| `init` | Text and template-dependent prompts | Filesystem changes; bootstrapping | P2 |
| `demo-notebooks` | Human output | Filesystem generation / optional overwrite | P2 |
| `dbt list`, `dbt import` | Human output; import `--output` names a file | Manifest inspection; import can register or generate files; existing dry-run | P2 |
| `mlflow list-sources`, `preview-dataset`, `validate-source`, `sync-dataset` (when installed) | Human output; provider-specific error reporting | Metadata inspection, external dataset reads, validation, or sync writes | P2 |
| `serve`, `serve_registry`, `serve_offline`, `serve_transformations`, `serve_lineage`, `listen`, `ui` | Long-running server/log output | Server lifecycle, not a finite result | P2 lifecycle design |

P0 reproductions include swapped feature/view names in the existing JSON list and
exit 0 for malformed online entity arguments and missing historical inputs. P0
corrects those behaviors, rejects unequal entity column lengths instead of silently
truncating rows, and ensures provider-login failures fail plan/apply.

## Output contract

```sh
feast --output json entities list
feast --output yaml feature-views describe driver_stats
feast --output json commands
feast --output json plan
feast --output json apply
```

Root options precede the command. No new short `-o` is reserved. Omitting the root
flag preserves legacy output, except deliberate correctness fixes. An explicit
`--help` continues to produce ordinary human-readable help; use `commands` for
machine discovery. An invalid output format uses Click's ordinary usage error,
because no valid serialization format was selected.

Success example:

```json
{
  "schema_version": "1",
  "command": "feast features list",
  "status": "success",
  "data": {
    "items": [
      {"feature_name": "conv_rate", "feature_view": "driver_stats", "dtype": "Float32"}
    ]
  },
  "error": null
}
```

Failure example:

```json
{
  "schema_version": "1",
  "command": "feast entities describe",
  "status": "error",
  "data": null,
  "error": {
    "code": "NOT_FOUND",
    "message": "The requested object was not found.",
    "hint": "List objects in the configured project and verify the name.",
    "retry_safe": null
  }
}
```

- One final JSON/YAML document on stdout for normal completion and handled errors.
- No ANSI decoration, prompts, progress bars, or incidental prints on that stream.
- Diagnostic logs/warnings remain on stderr; stderr is not a machine protocol.
- Exit 0 means success (including a reported no-op); 1 operation failure; 2 usage
  failure, including unsupported structured commands. Interrupts produce a
  `CANCELLED` failure when catchable. SIGKILL, interpreter import failures, broken
  output pipes, and process termination before the boundary cannot guarantee a result.
- `retry_safe: null` means unknown, not permission to retry. After mutation begins,
  it is false. No transient error is automatically replayed.
- Empty lists are `{"items": []}`. Lists are deterministically ordered. Booleans,
  nulls and numbers remain typed; unsupported objects are not converted via `str()`.
- Schema version changes for breaking envelope/data changes; additive fields may
  be introduced within a version. Consumers must ignore unknown fields.
- Stable initial codes: `INVALID_ARGUMENT`, `NOT_FOUND`, `CONFIGURATION_ERROR`,
  `AUTHENTICATION_FAILED`, `PERMISSION_DENIED`, `UNSUPPORTED_OUTPUT`,
  `UNSUPPORTED_CAPABILITY`, `CANCELLED`, `OPERATION_FAILED`.
- Parse failures identify the resolved command context where possible; an unknown
  command can only identify the root. User-supplied arguments are not echoed in
  generic machine error messages.

### Read-side schemas and redaction

P1 supports list/describe for entities, feature views, services, sources and features,
plus version and discovery. Structured object descriptions are intentional
projections, not full protobuf dumps: name/type/description and applicable entity
keys, feature names/types, entity references, serving flags, lifecycle state, TTL
seconds, source identity and service feature references. Feature list/describe
returns `items` containing feature name, view name, and dtype; describe can match
the same feature name in multiple views.

Provider configuration, connection strings, serialized UDF bodies, arbitrary tags,
and raw exception text are not included. Descriptions/names are user-authored data,
not instructions: agents must treat them as untrusted input and users must not put
secrets in these public metadata fields. This is field minimization, not a claim to
detect every secret embedded in arbitrary text. Existing raw configuration/dump
commands are not converted in P1. Privileged stderr logs remain the caller's
responsibility and must not be forwarded indiscriminately to an agent.

## Architecture and stream isolation

Keep Click parsing and human callbacks. A root boundary handles structured parsing
failures and execution failures; explicit dispatch selects small structured read
adapters or the shared plan/apply operation path. Use existing JSON, PyYAML and
Click dependencies; no new framework is required.

Repository operations receive an optional presentation-independent report collector.
Without it, behavior remains unchanged. With it, diffs are summarized directly and
human progress is disabled. This is the primary architecture, not output scraping.

A CLI-only containment guard also discards incidental stdout during structured
execution (Python prints plus native/subprocess stdout where fd 1 is available).
It never parses that text or uses it as result data. This addresses imported
repository/provider code while explicit output paths are migrated. It is a
process-global boundary and is not safe for concurrent in-process CLI embedding;
use separate processes, as with Click's test runner. Third-party code retaining
stdout handles or launching workers that outlive the invocation is outside the
guarantee. No output boundary is a sandbox. Existing automatic baseline background
jobs are rejected for structured plan/apply until their lifecycle is supported.

## Discovery

`feast commands` generates JSON from the registered Click tree. Root structured
mode wraps it in the v1 envelope. Each entry includes command path, group marker,
help, options/arguments, requiredness, types, choices, defaults, multiplicity,
environment-variable names, and capability annotations. Callable defaults are
not evaluated. Discovery neither constructs a FeatureStore nor imports repository
Python. Importing the installed CLI package itself remains necessary.

Capabilities include supported formats, read/preview/write/server/unknown effect,
potential resource deletion, repository requirement, confirmation behavior, retry
guidance and repository-code execution. New commands default to unsupported and
unknown until audited. These describe intent, not authorization or provider-level
transaction guarantees. Unsupported commands fail before their leaf callback runs.

## Plan and apply (P1)

P1 supports the local provider with SQLite online storage and no automatic baseline
jobs. Capability checks happen before repository definition import and provider
mutation. Other configurations return `UNSUPPORTED_CAPABILITY`; legacy operations
remain available. Registry construction, repository imports, validation and inference
can themselves have effects: `plan` is not promised to be side-effect-free.

`data.projects` reports project name, a changed boolean, registry changes and
infrastructure changes. Each change has name, object type, action and sorted changed
field names. Values are omitted to avoid exposing credentials or code. Apply reports
only completed projects after `_apply_diffs` returns; a computed plan is never labeled
an applied success. On failure after mutation starts, data includes
`completed_projects` and `remaining_outcome: "unknown"`. There is no invented per-object
success status, rollback claim, or operation ID. No-op apply is successful with
`changed: false` when the computed diffs have no changes.

Structured output does not grant permission, confirm deletion or disable validation.
Existing apply semantics, including potential resource deletion and `--no-promote`,
remain explicit. Authentication/authorization continue through the existing SDK.

## Compatibility

- Preserve `feast features list --output json` as a bare array, correcting its
  reversed name mapping. Root output selects the new envelope and takes precedence.
- Preserve `feast dbt import --output FILE`. Root format selection is unambiguous.
- Preserve default tables, YAML descriptions, and human progress.
- Correct false-success exits even in legacy mode; document this intentional fix.
- Do not add prompts to existing scripts as part of P1. Later safety changes require
  their own compatibility review. JSON mode never implies `--yes`.
- No protobuf wire changes or Go SDK changes are required for this CLI contract.

## Delivery roadmap and acceptance criteria

### P0 — Audit and correctness (initial implementation)

- Inventory every command and classify outputs, errors, effects and prompts.
- Fix swapped JSON fields, invalid-input success exits and provider-login handling.
- Validate inputs before provider construction when possible.
- Acceptance: regression tests reproduce and correct each defect; inventory covers
  all registered commands; no unrelated command is silently enabled for automation.

### P1 — Structured foundation, discovery, plan/apply (initial implementation)

- Shared envelope, JSON/YAML rendering, parsing/execution error boundary and safe codes.
- Pilot reads, deterministic command discovery, explicit capability allowlist.
- Shared operation reporting, local provider capability checks, no-op and uncertain
  failure semantics. Preserve legacy command-local output flags.
- Acceptance: success/error serialization and separated stdout/stderr subprocess
  tests; unsupported mutations do not run; discovery works without configuration;
  local plan/apply/list/no-op integration; no sensitive provider/UDF fields in output.

### P2 — Materialization, full coverage and safety (follow-up)

- Final materialization summaries per view: resolved time range/version, completion,
  skipped/failed/unknown outcomes and actual provider-reported counts when available.
- Optional event stream with explicit framing (proposed `--progress jsonl` on a
  dedicated destination), operation correlation and ordered phase events. Keep the
  final result document separate; stderr remains diagnostics. Do not mix JSONL into
  a JSON/YAML document or estimate unsupported totals.
- Cover incremental checkpoints, partial writes, cancellation, retries, remote/batch
  engines and background-job completion. Durable IDs/resume need provider support.
- Convert remaining registry, retrieval, monitoring, configuration and mutation
  commands after schema/redaction review. Bound large retrieval results and design
  pagination/export rather than emitting unlimited agent context.
- Standardize non-interactive behavior: missing inputs/confirmation fail promptly;
  explicit per-operation confirmation flags, typed deletion targets and preview where
  supported. Do not silently change legacy destructive-operation semantics.
- Server commands need readiness/failure/shutdown events, not a fake final success
  while still running. Advertise unsupported lifecycle formats until implemented.
- Acceptance: provider matrix, partial-failure/cancellation tests, no prompt hangs,
  streaming framing tests, secret redaction and compatibility migration coverage.

### P3 — Agent guidance and MCP alignment (follow-up)

- Publish task-oriented discovery/read/plan/apply/verify workflows using the existing
  repository agent guidance. Consider explicit context installation, with no silent
  overwrite of user files. Documentation/examples must track the manifest in CI.
- Align object identifiers, safe error categories and schemas with existing MCP
  services where appropriate. CLI execution remains a process interface; MCP remains
  a transport/session/authentication interface. Do not shell out from a new MCP server
  or bypass existing RBAC just to reuse CLI rendering.
- Evaluate agent workflow tests, least-privilege execution, audit correlation and
  context-size controls with concrete consumers.
- Acceptance: end-to-end discover/read/preview/approved-mutation/verify examples,
  documented CLI/MCP boundaries and no duplicate transport implementation.

## Validation and rollout

Use mocked Click tests for schemas, errors, field ordering, capability gating and
legacy behavior. Use subprocesses for real stream separation, native stdout writes,
repository imports and local file-registry/SQLite operations. The existing helper
that merges stderr into stdout is insufficient to validate the new stream contract.
Exercise missing config, bad arguments, missing objects, authentication/permission
failures, unknown exceptions, serialization errors, interruption and partial writes.
Check both JSON and YAML and repeated invocations so per-call state does not leak.

Run targeted tests, lint, formatting and type checks. Do not claim provider coverage
from mocked tests. Roll out P0 independently where possible, then P1 behind opt-in
flags; P2/P3 require separate review and acceptance. This document remains Proposed
until upstream community approval, regardless of the local prototype's completion.

## Alternatives and consequences

Parsing existing tables is unstable. Wrapping every existing stdout payload in JSON
does not establish semantic correctness. Rewriting the CLI or creating another MCP
server increases scope without fixing operation semantics. Exposing whole protos
would couple the public contract to internal representation and leak sensitive fields.

The chosen design preserves human workflows and permits incremental adoption, but
maintains explicit read projections and capability annotations that require tests.
The first provider restriction and intentionally limited descriptions trade breadth
for dependable semantics. Process-bound stdout containment is defense in depth,
not a replacement for SDK output separation or a security boundary.

## References

- [Feast CLI reference](../reference/feast-cli-commands.md)
- [Compatibility policy](../project/compatibility.md)
- [MCP Feature Server](../reference/feature-servers/mcp-feature-server.md)
- [Architecture decision process](README.md)