---
paths:
  - skills/**
  - AGENTS.md
  - .cursor/rules/**
  - .claude/rules/**
---

## When editing skills or rules

Skills and rules are only useful if they accurately reflect the real codebase. Before finalising any skill/rule edit:

**Verify against source code:**
- Command examples (lint, test, type-check) — confirm they still match `Makefile` targets and `pyproject.toml`
- File paths and class names — confirm they exist in the repo
- Interface signatures (e.g. `OnlineStore`, `OfflineStore`, `BaseRegistry`) — confirm against the actual base class files
- Config field names — confirm against `RepoConfig` and `FeastConfigBaseModel` subclasses in `sdk/python/feast/repo_config.py`

**Keep scope consistent:**
- `AGENTS.md` — entry point only; commands, skills table, code style. Max ~120 lines.
- `skills/feast-architecture/SKILL.md` — how each component works internally; data flows; adding new backends
- `skills/feast-testing/SKILL.md` — how to run, write, and debug tests
- `skills/feast-dev/SKILL.md` — contributor workflow; setup; Docker; docs locations; PR process
- `skills/feast-user-guide/SKILL.md` — how to use Feast as an end user; feature definitions; retrieval; RAG
- `.cursor/rules/feast-components.mdc` / `.claude/rules/feast-components.md` — component checklist (tests, docs, skills); keep in sync with each other

**Keep the two rule files in sync:**
`.cursor/rules/feast-components.mdc` and `.claude/rules/feast-components.md` contain the same content with only different frontmatter (`globs:` vs `paths:`). Any content change must be applied to both.
