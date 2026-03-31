# Feast - Agent Instructions

This file provides instructions for AI coding agents (GitHub Copilot, Claude Code, OpenAI Codex, etc.) working in this repository.

## Project Overview

Feast is an open source feature store for machine learning. See `CLAUDE.md` for a full project overview and development commands.

## Repository Conventions

### Documentation and Blog Posts

- **Blog posts must be placed in `/infra/website/docs/blog/`** — do NOT create blog posts under `docs/blog/` or any other location.
- Blog post files must include YAML frontmatter with `title`, `description`, `date`, and `authors` fields, following the format of existing posts in that directory.
- All other reference documentation goes under `docs/`.

### Code Style

- Use type hints on all Python function signatures.
- Follow existing patterns in the module you are modifying.
- PR titles must follow semantic conventions: `feat:`, `fix:`, `ci:`, `chore:`, `docs:`.
- Sign off commits with `git commit -s` (DCO requirement).

### Testing

- Run `make test-python-unit` for unit tests.
- Run `make lint-python` before submitting changes.
- See `skills/feast-dev/SKILL.md` for the full development guide.
