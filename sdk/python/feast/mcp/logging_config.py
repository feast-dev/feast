"""Logging for the Feast MCP server.

    from feast.mcp.logging_config import configure_logging, load_logging_config, get_logger

    configure_logging(load_logging_config())
    log = get_logger(__name__)
    log.info("hello")

``configure_logging`` sets up the ``feast.mcp`` logger with a console handler.
All server modules use ``logging.getLogger(__name__)`` (children of
``feast.mcp``), so they flow through this configuration automatically.

Settings resolve from (highest priority first):

  1. CLI arguments
  2. Environment variables (``FEAST_MCP_LOG_*``)
  3. ``observability:`` section of ``feast_mcp.yaml``
  4. Defaults

Example ``feast_mcp.yaml``::

    observability:
      level: INFO
      format: json            # text | json
      stdio: true
"""

from __future__ import annotations

import json
import logging
import os
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

#: Root logger name; every module logger is a child of this.
ROOT_LOGGER_NAME = "feast.mcp"

#: Third-party loggers whose output we adopt so their lines land on the same
#: handlers as our own. FastMCP (and the MCP SDK / uvicorn / gunicorn it runs
#: under) log to their own logger trees; without this bridge those lines would
#: either be dropped or formatted differently.
BRIDGED_LOGGERS = (
    "fastmcp",
    "mcp",
    "uvicorn",
    "uvicorn.error",
    "uvicorn.access",
    "gunicorn",
    "gunicorn.error",
    "gunicorn.access",
)


@dataclass(frozen=True)
class LoggingConfig:
    """Resolved logging settings."""

    level: str = "INFO"
    format: str = "text"  # text | json
    stdio: bool = True


def _env(*keys: str) -> Optional[str]:
    """First non-empty value among ``keys`` from the environment."""
    for key in keys:
        value = os.environ.get(key)
        if value:
            return value
    return None


def _as_bool(value: Any) -> Optional[bool]:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def _load_yaml_section(config_path: Optional[str], section: str) -> dict:
    candidates = [config_path] if config_path else ["feast_mcp.yaml", "feast_mcp.yml"]
    for candidate in candidates:
        if candidate and Path(candidate).is_file():
            try:
                import yaml
            except ImportError:
                return {}
            with open(candidate) as f:
                data = yaml.safe_load(f) or {}
            sub = data.get(section)
            return sub if isinstance(sub, dict) else {}
    return {}


def load_logging_config(
    config_path: Optional[str] = None,
    cli_args: Optional[dict] = None,
) -> LoggingConfig:
    """Load logging config from CLI args, env vars, and YAML.

    Kept independent of the main server ``Config`` so logging can be set up
    *first* — before anything else is wired.
    """
    cli = cli_args or {}
    y = _load_yaml_section(config_path, "observability")

    level = (
        cli.get("log_level") or _env("FEAST_MCP_LOG_LEVEL") or y.get("level") or "INFO"
    )
    fmt = (
        cli.get("log_format")
        or _env("FEAST_MCP_LOG_FORMAT")
        or y.get("format")
        or "text"
    )

    stdio = _as_bool(_env("FEAST_MCP_LOG_STDIO"))
    if stdio is None:
        stdio = _as_bool(y.get("stdio"))
    stdio = True if stdio is None else stdio

    return LoggingConfig(
        level=str(level).upper(),
        format=str(fmt).lower(),
        stdio=stdio,
    )


class JsonFormatter(logging.Formatter):
    """Minimal structured formatter for machine-readable stdout logs."""

    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "timestamp": datetime.fromtimestamp(
                record.created, tz=timezone.utc
            ).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload)


def _build_formatter(fmt: str) -> logging.Formatter:
    if fmt == "json":
        return JsonFormatter()
    return logging.Formatter("%(asctime)s %(levelname)-8s %(name)s %(message)s")


def _apply_handlers(
    logger: logging.Logger,
    handlers: list[logging.Handler],
    level: int,
) -> None:
    """Reset ``logger`` to exactly ``handlers`` at ``level``. Idempotent.

    Handlers are *shared* across every logger we configure, so one console
    write happens per record no matter which logger emitted it. ``propagate``
    is turned off so a record isn't also handled by an ancestor (which would
    duplicate it).
    """
    logger.setLevel(level)
    logger.propagate = False
    for handler in list(logger.handlers):
        logger.removeHandler(handler)
    for handler in handlers:
        logger.addHandler(handler)
    if not logger.handlers:
        logger.addHandler(logging.NullHandler())


def configure_logging(config: LoggingConfig) -> logging.Logger:
    """Configure the ``feast.mcp`` logger from ``config``. Idempotent.

    Also bridges third-party loggers (FastMCP, MCP SDK, uvicorn, gunicorn)
    onto the same handlers so *their* output shows up on the console too.
    """
    level = getattr(logging, config.level, logging.INFO)
    formatter = _build_formatter(config.format)

    handlers: list[logging.Handler] = []

    if config.stdio:
        # stderr, not stdout: the MCP stdio transport reserves stdout for
        # JSON-RPC, so logging there would corrupt the protocol. stderr is
        # still shown on the console.
        stream = logging.StreamHandler(sys.stderr)
        stream.setFormatter(formatter)
        stream.setLevel(level)
        handlers.append(stream)

    logger = logging.getLogger(ROOT_LOGGER_NAME)
    _apply_handlers(logger, handlers, level)

    # Route FastMCP / MCP SDK / uvicorn / gunicorn through the same handlers.
    for name in BRIDGED_LOGGERS:
        _apply_handlers(logging.getLogger(name), handlers, level)

    return logger


def get_logger(name: Optional[str] = None) -> logging.Logger:
    """Return a logger under the ``feast.mcp`` namespace."""
    if not name:
        return logging.getLogger(ROOT_LOGGER_NAME)
    if name == ROOT_LOGGER_NAME or name.startswith(ROOT_LOGGER_NAME + "."):
        return logging.getLogger(name)
    return logging.getLogger(f"{ROOT_LOGGER_NAME}.{name}")
