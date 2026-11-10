"""Exposes the MCP server in :mod:`feast.mcp` as the ``feast mcp`` command.

The command is resolved *lazily*. :mod:`feast.mcp.server` pulls in FastMCP,
which is expensive to import, and every ``feast`` invocation would otherwise
pay for it -- including ``feast apply`` and ``feast materialize``, which have
nothing to do with MCP.

FastMCP is an optional dependency, so when the ``mcp-server`` extra is not
installed the command degrades to a clear error on use rather than breaking
the whole CLI at import time.
"""

from __future__ import annotations

from typing import Any, List, Optional

import click


class _McpServerUnavailable(Exception):
    """Raised internally when :mod:`feast.mcp.server` cannot be imported."""


def _load_mcp_cli() -> click.Command:
    """Import the ``feast mcp`` command from :mod:`feast.mcp.server`."""
    try:
        from feast.mcp.server import mcp_cli

        return mcp_cli
    except ImportError as exc:
        raise _McpServerUnavailable(
            f"The standalone MCP server could not be imported: {exc}.\n"
            "It needs FastMCP, which is not part of the base Feast install.\n"
            "Install the optional 'feast[mcp-server]' extra."
        )


def _unavailable_command(message: str) -> click.Command:
    """Build a stand-in command that reports why MCP support is missing."""

    @click.command(
        "mcp",
        context_settings={"ignore_unknown_options": True},
        short_help="Run the Feast MCP server (unavailable).",
    )
    @click.argument("args", nargs=-1, type=click.UNPROCESSED)
    def unavailable(args: tuple) -> None:
        """Run the Feast MCP server.

        Unavailable in this environment -- run the command to see why.
        """
        raise click.ClickException(message)

    return unavailable


class _LazyMcpCommand(click.Command):
    """Defers importing :mod:`feast.mcp.server` until the command is used.

    Both help rendering and argument parsing go through ``get_params``, so
    delegating it is enough for ``feast mcp --help`` to show the real option
    list while other Feast commands never trigger the import.
    """

    def __init__(self) -> None:
        super().__init__(
            "mcp",
            params=[],
            short_help="Run the Feast MCP server.",
        )
        self._delegate: Optional[click.Command] = None

    def _resolve(self) -> click.Command:
        if self._delegate is None:
            try:
                self._delegate = _load_mcp_cli()
            except _McpServerUnavailable as exc:
                self._delegate = _unavailable_command(str(exc))
        return self._delegate

    def make_context(
        self,
        info_name: Optional[str],
        args: List[str],
        parent: Optional[click.Context] = None,
        **extra: Any,
    ) -> click.Context:
        # click builds the Context from *this* command, so the delegate's
        # context_settings would otherwise be ignored -- which would make the
        # unavailable stub reject options instead of reporting why MCP is off.
        for key, value in self._resolve().context_settings.items():
            extra.setdefault(key, value)
        return super().make_context(info_name, args, parent=parent, **extra)

    def get_params(self, ctx: click.Context) -> List[click.Parameter]:
        return self._resolve().get_params(ctx)

    def get_help(self, ctx: click.Context) -> str:
        return self._resolve().get_help(ctx)

    def invoke(self, ctx: click.Context) -> Any:
        return self._resolve().invoke(ctx)


mcp_command = _LazyMcpCommand()
