"""Rehydrate trusted UDF callables from registry source text.

Feast persists both dill bytes (``body``) and source (``body_text`` / ``udf_string``).
Preferring source avoids:

* Spark driver segfaults (exit 139) when dill-restored callables touch DataFrames
* Cross-Python-version serve failures when apply and feature-server differ
"""

from __future__ import annotations

import logging
from typing import Callable, Optional

import dill

logger = logging.getLogger(__name__)


def _strip_leading_decorators(udf_string: str) -> str:
    """Remove leading ``@decorator`` lines before ``def`` / ``async def``.

    Uses a linear paren-balanced scan (O(n)). Avoids nested regex quantifiers that
    CodeQL flags as ReDoS-prone on adversarial ``@`` / newline spam in body_text.
    """
    text = udf_string.lstrip()
    if not text.startswith("@"):
        return text

    i = 0
    n = len(text)

    while i < n:
        while i < n and text[i] in " \t\r\n":
            i += 1
        if i >= n:
            break

        if text[i] == "#":
            while i < n and text[i] != "\n":
                i += 1
            continue

        rest = text[i:]
        if rest.startswith("async def ") or rest.startswith("def "):
            break
        if text[i] != "@":
            break

        i += 1  # past '@'
        while i < n and (text[i].isalnum() or text[i] in "._"):
            i += 1
        while i < n and text[i] in " \t":
            i += 1

        if i < n and text[i] == "(":
            depth = 0
            in_str: Optional[str] = None
            while i < n:
                ch = text[i]
                if in_str is not None:
                    if ch == "\\" and i + 1 < n:
                        i += 2
                        continue
                    if text.startswith(in_str, i):
                        i += len(in_str)
                        in_str = None
                        continue
                    i += 1
                    continue
                if text.startswith('"""', i) or text.startswith("'''", i):
                    in_str = text[i : i + 3]
                    i += 3
                    continue
                if ch in ("'", '"'):
                    in_str = ch
                    i += 1
                    continue
                if ch == "(":
                    depth += 1
                elif ch == ")":
                    depth -= 1
                    i += 1
                    if depth == 0:
                        break
                    continue
                i += 1
            while i < n and text[i] in " \t\r":
                i += 1
            if i < n and text[i] == "\n":
                i += 1
        else:
            while i < n and text[i] != "\n":
                i += 1
            if i < n and text[i] == "\n":
                i += 1

    stripped = text[i:]
    return stripped if stripped.strip() else udf_string


def _exec_namespace() -> dict:
    """Minimal globals so typical UDF source can exec without full feast apply ctx."""
    ns: dict = {"__name__": "feast_udf_rehydrate"}
    try:
        import pandas as pd

        ns["pd"] = pd
        ns["pandas"] = pd
    except ImportError:
        pass
    try:
        import numpy as np

        ns["np"] = np
        ns["numpy"] = np
    except ImportError:
        pass
    return ns


def rehydrate_udf_from_source(
    udf_string: str,
    *,
    preferred_name: Optional[str] = None,
) -> Optional[Callable]:
    """Exec ``udf_string`` and return the resulting callable, or None.

    ``udf_string`` is treated as trusted registry content written by ``feast apply``.
    Returns None on failure so callers can fall back to dill.
    """
    if not (udf_string or "").strip():
        return None

    source = _strip_leading_decorators(udf_string)
    ns = _exec_namespace()
    try:
        exec(source, ns)  # noqa: S102 — trusted registry source
    except Exception as e:
        logger.debug("udf source rehydrate failed: %s", e)
        return None

    if preferred_name and preferred_name in ns and callable(ns[preferred_name]):
        return ns[preferred_name]

    for value in ns.values():
        if not callable(value):
            continue
        name = getattr(value, "__name__", None)
        if name in (None, "<lambda>", "__build_class__"):
            continue
        # Skip imported modules / classes we seeded
        if name in ("DataFrame",):
            continue
        return value

    return None


def resolve_udf(
    *,
    udf_string: str = "",
    body: Optional[bytes] = None,
    fallback_udf: Optional[Callable] = None,
    preferred_name: Optional[str] = None,
) -> Callable:
    """Resolve a UDF: source first, then ``fallback_udf``, then dill ``body``."""
    rehydrated = rehydrate_udf_from_source(udf_string, preferred_name=preferred_name)
    if rehydrated is not None:
        return rehydrated

    if fallback_udf is not None:
        return fallback_udf

    if body:
        return dill.loads(body)

    raise ValueError(
        "Cannot resolve UDF: empty udf_string/body_text and no dill body or "
        "fallback callable. Re-run feast apply so body_text is persisted."
    )
