"""Small REST helpers for code that talks to the Feast feature server directly.

The agent uses MCP. The bridge and the taste job are plain services, so they
call the same endpoints over HTTP.
"""

from typing import Any

import httpx

from dj.config import FEAST_URL


def get_online_features(
    features: list[str], entities: dict[str, list[Any]]
) -> dict[str, list[Any]]:
    r = httpx.post(
        f"{FEAST_URL}/get-online-features",
        json={"features": features, "entities": entities},
        timeout=60,
    )
    r.raise_for_status()
    body = r.json()
    names = body["metadata"]["feature_names"]
    return {name: res["values"] for name, res in zip(names, body["results"])}


def push(
    source: str, rows: dict[str, list[Any]], to: str = "online_and_offline"
) -> None:
    r = httpx.post(
        f"{FEAST_URL}/push",
        json={"push_source_name": source, "df": rows, "to": to},
        timeout=60,
    )
    r.raise_for_status()
