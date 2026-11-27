"""Check every piece of the AI DJ while all five services are running.

uv run python scripts/smoke_test.py
"""

import asyncio
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import pandas as pd  # noqa: E402
from mcp import ClientSession  # noqa: E402
from mcp.client.streamable_http import streamable_http_client  # noqa: E402

from dj.agent import _parse, build_playlist, run_dj  # noqa: E402
from dj.config import MCP_URL, OLLAMA_MODEL, USER_ID  # noqa: E402
from dj.feast_client import get_online_features  # noqa: E402
from dj.navidrome import Navidrome  # noqa: E402


def check(name: str, ok: bool, detail: str = "") -> None:
    print(f"{'PASS' if ok else 'FAIL'}  {name}  {detail}")
    if not ok:
        raise SystemExit(1)


async def mcp_checks() -> None:
    async with (
        streamable_http_client(MCP_URL) as (read, write),
        ClientSession(read, write) as mcp,
    ):
        await mcp.initialize()
        tools = {t.name for t in (await mcp.list_tools()).tools}
        check(
            "Standalone feast mcp lists features_* tools",
            "features_vector_store_search" in tools,
            f"{len(tools)} tools",
        )
        stores = _parse(await mcp.call_tool("features_list_vector_stores", {}))
        vs = next(s["id"] for s in stores["data"] if s["name"] == "track_clamp3")
        res = _parse(
            await mcp.call_tool(
                "features_vector_store_search",
                {
                    "vector_store_id": vs,
                    "query": "hard rap vocals over beats",
                    "max_num_results": 10,
                },
            )
        )
        # No filters in feast mcp yet, so check the mood search alone finds the genre.
        ids = [int(d["file_id"].rsplit("_", 1)[1]) for d in res["data"]][:3]
        genres = get_online_features(["track_clamp3:genre"], {"track_id": ids})["genre"]
        check("CLaMP 3 mood search", "Hip-Hop" in genres, f"top 3: {genres}")


def main() -> None:
    nd = Navidrome()
    check("Navidrome Subsonic API", nd.subsonic("ping")["status"] == "ok")
    catalog = pd.read_parquet(ROOT / "data" / "catalog.parquet")
    have = len(nd.fma_song_ids())
    check("Navidrome sees the catalog", have >= len(catalog), f"{have} songs")

    asyncio.run(mcp_checks())

    got = get_online_features(
        ["user_listening:recent_track_ids", "user_taste:taste_summary"],
        {"user_id": [USER_ID]},
    )
    recent = json.loads(got["recent_track_ids"][0] or "[]")
    check("Plays reached Feast via the bridge", len(recent) > 0, f"{len(recent)} plays")
    check(
        "Taste clusters in Feast",
        bool(got["taste_summary"][0]),
        got["taste_summary"][0] or "",
    )

    plan = asyncio.run(
        run_dj("music for a rainy evening", USER_ID, OLLAMA_MODEL, False)
    )
    picks = build_playlist(plan)
    check(
        "LLM ran a mood search",
        bool(plan.get("mood_ids")),
        f"query={plan.get('query')!r}",
    )
    check(
        "Playlist skips recent plays",
        not set(picks) & set(recent),
        f"{len(picks)} picks",
    )


if __name__ == "__main__":
    main()
