"""The AI DJ: a small local LLM (Ollama) that uses Feast through MCP.

It talks to the standalone `feast mcp` server (feast_mcp.yaml). The LLM gets
two of its tools:
  - features_get_online_features    read the listener's taste and recent plays
  - features_vector_store_search    find songs by mood text (CLaMP 3 embeds it)
It picks a vibe and searches. Then this script adds a few "sounds like the last
song" picks (Feast /search on the spectral vector, also through MCP), drops
songs the listener just heard or skipped, and saves a playlist in Navidrome.
"""

import argparse
import asyncio
import json
import re
from collections.abc import Callable
from typing import Any

import ollama
from mcp import ClientSession
from mcp.client.streamable_http import streamable_http_client

from dj.config import MCP_URL, OLLAMA_MODEL, USER_ID
from dj.navidrome import Navidrome

SYSTEM = """You are an AI DJ. You build the next playlist for listener "{user_id}".
Use the tools, in this order:
1. Call {read_tool} with exactly:
   {{"features": ["user_taste:taste_summary", "user_taste:top_genres", "user_listening:recent_track_ids"], "entities": {{"user_id": ["{user_id}"]}}}}
2. Decide the vibe. The listener's REQUEST comes first; their taste is only a guide.
   Describe how the music should sound, in 5 to 10 words. Two examples:
     request "workout"  -> "fast energetic beats with heavy drums"
     request "study"    -> "calm ambient textures without vocals"
3. Call {search_tool} with:
   {{"vector_store_id": "{vs_id}", "query": "<your description>", "max_num_results": 20}}
4. Reply with only JSON: {{"vibe": "<2-5 words>", "intro": "<one friendly DJ sentence>"}}"""

USER = 'REQUEST: "{ask}". Build my next playlist.'


def _parse(result: Any) -> Any:
    text = "".join(c.text for c in result.content if getattr(c, "type", "") == "text")
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return text


def _tool_for(tools: list[Any], name: str) -> Any:
    for t in tools:
        if t.name == name:
            return t
    raise RuntimeError(f"No MCP tool {name}: {[t.name for t in tools]}")


def _as_ollama_tool(tool: Any) -> dict[str, Any]:
    return {
        "type": "function",
        "function": {
            "name": tool.name,
            "description": (tool.description or "")[:400],
            "parameters": tool.input_schema,
        },
    }


def _track_ids_from_openai_search(result: Any) -> list[int]:
    out = []
    for row in (result or {}).get("data", []) if isinstance(result, dict) else []:
        m = re.search(r"_(\d+)$", row.get("file_id", ""))
        if m:
            out.append(int(m.group(1)))
    return out


def _first_json_object(text: str) -> dict[str, Any]:
    m = re.search(r"\{.*\}", text or "", re.DOTALL)
    try:
        return json.loads(m.group(0)) if m else {}
    except json.JSONDecodeError:
        return {}


async def run_dj(
    ask: str,
    user_id: str,
    model: str,
    verbose: bool,
    on_step: Callable[[str], None] | None = None,
) -> dict[str, Any]:
    # on_step lets the web UI show each step as it happens.
    say = on_step or (print if verbose else lambda _: None)
    async with (
        streamable_http_client(MCP_URL) as (read, write),
        ClientSession(read, write) as mcp,
    ):
        await mcp.initialize()
        tools = (await mcp.list_tools()).tools
        read_tool = _tool_for(tools, "features_get_online_features")
        search_tool = _tool_for(tools, "features_vector_store_search")
        list_stores = _tool_for(tools, "features_list_vector_stores")
        vector_search = _tool_for(tools, "features_search")
        say(
            f"MCP: {len(tools)} Feast tools, using {read_tool.name}, {search_tool.name}"
        )

        stores = _parse(await mcp.call_tool(list_stores.name, {}))
        vs_id = next(s["id"] for s in stores["data"] if s["name"] == "track_clamp3")

        messages = [
            {
                "role": "system",
                "content": SYSTEM.format(
                    user_id=user_id,
                    read_tool=read_tool.name,
                    search_tool=search_tool.name,
                    vs_id=vs_id,
                ),
            },
            {
                "role": "user",
                "content": USER.format(ask=ask or "play what I usually like"),
            },
        ]
        llm_tools = [_as_ollama_tool(read_tool), _as_ollama_tool(search_tool)]
        mood_ids: list[int] = []
        final: dict[str, Any] = {}
        search_args: dict[str, Any] = {}

        for _ in range(6):
            # Run the blocking Ollama call in a thread so the event loop stays free.
            reply = await asyncio.to_thread(
                ollama.chat,
                model=model,
                messages=messages,
                tools=llm_tools,
                think=False,
            )
            msg = reply.message
            messages.append(msg)
            if not msg.tool_calls:
                final = _first_json_object(msg.content)
                if final or not mood_ids:
                    break
                # Searched but forgot the closing JSON: ask once more.
                messages.append(
                    {
                        "role": "user",
                        "content": "Now reply with only the JSON from step 4.",
                    }
                )
                continue
            for call in msg.tool_calls:
                args = dict(call.function.arguments)
                say(f"LLM -> {call.function.name}({json.dumps(args)[:300]})")
                result = _parse(await mcp.call_tool(call.function.name, args))
                if call.function.name == search_tool.name:
                    mood_ids = _track_ids_from_openai_search(result)
                    search_args = args
                messages.append(
                    {
                        "role": "tool",
                        "tool_name": call.function.name,
                        "content": json.dumps(result)[:3000],
                    }
                )

        sounds_like = await _sounds_like_last(mcp, read_tool, vector_search, user_id)
        say(
            f"Code -> features_search on track_spectral: {len(sounds_like)} sound-alikes"
        )
        return {
            **final,
            "query": search_args.get("query"),
            "mood_ids": mood_ids,
            "sounds_like_ids": sounds_like,
            # Plays and skips, read in code so a skipped song never comes back.
            "recent": await _recent(mcp, read_tool, user_id),
        }


async def _recent(mcp: ClientSession, read_tool: Any, user_id: str) -> list[int]:
    got = _parse(
        await mcp.call_tool(
            read_tool.name,
            {
                "features": [
                    "user_listening:recent_track_ids",
                    "user_listening:skipped_track_ids",
                ],
                "entities": {"user_id": [user_id]},
            },
        )
    )
    names = got["metadata"]["feature_names"]
    vals = {
        n: json.loads(r["values"][0] or "[]")
        for n, r in zip(names, got["results"])
        if n != "user_id"
    }
    return vals.get("recent_track_ids", []) + vals.get("skipped_track_ids", [])


async def _sounds_like_last(
    mcp: ClientSession, read_tool: Any, vector_search: Any, user_id: str, k: int = 6
) -> list[int]:
    """Feast /search on track_spectral with the last song's spectral vector."""
    got = _parse(
        await mcp.call_tool(
            read_tool.name,
            {
                "features": ["user_listening:last_track_id"],
                "entities": {"user_id": [user_id]},
            },
        )
    )
    last = got["results"][-1]["values"][0]
    if not last:
        return []
    vec = _parse(
        await mcp.call_tool(
            read_tool.name,
            {
                "features": ["track_spectral:spectral_vector"],
                "entities": {"track_id": [last]},
            },
        )
    )["results"][-1]["values"][0]
    found = _parse(
        await mcp.call_tool(
            vector_search.name,
            {
                "features": ["track_spectral:title", "track_spectral:genre"],
                "query": vec,
                "top_k": k + 1,
                "api_version": 2,
            },
        )
    )
    names = found["metadata"]["feature_names"]
    ids = found["results"][names.index("track_id")]["values"]
    return [int(t) for t in ids if t != last][:k]


def build_playlist(plan: dict[str, Any], size: int = 12) -> list[int]:
    seen = set(plan.get("recent", []))
    picks: list[int] = []
    # Mostly the mood search, with a few "sounds like the last song" picks mixed in.
    mood = [t for t in plan.get("mood_ids", []) if t not in seen]
    like = [t for t in plan.get("sounds_like_ids", []) if t not in seen]
    while (mood or like) and len(picks) < size:
        for source, n in ((mood, 3), (like, 1)):
            for _ in range(n):
                if source:
                    t = source.pop(0)
                    if t not in picks:
                        picks.append(t)
    return picks[:size]


def save_playlist(plan: dict[str, Any], picks: list[int]) -> tuple[str, int, str]:
    """Create the playlist in Navidrome. Returns (name, song count, playlist id)."""
    nd = Navidrome()
    song_ids = nd.fma_song_ids()
    ids = [song_ids[t] for t in picks if t in song_ids]
    name = f"AI DJ: {plan.get('vibe') or 'for you'}"
    return name, len(ids), nd.create_playlist(name, ids)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("ask", nargs="?", default="")
    parser.add_argument("--user", default=USER_ID)
    parser.add_argument("--model", default=OLLAMA_MODEL)
    parser.add_argument("--no-playlist", action="store_true")
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args(argv)

    plan = asyncio.run(run_dj(args.ask, args.user, args.model, args.verbose))
    picks = build_playlist(plan)
    print(f"\nDJ: {plan.get('intro', '')}")
    print(f"Vibe: {plan.get('vibe')}  |  search: {plan.get('query')!r}")
    print(
        f"{len(plan.get('mood_ids', []))} mood matches, {len(plan.get('sounds_like_ids', []))} sound-alikes -> {len(picks)} new tracks"
    )

    if picks and not args.no_playlist:
        name, count, playlist_id = save_playlist(plan, picks)
        print(f"Saved playlist {name!r} ({count} songs) in Navidrome, id {playlist_id}")
    print(json.dumps({"picks": picks}))


if __name__ == "__main__":
    main()
