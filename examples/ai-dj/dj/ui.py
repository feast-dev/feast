"""Web UI for the AI DJ.

See your taste and recent plays (read from Feast), ask the DJ for a playlist,
watch the agent call Feast through MCP, and play the songs in the browser.
Plays from the UI go through Navidrome's scrobble API, so they reach Feast the
same way plays in Navidrome do: Navidrome -> bridge -> Feast push.

uv run python -m dj.ui   ->  http://127.0.0.1:6600
"""

import asyncio
import json
import subprocess
import sys
from functools import lru_cache
from pathlib import Path
from typing import Any

import httpx
import pandas as pd
import uvicorn
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, StreamingResponse
from pydantic import BaseModel

from dj.agent import build_playlist, run_dj, save_playlist
from dj.config import (
    BRIDGE_PORT,
    CLAMP3_URL,
    DATA,
    FEAST_URL,
    MCP_URL,
    NAVIDROME_URL,
    OLLAMA_MODEL,
    OLLAMA_URL,
    ROOT,
    UI_PORT,
    USER_ID,
)
from dj.feast_client import get_online_features
from dj.navidrome import Navidrome
from dj.taste import build_taste

STATIC = Path(__file__).resolve().parent / "static"
MUSIC = DATA / "music"
SERVICES = {
    "CLaMP 3": f"{CLAMP3_URL}/health",
    "Feast": f"{FEAST_URL}/health",
    "feast mcp": MCP_URL,
    "Bridge": f"http://127.0.0.1:{BRIDGE_PORT}/docs",
    "Navidrome": f"{NAVIDROME_URL}/ping",
    "Ollama": f"{OLLAMA_URL}/api/tags",
}

app = FastAPI(title="Feast AI DJ")
catalog = pd.read_parquet(DATA / "catalog.parquet").set_index("track_id")


def track_info(track_id: int) -> dict[str, Any]:
    if track_id not in catalog.index:
        return {
            "track_id": track_id,
            "title": f"Track {track_id}",
            "artist": "",
            "genre": "",
        }
    row = catalog.loc[track_id]
    return {
        "track_id": int(track_id),
        "title": row["title"],
        "artist": row["artist"],
        "genre": row["genre"],
        "year": int(row["year"]),
    }


@lru_cache(maxsize=1)
def navidrome() -> Navidrome:
    return Navidrome()


@lru_cache(maxsize=1)
def song_ids() -> dict[int, str]:
    return navidrome().fma_song_ids()


@app.get("/")
def index() -> FileResponse:
    return FileResponse(STATIC / "index.html")


@app.get("/api/status")
async def status() -> dict[str, Any]:
    async def up(url: str) -> bool:
        try:
            async with httpx.AsyncClient(timeout=1.5) as client:
                await client.get(url)
            return True
        except httpx.HTTPError:
            return False

    states = await asyncio.gather(*(up(u) for u in SERVICES.values()))
    return {
        "user_id": USER_ID,
        "model": OLLAMA_MODEL,
        "navidrome_url": NAVIDROME_URL,
        "services": dict(zip(SERVICES, states)),
    }


@app.get("/api/listener")
def listener() -> dict[str, Any]:
    """The listener as Feast sees it: taste clusters and recent plays."""
    got = get_online_features(
        [
            "user_taste:taste_summary",
            "user_taste:top_genres",
            "user_listening:recent_track_ids",
            "user_listening:skipped_track_ids",
        ],
        {"user_id": [USER_ID]},
    )
    recent = json.loads(got["recent_track_ids"][0] or "[]")
    skipped = json.loads(got["skipped_track_ids"][0] or "[]")
    return {
        "taste_summary": got["taste_summary"][0] or "",
        "top_genres": json.loads(got["top_genres"][0] or "[]"),
        "plays": len(recent),
        "recent": [track_info(t) for t in reversed(recent[-12:])],
        "skipped": [track_info(t) for t in reversed(skipped[-6:])],
    }


@app.post("/api/taste")
async def taste() -> dict[str, Any]:
    try:
        return await asyncio.to_thread(build_taste, USER_ID)
    except SystemExit as e:  # build_taste exits when there are no plays yet
        raise HTTPException(400, str(e)) from e


@app.post("/api/simulate")
async def simulate() -> dict[str, Any]:
    """Run scripts/simulate_plays.py: a listener who likes Hip-Hop and Electronic."""
    proc = await asyncio.to_thread(
        subprocess.run,
        [sys.executable, "scripts/simulate_plays.py"],
        cwd=ROOT,
        capture_output=True,
        text=True,
    )
    if proc.returncode:
        raise HTTPException(500, proc.stderr[-2000:])
    return {"lines": proc.stdout.splitlines()}


@app.get("/api/dj")
async def dj(ask: str = "") -> StreamingResponse:
    """Run the agent and stream each step as a server-sent event."""
    queue: asyncio.Queue[tuple[str, Any]] = asyncio.Queue()

    async def produce() -> None:
        try:
            plan = await run_dj(
                ask,
                USER_ID,
                OLLAMA_MODEL,
                False,
                on_step=lambda s: queue.put_nowait(("step", s)),
            )
            picks = build_playlist(plan)
            playlist: dict[str, Any] = {}
            if picks:
                name, count, pid = await asyncio.to_thread(save_playlist, plan, picks)
                playlist = {
                    "name": name,
                    "count": count,
                    "url": f"{NAVIDROME_URL}/app/#/playlist/{pid}/show",
                }
                queue.put_nowait(("step", f"Navidrome -> createPlaylist {name!r}"))
            mood = set(plan.get("mood_ids", []))
            tracks = [
                {**track_info(t), "source": "mood" if t in mood else "sounds like"}
                for t in picks
            ]
            queue.put_nowait(
                (
                    "done",
                    {
                        "intro": plan.get("intro", ""),
                        "vibe": plan.get("vibe", ""),
                        "query": plan.get("query"),
                        "mood_matches": len(plan.get("mood_ids", [])),
                        "sound_alikes": len(plan.get("sounds_like_ids", [])),
                        "tracks": tracks,
                        "playlist": playlist,
                    },
                )
            )
        except Exception as e:  # noqa: BLE001 - show any failure in the UI
            # The MCP client wraps errors in task groups; show the real one.
            while getattr(e, "exceptions", None):
                e = e.exceptions[0]  # type: ignore[attr-defined]
            queue.put_nowait(("fail", f"{type(e).__name__}: {e}"))

    task = asyncio.create_task(produce())

    async def stream():
        while True:
            kind, data = await queue.get()
            yield f"event: {kind}\ndata: {json.dumps(data)}\n\n"
            if kind in ("done", "fail"):
                await task
                return

    return StreamingResponse(stream(), media_type="text/event-stream")


@app.get("/api/audio/{track_id}")
def audio(track_id: int) -> FileResponse:
    path = MUSIC / f"{track_id:06d}.mp3"
    if not path.exists():
        raise HTTPException(404, "no such track")
    return FileResponse(path, media_type="audio/mpeg")


class Scrobble(BaseModel):
    track_id: int
    listened: bool


@app.post("/api/scrobble")
def scrobble(req: Scrobble) -> dict[str, str]:
    """Tell Navidrome a song started (listened=False) or was listened to (True).

    Navidrome forwards it to the bridge, which pushes it to Feast. A song that
    starts but never gets a listen before the next one starts counts as a skip.
    """
    sid = song_ids().get(req.track_id)
    if sid is None:
        raise HTTPException(404, "track is not in Navidrome")
    params: dict[str, Any] = {"id": sid, "submission": str(req.listened).lower()}
    navidrome().subsonic("scrobble", **params)
    return {"status": "ok"}


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=UI_PORT)
