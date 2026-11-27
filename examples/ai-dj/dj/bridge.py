"""A tiny ListenBrainz-compatible API that turns Navidrome plays into Feast pushes.

Navidrome scrobbles to ListenBrainz. We point its ListenBrainz.BaseURL here, so:
  playing_now  -> a song started
  single       -> a song was really listened to (Navidrome's scrobble rules)
A song that started but never got a `single` before the next one started counts
as a skip.
"""

import json
import logging
import re
from datetime import UTC, datetime
from typing import Any

import httpx
import pandas as pd
import uvicorn
from fastapi import FastAPI, Header, Request

from dj.config import BRIDGE_PORT, DATA
from dj.feast_client import get_online_features, push
from dj.navidrome import Navidrome

log = logging.getLogger("bridge")
logging.basicConfig(level=logging.INFO, format="%(asctime)s bridge %(message)s")

MAX_RECENT = 50
app = FastAPI(title="ListenBrainz to Feast bridge")

catalog = pd.read_parquet(DATA / "catalog.parquet")
_by_title: dict[tuple, int] = {
    (r.artist.lower(), r.title.lower()): int(r.track_id)
    for r in catalog.itertuples(index=False)
}


def _load_navidrome_tags() -> None:
    # Navidrome scrobbles the tags it read from the file, which can differ from
    # the FMA CSV. Map those tags to the FMA id in the file name.
    try:
        for s in Navidrome().songs():
            m = re.search(r"(\d{6})\.mp3$", s.get("path", ""))
            if m:
                key = (s.get("artist", "").lower(), s.get("title", "").lower())
                _by_title[key] = int(m.group(1))
    except httpx.HTTPError as e:
        log.warning("could not read Navidrome songs: %s", e)


class UserState:
    def __init__(self, user_id: str) -> None:
        self.user_id = user_id
        self.recent: list[int] = []
        self.skipped: list[int] = []
        self.now_playing: int | None = None
        self.now_playing_listened = False
        # Start from what Feast already knows, so a restart keeps history.
        try:
            got = get_online_features(
                ["user_listening:recent_track_ids", "user_listening:skipped_track_ids"],
                {"user_id": [user_id]},
            )
            self.recent = json.loads(got["recent_track_ids"][0] or "[]")
            self.skipped = json.loads(got["skipped_track_ids"][0] or "[]")
        except (httpx.HTTPError, KeyError, ValueError) as e:
            log.info("no saved history for %s yet: %s", user_id, e)


users: dict[str, UserState] = {}


def track_id_of(listen: dict[str, Any]) -> int | None:
    meta = listen.get("track_metadata", {})
    key = (meta.get("artist_name", "").lower(), meta.get("track_name", "").lower())
    if key not in _by_title:
        _load_navidrome_tags()
    return _by_title.get(key)


def save(state: UserState) -> None:
    push(
        "user_listening_push",
        {
            "user_id": [state.user_id],
            "event_timestamp": [datetime.now(UTC).isoformat()],
            "last_track_id": [state.recent[-1] if state.recent else 0],
            "recent_track_ids": [json.dumps(state.recent[-MAX_RECENT:])],
            "skipped_track_ids": [json.dumps(state.skipped[-MAX_RECENT:])],
        },
    )


@app.get("/1/validate-token")
def validate_token(authorization: str = Header("")) -> dict[str, Any]:
    user = authorization.removeprefix("Token ").strip()
    return {"code": 200, "message": "Token valid.", "valid": True, "user_name": user}


@app.post("/1/submit-listens")
async def submit_listens(
    request: Request, authorization: str = Header("")
) -> dict[str, str]:
    user_id = authorization.removeprefix("Token ").strip() or "demo"
    body = await request.json()
    state = users.setdefault(user_id, UserState(user_id))
    changed = False
    for listen in body.get("payload", []):
        track_id = track_id_of(listen)
        if track_id is None:
            log.info("unknown track %s", listen.get("track_metadata"))
            continue
        if body.get("listen_type") == "playing_now":
            if state.now_playing and not state.now_playing_listened:
                state.skipped.append(state.now_playing)
                log.info("%s skipped %s", user_id, state.now_playing)
                changed = True
            state.now_playing, state.now_playing_listened = track_id, False
            log.info("%s started %s", user_id, track_id)
        else:  # "single" or "import": a real listen
            # Navidrome sends listens in batches, often after the next song has
            # started, so a song we marked as skipped may turn out to be a listen.
            if track_id in state.skipped:
                state.skipped.remove(track_id)
            state.recent.append(track_id)
            if track_id == state.now_playing:
                state.now_playing_listened = True
            log.info("%s listened to %s", user_id, track_id)
            changed = True
    if changed:
        save(state)
    return {"status": "ok"}


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=BRIDGE_PORT)
