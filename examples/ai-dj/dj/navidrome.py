"""Tiny Navidrome client: native API (login, songs, ListenBrainz link) and Subsonic API."""

import hashlib
import re
import secrets
from functools import cached_property
from typing import Any

import httpx

from dj.config import NAVIDROME_URL, navidrome_credentials


class Navidrome:
    def __init__(self, url: str = NAVIDROME_URL) -> None:
        self.url = url
        self.user, self.password = navidrome_credentials()

    # ---- native API (what the web UI uses) ----
    @cached_property
    def jwt(self) -> str:
        r = httpx.post(
            f"{self.url}/auth/login",
            json={"username": self.user, "password": self.password},
        )
        r.raise_for_status()
        return r.json()["token"]

    def _native(self, method: str, path: str, **kw) -> httpx.Response:
        headers = {"x-nd-authorization": f"Bearer {self.jwt}"}
        r = httpx.request(
            method, f"{self.url}{path}", headers=headers, timeout=30, **kw
        )
        r.raise_for_status()
        return r

    def songs(self) -> list[dict[str, Any]]:
        return self._native(
            "GET", "/api/song", params={"_start": 0, "_end": 10000}
        ).json()

    def link_listenbrainz(self, token: str) -> None:
        self._native("PUT", "/api/listenbrainz/link", json={"token": token})

    def fma_song_ids(self) -> dict[int, str]:
        """Map FMA track_id (from the file name, e.g. 000002.mp3) to Navidrome song id."""
        out = {}
        for s in self.songs():
            m = re.search(r"(\d{6})\.mp3$", s.get("path", ""))
            if m:
                out[int(m.group(1))] = s["id"]
        return out

    # ---- Subsonic API ----
    def subsonic(self, method: str, **params: Any) -> dict[str, Any]:
        salt = secrets.token_hex(6)
        auth = {
            "u": self.user,
            "t": hashlib.md5((self.password + salt).encode()).hexdigest(),
            "s": salt,
            "v": "1.16.1",
            "c": "feast-ai-dj",
            "f": "json",
        }
        r = httpx.get(
            f"{self.url}/rest/{method}", params={**auth, **params}, timeout=60
        )
        r.raise_for_status()
        body = r.json()["subsonic-response"]
        if body.get("status") != "ok":
            raise RuntimeError(f"Subsonic {method} failed: {body.get('error')}")
        return body

    def create_playlist(self, name: str, song_ids: list[str]) -> str:
        body = self.subsonic("createPlaylist", name=name, songId=song_ids)
        return body["playlist"]["id"]
