"""First-run Navidrome setup. Safe to run again; start.sh runs it every time.

1. Create the local login from config/.env (only works while Navidrome has no users).
2. Link the user's ListenBrainz account to the bridge, so every play reaches Feast.
3. Scan the music folder and wait until the whole catalog is in the library.
"""

import sys
import time
from pathlib import Path

import httpx
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from dj.config import NAVIDROME_URL, USER_ID, navidrome_credentials  # noqa: E402
from dj.navidrome import Navidrome  # noqa: E402


def main() -> None:
    user, password = navidrome_credentials()
    r = httpx.post(
        f"{NAVIDROME_URL}/auth/createAdmin",
        json={"username": user, "password": password},
    )
    print(f"  navidrome  {'created user ' + user if r.is_success else 'user exists'}")

    nd = Navidrome()
    # The bridge treats the ListenBrainz token as the Feast user_id.
    nd.link_listenbrainz(USER_ID)

    expected = len(pd.read_parquet(ROOT / "data" / "catalog.parquet"))
    have = len(nd.fma_song_ids())
    if have < expected:
        nd.subsonic("startScan")
        for _ in range(150):
            time.sleep(2)
            have = len(nd.fma_song_ids())
            if have >= expected:
                break
    print(f"  navidrome  {have}/{expected} tracks in the library")


if __name__ == "__main__":
    main()
