"""Play songs through Navidrome without clicking in the browser.

Uses Navidrome's Subsonic scrobble API, the same call a player app makes:
  submission=false  -> "now playing"
  submission=true   -> "listened"
Navidrome forwards both to its ListenBrainz scrobbler, which points at our
bridge, which pushes to Feast. A song with only "now playing" is a skip.
"""

import argparse
import sys
import time
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from dj.navidrome import Navidrome  # noqa: E402


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--likes", nargs="+", default=["Hip-Hop", "Electronic"])
    parser.add_argument("--skips", nargs="+", default=["Rock"])
    parser.add_argument("--plays", type=int, default=15)
    args = parser.parse_args()

    catalog = pd.read_parquet(ROOT / "data" / "catalog.parquet")
    nd = Navidrome()
    song_ids = nd.fma_song_ids()
    catalog = catalog[catalog["track_id"].isin(song_ids)]

    # The quick catalog has only a few songs per genre, so take what is there.
    liked = catalog[catalog["genre"].isin(args.likes)]
    liked = liked.sample(min(args.plays, len(liked)), random_state=1)
    skipped = catalog[catalog["genre"].isin(args.skips)]
    skipped = skipped.sample(min(3, len(skipped)), random_state=1)
    # Mix the skips in between real listens.
    order = [(t, True) for t in liked["track_id"]]
    for i, t in enumerate(skipped["track_id"]):
        order.insert(3 + i * 4, (t, False))

    for track_id, listened in order:
        sid = song_ids[int(track_id)]
        nd.subsonic("scrobble", id=sid, submission="false")
        time.sleep(0.5)
        if listened:
            nd.subsonic(
                "scrobble", id=sid, submission="true", time=int(time.time() * 1000)
            )
        row = catalog[catalog["track_id"] == track_id].iloc[0]
        print(
            f"{'played ' if listened else 'skipped'} {row.genre:<12} {row.artist} - {row.title}"
        )
        time.sleep(0.5)


if __name__ == "__main__":
    main()
