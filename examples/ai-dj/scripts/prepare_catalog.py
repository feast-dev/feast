"""Pick a genre-balanced set of FMA small tracks and download only those MP3s.

The FMA zips are big: 342 MB of metadata and 7.2 GiB of audio. A zip keeps its
file index at the end, and the server supports HTTP range requests, so we read
only tracks.csv and the MP3s we need instead of downloading whole zips.

  uv run python scripts/prepare_catalog.py                # 12 tracks, quick
  uv run python scripts/prepare_catalog.py --tracks 1000  # the blog's catalog

Writes:
  data/music/<id>.mp3         the audio files Navidrome will scan
  data/catalog.parquet        one row per track: id, title, artist, genre, year
"""

import argparse
import io
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pandas as pd
from remotezip import RemoteZip

ROOT = Path(__file__).resolve().parents[1]
MUSIC = ROOT / "data" / "music"
TRACKS_CACHE = ROOT / "data" / "fma_small_tracks.parquet"
FMA_METADATA_URL = "https://os.unil.cloud.switch.ch/fma/fma_metadata.zip"
FMA_SMALL_URL = "https://os.unil.cloud.switch.ch/fma/fma_small.zip"


def load_tracks() -> pd.DataFrame:
    """All 8,000 FMA small tracks with their tags. Cached after the first run."""
    if TRACKS_CACHE.exists():
        return pd.read_parquet(TRACKS_CACHE)
    print("Reading tracks.csv from the FMA metadata zip")
    with RemoteZip(FMA_METADATA_URL) as z:
        raw = z.read("fma_metadata/tracks.csv")
    tracks = pd.read_csv(io.BytesIO(raw), index_col=0, header=[0, 1], low_memory=False)
    small = tracks[tracks[("set", "subset")] == "small"]
    released = pd.to_datetime(small[("album", "date_released")], errors="coerce")
    created = pd.to_datetime(small[("track", "date_created")], errors="coerce")
    out = pd.DataFrame(
        {
            "track_id": small.index.astype("int64"),
            "title": small[("track", "title")].fillna("Untitled").astype(str),
            "artist": small[("artist", "name")].fillna("Unknown").astype(str),
            "genre": small[("track", "genre_top")].astype(str),
            "year": released.fillna(created).dt.year.fillna(0).astype("int64"),
        }
    ).reset_index(drop=True)
    TRACKS_CACHE.parent.mkdir(parents=True, exist_ok=True)
    out.to_parquet(TRACKS_CACHE, index=False)
    return out


def pick(tracks: pd.DataFrame, n: int) -> pd.DataFrame:
    """n tracks, spread as evenly as possible over the 8 genres."""
    shuffled = tracks.sample(frac=1, random_state=7)
    rank = shuffled.groupby("genre").cumcount()
    return (
        shuffled.assign(rank=rank)
        .sort_values(["rank", "genre"])
        .head(n)
        .drop(columns="rank")
        .sort_values("track_id")
        .reset_index(drop=True)
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--tracks", type=int, default=12)
    parser.add_argument("--workers", type=int, default=6)
    args = parser.parse_args()

    catalog = pick(load_tracks(), args.tracks)
    print(f"Picked {len(catalog)} tracks:", catalog["genre"].value_counts().to_dict())

    MUSIC.mkdir(parents=True, exist_ok=True)
    # Navidrome scans the whole folder, so keep only the picked tracks in it.
    wanted = {f"{t:06d}.mp3" for t in catalog["track_id"]}
    extra = [p for p in MUSIC.iterdir() if p.name not in wanted]
    for p in extra:
        p.unlink()
    if extra:
        print(f"Removed {len(extra)} files that are not in this catalog")
    todo = [t for t in catalog["track_id"] if not (MUSIC / f"{t:06d}.mp3").exists()]
    workers = max(1, min(args.workers, len(todo)))
    chunks = [todo[i::workers] for i in range(workers)]

    def fetch(chunk: list[int]) -> None:
        if not chunk:
            return
        # One RemoteZip per thread: each reads the zip index once, then
        # fetches only the byte ranges of its files.
        with RemoteZip(FMA_SMALL_URL) as z:
            for track_id in chunk:
                name = f"{track_id:06d}"
                tmp = MUSIC / f"{name}.part"
                tmp.write_bytes(z.read(f"fma_small/{name[:3]}/{name}.mp3"))
                tmp.rename(MUSIC / f"{name}.mp3")

    print(f"Downloading {len(todo)} files with {workers} connections")
    with ThreadPoolExecutor(workers) as pool:
        list(pool.map(fetch, chunks))

    # Keep only tracks whose audio we actually have.
    catalog = catalog[
        catalog["track_id"].map(lambda t: (MUSIC / f"{t:06d}.mp3").exists())
    ]
    catalog.to_parquet(ROOT / "data" / "catalog.parquet", index=False)
    print(f"Wrote data/catalog.parquet with {len(catalog)} tracks")


if __name__ == "__main__":
    main()
