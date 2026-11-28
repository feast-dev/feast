"""Turn each track into two vectors and write them as Feast offline data.

  spectral_vector: a mel spectrogram (a Fourier transform per short frame,
                   grouped into 384 frequency bands), then the mean and std
                   of each band over time. 384 + 384 = 768 numbers.
  clamp3_vector:   the CLaMP 3 embedding of the whole track, made by
                   clamp3/embed_catalog.py (run that first, in its own env).

Both are 768 numbers: the Milvus store uses one embedding_dim per project.

Writes data/track_clamp3.parquet and data/track_spectral.parquet, plus empty
offline files for the two push sources.
"""

from datetime import UTC, datetime
from pathlib import Path

import librosa
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"
MUSIC = DATA / "music"
SAMPLE_RATE = 48_000


def spectral_stats(y: np.ndarray) -> np.ndarray:
    mel = librosa.feature.melspectrogram(
        y=y, sr=SAMPLE_RATE, n_fft=8192, hop_length=2048, n_mels=384
    )
    db = librosa.power_to_db(mel)
    return np.concatenate([db.mean(axis=1), db.std(axis=1)]).astype(np.float32)


def main() -> None:
    catalog = pd.read_parquet(DATA / "catalog.parquet")
    clamp3 = pd.read_parquet(DATA / "clamp3_vectors.parquet").set_index("track_id")

    rows, spectral = [], []
    for i, track in enumerate(catalog.itertuples(index=False), 1):
        if track.track_id not in clamp3.index:
            print(f"  skip {track.track_id}: no CLaMP 3 vector")
            continue
        path = MUSIC / f"{track.track_id:06d}.mp3"
        try:
            y, _ = librosa.load(path, sr=SAMPLE_RATE, mono=True)
        except Exception as e:  # noqa: BLE001 - a few FMA files are broken
            print(f"  skip {path.name}: {e}")
            continue
        spectral.append(spectral_stats(y))
        rows.append(track._asdict())
        if i % 100 == 0:
            print(f"  {i}/{len(catalog)}")

    # Scale each band so loud bands don't dominate, then make unit length for cosine.
    spec = np.stack(spectral)
    spec = (spec - spec.mean(axis=0)) / (spec.std(axis=0) + 1e-6)
    spec /= np.linalg.norm(spec, axis=1, keepdims=True)

    base = pd.DataFrame(rows)
    base["event_timestamp"] = datetime.now(UTC)
    clamp3_df = base.assign(
        clamp3_vector=[clamp3.loc[t, "clamp3_vector"] for t in base["track_id"]]
    )
    spec_df = base.assign(spectral_vector=[v.astype(np.float32).tolist() for v in spec])
    clamp3_df.to_parquet(DATA / "track_clamp3.parquet", index=False)
    spec_df.to_parquet(DATA / "track_spectral.parquet", index=False)
    print(
        f"Wrote {len(base)} tracks to track_clamp3.parquet and track_spectral.parquet"
    )

    write_empty_push_sources()


def write_empty_push_sources() -> None:
    # Pushes with to="online_and_offline" append here. Empty pandas string
    # columns would be saved as type null, so give Arrow the schema directly.
    ts = ("event_timestamp", pa.timestamp("us", tz="UTC"))
    schemas = {
        "user_listening.parquet": [
            ("user_id", pa.string()),
            ts,
            ("last_track_id", pa.int64()),
            ("recent_track_ids", pa.string()),
            ("skipped_track_ids", pa.string()),
        ],
        "user_taste.parquet": [
            ("user_id", pa.string()),
            ts,
            ("taste_summary", pa.string()),
            ("top_genres", pa.string()),
            ("cluster_count", pa.int64()),
        ],
    }
    for name, fields in schemas.items():
        if not (DATA / name).exists():  # keep play history on a re-run
            pq.write_table(pa.schema(fields).empty_table(), DATA / name)


if __name__ == "__main__":
    main()
