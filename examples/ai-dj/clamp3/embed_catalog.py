"""Embed every catalog track with CLaMP 3. Writes data/clamp3_vectors.parquet.

1. ffmpeg: MP3 -> 24 kHz mono WAV (torchaudio 2.5 is picky about MP3).
2. MERT-v1-95M: one 768-number vector per 5-second window (official script).
3. CLaMP 3: a transformer reads all windows of a track together (up to 640 s)
   and returns one 768-number embedding for the whole track.
"""

import subprocess
import sys
import tempfile
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import numpy as np
import pandas as pd
from model import HERE, get_clamp3

ROOT = HERE.parent
MUSIC = ROOT / "data" / "music"
CATALOG = ROOT / "data" / "catalog.parquet"
OUT = ROOT / "data" / "clamp3_vectors.parquet"
MERT = ROOT / "data" / "clamp3" / "mert"  # one .npy per track, kept between runs


def to_wav(mp3: Path, out_dir: Path) -> None:
    subprocess.run(
        [
            "ffmpeg",
            "-nostdin",
            "-loglevel",
            "error",
            "-y",
            "-i",
            str(mp3),
            "-ac",
            "1",
            "-ar",
            "24000",
            str(out_dir / f"{mp3.stem}.wav"),
        ],
        check=True,
    )


def main() -> None:
    ids = pd.read_parquet(CATALOG)["track_id"].tolist()
    mp3s = [MUSIC / f"{t:06d}.mp3" for t in ids]
    MERT.mkdir(parents=True, exist_ok=True)
    todo = [p for p in mp3s if not (MERT / f"{p.stem}.npy").exists()]
    with tempfile.TemporaryDirectory() as tmp:
        wav_dir = Path(tmp)
        with ThreadPoolExecutor(8) as pool:
            list(pool.map(lambda p: to_wav(p, wav_dir), todo))
        print(
            f"{len(mp3s) - len(todo)} tracks already have MERT features, converting {len(todo)}",
            flush=True,
        )
        if todo:
            # The official script skips tracks whose .npy already exists.
            subprocess.run(
                [
                    sys.executable,
                    "extract_mert.py",
                    "--input_path",
                    str(wav_dir),
                    "--output_path",
                    str(MERT),
                    "--model_path",
                    "m-a-p/MERT-v1-95M",
                    "--mean_features",
                ],
                cwd=HERE / "vendor" / "clamp3" / "preprocessing" / "audio",
                check=True,
            )

    model = get_clamp3()
    found = {int(npy.stem): npy for npy in MERT.rglob("*.npy")}
    rows = [
        {"track_id": t, "clamp3_vector": model.embed_mert(np.load(found[t])).tolist()}
        for t in ids
        if t in found
    ]
    pd.DataFrame(rows).to_parquet(OUT, index=False)
    print(f"Wrote {len(rows)} vectors to {OUT.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
