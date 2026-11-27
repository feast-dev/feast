"""Clustering job: turn a listener's recent plays into taste clusters.

1. Read recent plays from Feast (user_listening).
2. Read the CLaMP 3 vector and genre of each played track (track_clamp3).
3. Run k-means on the vectors.
4. Label each cluster by its most common genre and push the result to user_taste.

The agent reads the text summary, so it can search with words.
"""

import argparse
import json
from collections import Counter
from datetime import UTC, datetime

import numpy as np
from sklearn.cluster import KMeans

from dj.clamp3_client import embed_text
from dj.config import USER_ID
from dj.feast_client import get_online_features, push

# Words CLaMP 3 can match against a cluster center (zero-shot tagging), so each
# cluster gets a label that describes how it sounds, not just a genre.
SOUND_WORDS = [
    "chill",
    "energetic",
    "dark",
    "upbeat",
    "dreamy",
    "aggressive",
    "melancholic",
    "acoustic guitar",
    "piano",
    "synthesizers",
    "electronic beats",
    "rap vocals",
    "female vocals",
    "male vocals",
    "ambient textures",
    "distorted guitars",
    "funky bass",
    "orchestral",
    "lo-fi",
    "dance",
]


def describe(center: np.ndarray, words: np.ndarray, n: int = 2) -> str:
    scores = words @ (center / np.linalg.norm(center))
    return ", ".join(SOUND_WORDS[i] for i in np.argsort(-scores)[:n])


def build_taste(user_id: str, max_clusters: int = 3) -> dict:
    listening = get_online_features(
        ["user_listening:recent_track_ids", "user_listening:skipped_track_ids"],
        {"user_id": [user_id]},
    )
    recent = json.loads(listening["recent_track_ids"][0] or "[]")
    skipped = set(json.loads(listening["skipped_track_ids"][0] or "[]"))
    played = [t for t in recent if t not in skipped]
    if not played:
        raise SystemExit(f"No plays for {user_id} yet. Play some songs first.")

    tracks = get_online_features(
        ["track_clamp3:clamp3_vector", "track_clamp3:genre"], {"track_id": played}
    )
    pairs = [
        (np.array(v, dtype=np.float32), g)
        for v, g in zip(tracks["clamp3_vector"], tracks["genre"])
        if v is not None
    ]
    vectors = np.stack([v for v, _ in pairs])
    genres = [g for _, g in pairs]

    k = max(1, min(max_clusters, len(pairs) // 3))
    kmeans = KMeans(n_clusters=k, n_init=10, random_state=0).fit(vectors)
    words = embed_text(SOUND_WORDS)

    clusters = []
    for c in range(k):
        member_genres = [g for g, lab in zip(genres, kmeans.labels_) if lab == c]
        genre = Counter(member_genres).most_common(1)[0][0]
        clusters.append(
            {
                "label": f"{describe(kmeans.cluster_centers_[c], words)} {genre}",
                "share": len(member_genres) / len(genres),
            }
        )
    clusters.sort(key=lambda c: -c["share"])
    summary = ", ".join(f"{c['label']} ({c['share']:.0%})" for c in clusters)
    top_genres = [g for g, _ in Counter(genres).most_common(3)]

    push(
        "user_taste_push",
        {
            "user_id": [user_id],
            "event_timestamp": [datetime.now(UTC).isoformat()],
            "taste_summary": [summary],
            "top_genres": [json.dumps(top_genres)],
            "cluster_count": [k],
        },
    )
    return {
        "summary": summary,
        "top_genres": top_genres,
        "clusters": k,
        "plays": len(pairs),
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--user", default=USER_ID)
    print(build_taste(parser.parse_args().user))
