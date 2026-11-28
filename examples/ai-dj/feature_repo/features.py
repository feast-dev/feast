from datetime import timedelta
from pathlib import Path

from feast import Entity, FeatureView, Field, FileSource, PushSource, ValueType
from feast.types import Array, Float32, Int64, String

DATA = Path(__file__).resolve().parents[1] / "data"

track = Entity(name="track", join_keys=["track_id"], value_type=ValueType.INT64)
user = Entity(name="user", join_keys=["user_id"], value_type=ValueType.STRING)


def track_tags() -> list[Field]:
    # Copied into both track views: a search reads one feature view, and
    # filters only work on fields of that view.
    return [
        Field(name="title", dtype=String),
        Field(name="artist", dtype=String),
        Field(name="genre", dtype=String),
        Field(name="year", dtype=Int64),
    ]


# CLaMP 3 puts music and text in the same space, so a mood written in words
# can find songs. It reads the whole track (up to 640 s) as one sequence.
# Feast allows one vector field per feature view.
track_clamp3 = FeatureView(
    name="track_clamp3",
    entities=[track],
    schema=[
        Field(
            name="clamp3_vector",
            dtype=Array(Float32),
            vector_index=True,
            vector_search_metric="COSINE",
            vector_length=768,
        ),
        *track_tags(),
    ],
    source=FileSource(
        name="track_clamp3_source",
        path=str(DATA / "track_clamp3.parquet"),
        timestamp_field="event_timestamp",
    ),
    ttl=timedelta(days=3650),
)

# Mel spectrogram stats (a Fourier transform per frame): how a song *sounds*.
track_spectral = FeatureView(
    name="track_spectral",
    entities=[track],
    schema=[
        Field(
            name="spectral_vector",
            dtype=Array(Float32),
            vector_index=True,
            vector_search_metric="COSINE",
            vector_length=768,
        ),
        *track_tags(),
    ],
    source=FileSource(
        name="track_spectral_source",
        path=str(DATA / "track_spectral.parquet"),
        timestamp_field="event_timestamp",
    ),
    ttl=timedelta(days=3650),
)

# What each listener played recently, pushed live by the Navidrome bridge.
# Track lists are JSON strings: the Milvus store turns numeric arrays into vectors.
user_listening = FeatureView(
    name="user_listening",
    entities=[user],
    schema=[
        Field(name="last_track_id", dtype=Int64),
        Field(name="recent_track_ids", dtype=String),
        Field(name="skipped_track_ids", dtype=String),
    ],
    source=PushSource(
        name="user_listening_push",
        batch_source=FileSource(
            name="user_listening_source",
            path=str(DATA / "user_listening.parquet"),
            timestamp_field="event_timestamp",
        ),
    ),
    ttl=timedelta(days=30),
)

# Taste clusters, written by the clustering job.
user_taste = FeatureView(
    name="user_taste",
    entities=[user],
    schema=[
        Field(name="taste_summary", dtype=String),
        Field(name="top_genres", dtype=String),
        Field(name="cluster_count", dtype=Int64),
    ],
    source=PushSource(
        name="user_taste_push",
        batch_source=FileSource(
            name="user_taste_source",
            path=str(DATA / "user_taste.parquet"),
            timestamp_field="event_timestamp",
        ),
    ),
    ttl=timedelta(days=30),
)
